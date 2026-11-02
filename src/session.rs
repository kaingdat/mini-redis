use std::{sync::Arc, time::Duration};

use bytes::Bytes;
use dashmap::DashMap;
use tokio::time::Instant;

use crate::{
    aof::Aof,
    command::{
        ConnectionCommand, connection_command, handle_command, is_psync_command, is_wait_command,
    },
    pubsub::{PubSub, Subscriber},
    server::{ReplicationState, ServerConfig},
    stream::{self, BlockedRead, StreamWaiters, XRead},
    transaction::{ExecLock, Transaction},
    types::RedisValueRef,
    value::ValueEntry,
    watch::Watches,
};

pub struct Session {
    transaction: Transaction,
    pubsub: Arc<PubSub>,
    subscriber: Subscriber,
    waiters: Arc<StreamWaiters>,
    watches: Arc<Watches>,
    blocked: Option<BlockedRead>,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum After {
    Continue,
    Close,
    StreamToReplica,
    /// A blocking `XREAD` found nothing. The connection must call
    /// [`Session::wait_blocked`] and send its reply before reading on.
    Block,
}

pub struct Response {
    pub replies: Vec<RedisValueRef>,
    pub after: After,
}

impl Response {
    fn one(reply: RedisValueRef) -> Self {
        Self {
            replies: vec![reply],
            after: After::Continue,
        }
    }
}

impl Session {
    pub fn new(pubsub: Arc<PubSub>, waiters: Arc<StreamWaiters>, watches: Arc<Watches>) -> Self {
        Self {
            transaction: Transaction::new(Arc::clone(&watches)),
            subscriber: Subscriber::new(Arc::clone(&pubsub)),
            pubsub,
            waiters,
            watches,
            blocked: None,
        }
    }

    fn reset(&mut self) {
        self.transaction.discard();
        self.subscriber.reset();
        self.blocked = None;
    }

    pub async fn next_message(&mut self) -> Option<Bytes> {
        self.subscriber.recv().await
    }

    pub fn in_transaction(&self) -> bool {
        self.transaction.is_active()
    }

    pub async fn handle(
        &mut self,
        value: &RedisValueRef,
        storage: &Arc<DashMap<Bytes, ValueEntry>>,
        config: &Arc<ServerConfig>,
        replication: &Arc<ReplicationState>,
        aof: &Arc<Aof>,
        lock: &ExecLock,
    ) -> Response {
        self.blocked = None;
        match connection_command(value) {
            Some(ConnectionCommand::Quit) => {
                return Response {
                    replies: vec![RedisValueRef::SimpleString(Bytes::from_static(b"OK"))],
                    after: After::Close,
                };
            }
            Some(ConnectionCommand::Reset) => {
                self.reset();
                return Response::one(RedisValueRef::SimpleString(Bytes::from_static(b"RESET")));
            }
            Some(ConnectionCommand::Invalid(e)) => return Response::one(e),
            None => {}
        }

        if !self.in_transaction() {
            if let Some(replies) = self.subscriber.handle(value) {
                return Response {
                    replies,
                    after: After::Continue,
                };
            }
            if is_psync_command(value) {
                let reply = self
                    .dispatch(value, storage, config, replication, aof, lock)
                    .await;
                return Response {
                    replies: vec![reply],
                    after: After::StreamToReplica,
                };
            }
            if let Some(read) = stream::parse_blocking_xread(value) {
                return match read {
                    Ok(read) => self.start_blocking_read(read, storage, lock).await,
                    Err(e) => Response::one(e),
                };
            }
        }

        Response::one(
            self.dispatch(value, storage, config, replication, aof, lock)
                .await,
        )
    }

    async fn dispatch(
        &mut self,
        value: &RedisValueRef,
        storage: &Arc<DashMap<Bytes, ValueEntry>>,
        config: &Arc<ServerConfig>,
        replication: &Arc<ReplicationState>,
        aof: &Arc<Aof>,
        lock: &ExecLock,
    ) -> RedisValueRef {
        if let Some(reply) = self
            .transaction
            .handle(
                value,
                storage,
                config,
                replication,
                aof,
                lock,
                &self.pubsub,
                &self.waiters,
            )
            .await
        {
            return reply;
        }
        if let Some(reply) = self.pubsub.execute_publish(value) {
            if !matches!(reply, RedisValueRef::ErrorMsg(_))
                && let RedisValueRef::Array(parts) = value
            {
                replication.propagate(parts);
            }
            return reply;
        }
        if is_wait_command(value) {
            return handle_command(value, storage, config, replication, aof, &self.watches).await;
        }
        let reply = {
            let _shared = lock.read().await;
            handle_command(value, storage, config, replication, aof, &self.watches).await
        };
        if let Some(key) = stream::added_key(value, &reply) {
            self.waiters.notify(key);
        }
        reply
    }

    async fn start_blocking_read(
        &mut self,
        read: XRead,
        storage: &Arc<DashMap<Bytes, ValueEntry>>,
        lock: &ExecLock,
    ) -> Response {
        // Registered before the first check, so an XADD that lands after the
        // check but before the wait still leaves a wake-up behind.
        let guard = self.waiters.register(&read.keys);
        let ids = {
            let _shared = lock.read().await;
            let ids = match stream::resolve_ids(storage, &read.keys, &read.ids) {
                Ok(ids) => ids,
                Err(e) => return Response::one(e),
            };
            match stream::read_after(storage, &read.keys, &ids) {
                Ok(None) => ids,
                Ok(Some(reply)) | Err(reply) => return Response::one(reply),
            }
        };
        let timeout_ms = read.block_ms.unwrap_or(0);
        self.blocked = Some(BlockedRead {
            keys: read.keys,
            ids,
            deadline: (timeout_ms > 0).then(|| Instant::now() + Duration::from_millis(timeout_ms)),
            guard,
        });
        Response {
            replies: Vec::new(),
            after: After::Block,
        }
    }

    /// Waits for the read that returned [`After::Block`] and gives its reply.
    /// Holds no lock while waiting, and dropping the future cancels the read.
    pub async fn wait_blocked(
        &mut self,
        storage: &Arc<DashMap<Bytes, ValueEntry>>,
        lock: &ExecLock,
    ) -> RedisValueRef {
        let Some(blocked) = self.blocked.take() else {
            return RedisValueRef::NullArray;
        };
        loop {
            match blocked.deadline {
                None => blocked.guard.notified().await,
                Some(deadline) => {
                    if tokio::time::timeout_at(deadline, blocked.guard.notified())
                        .await
                        .is_err()
                    {
                        return RedisValueRef::NullArray;
                    }
                }
            }
            let _shared = lock.read().await;
            match stream::read_after(storage, &blocked.keys, &blocked.ids) {
                Ok(None) => {}
                Ok(Some(reply)) | Err(reply) => return reply,
            }
        }
    }
}

#[cfg(test)]
pub(crate) mod testing {
    use std::sync::Arc;

    use bytes::Bytes;
    use dashmap::DashMap;

    use super::{Response, Session};
    use crate::{
        aof::Aof,
        pubsub::PubSub,
        server::{ReplicationState, Role, ServerConfig},
        stream::StreamWaiters,
        transaction::ExecLock,
        types::RedisValueRef,
        value::ValueEntry,
        watch::Watches,
    };

    pub(crate) struct Server {
        pub(crate) storage: Arc<DashMap<Bytes, ValueEntry>>,
        pub(crate) config: Arc<ServerConfig>,
        pub(crate) replication: Arc<ReplicationState>,
        pub(crate) aof: Arc<Aof>,
        pub(crate) lock: Arc<ExecLock>,
        pub(crate) pubsub: Arc<PubSub>,
        pub(crate) waiters: Arc<StreamWaiters>,
        pub(crate) watches: Arc<Watches>,
    }

    impl Server {
        pub(crate) fn new() -> Self {
            Self {
                storage: Arc::new(DashMap::new()),
                config: Arc::new(ServerConfig {
                    port: 0,
                    role: Role::Master,
                    replid: String::new(),
                    dir: String::new(),
                    dbfilename: String::new(),
                    appendonly: "no".into(),
                    appenddirname: String::new(),
                    appendfilename: String::new(),
                    appendfsync: "everysec".into(),
                }),
                replication: Arc::new(ReplicationState::new()),
                aof: Arc::new(Aof::disabled()),
                lock: Arc::new(ExecLock::default()),
                pubsub: Arc::new(PubSub::new()),
                waiters: Arc::new(StreamWaiters::new()),
                watches: Arc::new(Watches::new()),
            }
        }

        pub(crate) fn session(&self) -> Session {
            Session::new(
                Arc::clone(&self.pubsub),
                Arc::clone(&self.waiters),
                Arc::clone(&self.watches),
            )
        }

        pub(crate) async fn send(&self, session: &mut Session, parts: &[&str]) -> RedisValueRef {
            let value = RedisValueRef::Array(
                parts
                    .iter()
                    .map(|part| RedisValueRef::BulkString(Bytes::copy_from_slice(part.as_bytes())))
                    .collect(),
            );
            let mut response = self.send_raw(session, &value).await;
            assert_eq!(response.replies.len(), 1);
            response.replies.pop().unwrap()
        }

        pub(crate) async fn send_raw(
            &self,
            session: &mut Session,
            value: &RedisValueRef,
        ) -> Response {
            session
                .handle(
                    value,
                    &self.storage,
                    &self.config,
                    &self.replication,
                    &self.aof,
                    &self.lock,
                )
                .await
        }
    }

    pub(crate) fn simple(value: &'static str) -> RedisValueRef {
        RedisValueRef::SimpleString(Bytes::from_static(value.as_bytes()))
    }

    pub(crate) fn bulk(value: &'static str) -> RedisValueRef {
        RedisValueRef::BulkString(Bytes::from_static(value.as_bytes()))
    }

    pub(crate) fn err(value: &str) -> RedisValueRef {
        RedisValueRef::ErrorMsg(value.as_bytes().to_vec())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use bytes::Bytes;

    use super::{After, Session, testing::*};
    use crate::types::RedisValueRef;

    fn command(parts: &[&str]) -> RedisValueRef {
        RedisValueRef::Array(
            parts
                .iter()
                .map(|part| RedisValueRef::BulkString(Bytes::copy_from_slice(part.as_bytes())))
                .collect(),
        )
    }

    fn entry(id: &'static str, field: &'static str, value: &'static str) -> RedisValueRef {
        RedisValueRef::Array(vec![
            bulk(id),
            RedisValueRef::Array(vec![bulk(field), bulk(value)]),
        ])
    }

    fn stream(key: &'static str, entries: Vec<RedisValueRef>) -> RedisValueRef {
        RedisValueRef::Array(vec![bulk(key), RedisValueRef::Array(entries)])
    }

    async fn block(server: &Server, session: &mut Session, parts: &[&str]) {
        let response = server.send_raw(session, &command(parts)).await;
        assert!(response.replies.is_empty());
        assert_eq!(response.after, After::Block);
    }

    #[tokio::test]
    async fn xread_returns_streams_in_order_and_only_newer_entries() {
        let server = Server::new();
        let mut session = server.session();
        server
            .send(&mut session, &["XADD", "a", "1-1", "f", "a1"])
            .await;
        server
            .send(&mut session, &["XADD", "a", "1-2", "f", "a2"])
            .await;
        server
            .send(&mut session, &["XADD", "b", "2-1", "f", "b1"])
            .await;

        let reply = server
            .send(
                &mut session,
                &["XREAD", "STREAMS", "b", "a", "missing", "0", "1-1", "0"],
            )
            .await;
        assert!(
            reply
                == RedisValueRef::Array(vec![
                    stream("b", vec![entry("2-1", "f", "b1")]),
                    stream("a", vec![entry("1-2", "f", "a2")]),
                ])
        );
        assert!(
            server
                .send(&mut session, &["XREAD", "STREAMS", "a", "1-2"])
                .await
                == RedisValueRef::NullArray
        );
        assert!(
            server
                .send(&mut session, &["XREAD", "STREAMS", "a", "$"])
                .await
                == RedisValueRef::NullArray
        );
        assert!(
            server
                .send(
                    &mut session,
                    &["XREAD", "BLOCK", "1000", "STREAMS", "a", "0"]
                )
                .await
                == RedisValueRef::Array(vec![stream(
                    "a",
                    vec![entry("1-1", "f", "a1"), entry("1-2", "f", "a2")]
                )]),
            "a blocking read with data replies at once"
        );
    }

    #[tokio::test]
    async fn xread_block_wakes_on_another_sessions_xadd() {
        let server = Server::new();
        let mut reader = server.session();
        let mut writer = server.session();
        server
            .send(&mut writer, &["XADD", "s", "1-1", "old", "x"])
            .await;
        block(
            &server,
            &mut reader,
            &["XREAD", "BLOCK", "0", "STREAMS", "other", "s", "$", "$"],
        )
        .await;

        let write = async {
            tokio::time::sleep(Duration::from_millis(10)).await;
            // A wake-up with nothing new must go back to waiting.
            server.waiters.notify(&Bytes::from_static(b"s"));
            tokio::time::sleep(Duration::from_millis(10)).await;
            server
                .send(&mut writer, &["XADD", "s", "2-0", "temp", "37"])
                .await
        };
        let (reply, _) = tokio::join!(reader.wait_blocked(&server.storage, &server.lock), write);
        assert!(reply == RedisValueRef::Array(vec![stream("s", vec![entry("2-0", "temp", "37")])]));
        assert!(server.waiters.is_empty());
    }

    #[tokio::test]
    async fn xread_block_times_out_with_a_null_array() {
        let server = Server::new();
        let mut reader = server.session();
        block(
            &server,
            &mut reader,
            &["XREAD", "BLOCK", "30", "STREAMS", "s", "$"],
        )
        .await;
        let started = std::time::Instant::now();
        let reply = reader.wait_blocked(&server.storage, &server.lock).await;
        assert!(reply == RedisValueRef::NullArray);
        assert!(started.elapsed() >= Duration::from_millis(25));
        assert!(server.waiters.is_empty());
    }

    #[tokio::test]
    async fn xread_block_wakes_on_xadd_inside_exec() {
        let server = Server::new();
        let mut reader = server.session();
        let mut writer = server.session();
        block(
            &server,
            &mut reader,
            &["XREAD", "BLOCK", "0", "STREAMS", "s", "0"],
        )
        .await;

        let write = async {
            server.send(&mut writer, &["MULTI"]).await;
            server
                .send(&mut writer, &["XADD", "s", "1-1", "f", "v"])
                .await;
            server.send(&mut writer, &["EXEC"]).await
        };
        let (reply, exec) = tokio::join!(reader.wait_blocked(&server.storage, &server.lock), write);
        assert!(exec == RedisValueRef::Array(vec![bulk("1-1")]));
        assert!(reply == RedisValueRef::Array(vec![stream("s", vec![entry("1-1", "f", "v")])]));
    }

    #[tokio::test]
    async fn xread_block_inside_multi_does_not_block() {
        let server = Server::new();
        let mut session = server.session();
        server.send(&mut session, &["MULTI"]).await;
        assert!(
            server
                .send(&mut session, &["XREAD", "BLOCK", "0", "STREAMS", "s", "$"])
                .await
                == simple("QUEUED")
        );
        assert!(
            server.send(&mut session, &["EXEC"]).await
                == RedisValueRef::Array(vec![RedisValueRef::NullArray])
        );
    }

    #[tokio::test]
    async fn xread_blocked_reader_holds_no_lock() {
        let server = Server::new();
        let mut reader = server.session();
        let mut writer = server.session();
        block(
            &server,
            &mut reader,
            &["XREAD", "BLOCK", "0", "STREAMS", "s", "$"],
        )
        .await;

        let wait = reader.wait_blocked(&server.storage, &server.lock);
        tokio::pin!(wait);
        assert!(
            tokio::time::timeout(Duration::from_millis(10), &mut wait)
                .await
                .is_err()
        );
        server.send(&mut writer, &["MULTI"]).await;
        server.send(&mut writer, &["SET", "k", "v"]).await;
        let exec = tokio::time::timeout(
            Duration::from_millis(100),
            server.send(&mut writer, &["EXEC"]),
        )
        .await;
        assert!(exec.ok() == Some(RedisValueRef::Array(vec![simple("OK")])));
    }

    #[tokio::test]
    async fn xread_dropping_a_blocked_read_unregisters_it() {
        let server = Server::new();
        let mut reader = server.session();
        block(
            &server,
            &mut reader,
            &["XREAD", "BLOCK", "0", "STREAMS", "a", "b", "$", "$"],
        )
        .await;
        assert!(!server.waiters.is_empty());
        {
            let wait = reader.wait_blocked(&server.storage, &server.lock);
            tokio::pin!(wait);
            assert!(
                tokio::time::timeout(Duration::from_millis(10), &mut wait)
                    .await
                    .is_err()
            );
        }
        assert!(server.waiters.is_empty());

        block(
            &server,
            &mut reader,
            &["XREAD", "BLOCK", "0", "STREAMS", "a", "$"],
        )
        .await;
        drop(reader);
        assert!(server.waiters.is_empty());
    }

    #[tokio::test]
    async fn publish_is_replicated_but_not_failed_ones() {
        let server = Server::new();
        let (_, mut replica) = server.replication.register();
        let mut session = server.session();

        assert!(
            server.send(&mut session, &["PUBLISH", "chan"]).await
                == err("ERR wrong number of arguments for 'publish' command")
        );
        assert_eq!(server.replication.master_offset(), 0);

        assert!(
            server.send(&mut session, &["PUBLISH", "chan", "hi"]).await == RedisValueRef::Int(0)
        );
        assert_eq!(
            replica.try_recv().unwrap(),
            Bytes::from_static(b"*3\r\n$7\r\nPUBLISH\r\n$4\r\nchan\r\n$2\r\nhi\r\n")
        );

        server.send(&mut session, &["MULTI"]).await;
        server.send(&mut session, &["SET", "k", "v"]).await;
        server.send(&mut session, &["PUBLISH", "chan", "hi"]).await;
        server.send(&mut session, &["EXEC"]).await;
        assert_eq!(
            replica.try_recv().unwrap(),
            Bytes::from_static(
                b"*1\r\n$5\r\nMULTI\r\n*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n*3\r\n$7\r\nPUBLISH\r\n$4\r\nchan\r\n$2\r\nhi\r\n*1\r\n$4\r\nEXEC\r\n"
            )
        );
    }

    #[tokio::test]
    async fn session_routes_pubsub_and_connection_commands() {
        let server = Server::new();
        let mut session = server.session();
        let subscribe = server.send(&mut session, &["SUBSCRIBE", "chan"]).await;
        assert!(
            subscribe
                == RedisValueRef::Array(vec![
                    bulk("subscribe"),
                    bulk("chan"),
                    RedisValueRef::Int(1)
                ])
        );
        assert!(matches!(
            server.send(&mut session, &["GET", "k"]).await,
            RedisValueRef::ErrorMsg(_)
        ));
        assert!(matches!(
            server.send(&mut session, &["MULTI"]).await,
            RedisValueRef::ErrorMsg(_)
        ));

        assert!(server.send(&mut session, &["RESET"]).await == simple("RESET"));
        assert!(server.send(&mut session, &["GET", "k"]).await == RedisValueRef::NullBulkString);

        server.send(&mut session, &["MULTI"]).await;
        assert!(server.send(&mut session, &["RESET"]).await == simple("RESET"));
        assert!(!session.in_transaction());

        let quit = RedisValueRef::Array(vec![bulk("QUIT")]);
        let response = server.send_raw(&mut session, &quit).await;
        assert!(response.replies == vec![simple("OK")]);
        assert_eq!(response.after, After::Close);

        let psync = RedisValueRef::Array(vec![bulk("PSYNC"), bulk("?"), bulk("-1")]);
        assert_eq!(
            server.send_raw(&mut session, &psync).await.after,
            After::StreamToReplica
        );
    }
}
