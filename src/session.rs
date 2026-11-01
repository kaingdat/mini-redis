use std::sync::Arc;

use bytes::Bytes;
use dashmap::DashMap;

use crate::{
    aof::Aof,
    command::{
        ConnectionCommand, connection_command, handle_command, is_psync_command, is_wait_command,
    },
    pubsub::{PubSub, Subscriber},
    server::{ReplicationState, ServerConfig},
    transaction::{ExecLock, Transaction},
    types::RedisValueRef,
    value::ValueEntry,
};

pub struct Session {
    transaction: Transaction,
    pubsub: Arc<PubSub>,
    subscriber: Subscriber,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum After {
    Continue,
    Close,
    StreamToReplica,
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
    pub fn new(pubsub: Arc<PubSub>) -> Self {
        Self {
            transaction: Transaction::default(),
            subscriber: Subscriber::new(Arc::clone(&pubsub)),
            pubsub,
        }
    }

    fn reset(&mut self) {
        self.transaction.discard();
        self.subscriber.reset();
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
            .handle(value, storage, config, replication, aof, lock, &self.pubsub)
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
            return handle_command(value, storage, config, replication, aof).await;
        }
        let _shared = lock.read().await;
        handle_command(value, storage, config, replication, aof).await
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
        transaction::ExecLock,
        types::RedisValueRef,
        value::ValueEntry,
    };

    pub(crate) struct Server {
        pub(crate) storage: Arc<DashMap<Bytes, ValueEntry>>,
        pub(crate) config: Arc<ServerConfig>,
        pub(crate) replication: Arc<ReplicationState>,
        pub(crate) aof: Arc<Aof>,
        pub(crate) lock: Arc<ExecLock>,
        pub(crate) pubsub: Arc<PubSub>,
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
            }
        }

        pub(crate) fn session(&self) -> Session {
            Session::new(Arc::clone(&self.pubsub))
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
    use bytes::Bytes;

    use super::{After, testing::*};
    use crate::types::RedisValueRef;

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
