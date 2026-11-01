use std::sync::Arc;

use bytes::Bytes;
use dashmap::DashMap;
use tokio::sync::RwLock;

use crate::{
    aof::Aof,
    command::execute_queued,
    pubsub::PubSub,
    server::{ReplicationState, ServerConfig},
    types::RedisValueRef,
    value::ValueEntry,
};

pub type ExecLock = RwLock<()>;

#[derive(Default)]
pub(crate) struct Transaction {
    queued: Option<Vec<RedisValueRef>>,
}

#[derive(Clone, Copy)]
enum Control {
    Multi,
    Exec,
    Discard,
}

fn control(value: &RedisValueRef) -> Option<Control> {
    let RedisValueRef::Array(parts) = value else {
        return None;
    };
    let Some(RedisValueRef::BulkString(name)) = parts.first() else {
        return None;
    };
    match name.to_ascii_uppercase().as_slice() {
        b"MULTI" => Some(Control::Multi),
        b"EXEC" => Some(Control::Exec),
        b"DISCARD" => Some(Control::Discard),
        _ => None,
    }
}

fn ok() -> RedisValueRef {
    RedisValueRef::SimpleString(Bytes::from_static(b"OK"))
}

fn error(message: &[u8]) -> RedisValueRef {
    RedisValueRef::ErrorMsg(message.to_vec())
}

impl Transaction {
    pub(crate) fn is_active(&self) -> bool {
        self.queued.is_some()
    }

    pub(crate) fn discard(&mut self) {
        self.queued = None;
    }

    pub(crate) async fn handle(
        &mut self,
        value: &RedisValueRef,
        storage: &Arc<DashMap<Bytes, ValueEntry>>,
        config: &Arc<ServerConfig>,
        replication: &Arc<ReplicationState>,
        aof: &Arc<Aof>,
        lock: &ExecLock,
        pubsub: &PubSub,
    ) -> Option<RedisValueRef> {
        Some(match (control(value), &mut self.queued) {
            (Some(Control::Multi), Some(_)) => error(b"ERR MULTI calls can not be nested"),
            (Some(Control::Multi), None) => {
                self.queued = Some(Vec::new());
                ok()
            }
            (Some(Control::Exec), None) => error(b"ERR EXEC without MULTI"),
            (Some(Control::Exec), Some(_)) => {
                let queued = self.queued.take().unwrap();
                exec(&queued, storage, config, replication, aof, lock, pubsub).await
            }
            (Some(Control::Discard), None) => error(b"ERR DISCARD without MULTI"),
            (Some(Control::Discard), Some(_)) => {
                self.queued = None;
                ok()
            }
            // TODO: validate the command name and arity here, and on failure
            // reply with the error and make EXEC answer "EXECABORT Transaction
            // discarded because of previous errors." as Redis does. For now
            // every command is queued and its error shows up in EXEC's reply.
            (None, Some(queued)) => {
                queued.push(value.clone());
                RedisValueRef::SimpleString(Bytes::from_static(b"QUEUED"))
            }
            (None, None) => return None,
        })
    }
}

async fn exec(
    queued: &[RedisValueRef],
    storage: &Arc<DashMap<Bytes, ValueEntry>>,
    config: &Arc<ServerConfig>,
    replication: &Arc<ReplicationState>,
    aof: &Arc<Aof>,
    lock: &ExecLock,
    pubsub: &PubSub,
) -> RedisValueRef {
    let _exclusive = lock.write().await;
    let mut propagated = Vec::new();
    let result = aof
        .apply_transaction(|| {
            let mut writes = Vec::new();
            let replies = queued
                .iter()
                .map(|value| {
                    let (reply, logged, replicated) = match pubsub.execute_publish(value) {
                        Some(reply) => {
                            let ok = !matches!(reply, RedisValueRef::ErrorMsg(_));
                            (reply, false, ok)
                        }
                        None => {
                            let (reply, wrote) =
                                execute_queued(value, storage, config, replication);
                            (reply, wrote, wrote)
                        }
                    };
                    if let RedisValueRef::Array(parts) = value {
                        if logged {
                            writes.push(parts.as_slice());
                        }
                        if replicated {
                            propagated.push(parts.as_slice());
                        }
                    }
                    reply
                })
                .collect();
            (replies, writes)
        })
        .await;
    match result {
        Ok((reply, _)) => {
            replication.propagate_transaction(&propagated);
            reply
        }
        Err(rejected) => rejected,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;

    use crate::{session::testing::*, types::RedisValueRef};

    #[tokio::test]
    async fn exec_runs_the_queue_in_order_past_failures() {
        let server = Server::new();
        let mut session = server.session();
        assert!(server.send(&mut session, &["MULTI"]).await == simple("OK"));
        assert!(server.send(&mut session, &["SET", "foo", "41"]).await == simple("QUEUED"));
        server.send(&mut session, &["INCR", "foo"]).await;
        server.send(&mut session, &["SET", "bad", "xyz"]).await;
        server.send(&mut session, &["INCR", "bad"]).await;
        server.send(&mut session, &["SET", "bar", "7"]).await;
        assert!(
            server.storage.is_empty(),
            "queued commands must not run yet"
        );

        let reply = server.send(&mut session, &["EXEC"]).await;
        assert!(
            reply
                == RedisValueRef::Array(vec![
                    simple("OK"),
                    RedisValueRef::Int(42),
                    simple("OK"),
                    err("ERR value is not an integer or out of range"),
                    simple("OK"),
                ])
        );
        assert!(!session.in_transaction());
        assert!(server.send(&mut session, &["GET", "bar"]).await == bulk("7"));
    }

    #[tokio::test]
    async fn exec_waits_for_in_flight_commands() {
        let server = Server::new();
        let shared = server.lock.clone().read_owned().await;
        let mut session = server.session();
        server.send(&mut session, &["MULTI"]).await;
        server.send(&mut session, &["SET", "foo", "1"]).await;

        let exec = server.send(&mut session, &["EXEC"]);
        tokio::pin!(exec);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), &mut exec)
                .await
                .is_err(),
            "EXEC must not run while another command holds the lock"
        );
        drop(shared);
        assert!(exec.await == RedisValueRef::Array(vec![simple("OK")]));
    }

    #[tokio::test]
    async fn exec_runs_queued_publish() {
        let server = Server::new();
        let mut subscriber = crate::pubsub::Subscriber::new(Arc::clone(&server.pubsub));
        subscriber.handle(&RedisValueRef::Array(vec![bulk("SUBSCRIBE"), bulk("chan")]));

        let mut session = server.session();
        server.send(&mut session, &["MULTI"]).await;
        assert!(server.send(&mut session, &["PUBLISH", "chan", "hi"]).await == simple("QUEUED"));
        assert!(
            server.send(&mut session, &["EXEC"]).await
                == RedisValueRef::Array(vec![RedisValueRef::Int(1)])
        );
        assert_eq!(
            subscriber.recv().await,
            Some(Bytes::from_static(
                b"*3\r\n$7\r\nmessage\r\n$4\r\nchan\r\n$2\r\nhi\r\n"
            ))
        );
    }
}
