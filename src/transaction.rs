use std::sync::Arc;

use bytes::Bytes;
use dashmap::DashMap;
use tokio::sync::RwLock;

use crate::{
    aof::Aof,
    command::{execute_queued, handle_command, is_wait_command},
    server::{ReplicationState, ServerConfig},
    types::RedisValueRef,
    value::ValueEntry,
};

pub type ExecLock = RwLock<()>;

#[derive(Default)]
pub struct Session {
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

impl Session {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn in_transaction(&self) -> bool {
        self.queued.is_some()
    }

    pub async fn handle(
        &mut self,
        value: &RedisValueRef,
        storage: &Arc<DashMap<Bytes, ValueEntry>>,
        config: &Arc<ServerConfig>,
        replication: &Arc<ReplicationState>,
        aof: &Arc<Aof>,
        lock: &ExecLock,
    ) -> RedisValueRef {
        match (control(value), &mut self.queued) {
            (Some(Control::Multi), Some(_)) => error(b"ERR MULTI calls can not be nested"),
            (Some(Control::Multi), None) => {
                self.queued = Some(Vec::new());
                ok()
            }
            (Some(Control::Exec), None) => error(b"ERR EXEC without MULTI"),
            (Some(Control::Exec), Some(_)) => {
                let queued = self.queued.take().unwrap();
                exec(&queued, storage, config, replication, aof, lock).await
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
            (None, None) => {
                if is_wait_command(value) {
                    return handle_command(value, storage, config, replication, aof).await;
                }
                let _shared = lock.read().await;
                handle_command(value, storage, config, replication, aof).await
            }
        }
    }
}

async fn exec(
    queued: &[RedisValueRef],
    storage: &Arc<DashMap<Bytes, ValueEntry>>,
    config: &Arc<ServerConfig>,
    replication: &Arc<ReplicationState>,
    aof: &Arc<Aof>,
    lock: &ExecLock,
) -> RedisValueRef {
    let _exclusive = lock.write().await;
    let result = aof
        .apply_transaction(|| {
            let mut writes = Vec::new();
            let replies = queued
                .iter()
                .map(|value| {
                    let (reply, wrote) = execute_queued(value, storage, config, replication);
                    if wrote && let RedisValueRef::Array(parts) = value {
                        writes.push(parts.as_slice());
                    }
                    reply
                })
                .collect();
            (replies, writes)
        })
        .await;
    match result {
        Ok((reply, writes)) => {
            replication.propagate_transaction(&writes);
            reply
        }
        Err(rejected) => rejected,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::Role;

    struct Server {
        storage: Arc<DashMap<Bytes, ValueEntry>>,
        config: Arc<ServerConfig>,
        replication: Arc<ReplicationState>,
        aof: Arc<Aof>,
        lock: Arc<ExecLock>,
    }

    impl Server {
        fn new() -> Self {
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
            }
        }

        async fn send(&self, session: &mut Session, parts: &[&str]) -> RedisValueRef {
            let value = RedisValueRef::Array(
                parts
                    .iter()
                    .map(|part| RedisValueRef::BulkString(Bytes::copy_from_slice(part.as_bytes())))
                    .collect(),
            );
            session
                .handle(
                    &value,
                    &self.storage,
                    &self.config,
                    &self.replication,
                    &self.aof,
                    &self.lock,
                )
                .await
        }
    }

    fn simple(value: &'static str) -> RedisValueRef {
        RedisValueRef::SimpleString(Bytes::from_static(value.as_bytes()))
    }

    fn bulk(value: &'static str) -> RedisValueRef {
        RedisValueRef::BulkString(Bytes::from_static(value.as_bytes()))
    }

    fn err(value: &str) -> RedisValueRef {
        RedisValueRef::ErrorMsg(value.as_bytes().to_vec())
    }

    #[tokio::test]
    async fn exec_runs_the_queue_in_order_past_failures() {
        let server = Server::new();
        let mut session = Session::new();
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
        let mut session = Session::new();
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
}
