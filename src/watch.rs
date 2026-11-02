use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

use bytes::Bytes;
use dashmap::{DashMap, mapref::entry::Entry};
use tokio::time::Instant;

use crate::{types::RedisValueRef, value::ValueEntry};

#[derive(Default)]
pub struct Watches {
    keys: DashMap<Bytes, HashMap<u64, Arc<AtomicBool>>>,
    next_id: AtomicU64,
}

impl Watches {
    pub fn new() -> Self {
        Self::default()
    }
    pub(crate) fn touch(&self, parts: &[RedisValueRef]) {
        let Some(RedisValueRef::BulkString(key)) = parts.get(1) else {
            return;
        };
        if let Some(watchers) = self.keys.get(key) {
            for dirty in watchers.values() {
                dirty.store(true, Ordering::Release);
            }
        }
    }

    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }
}
pub(crate) struct WatchGuard {
    watches: Arc<Watches>,
    id: u64,
    dirty: Arc<AtomicBool>,
    keys: Vec<Bytes>,
    deadlines: Vec<Instant>,
}

impl WatchGuard {
    pub(crate) fn new(watches: &Arc<Watches>) -> Self {
        Self {
            id: watches.next_id.fetch_add(1, Ordering::Relaxed),
            watches: Arc::clone(watches),
            dirty: Arc::new(AtomicBool::new(false)),
            keys: Vec::new(),
            deadlines: Vec::new(),
        }
    }
    pub(crate) fn watch(&mut self, key: &Bytes, storage: &DashMap<Bytes, ValueEntry>) {
        if let Some(entry) = storage.get(key)
            && !entry.is_expired()
            && let Some(deadline) = entry.expires_at
        {
            self.deadlines.push(deadline);
        }
        if self.keys.contains(key) {
            return;
        }
        self.watches
            .keys
            .entry(key.clone())
            .or_default()
            .insert(self.id, Arc::clone(&self.dirty));
        self.keys.push(key.clone());
    }
    pub(crate) fn is_stale(&self) -> bool {
        let now = Instant::now();
        self.dirty.load(Ordering::Acquire) || self.deadlines.iter().any(|&d| now >= d)
    }
}

impl Drop for WatchGuard {
    fn drop(&mut self) {
        for key in &self.keys {
            if let Entry::Occupied(mut entry) = self.watches.keys.entry(key.clone()) {
                entry.get_mut().remove(&self.id);
                if entry.get().is_empty() {
                    entry.remove();
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::{command::handle_command, session::testing::*, types::RedisValueRef};

    fn command(parts: &[&str]) -> RedisValueRef {
        RedisValueRef::Array(parts.iter().map(|part| bulk_owned(part)).collect())
    }

    fn bulk_owned(value: &str) -> RedisValueRef {
        RedisValueRef::BulkString(bytes::Bytes::copy_from_slice(value.as_bytes()))
    }
    async fn set_in_transaction(
        server: &Server,
        session: &mut crate::session::Session,
        key: &str,
        value: &str,
    ) -> RedisValueRef {
        assert!(server.send(session, &["MULTI"]).await == simple("OK"));
        assert!(server.send(session, &["SET", key, value]).await == simple("QUEUED"));
        server.send(session, &["EXEC"]).await
    }

    fn ran() -> RedisValueRef {
        RedisValueRef::Array(vec![simple("OK")])
    }

    #[tokio::test]
    async fn another_sessions_write_aborts_exec() {
        let server = Server::new();
        let mut a = server.session();
        let mut b = server.session();
        server.send(&mut a, &["SET", "counter", "10"]).await;
        assert!(server.send(&mut a, &["WATCH", "counter"]).await == simple("OK"));
        assert!(server.send(&mut a, &["GET", "counter"]).await == bulk("10"));
        server.send(&mut b, &["SET", "counter", "20"]).await;

        let reply = set_in_transaction(&server, &mut a, "counter", "11").await;
        assert!(reply == RedisValueRef::NullArray);
        assert!(server.send(&mut a, &["GET", "counter"]).await == bulk("20"));
        assert!(
            server.watches.is_empty(),
            "EXEC unwatches even when it aborts"
        );

        let reply = set_in_transaction(&server, &mut a, "counter", "11").await;
        assert!(reply == ran(), "the next transaction is no longer watched");
    }

    #[tokio::test]
    async fn own_write_before_multi_aborts_exec() {
        let server = Server::new();
        let mut a = server.session();
        server.send(&mut a, &["WATCH", "k"]).await;
        server.send(&mut a, &["SET", "k", "same"]).await;
        let reply = set_in_transaction(&server, &mut a, "k", "v").await;
        assert!(reply == RedisValueRef::NullArray);
    }

    #[tokio::test]
    async fn untouched_watch_lets_exec_run() {
        let server = Server::new();
        let mut a = server.session();
        let mut b = server.session();
        server.send(&mut a, &["WATCH", "k", "k"]).await;
        server.send(&mut b, &["SET", "other", "1"]).await;
        assert!(set_in_transaction(&server, &mut a, "k", "v").await == ran());
        assert!(server.send(&mut a, &["GET", "k"]).await == bulk("v"));
    }

    #[tokio::test]
    async fn failed_write_does_not_abort() {
        let server = Server::new();
        let mut a = server.session();
        let mut b = server.session();
        server.send(&mut a, &["SET", "k", "abc"]).await;
        server.send(&mut a, &["WATCH", "k"]).await;
        assert!(matches!(
            server.send(&mut b, &["INCR", "k"]).await,
            RedisValueRef::ErrorMsg(_)
        ));
        assert!(set_in_transaction(&server, &mut a, "k", "v").await == ran());
    }

    #[tokio::test]
    async fn creating_a_missing_watched_key_aborts() {
        let server = Server::new();
        let mut a = server.session();
        let mut b = server.session();
        server.send(&mut a, &["WATCH", "k"]).await;
        server.send(&mut b, &["XADD", "k", "1-1", "f", "v"]).await;
        assert!(set_in_transaction(&server, &mut a, "x", "v").await == RedisValueRef::NullArray);
    }

    #[tokio::test]
    async fn write_inside_another_exec_aborts() {
        let server = Server::new();
        let mut a = server.session();
        let mut b = server.session();
        server.send(&mut a, &["WATCH", "k"]).await;
        assert!(set_in_transaction(&server, &mut b, "k", "b").await == ran());
        assert!(set_in_transaction(&server, &mut a, "k", "a").await == RedisValueRef::NullArray);
        assert!(server.send(&mut a, &["GET", "k"]).await == bulk("b"));
    }

    #[tokio::test]
    async fn expiry_between_watch_and_exec_aborts() {
        let server = Server::new();
        let mut a = server.session();
        server.send(&mut a, &["SET", "k", "v", "PX", "20"]).await;
        server.send(&mut a, &["WATCH", "k"]).await;
        tokio::time::sleep(Duration::from_millis(40)).await;
        assert!(set_in_transaction(&server, &mut a, "x", "v").await == RedisValueRef::NullArray);
    }

    #[tokio::test]
    async fn key_already_expired_at_watch_does_not_abort() {
        let server = Server::new();
        let mut a = server.session();
        server.send(&mut a, &["SET", "k", "v", "PX", "10"]).await;
        tokio::time::sleep(Duration::from_millis(30)).await;
        server.send(&mut a, &["WATCH", "k"]).await;
        assert!(server.send(&mut a, &["GET", "k"]).await == RedisValueRef::NullBulkString);
        assert!(set_in_transaction(&server, &mut a, "x", "v").await == ran());
    }

    #[tokio::test]
    async fn unwatch_discard_and_reset_clear_watches() {
        let server = Server::new();
        let mut a = server.session();
        let mut b = server.session();

        server.send(&mut a, &["WATCH", "k"]).await;
        assert!(server.send(&mut a, &["UNWATCH"]).await == simple("OK"));
        assert!(server.watches.is_empty());

        server.send(&mut a, &["WATCH", "k"]).await;
        server.send(&mut a, &["MULTI"]).await;
        assert!(server.send(&mut a, &["DISCARD"]).await == simple("OK"));
        assert!(server.watches.is_empty());

        server.send(&mut a, &["WATCH", "k"]).await;
        assert!(server.send(&mut a, &["RESET"]).await == simple("RESET"));
        assert!(server.watches.is_empty());

        server.send(&mut b, &["SET", "k", "b"]).await;
        assert!(set_in_transaction(&server, &mut a, "k", "a").await == ran());
    }

    #[tokio::test]
    async fn exec_or_discard_without_multi_keeps_watches() {
        let server = Server::new();
        let mut a = server.session();
        let mut b = server.session();
        server.send(&mut a, &["WATCH", "k"]).await;
        assert!(server.send(&mut a, &["EXEC"]).await == err("ERR EXEC without MULTI"));
        assert!(server.send(&mut a, &["DISCARD"]).await == err("ERR DISCARD without MULTI"));
        server.send(&mut b, &["SET", "k", "b"]).await;
        assert!(set_in_transaction(&server, &mut a, "k", "a").await == RedisValueRef::NullArray);
    }

    #[tokio::test]
    async fn watch_inside_multi_is_refused_and_unwatch_is_queued() {
        let server = Server::new();
        let mut a = server.session();
        server.send(&mut a, &["MULTI"]).await;
        assert!(
            server.send(&mut a, &["WATCH", "k"]).await
                == err("ERR WATCH inside MULTI is not allowed")
        );
        assert!(a.in_transaction());
        assert!(server.watches.is_empty());
        assert!(server.send(&mut a, &["UNWATCH"]).await == simple("QUEUED"));
        assert!(server.send(&mut a, &["EXEC"]).await == ran());
    }

    #[tokio::test]
    async fn arity_errors() {
        let server = Server::new();
        let mut a = server.session();
        assert!(
            server.send(&mut a, &["WATCH"]).await
                == err("ERR wrong number of arguments for 'watch' command")
        );
        assert!(
            server.send(&mut a, &["UNWATCH", "k"]).await
                == err("ERR wrong number of arguments for 'unwatch' command")
        );
    }

    #[tokio::test]
    async fn dropping_a_watching_session_empties_the_registry() {
        let server = Server::new();
        let mut a = server.session();
        server.send(&mut a, &["WATCH", "a", "b"]).await;
        assert!(!server.watches.is_empty());
        drop(a);
        assert!(server.watches.is_empty());
    }

    #[tokio::test]
    async fn write_holding_the_shared_lock_aborts_a_waiting_exec() {
        let server = Server::new();
        let mut a = server.session();
        server.send(&mut a, &["WATCH", "k"]).await;
        server.send(&mut a, &["MULTI"]).await;
        server.send(&mut a, &["SET", "k", "a"]).await;
        let shared = server.lock.clone().read_owned().await;
        {
            let exec = server.send(&mut a, &["EXEC"]);
            tokio::pin!(exec);
            assert!(
                tokio::time::timeout(Duration::from_millis(20), &mut exec)
                    .await
                    .is_err(),
                "EXEC must wait for the write"
            );
            handle_command(
                &command(&["SET", "k", "b"]),
                &server.storage,
                &server.config,
                &server.replication,
                &server.aof,
                &server.watches,
            )
            .await;
            drop(shared);
            assert!(exec.await == RedisValueRef::NullArray);
        }
        assert!(server.send(&mut a, &["GET", "k"]).await == bulk("b"));
    }
}
