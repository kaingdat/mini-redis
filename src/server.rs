use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use dashmap::DashMap;
use rand::RngExt;
use tokio::sync::{Notify, mpsc};

use crate::{resp::encode_command, types::RedisValueRef};

pub struct ReplicaHandle {
    tx: mpsc::UnboundedSender<Bytes>,
    acked_offset: AtomicU64,
}

#[derive(Default)]
pub struct ReplicationState {
    replicas: DashMap<u64, ReplicaHandle>,
    master_offset: AtomicU64,
    next_conn_id: AtomicU64,
    ack_notify: Notify,
}

impl ReplicationState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&self) -> (u64, mpsc::UnboundedReceiver<Bytes>) {
        let conn_id = self.next_conn_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = mpsc::unbounded_channel();
        self.replicas.insert(
            conn_id,
            ReplicaHandle {
                tx,
                acked_offset: AtomicU64::new(0),
            },
        );
        (conn_id, rx)
    }

    pub fn unregister(&self, conn_id: u64) {
        self.replicas.remove(&conn_id);
        self.ack_notify.notify_waiters();
    }

    pub fn replica_count(&self) -> usize {
        self.replicas.len()
    }

    pub fn master_offset(&self) -> u64 {
        self.master_offset.load(Ordering::SeqCst)
    }

    pub fn propagate(&self, parts: &[RedisValueRef]) {
        let encoded = encode_command(parts);
        self.master_offset
            .fetch_add(encoded.len() as u64, Ordering::SeqCst);
        self.replicas
            .retain(|_, handle| handle.tx.send(encoded.clone()).is_ok());
    }

    pub fn request_acks(&self) {
        let getack = [
            RedisValueRef::BulkString(Bytes::from_static(b"REPLCONF")),
            RedisValueRef::BulkString(Bytes::from_static(b"GETACK")),
            RedisValueRef::BulkString(Bytes::from_static(b"*")),
        ];
        self.propagate(&getack);
    }

    pub fn record_ack(&self, conn_id: u64, offset: u64) {
        if let Some(handle) = self.replicas.get(&conn_id) {
            handle.acked_offset.fetch_max(offset, Ordering::SeqCst);
        }
        self.ack_notify.notify_waiters();
    }

    pub fn acked_count(&self, target: u64) -> usize {
        self.replicas
            .iter()
            .filter(|handle| handle.acked_offset.load(Ordering::SeqCst) >= target)
            .count()
    }

    pub fn ack_notified(&self) -> tokio::sync::futures::Notified<'_> {
        self.ack_notify.notified()
    }
}

pub enum Role {
    Master,
    Replica { host: String, port: u16 },
}

impl Role {
    pub fn name(&self) -> &'static str {
        match self {
            Role::Master => "master",
            Role::Replica { .. } => "slave",
        }
    }
}

pub struct ServerConfig {
    pub port: u16,
    pub role: Role,
    pub replid: String,
    pub dir: String,
    pub dbfilename: String,
    pub rdb: Bytes,
}

impl ServerConfig {
    pub fn from_args() -> Self {
        let mut port = 6379;
        let mut role = Role::Master;
        let mut dir = ".".to_string();
        let mut dbfilename = "empty.rdb".to_string();

        let mut args = std::env::args().skip(1);
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--port" => {
                    let value = args.next().expect("--port requires a value");
                    port = value
                        .parse::<u16>()
                        .expect("--port value must be a valid number");
                }
                "--replicaof" => {
                    let value = args.next().expect("--replicaof requires a value");
                    let mut it = value.split_whitespace();
                    let host = it.next().expect("--replicaof requires host").to_string();
                    let master_port = it
                        .next()
                        .expect("--replicaof requires port")
                        .parse::<u16>()
                        .expect("--replicaof port must be a valid number");
                    role = Role::Replica {
                        host,
                        port: master_port,
                    };
                }
                "--dir" => dir = args.next().expect("--dir requires a value"),
                "--dbfilename" => dbfilename = args.next().expect("--dbfilename requires a value"),
                _ => {}
            }
        }

        Self {
            port,
            role,
            replid: generate_replid(),
            dir,
            dbfilename,
            rdb: Bytes::new(),
        }
    }

    pub fn load_rdb(&mut self) {
        let rdb_path = std::path::Path::new(&self.dir).join(&self.dbfilename);
        let raw = std::fs::read(rdb_path).unwrap();
        self.rdb = Bytes::from(raw);
    }
}

fn generate_replid() -> String {
    const HEX: &[u8] = b"0123456789abcdef";
    let mut rng = rand::rng();
    (0..40)
        .map(|_| HEX[rng.random_range(0..HEX.len())] as char)
        .collect()
}
