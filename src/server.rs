use std::path::PathBuf;

use bytes::Bytes;
use dashmap::DashMap;
use rand::RngExt;
use tokio::sync::mpsc;

pub type ReplicaRegistry = DashMap<u64, mpsc::UnboundedSender<Bytes>>;

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
    pub repl_offset: u64,
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
            repl_offset: 0,
            dir,
            dbfilename,
            rdb: Bytes::new(),
        }
    }

    pub fn rdb_path(&self) -> PathBuf {
        std::path::Path::new(&self.dir).join(&self.dbfilename)
    }

    pub fn load_rdb(&mut self) {
        let raw = std::fs::read(&self.rdb_path()).unwrap();
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
