use std::sync::{Arc, atomic::AtomicU64};

use bytes::Bytes;
use dashmap::DashMap;
use futures::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tokio_util::codec::Framed;

#[rustfmt::skip]
use codecrafters_redis::prelude::*;

#[tokio::main]
async fn main() {
    let mut config = ServerConfig::from_args();
    config.load_rdb();
    let config = Arc::new(config);
    let listener = TcpListener::bind(("127.0.0.1", config.port)).await.unwrap();
    let storage = Arc::new(DashMap::<Bytes, ValueEntry>::new());
    let replicas = Arc::new(ReplicaRegistry::new());
    let next_conn_id = Arc::new(AtomicU64::new(0));

    if matches!(config.role, Role::Replica { .. }) {
        let config = Arc::clone(&config);
        let storage = Arc::clone(&storage);
        let replicas = Arc::clone(&replicas);
        tokio::spawn(async move {
            if let Err(e) = replicate_from_master(&config, &storage, &replicas).await {
                eprintln!("failed to replicate from master: {}", e);
            }
        });
    }

    loop {
        match listener.accept().await {
            Ok((stream, _addr)) => {
                let storage = Arc::clone(&storage);
                let config = Arc::clone(&config);
                let replicas = Arc::clone(&replicas);
                let next_conn_id = Arc::clone(&next_conn_id);
                tokio::spawn(async move {
                    let mut framed = Framed::new(stream, RespParser::default());
                    while let Some(Ok(value)) = framed.next().await {
                        let is_replica = is_psync_command(&value);
                        let response = handle_command(&value, &storage, &config, &replicas);
                        if framed.send(response).await.is_err() {
                            break;
                        }

                        if is_replica {
                            stream_to_replica(framed, &replicas, &next_conn_id).await;
                            break;
                        }
                    }
                });
            }
            Err(e) => eprintln!("error: {}", e),
        }
    }
}
