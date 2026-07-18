use std::sync::{Arc, atomic::AtomicU64};

use bytes::Bytes;
use codecrafters_redis::{
    command::{handle_command, is_psync_command},
    replication::{handshake, stream_to_replica},
    resp::RespParser,
    server::{ReplicaRegistry, Role, ServerConfig},
    value::ValueEntry,
};
use dashmap::DashMap;
use futures::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tokio_util::codec::Framed;

#[tokio::main]
async fn main() {
    let mut config = ServerConfig::from_args();
    config.load_rdb();
    let config = Arc::new(config);
    let listener = TcpListener::bind(("127.0.0.1", config.port)).await.unwrap();
    let storage = Arc::new(DashMap::<Bytes, ValueEntry>::new());
    let replicas = Arc::new(ReplicaRegistry::new());
    let next_conn_id = Arc::new(AtomicU64::new(0));

    if let Role::Replica { host, port } = &config.role {
        let host = host.clone();
        let master_port = *port;
        let listening_port = config.port;
        tokio::spawn(async move {
            if let Err(e) = handshake(&host, master_port, listening_port).await {
                eprintln!("handshake with master failed: {}", e);
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
                    let mut framed = Framed::new(stream, RespParser);
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
