use std::sync::Arc;

use bytes::Bytes;
use dashmap::DashMap;
use futures::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tokio_util::codec::Framed;

use codecrafters_redis::command::{Role, ServerConfig, ValueEntry, handle_command};
use codecrafters_redis::resp::RespParser;

fn parse_config() -> (u16, ServerConfig) {
    let mut port = 6379;
    let mut role = Role::Master;

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
            _ => {}
        }
    }

    (port, ServerConfig { role })
}

#[tokio::main]
async fn main() {
    let (port, config) = parse_config();
    let config = Arc::new(config);
    let listener = TcpListener::bind(("127.0.0.1", port)).await.unwrap();
    let storage = Arc::new(DashMap::<Bytes, ValueEntry>::new());
    loop {
        match listener.accept().await {
            Ok((stream, _addr)) => {
                let storage = Arc::clone(&storage);
                let config = Arc::clone(&config);
                tokio::spawn(async move {
                    let mut framed = Framed::new(stream, RespParser::default());
                    while let Some(Ok(value)) = framed.next().await {
                        let response = handle_command(value, &storage, &config);
                        if framed.send(response).await.is_err() {
                            break;
                        }
                    }
                });
            }
            Err(e) => eprintln!("error: {}", e),
        }
    }
}
