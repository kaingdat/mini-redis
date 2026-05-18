use std::sync::Arc;

use bytes::Bytes;
use dashmap::DashMap;
use futures::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tokio_util::codec::Framed;

use codecrafters_redis::command::{ValueEntry, handle_command};
use codecrafters_redis::resp::RespParser;

fn parse_port() -> u16 {
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        if arg == "--port" {
            let port = args.next().expect("--port requires a value");
            return port
                .parse::<u16>()
                .expect("--port value must be a valid number");
        }
    }
    6379
}

#[tokio::main]
async fn main() {
    let port = parse_port();
    let listener = TcpListener::bind(("127.0.0.1", port)).await.unwrap();
    let storage = Arc::new(DashMap::<Bytes, ValueEntry>::new());
    loop {
        match listener.accept().await {
            Ok((stream, _addr)) => {
                let storage = Arc::clone(&storage);
                tokio::spawn(async move {
                    let mut framed = Framed::new(stream, RespParser::default());
                    while let Some(Ok(value)) = framed.next().await {
                        let response = handle_command(value, &storage);
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
