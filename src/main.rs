use std::sync::Arc;

use bytes::Bytes;
use dashmap::DashMap;
use futures::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tokio_util::codec::Framed;

use codecrafters_redis::command::{ValueEntry, handle_command};
use codecrafters_redis::resp::RespParser;

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
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
