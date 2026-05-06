use std::sync::Arc;

use bytes::Bytes;
use codecrafters_redis::resp::RespParser;
use codecrafters_redis::types::RedisValueRef;
use dashmap::DashMap;
use futures::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tokio_util::codec::Framed;

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    let storage = Arc::new(DashMap::<Bytes, Bytes>::new());
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

fn handle_command(value: RedisValueRef, storage: &Arc<DashMap<Bytes, Bytes>>) -> RedisValueRef {
    let RedisValueRef::Array(parts) = value else {
        return RedisValueRef::ErrorMsg(b"ERR expected array command".to_vec());
    };

    let Some(RedisValueRef::BulkString(cmd)) = parts.first() else {
        return RedisValueRef::ErrorMsg(b"ERR missing command".to_vec());
    };

    match cmd.to_ascii_uppercase().as_slice() {
        b"PING" => RedisValueRef::SimpleString(Bytes::from_static(b"PONG")),
        b"ECHO" => match parts.get(1) {
            Some(RedisValueRef::BulkString(msg)) => RedisValueRef::BulkString(msg.clone()),
            _ => RedisValueRef::ErrorMsg(
                b"ERR wrong number of arguments for 'echo' command".to_vec(),
            ),
        },
        b"SET" => {
            if parts.len() != 3 {
                return RedisValueRef::ErrorMsg(
                    b"ERR wrong number of arguments for 'set' command".to_vec(),
                );
            }

            let key = match parts.get(1) {
                Some(RedisValueRef::BulkString(msg)) => msg.clone(),
                _ => return RedisValueRef::ErrorMsg(b"ERR invalid key type".to_vec()),
            };

            let value = match parts.get(2) {
                Some(RedisValueRef::BulkString(msg)) => msg.clone(),
                _ => return RedisValueRef::ErrorMsg(b"ERR invalid value type".to_vec()),
            };

            storage.insert(key, value);

            RedisValueRef::SimpleString(Bytes::from_static(b"OK"))
        }
        b"GET" => {
            if parts.len() != 2 {
                return RedisValueRef::ErrorMsg(
                    b"ERR wrong number of arguments for 'get' command".to_vec(),
                );
            }

            let key = match parts.get(1) {
                Some(RedisValueRef::BulkString(key)) => key,
                _ => return RedisValueRef::ErrorMsg(b"ERR invalid key type".to_vec()),
            };

            match storage.get(key) {
                Some(entry) => RedisValueRef::BulkString(entry.value().clone()),
                None => RedisValueRef::NullBulkString,
            }
        }
        _ => RedisValueRef::ErrorMsg(b"ERR unknown command".to_vec()),
    }
}
