use codecrafters_redis::resp::RespParser;
use codecrafters_redis::types::RedisValueRef;
use futures::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tokio_util::codec::Framed;

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    loop {
        match listener.accept().await {
            Ok((stream, _addr)) => {
                tokio::spawn(async move {
                    let mut framed = Framed::new(stream, RespParser::default());
                    while let Some(Ok(value)) = framed.next().await {
                        let response = handle_command(value);
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

fn handle_command(value: RedisValueRef) -> RedisValueRef {
    let RedisValueRef::Array(parts) = value else {
        return RedisValueRef::ErrorMsg(b"ERR expected array command".to_vec());
    };

    let Some(RedisValueRef::BulkString(cmd)) = parts.first() else {
        return RedisValueRef::ErrorMsg(b"ERR missing command".to_vec());
    };

    match cmd.to_ascii_uppercase().as_slice() {
        b"PING" => RedisValueRef::SimpleString(bytes::Bytes::from_static(b"PONG")),
        b"ECHO" => match parts.get(1) {
            Some(RedisValueRef::BulkString(msg)) => RedisValueRef::BulkString(msg.clone()),
            _ => RedisValueRef::ErrorMsg(b"ERR wrong number of arguments for 'echo' command".to_vec()),
        },
        _ => RedisValueRef::ErrorMsg(b"ERR unknown command".to_vec()),
    }
}
