use std::sync::Arc;

use bytes::Bytes;
use codecrafters_redis::types::RedisValueRef;
use dashmap::DashMap;
use futures::{SinkExt, StreamExt};
use rand::RngExt;
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::Framed;

use codecrafters_redis::command::{Role, ServerConfig, ValueEntry, handle_command};
use codecrafters_redis::resp::RespParser;

fn parse_config() -> ServerConfig {
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

    ServerConfig {
        port,
        role,
        replid: generate_replid(),
        repl_offset: 0,
    }
}

fn generate_replid() -> String {
    const HEX: &[u8] = b"0123456789abcdef";
    let mut rng = rand::rng();
    (0..40)
        .map(|_| HEX[rng.random_range(0..HEX.len())] as char)
        .collect()
}

#[tokio::main]
async fn main() {
    let config = parse_config();
    let config = Arc::new(config);
    let listener = TcpListener::bind(("127.0.0.1", config.port)).await.unwrap();
    let storage = Arc::new(DashMap::<Bytes, ValueEntry>::new());

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
                tokio::spawn(async move {
                    let mut framed = Framed::new(stream, RespParser);
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

async fn handshake(host: &str, master_port: u16, listening_port: u16) -> anyhow::Result<()> {
    let stream = TcpStream::connect((host, master_port)).await?;
    let mut framed = Framed::new(stream, RespParser);

    framed.send(resp_command(&[b"PING"])).await?;
    read_reply(&mut framed).await?;

    let port_str = listening_port.to_string();
    framed
        .send(resp_command(&[
            b"REPLCONF",
            b"listening-port",
            port_str.as_bytes(),
        ]))
        .await?;
    read_reply(&mut framed).await?;

    framed
        .send(resp_command(&[b"REPLCONF", b"capa", b"psync2"]))
        .await?;
    read_reply(&mut framed).await?;

    framed.send(resp_command(&[b"PSYNC", b"?", b"-1"])).await?;
    read_reply(&mut framed).await?;

    Ok(())
}

fn resp_command(args: &[&[u8]]) -> RedisValueRef {
    RedisValueRef::Array(
        args.iter()
            .map(|a| RedisValueRef::BulkString(Bytes::copy_from_slice(a)))
            .collect(),
    )
}

async fn read_reply(framed: &mut Framed<TcpStream, RespParser>) -> anyhow::Result<RedisValueRef> {
    match framed.next().await {
        Some(Ok(reply)) => Ok(reply),
        Some(Err(_)) => Err(anyhow::anyhow!("failed to parse master reply")),
        None => Err(anyhow::anyhow!("master closed connection during handshake")),
    }
}
