use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use bytes::Bytes;
use dashmap::DashMap;
use futures::{SinkExt, StreamExt};
use tokio::{net::TcpStream, sync::mpsc};
use tokio_util::codec::Framed;

use crate::{
    command::{handle_command, is_getack_command},
    resp::{RespParser, encoded_len},
    server::{ReplicaRegistry, Role, ServerConfig},
    types::RedisValueRef,
    value::ValueEntry,
};

pub async fn stream_to_replica(
    mut framed: Framed<TcpStream, RespParser>,
    replicas: &Arc<ReplicaRegistry>,
    next_conn_id: &Arc<AtomicU64>,
) {
    let conn_id = next_conn_id.fetch_add(1, Ordering::Relaxed);
    let (tx, mut rx) = mpsc::unbounded_channel::<Bytes>();
    replicas.insert(conn_id, tx);

    loop {
        tokio::select! {
            incoming = framed.next() => {
                match incoming {
                    Some(Ok(_)) => {}
                    _ => break,
                }
            }
            msg = rx.recv() => {
                match msg {
                    Some(bytes) => {
                        if framed.send(RedisValueRef::Raw(bytes)).await.is_err() {
                            break;
                        }
                    }
                    None => break,
                }
            }
        }
    }

    replicas.remove(&conn_id);
}

pub async fn replicate_from_master(
    config: &Arc<ServerConfig>,
    storage: &Arc<DashMap<Bytes, ValueEntry>>,
    replicas: &Arc<ReplicaRegistry>,
) -> anyhow::Result<()> {
    let Role::Replica { host, port } = &config.role else {
        return Ok(());
    };

    let mut framed = handshake(host, *port, config.port).await?;

    let mut offset: u64 = 0;

    while let Some(incoming) = framed.next().await {
        let Ok(value) = incoming else {
            return Err(anyhow::anyhow!("failed to parse command from master"));
        };

        let consumed = encoded_len(&value) as u64;

        if is_getack_command(&value) {
            let offset_str = offset.to_string();
            framed
                .send(resp_command(&[b"REPLCONF", b"ACK", offset_str.as_bytes()]))
                .await?;
        } else {
            handle_command(&value, storage, config, replicas);
        }

        offset += consumed;
    }

    Ok(())
}

async fn handshake(
    host: &str,
    master_port: u16,
    listening_port: u16,
) -> anyhow::Result<Framed<TcpStream, RespParser>> {
    let stream = TcpStream::connect((host, master_port)).await?;
    let mut framed = Framed::new(stream, RespParser::default());

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

    framed.codec_mut().expect_rdb();
    read_reply(&mut framed).await?;

    Ok(framed)
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
