use std::sync::Arc;

use bytes::Bytes;
use dashmap::DashMap;
use futures::{SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::Framed;

#[rustfmt::skip]
use mini_redis::prelude::*;
use mini_redis::types::RedisValueRef;

#[tokio::main]
async fn main() {
    let config = Arc::new(ServerConfig::from_args());
    let storage = Arc::new(DashMap::<Bytes, ValueEntry>::new());
    let aof = Arc::new(Aof::open(&config).expect("failed to initialize append-only files"));
    if config.aof_enabled() {
        aof.replay(&storage, &config)
            .await
            .expect("failed to replay append-only file");
    } else {
        load_into(&config.rdb_path(), &storage);
    }
    aof.spawn_flusher();
    let listener = TcpListener::bind(("127.0.0.1", config.port)).await.unwrap();
    let replication = Arc::new(ReplicationState::new());
    let lock = Arc::new(ExecLock::default());
    let pubsub = Arc::new(PubSub::new());
    let waiters = Arc::new(StreamWaiters::new());
    let watches = Arc::new(Watches::new());

    if matches!(config.role, Role::Replica { .. }) {
        let config = Arc::clone(&config);
        let storage = Arc::clone(&storage);
        let replication = Arc::clone(&replication);
        let aof = Arc::clone(&aof);
        let lock = Arc::clone(&lock);
        let pubsub = Arc::clone(&pubsub);
        let waiters = Arc::clone(&waiters);
        let watches = Arc::clone(&watches);
        tokio::spawn(async move {
            if let Err(e) = replicate_from_master(
                &config,
                &storage,
                &replication,
                &aof,
                &lock,
                &pubsub,
                &waiters,
                &watches,
            )
            .await
            {
                eprintln!("failed to replicate from master: {}", e);
            }
        });
    }

    loop {
        match listener.accept().await {
            Ok((stream, _addr)) => {
                let storage = Arc::clone(&storage);
                let config = Arc::clone(&config);
                let replication = Arc::clone(&replication);
                let aof = Arc::clone(&aof);
                let lock = Arc::clone(&lock);
                let pubsub = Arc::clone(&pubsub);
                let waiters = Arc::clone(&waiters);
                let watches = Arc::clone(&watches);
                tokio::spawn(async move {
                    let mut framed = Framed::new(stream, RespParser::default());
                    let mut session = Session::new(pubsub, waiters, watches);
                    let mut after = After::Continue;
                    while after == After::Continue {
                        tokio::select! {
                            frame = framed.next() => {
                                let Some(Ok(value)) = frame else { break };
                                let response = session
                                    .handle(&value, &storage, &config, &replication, &aof, &lock)
                                    .await;
                                if send_all(&mut framed, response.replies).await.is_err() {
                                    break;
                                }
                                after = response.after;
                                if after == After::Block {
                                    let Some(reply) =
                                        wait_blocked(&mut framed, &mut session, &storage, &lock)
                                            .await
                                    else {
                                        break;
                                    };
                                    if send_all(&mut framed, vec![reply]).await.is_err() {
                                        break;
                                    }
                                    after = After::Continue;
                                }
                            }
                            message = session.next_message() => {
                                let Some(message) = message else { break };
                                if framed.send(RedisValueRef::Raw(message)).await.is_err() {
                                    break;
                                }
                            }
                        }
                    }
                    drop(session);
                    if after == After::StreamToReplica {
                        stream_to_replica(framed, &replication).await;
                    }
                });
            }
            Err(e) => eprintln!("error: {}", e),
        }
    }
}

/// Runs a blocked read until it replies or the client disconnects. Bytes
/// that arrive meanwhile are buffered undecoded, so pipelined commands run
/// afterwards in order. Returns `None` once the connection is gone.
async fn wait_blocked(
    framed: &mut Framed<TcpStream, RespParser>,
    session: &mut Session,
    storage: &Arc<DashMap<Bytes, ValueEntry>>,
    lock: &ExecLock,
) -> Option<RedisValueRef> {
    let wait = session.wait_blocked(storage, lock);
    tokio::pin!(wait);
    let mut buf = [0u8; 4096];
    loop {
        tokio::select! {
            reply = &mut wait => return Some(reply),
            ready = framed.get_ref().readable() => {
                ready.ok()?;
                match framed.get_ref().try_read(&mut buf) {
                    Ok(0) => return None,
                    Ok(n) => framed.read_buffer_mut().extend_from_slice(&buf[..n]),
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
                    Err(_) => return None,
                }
            }
        }
    }
}

async fn send_all(
    framed: &mut Framed<TcpStream, RespParser>,
    replies: Vec<RedisValueRef>,
) -> std::io::Result<()> {
    for reply in replies {
        framed.feed(reply).await?;
    }
    framed.flush().await
}
