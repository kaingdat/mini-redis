use core::f64;
use std::{sync::Arc, time::Duration};

use bytes::{Bytes, BytesMut};
use dashmap::DashMap;
use tokio::time::Instant;

use crate::{
    resp::encode_command,
    server::{ReplicaRegistry, ServerConfig},
    types::RedisValueRef,
    value::{RedisValue, SortedSetData, ValueEntry},
};

#[derive(Clone, Copy)]
enum Command {
    Ping,
    Echo,
    Set,
    Get,
    ZAdd,
    ZRank,
    ZRange,
    ZCard,
    ZScore,
    ZRem,
    Info,
    ReplConf,
    Psync,
}

impl Command {
    fn parse(name: &[u8]) -> Option<Self> {
        Some(match name.to_ascii_uppercase().as_slice() {
            b"PING" => Self::Ping,
            b"ECHO" => Self::Echo,
            b"SET" => Self::Set,
            b"GET" => Self::Get,
            b"ZADD" => Self::ZAdd,
            b"ZRANK" => Self::ZRank,
            b"ZRANGE" => Self::ZRange,
            b"ZCARD" => Self::ZCard,
            b"ZSCORE" => Self::ZScore,
            b"ZREM" => Self::ZRem,
            b"INFO" => Self::Info,
            b"REPLCONF" => Self::ReplConf,
            b"PSYNC" => Self::Psync,
            _ => return None,
        })
    }

    fn is_write(&self) -> bool {
        matches!(self, Command::Set | Command::ZAdd | Command::ZRem)
    }
}

pub fn is_psync_command(value: &RedisValueRef) -> bool {
    matches!(value, RedisValueRef::Array(parts) if matches!(parts.first(), Some(RedisValueRef::BulkString(cmd)) if cmd.eq_ignore_ascii_case(b"PSYNC")))
}

const WRONGTYPE: &[u8] = b"WRONGTYPE Operation against a key holding wrong type value";

fn require_bulk(parts: &[RedisValueRef], i: usize) -> Result<&Bytes, RedisValueRef> {
    match parts.get(i) {
        Some(RedisValueRef::BulkString(b)) => Ok(b),
        _ => Err(RedisValueRef::ErrorMsg(b"ERR invalid argument".to_vec())),
    }
}

fn parse_i64(parts: &[RedisValueRef], i: usize) -> Result<i64, RedisValueRef> {
    let b = require_bulk(parts, i)?;
    std::str::from_utf8(b)
        .ok()
        .and_then(|s| s.parse::<i64>().ok())
        .ok_or_else(|| {
            RedisValueRef::ErrorMsg(b"ERR value is not an integer or out of range".to_vec())
        })
}

fn wrong_arity(cmd: &str) -> RedisValueRef {
    RedisValueRef::ErrorMsg(
        format!("ERR wrong number of arguments for '{}' command", cmd).into_bytes(),
    )
}

macro_rules! arity {
    ($parts:expr, $cmd:literal, == $n:expr) => {
        if $parts.len() != $n {
            return wrong_arity($cmd);
        }
    };
    ($parts:expr, $cmd:literal, >= $n:expr) => {
        if $parts.len() < $n {
            return wrong_arity($cmd);
        }
    };
    ($parts:expr, $cmd:literal, one_of [$($n:expr),+]) => {
        if !matches!($parts.len(), $($n)|+) {
            return wrong_arity($cmd);
        }
    };
}

pub fn handle_command(
    value: &RedisValueRef,
    storage: &Arc<DashMap<Bytes, ValueEntry>>,
    config: &Arc<ServerConfig>,
    replicas: &Arc<ReplicaRegistry>,
) -> RedisValueRef {
    let RedisValueRef::Array(parts) = value else {
        return RedisValueRef::ErrorMsg(b"ERR expected array command".to_vec());
    };

    let Some(RedisValueRef::BulkString(cmd)) = parts.first() else {
        return RedisValueRef::ErrorMsg(b"ERR missing command".to_vec());
    };

    let Some(command) = Command::parse(cmd) else {
        return RedisValueRef::ErrorMsg(b"ERR unknown command".to_vec());
    };

    let response = match command {
        Command::Ping => RedisValueRef::SimpleString(Bytes::from_static(b"PONG")),
        Command::Echo => {
            arity!(parts, "echo", == 2);
            match require_bulk(parts, 1) {
                Ok(b) => RedisValueRef::BulkString(b.clone()),
                Err(e) => e,
            }
        }
        Command::Set => handle_set(parts, storage),
        Command::Get => handle_get(parts, storage),
        Command::ZAdd => handle_zadd(parts, storage),
        Command::ZRank => handle_zrank(parts, storage),
        Command::ZRange => handle_zrange(parts, storage),
        Command::ZCard => handle_zcard(parts, storage),
        Command::ZScore => handle_zscore(parts, storage),
        Command::ZRem => handle_zrem(parts, storage),
        Command::Info => handle_info(parts, config),
        Command::ReplConf => handle_replconf(),
        Command::Psync => handle_psync(parts, config),
    };

    if command.is_write() {
        propagate(parts, replicas);
    }

    response
}

fn propagate(parts: &[RedisValueRef], replicas: &Arc<ReplicaRegistry>) {
    if replicas.is_empty() {
        return;
    }
    let encoded = encode_command(parts);
    replicas.retain(|_, tx| tx.send(encoded.clone()).is_ok());
}

fn handle_get(parts: &[RedisValueRef], storage: &Arc<DashMap<Bytes, ValueEntry>>) -> RedisValueRef {
    arity!(parts, "get", == 2);
    let key = match require_bulk(parts, 1) {
        Ok(b) => b,
        Err(e) => return e,
    };

    storage.remove_if(key, |_, entry| entry.is_expired());

    match storage.get(key) {
        Some(entry) => match &entry.data {
            RedisValue::String(data) => RedisValueRef::BulkString(data.clone()),
            _ => RedisValueRef::ErrorMsg(WRONGTYPE.to_vec()),
        },
        None => RedisValueRef::NullBulkString,
    }
}

fn handle_set(parts: &[RedisValueRef], storage: &Arc<DashMap<Bytes, ValueEntry>>) -> RedisValueRef {
    arity!(parts, "set", one_of [3, 5]);

    let key = match require_bulk(parts, 1) {
        Ok(b) => b.clone(),
        Err(e) => return e,
    };
    let value = match require_bulk(parts, 2) {
        Ok(b) => b.clone(),
        Err(e) => return e,
    };

    let expires_at = if parts.len() == 5 {
        let option = match require_bulk(parts, 3) {
            Ok(b) => b,
            Err(_) => return RedisValueRef::ErrorMsg(b"ERR syntax error".to_vec()),
        };
        if !option.eq_ignore_ascii_case(b"PX") {
            return RedisValueRef::ErrorMsg(b"ERR syntax error".to_vec());
        }
        let px_bytes = match require_bulk(parts, 4) {
            Ok(b) => b,
            Err(_) => return RedisValueRef::ErrorMsg(b"ERR syntax error".to_vec()),
        };
        let s = match std::str::from_utf8(px_bytes) {
            Ok(s) => s,
            Err(_) => {
                return RedisValueRef::ErrorMsg(
                    b"ERR value is not integer or out of range".to_vec(),
                );
            }
        };
        match s.parse::<i64>() {
            Ok(ms) if ms > 0 => Some(Instant::now() + Duration::from_millis(ms as u64)),
            Ok(_) => {
                return RedisValueRef::ErrorMsg(
                    b"ERR invalid expire time in 'set' command".to_vec(),
                );
            }
            Err(_) => {
                return RedisValueRef::ErrorMsg(
                    b"ERR value is not integer or out of range".to_vec(),
                );
            }
        }
    } else {
        None
    };

    storage.insert(key, ValueEntry::new(RedisValue::String(value), expires_at));
    RedisValueRef::SimpleString(Bytes::from_static(b"OK"))
}

fn handle_zadd(
    parts: &[RedisValueRef],
    storage: &Arc<DashMap<Bytes, ValueEntry>>,
) -> RedisValueRef {
    if parts.len() < 4 || !(parts.len() - 2).is_multiple_of(2) {
        return wrong_arity("zadd");
    }

    let key = match require_bulk(parts, 1) {
        Ok(b) => b.clone(),
        Err(e) => return e,
    };

    let mut pairs = Vec::with_capacity((parts.len() - 2) / 2);
    let mut idx = 2;
    while idx < parts.len() {
        let score_bytes = match require_bulk(parts, idx) {
            Ok(b) => b,
            Err(_) => return RedisValueRef::ErrorMsg(b"ERR value is not a valid float".to_vec()),
        };
        let score_str = match std::str::from_utf8(score_bytes) {
            Ok(s) => s,
            Err(_) => return RedisValueRef::ErrorMsg(b"ERR value is not a valid float".to_vec()),
        };
        let score = match parse_score(score_str) {
            Ok(s) => s,
            Err(e) => return e,
        };
        let member = match require_bulk(parts, idx + 1) {
            Ok(b) => b.clone(),
            Err(e) => return e,
        };
        pairs.push((score, member));
        idx += 2;
    }

    let mut entry = storage
        .entry(key)
        .or_insert_with(|| ValueEntry::new(RedisValue::SortedSet(SortedSetData::new()), None));

    let zset = match &mut entry.data {
        RedisValue::SortedSet(z) => z,
        _ => return RedisValueRef::ErrorMsg(WRONGTYPE.to_vec()),
    };

    let mut added_count = 0;
    for (score, member) in pairs {
        if zset.add(member, score) {
            added_count += 1;
        }
    }

    RedisValueRef::Int(added_count)
}

fn handle_zrange(
    parts: &[RedisValueRef],
    storage: &Arc<DashMap<Bytes, ValueEntry>>,
) -> RedisValueRef {
    arity!(parts, "zrange", == 4);

    let key = match require_bulk(parts, 1) {
        Ok(b) => b,
        Err(e) => return e,
    };
    let start = match parse_i64(parts, 2) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let stop = match parse_i64(parts, 3) {
        Ok(v) => v,
        Err(e) => return e,
    };

    let entry = match storage.get(key) {
        Some(e) => e,
        None => return RedisValueRef::Array(vec![]),
    };

    let zset = match &entry.data {
        RedisValue::SortedSet(z) => z,
        _ => return RedisValueRef::ErrorMsg(WRONGTYPE.to_vec()),
    };

    let members = zset.range(start, stop);
    RedisValueRef::Array(members.into_iter().map(RedisValueRef::BulkString).collect())
}

fn handle_zcard(
    parts: &[RedisValueRef],
    storage: &Arc<DashMap<Bytes, ValueEntry>>,
) -> RedisValueRef {
    arity!(parts, "zcard", == 2);

    let key = match require_bulk(parts, 1) {
        Ok(b) => b,
        Err(e) => return e,
    };

    let entry = match storage.get(key) {
        Some(e) => e,
        None => return RedisValueRef::Int(0),
    };

    match &entry.data {
        RedisValue::SortedSet(z) => RedisValueRef::Int(z.len() as i64),
        _ => RedisValueRef::ErrorMsg(WRONGTYPE.to_vec()),
    }
}

fn parse_score(s: &str) -> Result<f64, RedisValueRef> {
    match s {
        "+inf" | "inf" => return Ok(f64::INFINITY),
        "-inf" => return Ok(f64::NEG_INFINITY),
        _ => {}
    }

    match s.parse::<f64>() {
        Ok(score) if !score.is_nan() => Ok(score),
        _ => Err(RedisValueRef::ErrorMsg(
            b"ERR value is not a valid float".to_vec(),
        )),
    }
}

fn handle_zrank(
    parts: &[RedisValueRef],
    storage: &Arc<DashMap<Bytes, ValueEntry>>,
) -> RedisValueRef {
    arity!(parts, "zrank", == 3);

    let key = match require_bulk(parts, 1) {
        Ok(b) => b,
        Err(e) => return e,
    };
    let member = match require_bulk(parts, 2) {
        Ok(b) => b,
        Err(e) => return e,
    };

    let entry = match storage.get(key) {
        Some(entry) => entry,
        None => return RedisValueRef::NullBulkString,
    };

    let zset = match &entry.data {
        RedisValue::SortedSet(z) => z,
        _ => return RedisValueRef::ErrorMsg(WRONGTYPE.to_vec()),
    };

    match zset.rank(member) {
        Some(r) => RedisValueRef::Int(r),
        None => RedisValueRef::NullBulkString,
    }
}

fn handle_zscore(
    parts: &[RedisValueRef],
    storage: &Arc<DashMap<Bytes, ValueEntry>>,
) -> RedisValueRef {
    arity!(parts, "zscore", == 3);

    let key = match require_bulk(parts, 1) {
        Ok(b) => b,
        Err(e) => return e,
    };
    let member = match require_bulk(parts, 2) {
        Ok(b) => b,
        Err(e) => return e,
    };

    let entry = match storage.get(key) {
        Some(entry) => entry,
        None => return RedisValueRef::NullBulkString,
    };

    let zset = match &entry.data {
        RedisValue::SortedSet(z) => z,
        _ => return RedisValueRef::ErrorMsg(WRONGTYPE.to_vec()),
    };

    match zset.score(member) {
        Some(s) => RedisValueRef::BulkString(Bytes::from(format!("{}", s))),
        None => RedisValueRef::NullBulkString,
    }
}

fn handle_zrem(
    parts: &[RedisValueRef],
    storage: &Arc<DashMap<Bytes, ValueEntry>>,
) -> RedisValueRef {
    arity!(parts, "zrem", >= 3);

    let key = match require_bulk(parts, 1) {
        Ok(b) => b.clone(),
        Err(e) => return e,
    };

    let mut entry = match storage.get_mut(&key) {
        Some(entry) => entry,
        None => return RedisValueRef::Int(0),
    };

    let zset = match &mut entry.data {
        RedisValue::SortedSet(z) => z,
        _ => return RedisValueRef::ErrorMsg(WRONGTYPE.to_vec()),
    };

    let removed = parts[2..]
        .iter()
        .filter(|p| match p {
            RedisValueRef::BulkString(m) => zset.remove(m),
            _ => false,
        })
        .count();

    RedisValueRef::Int(removed as i64)
}

fn handle_info(_parts: &[RedisValueRef], config: &Arc<ServerConfig>) -> RedisValueRef {
    let body = format!(
        "# Replication\r\n\
         role:{}\r\n\
         master_replid:{}\r\n\
         master_repl_offset:{}\r\n",
        config.role.name(),
        config.replid,
        config.repl_offset,
    );
    RedisValueRef::BulkString(Bytes::from(body))
}

fn handle_replconf() -> RedisValueRef {
    RedisValueRef::SimpleString(Bytes::from_static(b"OK"))
}

fn handle_psync(_parts: &[RedisValueRef], config: &Arc<ServerConfig>) -> RedisValueRef {
    let mut out = BytesMut::new();

    out.extend_from_slice(
        format!("+FULLRESYNC {} {}\r\n", config.replid, config.repl_offset).as_bytes(),
    );

    out.extend_from_slice(format!("${}\r\n", config.rdb.len()).as_bytes());
    out.extend_from_slice(&config.rdb);

    RedisValueRef::Raw(out.freeze())
}
