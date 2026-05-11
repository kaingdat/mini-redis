use core::f64;
use std::{sync::Arc, time::Duration};

use bytes::Bytes;
use dashmap::DashMap;
use tokio::time::Instant;

use crate::{
    types::RedisValueRef,
    value::{RedisValue, SortedSetData},
};

pub struct ValueEntry {
    data: RedisValue,
    expires_at: Option<Instant>,
}

impl ValueEntry {
    pub fn new(data: RedisValue, expires_at: Option<Instant>) -> Self {
        Self { data, expires_at }
    }

    pub fn is_expired(&self) -> bool {
        self.expires_at.map_or(false, |exp| Instant::now() >= exp)
    }
}

pub fn handle_command(
    value: RedisValueRef,
    storage: &Arc<DashMap<Bytes, ValueEntry>>,
) -> RedisValueRef {
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
        b"SET" => handle_set(&parts, storage),
        b"GET" => handle_get(&parts, storage),
        b"ZADD" => handle_zadd(&parts, storage),
        b"ZRANK" => handle_zrank(&parts, storage),
        b"ZRANGE" => handle_zrange(&parts, storage),
        b"ZCARD" => handle_zcard(&parts, storage),
        b"ZSCORE" => handle_zscore(&parts, storage),
        _ => RedisValueRef::ErrorMsg(b"ERR unknown command".to_vec()),
    }
}

fn handle_get(parts: &[RedisValueRef], storage: &Arc<DashMap<Bytes, ValueEntry>>) -> RedisValueRef {
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
        Some(entry) => {
            if entry.is_expired() {
                drop(entry);
                storage.remove(key);
                RedisValueRef::NullBulkString
            } else {
                match &entry.data {
                    RedisValue::String(data) => RedisValueRef::BulkString(data.clone()),
                    _ => RedisValueRef::ErrorMsg(
                        b"WRONGTYPE Operation against a key holding wrong type value".to_vec(),
                    ),
                }
            }
        }
        None => RedisValueRef::NullBulkString,
    }
}

fn handle_set(parts: &[RedisValueRef], storage: &Arc<DashMap<Bytes, ValueEntry>>) -> RedisValueRef {
    if parts.len() != 3 && parts.len() != 5 {
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

    let expires_at = if parts.len() == 5 {
        let option = match parts.get(3) {
            Some(RedisValueRef::BulkString(opt)) => opt,
            _ => return RedisValueRef::ErrorMsg(b"ERR syntax error".to_vec()),
        };

        if option.to_ascii_uppercase().as_slice() != b"PX" {
            return RedisValueRef::ErrorMsg(b"ERR syntax error".to_vec());
        }

        let px_value = match parts.get(4) {
            Some(RedisValueRef::BulkString(val)) => match std::str::from_utf8(val) {
                Ok(s) => match s.parse::<i64>() {
                    Ok(ms) if ms > 0 => ms,
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
                },
                Err(_) => {
                    return RedisValueRef::ErrorMsg(
                        b"ERR value is not integer or out of range".to_vec(),
                    );
                }
            },
            _ => return RedisValueRef::ErrorMsg(b"ERR syntax error".to_vec()),
        };

        Some(Instant::now() + Duration::from_millis(px_value as u64))
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
    if parts.len() < 4 || (parts.len() - 2) % 2 != 0 {
        return RedisValueRef::ErrorMsg(
            b"ERR wrong number of arguments for 'zadd' command".to_vec(),
        );
    }

    let key = match parts.get(1) {
        Some(RedisValueRef::BulkString(msg)) => msg.clone(),
        _ => return RedisValueRef::ErrorMsg(b"ERR invalid key type".to_vec()),
    };

    let mut pairs = Vec::with_capacity((parts.len() - 2) / 2);
    let mut idx = 2;

    while idx < parts.len() {
        let score_bytes = match parts.get(idx) {
            Some(RedisValueRef::BulkString(s)) => s.clone(),
            _ => return RedisValueRef::ErrorMsg(b"ERR invalid score type".to_vec()),
        };

        let score_str = match std::str::from_utf8(&score_bytes) {
            Ok(s) => s,
            Err(_) => return RedisValueRef::ErrorMsg(b"ERR value is not a valid float".to_vec()),
        };

        let score = match parse_score(score_str) {
            Ok(s) => s,
            Err(e) => return e,
        };

        let member = match parts.get(idx + 1) {
            Some(RedisValueRef::BulkString(m)) => m.clone(),
            _ => return RedisValueRef::ErrorMsg(b"ERR invalid member type".to_vec()),
        };

        pairs.push((score, member));
        idx += 2;
    }

    let mut entry = storage
        .entry(key.clone())
        .or_insert_with(|| ValueEntry::new(RedisValue::SortedSet(SortedSetData::new()), None));

    let zset = match &mut entry.data {
        RedisValue::SortedSet(z) => z,
        _ => {
            return RedisValueRef::ErrorMsg(
                b"WRONGTYPE Operation against a key holding wrong type value".to_vec(),
            );
        }
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
    if parts.len() != 4 {
        return RedisValueRef::ErrorMsg(
            b"ERR wrong number of arguments for 'zrange' command".to_vec(),
        );
    }

    let key = match parts.get(1) {
        Some(RedisValueRef::BulkString(msg)) => msg.clone(),
        _ => return RedisValueRef::ErrorMsg(b"ERR invalid key type".to_vec()),
    };

    let parse_index = |part: &RedisValueRef| -> Result<i64, RedisValueRef> {
        match part {
            RedisValueRef::BulkString(s) => std::str::from_utf8(s)
                .ok()
                .and_then(|s| s.parse::<i64>().ok())
                .ok_or_else(|| {
                    RedisValueRef::ErrorMsg(b"ERR value is not an integer or out of range".to_vec())
                }),
            _ => Err(RedisValueRef::ErrorMsg(b"ERR invalid argument".to_vec())),
        }
    };

    let start = match parts.get(2).map(parse_index) {
        Some(Ok(v)) => v,
        Some(Err(e)) => return e,
        None => return RedisValueRef::ErrorMsg(b"ERR missing start index".to_vec()),
    };

    let stop = match parts.get(3).map(parse_index) {
        Some(Ok(v)) => v,
        Some(Err(e)) => return e,
        None => return RedisValueRef::ErrorMsg(b"ERR missing stop index".to_vec()),
    };

    let entry = match storage.get(&key) {
        Some(e) => e,
        None => return RedisValueRef::Array(vec![]),
    };

    let zset = match &entry.data {
        RedisValue::SortedSet(z) => z,
        _ => {
            return RedisValueRef::ErrorMsg(
                b"WRONGTYPE Operation against a key holding wrong type value".to_vec(),
            );
        }
    };

    let members = zset.range(start, stop);
    RedisValueRef::Array(members.into_iter().map(RedisValueRef::BulkString).collect())
}

fn handle_zcard(
    parts: &[RedisValueRef],
    storage: &Arc<DashMap<Bytes, ValueEntry>>,
) -> RedisValueRef {
    if parts.len() != 2 {
        return RedisValueRef::ErrorMsg(
            b"ERR wrong number of arguments for 'zcard' command".to_vec(),
        );
    }

    let key = match parts.get(1) {
        Some(RedisValueRef::BulkString(msg)) => msg,
        _ => return RedisValueRef::ErrorMsg(b"ERR invalid key type".to_vec()),
    };

    let entry = match storage.get(key) {
        Some(e) => e,
        None => return RedisValueRef::Int(0),
    };

    match &entry.data {
        RedisValue::SortedSet(z) => RedisValueRef::Int(z.len() as i64),
        _ => RedisValueRef::ErrorMsg(
            b"WRONGTYPE Operation against a key holding wrong type value".to_vec(),
        ),
    }
}

fn parse_score(s: &str) -> Result<f64, RedisValueRef> {
    match s {
        "+inf" | "inf" => return Ok(f64::INFINITY),
        "-inf" => return Ok(f64::NEG_INFINITY),
        _ => {}
    }

    match s.parse::<f64>() {
        Ok(score) => {
            if score.is_nan() {
                return Err(RedisValueRef::ErrorMsg(
                    b"ERR value is not a valid float".to_vec(),
                ));
            } else {
                return Ok(score);
            }
        }
        Err(_) => Err(RedisValueRef::ErrorMsg(
            b"ERR value is not a valid float".to_vec(),
        )),
    }
}

fn handle_zrank(
    parts: &[RedisValueRef],
    storage: &Arc<DashMap<Bytes, ValueEntry>>,
) -> RedisValueRef {
    if parts.len() != 3 {
        return RedisValueRef::ErrorMsg(
            b"ERR wrong number of arguments for 'zrank' command".to_vec(),
        );
    }

    let key = match parts.get(1) {
        Some(RedisValueRef::BulkString(msg)) => msg.clone(),
        _ => return RedisValueRef::ErrorMsg(b"ERR invalid key type".to_vec()),
    };

    let member = match parts.get(2) {
        Some(RedisValueRef::BulkString(msg)) => msg.clone(),
        _ => return RedisValueRef::ErrorMsg(b"ERR invalid member type".to_vec()),
    };

    let entry = match storage.get(&key) {
        Some(entry) => entry,
        None => {
            return RedisValueRef::NullBulkString;
        }
    };

    let zset = match &entry.data {
        RedisValue::SortedSet(z) => z,
        _ => {
            return RedisValueRef::ErrorMsg(
                b"WRONGTYPE operation against a key holding wrong type value".to_vec(),
            );
        }
    };

    match zset.rank(&member) {
        Some(r) => RedisValueRef::Int(r),
        None => RedisValueRef::NullBulkString,
    }
}

fn handle_zscore(
    parts: &[RedisValueRef],
    storage: &Arc<DashMap<Bytes, ValueEntry>>,
) -> RedisValueRef {
    if parts.len() != 3 {
        return RedisValueRef::ErrorMsg(
            b"ERR wrong number of arguments for 'zscore' command".to_vec(),
        );
    }

    let key = match parts.get(1) {
        Some(RedisValueRef::BulkString(msg)) => msg.clone(),
        _ => return RedisValueRef::ErrorMsg(b"ERR invalid key type".to_vec()),
    };

    let member = match parts.get(2) {
        Some(RedisValueRef::BulkString(msg)) => msg.clone(),
        _ => return RedisValueRef::ErrorMsg(b"ERR invalid member type".to_vec()),
    };

    let entry = match storage.get(&key) {
        Some(entry) => entry,
        None => return RedisValueRef::NullBulkString,
    };

    let zset = match &entry.data {
        RedisValue::SortedSet(z) => z,
        _ => {
            return RedisValueRef::ErrorMsg(
                b"WRONGTYPE operation against a key holding wrong type value".to_vec(),
            );
        }
    };

    match zset.score(&member) {
        Some(s) => RedisValueRef::BulkString(Bytes::from(format!("{}", s))),
        None => RedisValueRef::NullBulkString,
    }
}
