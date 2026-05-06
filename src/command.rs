use std::{sync::Arc, time::Duration};

use bytes::Bytes;
use codecrafters_redis::types::RedisValueRef;
use dashmap::DashMap;
use tokio::time::Instant;

pub struct ValueEntry {
    data: Bytes,
    expires_at: Option<Instant>,
}

impl ValueEntry {
    pub fn new(data: Bytes, expires_at: Option<Instant>) -> Self {
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
        b"SET" => {
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

            storage.insert(key, ValueEntry::new(value, expires_at));

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
                Some(entry) => {
                    if entry.is_expired() {
                        drop(entry);
                        storage.remove(key);
                        RedisValueRef::NullBulkString
                    } else {
                        RedisValueRef::BulkString(entry.data.clone())
                    }
                }
                None => RedisValueRef::NullBulkString,
            }
        }
        _ => RedisValueRef::ErrorMsg(b"ERR unknown command".to_vec()),
    }
}
