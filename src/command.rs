use std::{sync::Arc, time::Duration};

use bytes::{Bytes, BytesMut};
use dashmap::DashMap;
use tokio::time::Instant;

use crate::{
    aof::Aof,
    geo,
    rdb::{to_deadline, unix_time_ms},
    server::{EMPTY_RDB, ReplicationState, ServerConfig},
    stream::{self, concrete_form},
    types::RedisValueRef,
    value::{RedisValue, SortedSetData, ValueEntry},
    watch::Watches,
};

type WriteHandler = fn(&[RedisValueRef], &Arc<DashMap<Bytes, ValueEntry>>) -> RedisValueRef;

#[derive(Clone, Copy)]
enum Command {
    Ping,
    Echo,
    Set,
    Get,
    Incr,
    ZAdd,
    ZRank,
    ZRange,
    ZCard,
    ZScore,
    ZRem,
    Info,
    ReplConf,
    Psync,
    Wait,
    Config,
    Keys,
    Type,
    XAdd,
    XRange,
    XRead,
    GeoAdd,
    GeoPos,
    GeoDist,
    GeoSearch,
    Unwatch,
}

impl Command {
    fn parse(name: &[u8]) -> Option<Self> {
        Some(match name.to_ascii_uppercase().as_slice() {
            b"PING" => Self::Ping,
            b"ECHO" => Self::Echo,
            b"SET" => Self::Set,
            b"GET" => Self::Get,
            b"INCR" => Self::Incr,
            b"ZADD" => Self::ZAdd,
            b"ZRANK" => Self::ZRank,
            b"ZRANGE" => Self::ZRange,
            b"ZCARD" => Self::ZCard,
            b"ZSCORE" => Self::ZScore,
            b"ZREM" => Self::ZRem,
            b"INFO" => Self::Info,
            b"REPLCONF" => Self::ReplConf,
            b"PSYNC" => Self::Psync,
            b"WAIT" => Self::Wait,
            b"CONFIG" => Self::Config,
            b"KEYS" => Self::Keys,
            b"TYPE" => Self::Type,
            b"XADD" => Self::XAdd,
            b"XRANGE" => Self::XRange,
            b"XREAD" => Self::XRead,
            b"GEOADD" => Self::GeoAdd,
            b"GEOPOS" => Self::GeoPos,
            b"GEODIST" => Self::GeoDist,
            b"GEOSEARCH" => Self::GeoSearch,
            b"UNWATCH" => Self::Unwatch,
            _ => return None,
        })
    }

    fn write_handler(&self) -> Option<WriteHandler> {
        match self {
            Command::Set => Some(handle_set),
            Command::Incr => Some(handle_incr),
            Command::ZAdd => Some(handle_zadd),
            Command::ZRem => Some(handle_zrem),
            Command::XAdd => Some(stream::handle_xadd),
            Command::GeoAdd => Some(geo::handle_geoadd),
            _ => None,
        }
    }
}

pub fn is_psync_command(value: &RedisValueRef) -> bool {
    matches!(value, RedisValueRef::Array(parts) if matches!(parts.first(), Some(RedisValueRef::BulkString(cmd)) if cmd.eq_ignore_ascii_case(b"PSYNC")))
}

pub enum ConnectionCommand {
    Quit,
    Reset,
    Invalid(RedisValueRef),
}

pub fn connection_command(value: &RedisValueRef) -> Option<ConnectionCommand> {
    let RedisValueRef::Array(parts) = value else {
        return None;
    };
    let Some(RedisValueRef::BulkString(name)) = parts.first() else {
        return None;
    };
    if name.eq_ignore_ascii_case(b"QUIT") {
        Some(ConnectionCommand::Quit)
    } else if name.eq_ignore_ascii_case(b"RESET") {
        Some(match parts.len() {
            1 => ConnectionCommand::Reset,
            _ => ConnectionCommand::Invalid(wrong_arity("reset")),
        })
    } else {
        None
    }
}

pub fn is_getack_command(value: &RedisValueRef) -> bool {
    let RedisValueRef::Array(parts) = value else {
        return false;
    };
    matches!(parts.first(), Some(RedisValueRef::BulkString(cmd)) if cmd.eq_ignore_ascii_case(b"REPLCONF"))
        && matches!(parts.get(1), Some(RedisValueRef::BulkString(sub)) if sub.eq_ignore_ascii_case(b"GETACK"))
}

pub fn parse_ack_offset(value: &RedisValueRef) -> Option<u64> {
    let RedisValueRef::Array(parts) = value else {
        return None;
    };
    let RedisValueRef::BulkString(cmd) = parts.first()? else {
        return None;
    };
    let RedisValueRef::BulkString(sub) = parts.get(1)? else {
        return None;
    };
    if !cmd.eq_ignore_ascii_case(b"REPLCONF") || !sub.eq_ignore_ascii_case(b"ACK") {
        return None;
    }
    let RedisValueRef::BulkString(offset) = parts.get(2)? else {
        return None;
    };
    std::str::from_utf8(offset).ok()?.parse().ok()
}

pub(crate) const WRONGTYPE: &[u8] = b"WRONGTYPE Operation against a key holding wrong type value";

pub(crate) fn invalid_argument() -> RedisValueRef {
    RedisValueRef::ErrorMsg(b"ERR invalid argument".to_vec())
}

fn require_bulk(parts: &[RedisValueRef], i: usize) -> Result<&Bytes, RedisValueRef> {
    match parts.get(i) {
        Some(RedisValueRef::BulkString(b)) => Ok(b),
        _ => Err(invalid_argument()),
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

pub(crate) fn wrong_arity(cmd: &str) -> RedisValueRef {
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

pub fn is_wait_command(value: &RedisValueRef) -> bool {
    matches!(parse_command(value), Ok((Command::Wait, _)))
}

fn parse_command(value: &RedisValueRef) -> Result<(Command, &[RedisValueRef]), RedisValueRef> {
    let RedisValueRef::Array(parts) = value else {
        return Err(RedisValueRef::ErrorMsg(
            b"ERR expected array command".to_vec(),
        ));
    };

    let Some(RedisValueRef::BulkString(cmd)) = parts.first() else {
        return Err(RedisValueRef::ErrorMsg(b"ERR missing command".to_vec()));
    };

    match Command::parse(cmd) {
        Some(command) => Ok((command, parts)),
        None => Err(RedisValueRef::ErrorMsg(b"ERR unknown command".to_vec())),
    }
}

pub async fn handle_command(
    value: &RedisValueRef,
    storage: &Arc<DashMap<Bytes, ValueEntry>>,
    config: &Arc<ServerConfig>,
    replication: &Arc<ReplicationState>,
    aof: &Arc<Aof>,
    watches: &Watches,
) -> RedisValueRef {
    let (command, parts) = match parse_command(value) {
        Ok(parsed) => parsed,
        Err(e) => return e,
    };

    if let Some(handler) = command.write_handler() {
        let execute = || {
            let response = handler(parts, storage);
            if !matches!(response, RedisValueRef::ErrorMsg(_)) {
                watches.touch(parts);
            }
            response
        };
        let response = match aof.apply(parts, execute).await {
            Ok(response) => response,
            Err(rejected) => return rejected,
        };
        replication.propagate(&concrete_form(parts, &response));
        return response;
    }

    match command {
        Command::Wait => handle_wait(parts, replication).await,
        _ => handle_read(command, parts, storage, config, replication),
    }
}

pub(crate) fn execute_queued(
    value: &RedisValueRef,
    storage: &Arc<DashMap<Bytes, ValueEntry>>,
    config: &Arc<ServerConfig>,
    replication: &Arc<ReplicationState>,
    watches: &Watches,
) -> (RedisValueRef, bool) {
    let (command, parts) = match parse_command(value) {
        Ok(parsed) => parsed,
        Err(e) => return (e, false),
    };

    if let Some(handler) = command.write_handler() {
        let response = handler(parts, storage);
        let applied = !matches!(response, RedisValueRef::ErrorMsg(_));
        if applied {
            watches.touch(parts);
        }
        return (response, applied);
    }

    let response = match command {
        Command::Wait => match start_wait(parts, replication) {
            Wait::Done(response) => response,
            Wait::Block { target, .. } => {
                RedisValueRef::Int(replication.acked_count(target) as i64)
            }
        },
        Command::Psync => {
            RedisValueRef::ErrorMsg(b"ERR Command not allowed inside a transaction".to_vec())
        }
        _ => handle_read(command, parts, storage, config, replication),
    };
    (response, false)
}

fn handle_read(
    command: Command,
    parts: &[RedisValueRef],
    storage: &Arc<DashMap<Bytes, ValueEntry>>,
    config: &Arc<ServerConfig>,
    replication: &Arc<ReplicationState>,
) -> RedisValueRef {
    match command {
        Command::Ping => RedisValueRef::SimpleString(Bytes::from_static(b"PONG")),
        Command::Echo => {
            arity!(parts, "echo", == 2);
            match require_bulk(parts, 1) {
                Ok(b) => RedisValueRef::BulkString(b.clone()),
                Err(e) => e,
            }
        }
        Command::Get => handle_get(parts, storage),
        Command::ZRank => handle_zrank(parts, storage),
        Command::ZRange => handle_zrange(parts, storage),
        Command::ZCard => handle_zcard(parts, storage),
        Command::ZScore => handle_zscore(parts, storage),
        Command::Info => handle_info(parts, config, replication),
        Command::ReplConf => handle_replconf(),
        Command::Psync => handle_psync(parts, config, replication),
        Command::Config => handle_config(parts, config),
        Command::Keys => handle_keys(parts, storage),
        Command::Type => handle_type(parts, storage),
        Command::XRange => stream::handle_xrange(parts, storage),
        Command::XRead => stream::handle_xread(parts, storage),
        Command::GeoPos => geo::handle_geopos(parts, storage),
        Command::GeoDist => geo::handle_geodist(parts, storage),
        Command::GeoSearch => geo::handle_geosearch(parts, storage),
        Command::Unwatch => {
            arity!(parts, "unwatch", == 1);
            RedisValueRef::SimpleString(Bytes::from_static(b"OK"))
        }
        Command::Wait => unreachable!("WAIT is dispatched by the caller"),
        Command::Set
        | Command::Incr
        | Command::ZAdd
        | Command::ZRem
        | Command::XAdd
        | Command::GeoAdd => {
            unreachable!("dispatched as writes by the caller")
        }
    }
}

fn handle_config(parts: &[RedisValueRef], config: &ServerConfig) -> RedisValueRef {
    arity!(parts, "config", >= 3);
    let sub = match require_bulk(parts, 1) {
        Ok(sub) => sub,
        Err(e) => return e,
    };
    if !sub.eq_ignore_ascii_case(b"GET") {
        return RedisValueRef::ErrorMsg(
            format!(
                "ERR Unknown CONFIG subcommand or wrong number of arguments for '{}'",
                String::from_utf8_lossy(sub)
            )
            .into_bytes(),
        );
    }

    let mut result = Vec::new();
    for name in &parts[2..] {
        let RedisValueRef::BulkString(name) = name else {
            continue;
        };
        let value = if name.eq_ignore_ascii_case(b"dir") {
            Some(config.dir.as_str())
        } else if name.eq_ignore_ascii_case(b"dbfilename") {
            Some(config.dbfilename.as_str())
        } else if name.eq_ignore_ascii_case(b"appendonly") {
            Some(config.appendonly.as_str())
        } else if name.eq_ignore_ascii_case(b"appenddirname") {
            Some(config.appenddirname.as_str())
        } else if name.eq_ignore_ascii_case(b"appendfilename") {
            Some(config.appendfilename.as_str())
        } else if name.eq_ignore_ascii_case(b"appendfsync") {
            Some(config.appendfsync.as_str())
        } else {
            None
        };
        if let Some(value) = value {
            result.push(RedisValueRef::BulkString(name.clone()));
            result.push(RedisValueRef::BulkString(Bytes::copy_from_slice(
                value.as_bytes(),
            )));
        }
    }
    RedisValueRef::Array(result)
}

fn handle_keys(parts: &[RedisValueRef], storage: &DashMap<Bytes, ValueEntry>) -> RedisValueRef {
    arity!(parts, "keys", == 2);
    let pattern = match require_bulk(parts, 1) {
        Ok(pattern) => pattern,
        Err(e) => return e,
    };
    if pattern.as_ref() != b"*" {
        return RedisValueRef::Array(vec![]);
    }

    // Do not mutate a DashMap while its iterator holds shard locks.
    RedisValueRef::Array(
        storage
            .iter()
            .filter(|entry| !entry.is_expired())
            .map(|entry| RedisValueRef::BulkString(entry.key().clone()))
            .collect(),
    )
}

enum Wait {
    Done(RedisValueRef),
    Block {
        target: u64,
        numreplicas: usize,
        timeout_ms: u64,
    },
}

fn start_wait(parts: &[RedisValueRef], replication: &ReplicationState) -> Wait {
    if parts.len() != 3 {
        return Wait::Done(wrong_arity("wait"));
    }

    let numreplicas = match parse_i64(parts, 1) {
        Ok(v) => v.max(0) as usize,
        Err(e) => return Wait::Done(e),
    };
    let timeout_ms = match parse_i64(parts, 2) {
        Ok(v) => v.max(0) as u64,
        Err(e) => return Wait::Done(e),
    };

    let target = replication.master_offset();
    if target == 0 {
        return Wait::Done(RedisValueRef::Int(replication.replica_count() as i64));
    }

    let acked = replication.acked_count(target);
    if acked >= numreplicas {
        return Wait::Done(RedisValueRef::Int(acked as i64));
    }

    Wait::Block {
        target,
        numreplicas,
        timeout_ms,
    }
}

async fn handle_wait(
    parts: &[RedisValueRef],
    replication: &Arc<ReplicationState>,
) -> RedisValueRef {
    let (target, numreplicas, timeout_ms) = match start_wait(parts, replication) {
        Wait::Done(response) => return response,
        Wait::Block {
            target,
            numreplicas,
            timeout_ms,
        } => (target, numreplicas, timeout_ms),
    };

    let mut acked;
    replication.request_acks();

    // A timeout of 0 means block until numreplicas have acked.
    let deadline = (timeout_ms > 0).then(|| Instant::now() + Duration::from_millis(timeout_ms));
    loop {
        // Constructed before the count check on purpose: a `Notified` records
        // the `notify_waiters` counter at construction, so a notification that
        // lands between here and the await is still observed by the first poll.
        let notified = replication.ack_notified();

        acked = replication.acked_count(target);
        if acked >= numreplicas {
            break;
        }

        let Some(deadline) = deadline else {
            notified.await;
            continue;
        };

        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() || tokio::time::timeout(remaining, notified).await.is_err() {
            acked = replication.acked_count(target);
            break;
        }
    }

    RedisValueRef::Int(acked as i64)
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

fn handle_type(
    parts: &[RedisValueRef],
    storage: &Arc<DashMap<Bytes, ValueEntry>>,
) -> RedisValueRef {
    arity!(parts, "type", == 2);
    let key = match require_bulk(parts, 1) {
        Ok(b) => b,
        Err(e) => return e,
    };

    storage.remove_if(key, |_, entry| entry.is_expired());

    let name: &'static [u8] = match storage.get(key) {
        Some(entry) => match &entry.data {
            RedisValue::String(_) => b"string",
            RedisValue::SortedSet(_) => b"zset",
            RedisValue::Stream(_) => b"stream",
        },
        None => b"none",
    };
    RedisValueRef::SimpleString(Bytes::from_static(name))
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
        let absolute = if option.eq_ignore_ascii_case(b"PX") {
            false
        } else if option.eq_ignore_ascii_case(b"PXAT") {
            true
        } else {
            return RedisValueRef::ErrorMsg(b"ERR syntax error".to_vec());
        };
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
            Ok(ms) if ms > 0 && absolute => {
                match to_deadline(ms as u64, unix_time_ms(), Instant::now()) {
                    Some(deadline) => Some(deadline),
                    None => {
                        storage.remove(&key);
                        return RedisValueRef::SimpleString(Bytes::from_static(b"OK"));
                    }
                }
            }
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

fn handle_incr(
    parts: &[RedisValueRef],
    storage: &Arc<DashMap<Bytes, ValueEntry>>,
) -> RedisValueRef {
    arity!(parts, "incr", == 2);
    let key = match require_bulk(parts, 1) {
        Ok(b) => b.clone(),
        Err(e) => return e,
    };

    storage.remove_if(&key, |_, entry| entry.is_expired());

    let mut entry = storage
        .entry(key)
        .or_insert_with(|| ValueEntry::new(RedisValue::String(Bytes::from_static(b"0")), None));

    let RedisValue::String(data) = &mut entry.data else {
        return RedisValueRef::ErrorMsg(WRONGTYPE.to_vec());
    };
    let Some(current) = parse_strict_i64(data) else {
        return RedisValueRef::ErrorMsg(b"ERR value is not an integer or out of range".to_vec());
    };
    let Some(next) = current.checked_add(1) else {
        return RedisValueRef::ErrorMsg(b"ERR increment or decrement would overflow".to_vec());
    };

    *data = Bytes::from(next.to_string());
    RedisValueRef::Int(next)
}

fn parse_strict_i64(bytes: &[u8]) -> Option<i64> {
    let digits = bytes.strip_prefix(b"-").unwrap_or(bytes);
    match digits {
        [] => return None,
        [b'0'] if digits.len() == bytes.len() => return Some(0),
        [b'1'..=b'9', rest @ ..] if rest.iter().all(u8::is_ascii_digit) => {}
        _ => return None,
    }
    std::str::from_utf8(bytes).ok()?.parse().ok()
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

pub(crate) fn parse_score(s: &str) -> Result<f64, RedisValueRef> {
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

fn handle_info(
    _parts: &[RedisValueRef],
    config: &Arc<ServerConfig>,
    replication: &Arc<ReplicationState>,
) -> RedisValueRef {
    let body = format!(
        "# Replication\r\n\
         role:{}\r\n\
         master_replid:{}\r\n\
         master_repl_offset:{}\r\n",
        config.role.name(),
        config.replid,
        replication.master_offset(),
    );
    RedisValueRef::BulkString(Bytes::from(body))
}

fn handle_replconf() -> RedisValueRef {
    RedisValueRef::SimpleString(Bytes::from_static(b"OK"))
}

fn handle_psync(
    _parts: &[RedisValueRef],
    config: &Arc<ServerConfig>,
    replication: &Arc<ReplicationState>,
) -> RedisValueRef {
    let mut out = BytesMut::new();

    out.extend_from_slice(
        format!(
            "+FULLRESYNC {} {}\r\n",
            config.replid,
            replication.master_offset()
        )
        .as_bytes(),
    );

    out.extend_from_slice(format!("${}\r\n", EMPTY_RDB.len()).as_bytes());
    out.extend_from_slice(EMPTY_RDB);

    RedisValueRef::Raw(out.freeze())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bulk(value: &'static [u8]) -> RedisValueRef {
        RedisValueRef::BulkString(Bytes::from_static(value))
    }

    #[test]
    fn keys_lists_live_strings_and_omits_expired_entries() {
        let storage = DashMap::new();
        storage.insert(
            Bytes::from_static(b"live"),
            ValueEntry::new(RedisValue::String(Bytes::from_static(b"1")), None),
        );
        storage.insert(
            Bytes::from_static(b"expired"),
            ValueEntry::new(
                RedisValue::String(Bytes::from_static(b"2")),
                Some(Instant::now() - Duration::from_millis(1)),
            ),
        );
        let parts = vec![bulk(b"KEYS"), bulk(b"*")];
        let RedisValueRef::Array(keys) = handle_keys(&parts, &storage) else {
            panic!("expected array")
        };
        assert_eq!(keys.len(), 1);
        assert!(matches!(&keys[0], RedisValueRef::BulkString(key) if key == b"live".as_slice()));

        let parts = vec![bulk(b"KEYS"), bulk(b"l*")];
        assert!(
            matches!(handle_keys(&parts, &storage), RedisValueRef::Array(keys) if keys.is_empty())
        );
    }

    #[test]
    fn config_get_returns_known_parameters() {
        let config = ServerConfig {
            port: 6379,
            role: crate::server::Role::Master,
            replid: String::new(),
            dir: "/tmp/data".into(),
            dbfilename: "dump.rdb".into(),
            appendonly: "no".into(),
            appenddirname: "appendonlydir".into(),
            appendfilename: "appendonly.aof".into(),
            appendfsync: "everysec".into(),
        };
        let parts = vec![
            bulk(b"CONFIG"),
            bulk(b"GET"),
            bulk(b"dir"),
            bulk(b"unknown"),
            bulk(b"appendfsync"),
        ];
        assert!(
            matches!(handle_config(&parts, &config), RedisValueRef::Array(values) if values.len() == 4)
        );
    }

    async fn run(storage: &Arc<DashMap<Bytes, ValueEntry>>, args: &[&str]) -> RedisValueRef {
        let value = RedisValueRef::Array(
            args.iter()
                .map(|a| RedisValueRef::BulkString(Bytes::copy_from_slice(a.as_bytes())))
                .collect(),
        );
        let config = Arc::new(ServerConfig {
            port: 0,
            role: crate::server::Role::Master,
            replid: String::new(),
            dir: String::new(),
            dbfilename: String::new(),
            appendonly: "no".into(),
            appenddirname: String::new(),
            appendfilename: String::new(),
            appendfsync: "everysec".into(),
        });
        let replication = Arc::new(ReplicationState::new());
        handle_command(
            &value,
            storage,
            &config,
            &replication,
            &Arc::new(Aof::disabled()),
            &Watches::new(),
        )
        .await
    }

    fn reply_bulk(value: &str) -> RedisValueRef {
        RedisValueRef::BulkString(Bytes::copy_from_slice(value.as_bytes()))
    }

    fn reply_err(value: &str) -> RedisValueRef {
        RedisValueRef::ErrorMsg(value.as_bytes().to_vec())
    }

    #[tokio::test]
    async fn xadd_stores_explicit_and_generated_ids() {
        let storage = Arc::new(DashMap::new());
        assert!(
            run(&storage, &["XADD", "s", "1526919030473-0", "a", "1"]).await
                == reply_bulk("1526919030473-0")
        );
        assert!(
            run(&storage, &["XADD", "s", "1526919030474-*", "a", "1"]).await
                == reply_bulk("1526919030474-0")
        );
        assert!(
            run(&storage, &["XADD", "s", "1526919030474-*", "a", "1"]).await
                == reply_bulk("1526919030474-1")
        );
        let RedisValueRef::BulkString(auto) = run(&storage, &["XADD", "s", "*", "a", "1"]).await
        else {
            panic!("expected an ID")
        };
        let ms: u64 = std::str::from_utf8(&auto)
            .unwrap()
            .split('-')
            .next()
            .unwrap()
            .parse()
            .unwrap();
        assert!(ms >= unix_time_ms() - 1000);
        assert!(run(&storage, &["XADD", "z", "0-*", "a", "1"]).await == reply_bulk("0-1"));
        assert!(
            run(&storage, &["TYPE", "s"]).await
                == RedisValueRef::SimpleString(Bytes::from_static(b"stream"))
        );
    }

    #[tokio::test]
    async fn xadd_rejects_bad_ids_and_arguments() {
        let storage = Arc::new(DashMap::new());
        let not_greater =
            "ERR The ID specified in XADD is equal or smaller than the target stream top item";
        assert!(
            run(&storage, &["XADD", "s", "0-0", "a", "1"]).await
                == reply_err("ERR The ID specified in XADD must be greater than 0-0")
        );
        assert!(
            run(&storage, &["XADD", "s", "x-1", "a", "1"]).await
                == reply_err("ERR Invalid stream ID specified as stream command argument")
        );
        assert!(
            run(&storage, &["XADD", "s", "1-1", "a"]).await
                == reply_err("ERR wrong number of arguments for 'xadd' command")
        );
        assert!(
            run(&storage, &["XADD", "s", "1-1"]).await
                == reply_err("ERR wrong number of arguments for 'xadd' command")
        );
        assert!(
            storage.is_empty(),
            "a failed XADD must not create the stream"
        );

        run(&storage, &["XADD", "s", "5-5", "a", "1"]).await;
        assert!(run(&storage, &["XADD", "s", "5-5", "a", "1"]).await == reply_err(not_greater));
        assert!(run(&storage, &["XADD", "s", "4-9", "a", "1"]).await == reply_err(not_greater));
        assert!(run(&storage, &["XADD", "s", "4-*", "a", "1"]).await == reply_err(not_greater));
    }

    #[tokio::test]
    async fn xadd_and_stream_reads_check_types() {
        let storage = Arc::new(DashMap::new());
        let wrongtype = reply_err("WRONGTYPE Operation against a key holding wrong type value");
        run(&storage, &["SET", "str", "v"]).await;
        assert!(run(&storage, &["XADD", "str", "*", "a", "1"]).await == wrongtype);
        assert!(run(&storage, &["XRANGE", "str", "-", "+"]).await == wrongtype);
        assert!(run(&storage, &["XREAD", "STREAMS", "str", "0"]).await == wrongtype);
        run(&storage, &["XADD", "s", "1-1", "a", "1"]).await;
        assert!(run(&storage, &["GET", "s"]).await == wrongtype);
    }

    #[tokio::test]
    async fn xrange_is_inclusive_and_accepts_short_ids() {
        let storage = Arc::new(DashMap::new());
        for id in ["1-0", "1-1", "2-0", "3-0"] {
            run(&storage, &["XADD", "s", id, "f", id]).await;
        }
        let ids = |reply: RedisValueRef| -> Vec<String> {
            let RedisValueRef::Array(entries) = reply else {
                panic!("expected array")
            };
            entries
                .into_iter()
                .map(|entry| match entry {
                    RedisValueRef::Array(pair) => match &pair[0] {
                        RedisValueRef::BulkString(id) => String::from_utf8(id.to_vec()).unwrap(),
                        _ => panic!("expected id"),
                    },
                    _ => panic!("expected entry"),
                })
                .collect()
        };
        assert_eq!(
            ids(run(&storage, &["XRANGE", "s", "1-1", "2-0"]).await),
            ["1-1", "2-0"]
        );
        assert_eq!(
            ids(run(&storage, &["XRANGE", "s", "1", "2"]).await),
            ["1-0", "1-1", "2-0"]
        );
        assert_eq!(
            ids(run(&storage, &["XRANGE", "s", "-", "+"]).await).len(),
            4
        );
        assert_eq!(
            ids(run(&storage, &["XRANGE", "s", "-", "1"]).await),
            ["1-0", "1-1"]
        );
        assert_eq!(
            ids(run(&storage, &["XRANGE", "s", "3", "+"]).await),
            ["3-0"]
        );
        assert!(ids(run(&storage, &["XRANGE", "missing", "-", "+"]).await).is_empty());
        assert!(
            run(&storage, &["XRANGE", "s", "1-0", "1-0"]).await
                == RedisValueRef::Array(vec![RedisValueRef::Array(vec![
                    reply_bulk("1-0"),
                    RedisValueRef::Array(vec![reply_bulk("f"), reply_bulk("1-0")]),
                ])])
        );
    }

    #[tokio::test]
    async fn failed_writes_are_not_propagated() {
        let storage = Arc::new(DashMap::new());
        storage.insert(
            Bytes::from_static(b"k"),
            ValueEntry::new(RedisValue::String(Bytes::from_static(b"v")), None),
        );
        let config = Arc::new(ServerConfig {
            port: 0,
            role: crate::server::Role::Master,
            replid: String::new(),
            dir: String::new(),
            dbfilename: String::new(),
            appendonly: "no".into(),
            appenddirname: String::new(),
            appendfilename: String::new(),
            appendfsync: "everysec".into(),
        });
        let replication = Arc::new(ReplicationState::new());
        let aof = Arc::new(Aof::disabled());
        let zadd = RedisValueRef::Array(vec![bulk(b"ZADD"), bulk(b"k"), bulk(b"1"), bulk(b"m")]);

        let response = handle_command(
            &zadd,
            &storage,
            &config,
            &replication,
            &aof,
            &Watches::new(),
        )
        .await;
        assert!(matches!(response, RedisValueRef::ErrorMsg(_)));
        assert_eq!(replication.master_offset(), 0);
    }

    #[tokio::test]
    async fn geo_commands_follow_the_redis_examples() {
        let storage = Arc::new(DashMap::new());
        let add = [
            "GEOADD",
            "Sicily",
            "13.361389",
            "38.115556",
            "Palermo",
            "15.087269",
            "37.502669",
            "Catania",
        ];
        assert!(run(&storage, &add).await == RedisValueRef::Int(2));
        assert!(run(&storage, &add).await == RedisValueRef::Int(0));
        assert!(
            run(&storage, &["GEODIST", "Sicily", "Palermo", "Catania"]).await
                == reply_bulk("166274.1516")
        );
        assert!(
            run(&storage, &["GEODIST", "Sicily", "Palermo", "Catania", "KM"]).await
                == reply_bulk("166.2742")
        );
        assert!(
            run(&storage, &["GEOPOS", "Sicily", "Palermo", "nope"]).await
                == RedisValueRef::Array(vec![
                    RedisValueRef::Array(vec![
                        reply_bulk("13.361389338970184"),
                        reply_bulk("38.1155563954963"),
                    ]),
                    RedisValueRef::NullArray,
                ])
        );
        assert!(
            run(&storage, &["ZSCORE", "Sicily", "Palermo"]).await == reply_bulk("3479099956230698")
        );
        assert!(
            run(&storage, &["TYPE", "Sicily"]).await
                == RedisValueRef::SimpleString(Bytes::from_static(b"zset"))
        );

        let search = |order: &'static str| {
            let storage = storage.clone();
            async move {
                let mut args = vec![
                    "GEOSEARCH",
                    "Sicily",
                    "FROMLONLAT",
                    "15",
                    "37",
                    "BYRADIUS",
                    "200",
                    "km",
                ];
                if !order.is_empty() {
                    args.push(order);
                }
                run(&storage, &args).await
            }
        };
        let names = |v: &[&str]| RedisValueRef::Array(v.iter().map(|m| reply_bulk(m)).collect());
        assert!(search("").await == names(&["Palermo", "Catania"]));
        assert!(search("ASC").await == names(&["Catania", "Palermo"]));
        assert!(search("desc").await == names(&["Palermo", "Catania"]));
        assert!(
            run(
                &storage,
                &[
                    "GEOSEARCH",
                    "Sicily",
                    "FROMLONLAT",
                    "15",
                    "37",
                    "BYRADIUS",
                    "100",
                    "km"
                ]
            )
            .await
                == names(&["Catania"])
        );
        assert!(
            run(
                &storage,
                &[
                    "GEOSEARCH",
                    "Sicily",
                    "FROMLONLAT",
                    "15",
                    "37",
                    "BYRADIUS",
                    "200",
                    "km",
                    "FROMLONLAT",
                    "0",
                    "0"
                ]
            )
            .await
                == names(&[])
        );
    }

    #[tokio::test]
    async fn geo_missing_keys_and_members() {
        let storage = Arc::new(DashMap::new());
        assert!(
            run(&storage, &["GEOPOS", "nokey", "a", "b"]).await
                == RedisValueRef::Array(vec![RedisValueRef::NullArray, RedisValueRef::NullArray])
        );
        assert!(
            run(&storage, &["GEODIST", "nokey", "a", "b"]).await == RedisValueRef::NullBulkString
        );
        assert!(
            run(
                &storage,
                &[
                    "GEOSEARCH",
                    "nokey",
                    "FROMLONLAT",
                    "0",
                    "0",
                    "BYRADIUS",
                    "1",
                    "m"
                ]
            )
            .await
                == RedisValueRef::Array(vec![])
        );
        run(&storage, &["GEOADD", "g", "1", "2", "a"]).await;
        assert!(run(&storage, &["GEODIST", "g", "a", "b"]).await == RedisValueRef::NullBulkString);
        assert!(run(&storage, &["GEODIST", "g", "a", "a"]).await == reply_bulk("0.0000"));
    }

    #[tokio::test]
    async fn geo_errors_match_redis() {
        let storage = Arc::new(DashMap::new());
        let cases: &[(&[&str], &str)] = &[
            (
                &["GEOADD", "g", "1", "2"],
                "ERR wrong number of arguments for 'geoadd' command",
            ),
            (&["GEOADD", "g", "1", "2", "3", "4"], "ERR syntax error"),
            (
                &["GEOADD", "g", "200", "100", "x"],
                "ERR invalid longitude,latitude pair 200.000000,100.000000",
            ),
            (
                &["GEOADD", "g", "abc", "1", "x"],
                "ERR value is not a valid float",
            ),
            (
                &["GEOPOS"],
                "ERR wrong number of arguments for 'geopos' command",
            ),
            (
                &["GEODIST", "g", "a"],
                "ERR wrong number of arguments for 'geodist' command",
            ),
            (
                &["GEODIST", "g", "a", "b", "yd"],
                "ERR unsupported unit provided. please use M, KM, FT, MI",
            ),
            (&["GEODIST", "g", "a", "b", "km", "x"], "ERR syntax error"),
            (
                &["GEOSEARCH", "g", "FROMLONLAT", "0", "0"],
                "ERR wrong number of arguments for 'geosearch' command",
            ),
            (
                &[
                    "GEOSEARCH",
                    "g",
                    "FROMLONLAT",
                    "0",
                    "0",
                    "BYRADIUS",
                    "abc",
                    "m",
                ],
                "ERR need numeric radius",
            ),
            (
                &[
                    "GEOSEARCH",
                    "g",
                    "FROMLONLAT",
                    "0",
                    "0",
                    "BYRADIUS",
                    "-1",
                    "m",
                ],
                "ERR radius cannot be negative",
            ),
            (
                &[
                    "GEOSEARCH",
                    "g",
                    "FROMLONLAT",
                    "0",
                    "0",
                    "BYRADIUS",
                    "1",
                    "yd",
                ],
                "ERR unsupported unit provided. please use M, KM, FT, MI",
            ),
            (
                &[
                    "GEOSEARCH",
                    "g",
                    "FROMLONLAT",
                    "200",
                    "0",
                    "BYRADIUS",
                    "1",
                    "m",
                ],
                "ERR invalid longitude,latitude pair 200.000000,0.000000",
            ),
            (
                &["GEOSEARCH", "g", "BYRADIUS", "1", "m", "ASC", "ASC"],
                "ERR exactly one of FROMMEMBER or FROMLONLAT can be specified for GEOSEARCH",
            ),
            (
                &["geosearch", "g", "FROMLONLAT", "0", "0", "ASC", "ASC"],
                "ERR exactly one of BYRADIUS and BYBOX can be specified for geosearch",
            ),
            (
                &[
                    "GEOSEARCH",
                    "g",
                    "FROMLONLAT",
                    "0",
                    "0",
                    "BYRADIUS",
                    "1",
                    "m",
                    "WITHDIST",
                ],
                "ERR syntax error",
            ),
            (
                &["GEOSEARCH", "g", "FROMMEMBER", "a", "BYRADIUS", "1", "m"],
                "ERR syntax error",
            ),
            (
                &[
                    "GEOSEARCH",
                    "g",
                    "FROMLONLAT",
                    "0",
                    "0",
                    "BYBOX",
                    "1",
                    "1",
                    "m",
                ],
                "ERR syntax error",
            ),
            (
                &[
                    "GEOSEARCH",
                    "g",
                    "FROMLONLAT",
                    "0",
                    "0",
                    "BYRADIUS",
                    "1",
                    "m",
                    "COUNT",
                    "1",
                ],
                "ERR syntax error",
            ),
        ];
        for (args, want) in cases {
            assert!(run(&storage, args).await == reply_err(want), "{args:?}");
        }

        run(&storage, &["SET", "s", "v"]).await;
        let wrongtype = reply_err("WRONGTYPE Operation against a key holding wrong type value");
        assert!(run(&storage, &["GEOADD", "s", "1", "2", "a"]).await == wrongtype);
        assert!(run(&storage, &["GEOPOS", "s", "a"]).await == wrongtype);
        assert!(run(&storage, &["GEODIST", "s", "a", "b"]).await == wrongtype);
        assert!(
            run(
                &storage,
                &[
                    "GEOSEARCH",
                    "s",
                    "FROMLONLAT",
                    "0",
                    "0",
                    "BYRADIUS",
                    "1",
                    "zz"
                ]
            )
            .await
                == wrongtype
        );
    }

    #[tokio::test]
    async fn geoadd_adds_nothing_when_a_later_pair_is_invalid() {
        let storage = Arc::new(DashMap::new());
        let reply = run(
            &storage,
            &["GEOADD", "g", "1", "2", "a", "3", "4", "b", "181", "0", "c"],
        )
        .await;
        assert!(reply == reply_err("ERR invalid longitude,latitude pair 181.000000,0.000000"));
        assert!(storage.get(&Bytes::from_static(b"g")).is_none());
    }
}
