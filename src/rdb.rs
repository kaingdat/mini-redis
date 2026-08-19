use std::{
    path::Path,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use dashmap::DashMap;
use nom::{
    IResult, Parser,
    bytes::complete::{tag, take},
    error::{ErrorKind, ParseError},
    number::complete::{be_u32, be_u64, le_i16, le_i32, le_u32, le_u64, u8},
};
use tokio::time::Instant;

use crate::value::{RedisValue, ValueEntry};

#[derive(Debug, thiserror::Error)]
pub enum RdbError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("bad magic: expected REDIS")]
    BadMagic,
    #[error("LZF-compressed strings are not supported")]
    Lzf,
    #[error("unknown string encoding {0}")]
    UnknownStringEncoding(u8),
    #[error("unsupported value type {0:#04x}")]
    UnsupportedValueType(u8),
    #[error("truncated or malformed input")]
    Malformed,
}

impl ParseError<&[u8]> for RdbError {
    fn from_error_kind(_: &[u8], _: ErrorKind) -> Self {
        Self::Malformed
    }

    fn append(_: &[u8], _: ErrorKind, other: Self) -> Self {
        other
    }
}

#[derive(Debug, PartialEq)]
enum Length {
    Len(u64),
    Encoded(u8),
}

pub enum Record {
    Aux(Bytes, Bytes),
    SelectDb(u64),
    ResizeDb {
        keys: u64,
        expires: u64,
    },
    Entry {
        key: Bytes,
        value: RedisValue,
        expires_at_ms: Option<u64>,
    },
    Eof,
}

fn failure(error: RdbError) -> nom::Err<RdbError> {
    nom::Err::Failure(error)
}

fn length(input: &[u8]) -> IResult<&[u8], Length, RdbError> {
    let (input, first) = u8(input)?;
    match first >> 6 {
        0 => Ok((input, Length::Len(u64::from(first & 0x3f)))),
        1 => {
            let (input, second) = u8(input)?;
            Ok((
                input,
                Length::Len(u64::from(
                    (u16::from(first & 0x3f) << 8) | u16::from(second),
                )),
            ))
        }
        2 if first == 0x80 => {
            let (input, value) = be_u32(input)?;
            Ok((input, Length::Len(u64::from(value))))
        }
        2 if first == 0x81 => {
            let (input, value) = be_u64(input)?;
            Ok((input, Length::Len(value)))
        }
        2 => Err(failure(RdbError::Malformed)),
        _ => Ok((input, Length::Encoded(first & 0x3f))),
    }
}

fn string(input: &[u8]) -> IResult<&[u8], Bytes, RdbError> {
    let (input, len) = length(input)?;
    match len {
        Length::Len(len) => {
            let len = usize::try_from(len).map_err(|_| failure(RdbError::Malformed))?;
            let (input, value) = take(len).parse(input)?;
            Ok((input, Bytes::copy_from_slice(value)))
        }
        Length::Encoded(0) => {
            let (input, value) = u8(input)?;
            Ok((input, Bytes::from((value as i8).to_string())))
        }
        Length::Encoded(1) => {
            let (input, value) = le_i16(input)?;
            Ok((input, Bytes::from(value.to_string())))
        }
        Length::Encoded(2) => {
            let (input, value) = le_i32(input)?;
            Ok((input, Bytes::from(value.to_string())))
        }
        Length::Encoded(3) => Err(failure(RdbError::Lzf)),
        Length::Encoded(other) => Err(failure(RdbError::UnknownStringEncoding(other))),
    }
}

fn record(input: &[u8]) -> IResult<&[u8], Record, RdbError> {
    let (_, opcode) = u8(input)?;
    match opcode {
        0xfa => {
            let (input, _) = u8(input)?;
            let (input, key) = string(input)?;
            let (input, value) = string(input)?;
            Ok((input, Record::Aux(key, value)))
        }
        0xfb => {
            let (input, _) = u8(input)?;
            let (input, keys) = length(input)?;
            let (input, expires) = length(input)?;
            match (keys, expires) {
                (Length::Len(keys), Length::Len(expires)) => {
                    Ok((input, Record::ResizeDb { keys, expires }))
                }
                _ => Err(failure(RdbError::Malformed)),
            }
        }
        0xfe => {
            let (input, _) = u8(input)?;
            let (input, db) = length(input)?;
            match db {
                Length::Len(db) => Ok((input, Record::SelectDb(db))),
                _ => Err(failure(RdbError::Malformed)),
            }
        }
        0xff => Ok((&input[1..], Record::Eof)),
        _ => entry(input),
    }
}

fn entry(mut input: &[u8]) -> IResult<&[u8], Record, RdbError> {
    // Expiry, LRU idle time and LFU frequency are per-key prefixes that precede
    // the value type, and rdbLoadRio accepts them in any order and combination:
    // a dump written under `maxmemory-policy allkeys-lru` emits `FC <expiry>`
    // followed by `F8 <idle>` on the same key. Consume them in a loop rather
    // than assuming a fixed sequence. Each arm eats at least the opcode byte,
    // so the loop always makes progress.
    let mut expires_at_ms = None;
    loop {
        match input.first().copied() {
            // EXPIRETIME_MS: absolute milliseconds, little-endian.
            Some(0xfc) => {
                let (rest, _) = u8(input)?;
                let (rest, millis) = le_u64(rest)?;
                input = rest;
                expires_at_ms = Some(millis);
            }
            // EXPIRETIME: absolute seconds. u32::MAX * 1000 cannot overflow u64.
            Some(0xfd) => {
                let (rest, _) = u8(input)?;
                let (rest, seconds) = le_u32(rest)?;
                input = rest;
                expires_at_ms = Some(u64::from(seconds) * 1000);
            }
            // IDLE: length-encoded seconds, written only under an LRU policy.
            Some(0xf8) => {
                let (rest, _) = u8(input)?;
                let (rest, _) = length(rest)?;
                input = rest;
            }
            // FREQ: one byte, written only under an LFU policy.
            Some(0xf9) => {
                let (rest, _) = u8(input)?;
                let (rest, _) = u8(rest)?;
                input = rest;
            }
            _ => break,
        }
    }
    let (input, value_type) = u8(input)?;
    if value_type != 0 {
        return Err(failure(RdbError::UnsupportedValueType(value_type)));
    }
    let (input, key) = string(input)?;
    let (input, value) = string(input)?;
    Ok((
        input,
        Record::Entry {
            key,
            value: RedisValue::String(value),
            expires_at_ms,
        },
    ))
}

fn header(input: &[u8]) -> IResult<&[u8], u32, RdbError> {
    let (input, _) = tag::<_, _, RdbError>(&b"REDIS"[..])
        .parse(input)
        .map_err(|_| failure(RdbError::BadMagic))?;
    let (input, version) = take(4usize).parse(input)?;
    let version = std::str::from_utf8(version)
        .ok()
        .and_then(|value| value.parse::<u32>().ok())
        .ok_or_else(|| failure(RdbError::Malformed))?;
    // Deliberately no upper bound: nothing in the framing we read is
    // version-specific, and a feature this parser cannot handle already surfaces
    // per-record as UnsupportedValueType / UnknownStringEncoding, which is more
    // precise than rejecting a whole file on its version digits. Redis 8 writes
    // version 13, so a ceiling here discards every dump a current server makes.
    Ok((input, version))
}

fn into_error(error: nom::Err<RdbError>) -> RdbError {
    match error {
        nom::Err::Error(error) | nom::Err::Failure(error) => error,
        nom::Err::Incomplete(_) => RdbError::Malformed,
    }
}

pub fn parse(bytes: &[u8]) -> (Vec<Record>, Option<RdbError>) {
    let mut rest = match header(bytes) {
        Ok((rest, _)) => rest,
        Err(error) => return (vec![], Some(into_error(error))),
    };
    let mut records = Vec::new();
    loop {
        match record(rest) {
            // The optional eight-byte CRC after EOF is deliberately not verified.
            Ok((_, Record::Eof)) => return (records, None),
            Ok((next, value)) => {
                records.push(value);
                rest = next;
            }
            Err(error) => return (records, Some(into_error(error))),
        }
    }
}

/// `now` and `now_ms` must be sampled once for the whole load and describe the
/// same instant on the monotonic and wall clocks; taking a fresh `Instant::now()`
/// per key would stretch every deadline by however long the load has been running.
fn to_deadline(expires_at_ms: u64, now_ms: u64, now: Instant) -> Option<Instant> {
    (expires_at_ms > now_ms)
        .then(|| now.checked_add(Duration::from_millis(expires_at_ms - now_ms)))
        .flatten()
}

pub fn load_into(path: &Path, storage: &DashMap<Bytes, ValueEntry>) {
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return,
        Err(error) => {
            eprintln!("warning: failed to read RDB {}: {error}", path.display());
            return;
        }
    };
    let now = Instant::now();
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64;
    let (records, error) = parse(&bytes);
    for record in records {
        if let Record::Entry {
            key,
            value,
            expires_at_ms,
        } = record
        {
            let deadline = match expires_at_ms {
                Some(expiry) => match to_deadline(expiry, now_ms, now) {
                    Some(deadline) => Some(deadline),
                    None => continue,
                },
                None => None,
            };
            storage.insert(key, ValueEntry::new(value, deadline));
        }
    }
    if let Some(error) = error {
        eprintln!(
            "warning: failed to fully parse RDB {}: {error}",
            path.display()
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn file(body: &[u8]) -> Vec<u8> {
        let mut bytes = b"REDIS0011".to_vec();
        bytes.extend_from_slice(body);
        bytes
    }

    #[test]
    fn parses_all_length_forms() {
        assert_eq!(length(&[0x3f]).unwrap().1, Length::Len(63));
        assert_eq!(length(&[0x40, 0x01]).unwrap().1, Length::Len(1));
        assert_eq!(length(&[0x80, 0, 0, 1, 0]).unwrap().1, Length::Len(256));
        assert_eq!(
            length(&[0x81, 0, 0, 0, 0, 0, 0, 1, 0]).unwrap().1,
            Length::Len(256)
        );
    }

    #[test]
    fn parses_signed_integer_and_rejects_lzf() {
        assert_eq!(string(&[0xc0, 0xff]).unwrap().1, Bytes::from_static(b"-1"));
        assert!(matches!(
            string(&[0xc3]),
            Err(nom::Err::Failure(RdbError::Lzf))
        ));
    }

    #[test]
    fn rejects_unsupported_value_type() {
        let (_, error) = parse(&file(&[0x01, 0xff]));
        assert!(matches!(error, Some(RdbError::UnsupportedValueType(1))));
    }

    #[test]
    fn preserves_records_before_truncated_input() {
        let bytes = file(&[0xfe, 0x00, 0x00, 0x01, b'a', 0x01, b'1', 0x00]);
        let (records, error) = parse(&bytes);
        assert_eq!(records.len(), 2);
        assert!(error.is_some());
    }

    #[test]
    fn accepts_headers_newer_than_version_eleven() {
        // Redis 8 writes REDIS0013; a version ceiling here silently discarded
        // every key in a dump produced by a current server.
        for version in [b"0003", b"0011", b"0013", b"0099"] {
            let mut bytes = b"REDIS".to_vec();
            bytes.extend_from_slice(version);
            bytes.extend_from_slice(&[0x00, 0x01, b'a', 0x01, b'1', 0xff]);
            let (records, error) = parse(&bytes);
            assert!(error.is_none(), "{version:?}: {error:?}");
            assert_eq!(records.len(), 1);
        }
    }

    #[test]
    fn rejects_a_non_numeric_version() {
        let (_, error) = parse(b"REDISxxxx\xff");
        assert!(matches!(error, Some(RdbError::Malformed)));
    }

    #[test]
    fn bundled_empty_rdb_parses_cleanly() {
        let (records, error) = parse(include_bytes!("../assets/empty.rdb"));
        assert!(error.is_none(), "{error:?}");
        assert!(
            !records
                .iter()
                .any(|record| matches!(record, Record::Entry { .. }))
        );
    }

    #[test]
    fn load_skips_expired_entries() {
        let future = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
            + 60_000;
        let mut body = vec![0xfe, 0x00, 0xfb, 0x03, 0x02];
        body.extend_from_slice(&[0x00, 0x01, b'a', 0x01, b'1']);
        body.push(0xfc);
        body.extend_from_slice(&future.to_le_bytes());
        body.extend_from_slice(&[0x00, 0x01, b'b', 0x01, b'2']);
        body.push(0xfd);
        body.extend_from_slice(&1_u32.to_le_bytes());
        body.extend_from_slice(&[0x00, 0x01, b'c', 0x01, b'3', 0xff]);

        let path = std::env::temp_dir().join(format!("rdb-test-{}.rdb", std::process::id()));
        std::fs::write(&path, file(&body)).unwrap();
        let storage = DashMap::new();
        load_into(&path, &storage);
        std::fs::remove_file(path).unwrap();

        assert!(storage.contains_key(b"a".as_slice()));
        assert!(storage.get(b"b".as_slice()).unwrap().expires_at.is_some());
        assert!(!storage.contains_key(b"c".as_slice()));
    }

    #[test]
    fn parses_lru_and_lfu_key_prefixes() {
        // Bytes lifted from real dumps written by redis-server 8.6.2 under
        // `--maxmemory-policy allkeys-lru` and `allkeys-lfu`. The LRU case puts
        // `F8 <idle>` *after* the `FC <expiry>` of the same key.
        let lfu = file(&[
            0xfe, 0x00, 0xfb, 0x01, 0x00, 0xf9, 0x05, 0x00, 0x01, b'k', 0x01, b'v', 0xff,
        ]);
        let storage = DashMap::new();
        for record in parse(&lfu).0 {
            if let Record::Entry { key, .. } = record {
                storage.insert(key, ValueEntry::new(RedisValue::String(Bytes::new()), None));
            }
        }
        assert!(parse(&lfu).1.is_none());
        assert!(storage.contains_key(b"k".as_slice()));

        let mut lru = vec![0xfe, 0x00, 0xfb, 0x02, 0x01, 0xf8, 0x00, 0x00, 0x01, b'k', 0x01, b'v'];
        lru.push(0xfc);
        lru.extend_from_slice(&0x0000_01a0_17fd_81cf_u64.to_le_bytes());
        lru.extend_from_slice(&[0xf8, 0x00, 0x00, 0x07]);
        lru.extend_from_slice(b"withttl");
        lru.extend_from_slice(&[0x01, b'v', 0xff]);
        let (records, error) = parse(&file(&lru));
        assert!(error.is_none(), "{error:?}");
        let keys: Vec<_> = records
            .iter()
            .filter_map(|r| match r {
                Record::Entry { key, expires_at_ms, .. } => Some((key.clone(), *expires_at_ms)),
                _ => None,
            })
            .collect();
        assert_eq!(keys.len(), 2);
        assert_eq!(keys[0], (Bytes::from_static(b"k"), None));
        assert_eq!(keys[1].0, Bytes::from_static(b"withttl"));
        assert!(keys[1].1.is_some());
    }

    #[test]
    fn structured_random_inputs_never_panic() {
        // Deterministic LCG using the MMIX multiplier; wrapping is modulo 2^64.
        // The fixed seed makes failures reproducible.
        let mut state = 0x1234_5678_u64;
        for len in 0..=256 {
            for _ in 0..32 {
                let mut body = vec![0; len];
                for byte in &mut body {
                    state = state
                        .wrapping_mul(6_364_136_223_846_793_005)
                        .wrapping_add(1);
                    *byte = (state >> 32) as u8;
                }
                // Behind a valid header, so the random bytes actually reach
                // `record`/`entry`/`length`/`string`. Fed raw they would bounce
                // off `header` on the first five bytes essentially every time,
                // leaving the parsers this test exists to cover untouched.
                let _ = parse(&file(&body));
                let _ = parse(&body);
            }
        }
    }
}
