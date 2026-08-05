use std::io;

use bytes::{Buf, Bytes, BytesMut};
use memchr::memchr;
use tokio_util::codec::{Decoder, Encoder};

use crate::types::{NULL_ARRAY, NULL_BULK_STRING, RedisValueRef};

pub const MAX_MULTIBULK_LEN: i64 = 1_024 * 1_024;
pub const MAX_BULK_LEN: i64 = 512 * 1_024 * 1_024;

// Leave room for the RESP prefix and terminators around a maximum-sized bulk
// string while still placing a hard ceiling on incomplete frames.
const MAX_BUFFERED_FRAME_LEN: usize = MAX_BULK_LEN as usize + 64;
const SANE_ARRAY_PREALLOC: usize = 1_024;
const MAX_NESTING_DEPTH: usize = 8;

#[derive(Debug)]
pub enum RESPError {
    UnexpectedEnd,
    UnknownStartingByte,
    IOError(std::io::Error),
    IntParseFailure,
    BadBulkStringSize(i64),
    BadArraySize(i64),
    InvalidBulkLength(i64),
    InvalidMultibulkLength(i64),
    FrameTooLarge(usize),
    NestingDepthExceeded,
}

impl From<std::io::Error> for RESPError {
    fn from(e: std::io::Error) -> Self {
        RESPError::IOError(e)
    }
}

#[derive(Default)]
pub struct RespParser {
    expect_rdb: bool,
}

impl RespParser {
    pub fn expect_rdb(&mut self) {
        self.expect_rdb = true;
    }

    fn decode_rdb(&mut self, buf: &mut BytesMut) -> Result<Option<RedisValueRef>, RESPError> {
        if buf[0] != b'$' {
            return Err(RESPError::UnknownStartingByte);
        }

        let Some((pos, size)) = int(buf, 1)? else {
            return Ok(None);
        };
        if size < 0 {
            return Err(RESPError::BadBulkStringSize(size));
        }

        if size > MAX_BULK_LEN {
            return Err(RESPError::InvalidBulkLength(size));
        }

        let size = size as usize;
        let end = pos
            .checked_add(size)
            .ok_or(RESPError::InvalidBulkLength(size as i64))?;
        if end > MAX_BUFFERED_FRAME_LEN {
            return Err(RESPError::InvalidBulkLength(size as i64));
        }
        if buf.len() < end {
            return Ok(None);
        }

        buf.advance(pos);
        let payload = buf.split_to(size).freeze();
        self.expect_rdb = false;
        Ok(Some(RedisValueRef::Raw(payload)))
    }
}

type RedisResult = Result<Option<(usize, RedisBufSplit)>, RESPError>;

enum RedisBufSplit {
    String(BufSplit),
    Error(BufSplit),
    Array(Vec<RedisBufSplit>),
    NullBulkString,
    NullArray,
    Int(i64),
}

impl RedisBufSplit {
    fn redis_value(self, buf: &Bytes) -> RedisValueRef {
        match self {
            RedisBufSplit::String(bfs) => RedisValueRef::BulkString(bfs.as_bytes(buf)),
            RedisBufSplit::Error(bfs) => RedisValueRef::Error(bfs.as_bytes(buf)),
            RedisBufSplit::Array(arr) => {
                RedisValueRef::Array(arr.into_iter().map(|bfs| bfs.redis_value(buf)).collect())
            }
            RedisBufSplit::NullBulkString => RedisValueRef::NullBulkString,
            RedisBufSplit::NullArray => RedisValueRef::NullArray,
            RedisBufSplit::Int(i) => RedisValueRef::Int(i),
        }
    }
}

struct BufSplit(usize, usize);

impl BufSplit {
    #[inline]
    fn as_slice<'a>(&self, buf: &'a BytesMut) -> &'a [u8] {
        &buf[self.0..self.1]
    }

    #[inline]
    fn as_bytes(&self, buf: &Bytes) -> Bytes {
        buf.slice(self.0..self.1)
    }
}

#[inline]
fn word(buf: &BytesMut, pos: usize) -> Option<(usize, BufSplit)> {
    if buf.len() <= pos {
        return None;
    }
    memchr(b'\r', &buf[pos..]).and_then(|end| {
        if pos + end + 1 < buf.len() {
            Some((pos + end + 2, BufSplit(pos, pos + end)))
        } else {
            None
        }
    })
}

fn int(buf: &BytesMut, pos: usize) -> Result<Option<(usize, i64)>, RESPError> {
    match word(buf, pos) {
        Some((pos, word)) => {
            let s = str::from_utf8(word.as_slice(buf)).map_err(|_| RESPError::IntParseFailure)?;
            let i = s.parse().map_err(|_| RESPError::IntParseFailure)?;
            Ok(Some((pos, i)))
        }
        None => Ok(None),
    }
}

fn bulk_string(buf: &BytesMut, pos: usize) -> RedisResult {
    match int(buf, pos)? {
        Some((pos, -1)) => Ok(Some((pos, RedisBufSplit::NullBulkString))),
        Some((_pos, size)) if size > MAX_BULK_LEN => Err(RESPError::InvalidBulkLength(size)),
        Some((pos, size)) if size >= 0 => {
            let total_size = pos
                .checked_add(size as usize)
                .and_then(|end| end.checked_add(2))
                .ok_or(RESPError::InvalidBulkLength(size))?;
            if total_size > MAX_BUFFERED_FRAME_LEN {
                return Err(RESPError::InvalidBulkLength(size));
            }
            if buf.len() < total_size {
                Ok(None)
            } else {
                let payload_end = total_size - 2;
                let bb = RedisBufSplit::String(BufSplit(pos, payload_end));
                Ok(Some((total_size, bb)))
            }
        }
        Some((_pos, bad_size)) => Err(RESPError::BadBulkStringSize(bad_size)),
        None => Ok(None),
    }
}

fn simple_string(buf: &BytesMut, pos: usize) -> RedisResult {
    Ok(word(buf, pos).map(|(pos, word)| (pos, RedisBufSplit::String(word))))
}

fn error(buf: &BytesMut, pos: usize) -> RedisResult {
    Ok(word(buf, pos).map(|(pos, word)| (pos, RedisBufSplit::Error(word))))
}

fn resp_int(buf: &BytesMut, pos: usize) -> RedisResult {
    Ok(int(buf, pos)?.map(|(pos, int)| (pos, RedisBufSplit::Int(int))))
}

fn array(buf: &BytesMut, pos: usize, depth: usize) -> RedisResult {
    if depth >= MAX_NESTING_DEPTH {
        return Err(RESPError::NestingDepthExceeded);
    }

    match int(buf, pos)? {
        None => Ok(None),
        Some((pos, -1)) => Ok(Some((pos, RedisBufSplit::NullArray))),
        Some((_pos, num_elements)) if num_elements > MAX_MULTIBULK_LEN => {
            Err(RESPError::InvalidMultibulkLength(num_elements))
        }
        Some((pos, num_elements)) if num_elements >= 0 => {
            let capacity = array_prealloc_capacity(num_elements);
            let mut values = Vec::with_capacity(capacity);
            let mut cur_pos = pos;
            for _ in 0..num_elements {
                match parse(buf, cur_pos, depth + 1)? {
                    Some((new_pos, value)) => {
                        cur_pos = new_pos;
                        values.push(value);
                    }
                    None => return Ok(None),
                }
            }
            Ok(Some((cur_pos, RedisBufSplit::Array(values))))
        }
        Some((_pos, bad_num_elements)) => Err(RESPError::BadArraySize(bad_num_elements)),
    }
}

#[inline]
fn array_prealloc_capacity(num_elements: i64) -> usize {
    (num_elements as usize).min(SANE_ARRAY_PREALLOC)
}

fn parse(buf: &BytesMut, pos: usize, depth: usize) -> RedisResult {
    if buf.len() <= pos {
        return Ok(None);
    }

    match buf[pos] {
        b'+' => simple_string(buf, pos + 1),
        b'-' => error(buf, pos + 1),
        b'$' => bulk_string(buf, pos + 1),
        b':' => resp_int(buf, pos + 1),
        b'*' => array(buf, pos + 1, depth),
        _ => Err(RESPError::UnknownStartingByte),
    }
}

impl Decoder for RespParser {
    type Item = RedisValueRef;

    type Error = RESPError;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if buf.is_empty() {
            return Ok(None);
        }

        if buf.len() > MAX_BUFFERED_FRAME_LEN {
            return Err(RESPError::FrameTooLarge(buf.len()));
        }

        if self.expect_rdb {
            return self.decode_rdb(buf);
        }

        match parse(buf, 0, 0)? {
            Some((pos, value)) => {
                let data = buf.split_to(pos);
                Ok(Some(value.redis_value(&data.freeze())))
            }
            None => Ok(None),
        }
    }
}

impl Encoder<RedisValueRef> for RespParser {
    type Error = io::Error;

    fn encode(&mut self, item: RedisValueRef, dst: &mut BytesMut) -> io::Result<()> {
        write_redis_value(item, dst);
        Ok(())
    }
}

pub fn encode_command(parts: &[RedisValueRef]) -> Bytes {
    let mut buf = BytesMut::new();
    write_redis_value(RedisValueRef::Array(parts.to_vec()), &mut buf);
    buf.freeze()
}

pub fn encoded_len(value: &RedisValueRef) -> usize {
    let mut buf = BytesMut::new();
    write_redis_value(value.clone(), &mut buf);
    buf.len()
}

fn write_redis_value(item: RedisValueRef, dst: &mut BytesMut) {
    match item {
        RedisValueRef::Error(e) => {
            dst.extend_from_slice(b"-");
            dst.extend_from_slice(&e);
            dst.extend_from_slice(b"\r\n");
        }
        RedisValueRef::ErrorMsg(e) => {
            dst.extend_from_slice(b"-");
            dst.extend_from_slice(&e);
            dst.extend_from_slice(b"\r\n");
        }
        RedisValueRef::SimpleString(s) => {
            dst.extend_from_slice(b"+");
            dst.extend_from_slice(&s);
            dst.extend_from_slice(b"\r\n");
        }
        RedisValueRef::BulkString(s) => {
            dst.extend_from_slice(b"$");
            dst.extend_from_slice(s.len().to_string().as_bytes());
            dst.extend_from_slice(b"\r\n");
            dst.extend_from_slice(&s);
            dst.extend_from_slice(b"\r\n");
        }
        RedisValueRef::Array(array) => {
            dst.extend_from_slice(b"*");
            dst.extend_from_slice(array.len().to_string().as_bytes());
            dst.extend_from_slice(b"\r\n");
            for redis_value in array {
                write_redis_value(redis_value, dst);
            }
        }
        RedisValueRef::Int(i) => {
            dst.extend_from_slice(b":");
            dst.extend_from_slice(i.to_string().as_bytes());
            dst.extend_from_slice(b"\r\n");
        }
        RedisValueRef::NullArray => dst.extend_from_slice(NULL_ARRAY.as_bytes()),
        RedisValueRef::NullBulkString => dst.extend_from_slice(NULL_BULK_STRING.as_bytes()),
        RedisValueRef::Raw(raw) => dst.extend_from_slice(&raw),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decode(input: &[u8]) -> Result<Option<RedisValueRef>, RESPError> {
        RespParser::default().decode(&mut BytesMut::from(input))
    }

    #[test]
    fn rejects_attacker_controlled_length_reproductions() {
        assert!(matches!(
            decode(b"*9223372036854775807\r\n"),
            Err(RESPError::InvalidMultibulkLength(i64::MAX))
        ));
        assert!(matches!(
            decode(b"*1000000000\r\n"),
            Err(RESPError::InvalidMultibulkLength(1_000_000_000))
        ));
        assert!(matches!(
            decode(b"$536870912000\r\n"),
            Err(RESPError::InvalidBulkLength(536_870_912_000))
        ));
    }

    #[test]
    fn accepts_limits_but_rejects_the_next_length() {
        assert!(decode(format!("*{MAX_MULTIBULK_LEN}\r\n").as_bytes()).is_ok());
        assert!(matches!(
            decode(format!("*{}\r\n", MAX_MULTIBULK_LEN + 1).as_bytes()),
            Err(RESPError::InvalidMultibulkLength(_))
        ));
        assert!(decode(format!("${MAX_BULK_LEN}\r\n").as_bytes()).is_ok());
        assert!(matches!(
            decode(format!("${}\r\n", MAX_BULK_LEN + 1).as_bytes()),
            Err(RESPError::InvalidBulkLength(_))
        ));
    }

    #[test]
    fn rejects_arrays_nested_past_the_depth_limit() {
        let at_limit = [b"*1\r\n".repeat(MAX_NESTING_DEPTH), b"+OK\r\n".to_vec()].concat();
        assert!(matches!(decode(&at_limit), Ok(Some(_))));

        let too_deep = [b"*1\r\n".repeat(MAX_NESTING_DEPTH + 1), b"+OK\r\n".to_vec()].concat();
        assert!(matches!(
            decode(&too_deep),
            Err(RESPError::NestingDepthExceeded)
        ));
    }

    #[test]
    fn array_preallocation_never_exceeds_the_cap() {
        for declared_len in 0..=MAX_MULTIBULK_LEN {
            assert!(array_prealloc_capacity(declared_len) <= SANE_ARRAY_PREALLOC);
        }
        assert_eq!(
            array_prealloc_capacity(MAX_MULTIBULK_LEN),
            SANE_ARRAY_PREALLOC
        );
    }

    #[test]
    fn structured_random_inputs_never_panic() {
        // Deterministic LCG using the MMIX multiplier; wrapping is modulo 2^64.
        // The fixed seed makes failures reproducible.
        let mut state = 0x4d59_5df4_d0f3_3173_u64;
        for len in 0..=256 {
            for _ in 0..32 {
                let mut payload = vec![0; len];
                for byte in &mut payload {
                    state = state
                        .wrapping_mul(6_364_136_223_846_793_005)
                        .wrapping_add(1);
                    *byte = (state >> 32) as u8;
                }

                let mut input = format!("${len}\r\n").into_bytes();
                input.extend_from_slice(&payload);
                input.extend_from_slice(b"\r\n");

                let nesting = state as usize % (MAX_NESTING_DEPTH + 1);
                let mut frame = b"*1\r\n".repeat(nesting);
                frame.extend_from_slice(&input);
                assert!(matches!(decode(&frame), Ok(Some(_))), "frame: {frame:?}");
            }
        }
    }
}
