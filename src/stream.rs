use std::{
    borrow::Cow,
    collections::{BTreeMap, HashMap},
    fmt,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use bytes::Bytes;
use dashmap::{DashMap, mapref::entry::Entry};
use tokio::{sync::Notify, time::Instant};

use crate::{
    command::{WRONGTYPE, wrong_arity},
    rdb::unix_time_ms,
    types::RedisValueRef,
    value::{RedisValue, ValueEntry},
};

const ERR_INVALID_ID: &[u8] = b"ERR Invalid stream ID specified as stream command argument";
const ERR_NOT_GREATER: &[u8] =
    b"ERR The ID specified in XADD is equal or smaller than the target stream top item";
const ERR_ZERO_ID: &[u8] = b"ERR The ID specified in XADD must be greater than 0-0";
const ERR_EXHAUSTED: &[u8] =
    b"ERR The stream has exhausted the last possible ID, unable to add more items";
const ERR_UNBALANCED: &[u8] =
    b"ERR Unbalanced 'xread' list of streams: for each stream key an ID or '$' must be specified.";
const ERR_SYNTAX: &[u8] = b"ERR syntax error";
const ERR_TIMEOUT_INVALID: &[u8] = b"ERR timeout is not an integer or out of range";
const ERR_TIMEOUT_NEGATIVE: &[u8] = b"ERR timeout is negative";

fn error(message: &[u8]) -> RedisValueRef {
    RedisValueRef::ErrorMsg(message.to_vec())
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct StreamId {
    pub ms: u64,
    pub seq: u64,
}

impl StreamId {
    pub const MIN: Self = Self { ms: 0, seq: 0 };
    pub const MAX: Self = Self {
        ms: u64::MAX,
        seq: u64::MAX,
    };

    pub fn new(ms: u64, seq: u64) -> Self {
        Self { ms, seq }
    }
}

impl fmt::Display for StreamId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{}", self.ms, self.seq)
    }
}

/// The ID argument of `XADD`.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum AddId {
    Explicit(StreamId),
    AutoSeq(u64),
    Auto,
}

fn parse_u64(bytes: &[u8]) -> Option<u64> {
    if bytes.is_empty() || !bytes.iter().all(u8::is_ascii_digit) {
        return None;
    }
    std::str::from_utf8(bytes).ok()?.parse().ok()
}

/// Splits `<ms>[-<seq>]` and parses the `ms` part.
fn split_id(bytes: &[u8]) -> Option<(u64, Option<&[u8]>)> {
    match bytes.iter().position(|&b| b == b'-') {
        Some(dash) => Some((parse_u64(&bytes[..dash])?, Some(&bytes[dash + 1..]))),
        None => Some((parse_u64(bytes)?, None)),
    }
}

pub fn parse_add_id(bytes: &[u8]) -> Option<AddId> {
    if bytes == b"*" {
        return Some(AddId::Auto);
    }
    let (ms, seq) = split_id(bytes)?;
    Some(match seq {
        None => AddId::Explicit(StreamId::new(ms, 0)),
        Some(b"*") => AddId::AutoSeq(ms),
        Some(seq) => AddId::Explicit(StreamId::new(ms, parse_u64(seq)?)),
    })
}

/// Parses an ID that bounds a range. `default_seq` fills in a missing
/// sequence number: 0 for a start, `u64::MAX` for an end.
pub fn parse_range_id(bytes: &[u8], default_seq: u64) -> Option<StreamId> {
    match bytes {
        b"-" => Some(StreamId::MIN),
        b"+" => Some(StreamId::MAX),
        _ => {
            let (ms, seq) = split_id(bytes)?;
            let seq = match seq {
                None => default_seq,
                Some(seq) => parse_u64(seq)?,
            };
            Some(StreamId::new(ms, seq))
        }
    }
}

pub type Fields = Vec<(Bytes, Bytes)>;

#[derive(Clone, Default)]
pub struct StreamData {
    entries: BTreeMap<StreamId, Fields>,
}

impl StreamData {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn last_id(&self) -> StreamId {
        self.entries
            .last_key_value()
            .map_or(StreamId::MIN, |(id, _)| *id)
    }

    /// Works out the ID `XADD` would store, without storing anything.
    pub fn next_id(&self, requested: AddId, now_ms: u64) -> Result<StreamId, &'static [u8]> {
        let last = self.last_id();
        let id = match requested {
            AddId::Explicit(id) => id,
            // An empty stream's top is 0-0, so `0-*` on it gives 0-1.
            AddId::AutoSeq(ms) if ms == last.ms => {
                if last.seq == u64::MAX {
                    return Err(ERR_NOT_GREATER);
                }
                StreamId::new(ms, last.seq + 1)
            }
            AddId::AutoSeq(ms) => StreamId::new(ms, 0),
            AddId::Auto => {
                let ms = now_ms.max(last.ms);
                if ms > last.ms {
                    StreamId::new(ms, 0)
                } else if last.seq < u64::MAX {
                    StreamId::new(ms, last.seq + 1)
                } else if ms < u64::MAX {
                    StreamId::new(ms + 1, 0)
                } else {
                    return Err(ERR_EXHAUSTED);
                }
            }
        };
        if id == StreamId::MIN {
            return Err(ERR_ZERO_ID);
        }
        if id <= last {
            return Err(ERR_NOT_GREATER);
        }
        Ok(id)
    }

    pub fn insert(&mut self, id: StreamId, fields: Fields) {
        self.entries.insert(id, fields);
    }

    /// Entries with `start <= id <= end`.
    pub fn range(&self, start: StreamId, end: StreamId) -> Vec<RedisValueRef> {
        if start > end {
            return Vec::new();
        }
        self.entries
            .range(start..=end)
            .map(|(id, fields)| entry_reply(*id, fields))
            .collect()
    }

    /// Entries with `id > after`.
    pub fn after(&self, after: StreamId) -> Vec<RedisValueRef> {
        use std::ops::Bound::{Excluded, Unbounded};
        self.entries
            .range((Excluded(after), Unbounded))
            .map(|(id, fields)| entry_reply(*id, fields))
            .collect()
    }
}

fn entry_reply(id: StreamId, fields: &Fields) -> RedisValueRef {
    let mut flat = Vec::with_capacity(fields.len() * 2);
    for (field, value) in fields {
        flat.push(RedisValueRef::BulkString(field.clone()));
        flat.push(RedisValueRef::BulkString(value.clone()));
    }
    RedisValueRef::Array(vec![
        RedisValueRef::BulkString(Bytes::from(id.to_string())),
        RedisValueRef::Array(flat),
    ])
}

fn bulk_arg(parts: &[RedisValueRef], i: usize) -> Result<&Bytes, RedisValueRef> {
    match parts.get(i) {
        Some(RedisValueRef::BulkString(b)) => Ok(b),
        _ => Err(crate::command::invalid_argument()),
    }
}

pub fn handle_xadd(
    parts: &[RedisValueRef],
    storage: &Arc<DashMap<Bytes, ValueEntry>>,
) -> RedisValueRef {
    match xadd(parts, storage) {
        Ok(id) => RedisValueRef::BulkString(Bytes::from(id.to_string())),
        Err(e) => e,
    }
}

fn xadd(
    parts: &[RedisValueRef],
    storage: &Arc<DashMap<Bytes, ValueEntry>>,
) -> Result<StreamId, RedisValueRef> {
    if parts.len() < 5 || !(parts.len() - 3).is_multiple_of(2) {
        return Err(wrong_arity("xadd"));
    }
    let key = bulk_arg(parts, 1)?;
    let requested = parse_add_id(bulk_arg(parts, 2)?).ok_or_else(|| error(ERR_INVALID_ID))?;
    if requested == AddId::Explicit(StreamId::MIN) {
        return Err(error(ERR_ZERO_ID));
    }
    let fields = parts[3..]
        .chunks(2)
        .map(|pair| Ok((bulk_arg(pair, 0)?.clone(), bulk_arg(pair, 1)?.clone())))
        .collect::<Result<Fields, RedisValueRef>>()?;

    storage.remove_if(key, |_, entry| entry.is_expired());
    let now_ms = unix_time_ms();
    match storage.entry(key.clone()) {
        Entry::Occupied(mut occupied) => {
            let RedisValue::Stream(stream) = &mut occupied.get_mut().data else {
                return Err(error(WRONGTYPE));
            };
            let id = stream.next_id(requested, now_ms).map_err(error)?;
            stream.insert(id, fields);
            Ok(id)
        }
        Entry::Vacant(vacant) => {
            let mut stream = StreamData::new();
            let id = stream.next_id(requested, now_ms).map_err(error)?;
            stream.insert(id, fields);
            vacant.insert(ValueEntry::new(RedisValue::Stream(stream), None));
            Ok(id)
        }
    }
}

/// The key a successful `XADD` wrote to, so blocked readers can be woken.
pub fn added_key<'a>(value: &'a RedisValueRef, reply: &RedisValueRef) -> Option<&'a Bytes> {
    let RedisValueRef::Array(parts) = value else {
        return None;
    };
    match (parts.first(), parts.get(1), reply) {
        (
            Some(RedisValueRef::BulkString(name)),
            Some(RedisValueRef::BulkString(key)),
            RedisValueRef::BulkString(_),
        ) if name.eq_ignore_ascii_case(b"XADD") => Some(key),
        _ => None,
    }
}

/// The form of a successful write that the AOF and replicas receive. For an
/// `XADD` whose ID contains `*`, that is the same command with the ID it
/// stored, so replaying it can't pick a different one.
pub fn concrete_form<'a>(
    parts: &'a [RedisValueRef],
    reply: &RedisValueRef,
) -> Cow<'a, [RedisValueRef]> {
    if let (
        Some(RedisValueRef::BulkString(name)),
        Some(RedisValueRef::BulkString(id)),
        RedisValueRef::BulkString(stored),
    ) = (parts.first(), parts.get(2), reply)
        && name.eq_ignore_ascii_case(b"XADD")
        && id.contains(&b'*')
    {
        let mut owned = parts.to_vec();
        owned[2] = RedisValueRef::BulkString(stored.clone());
        return Cow::Owned(owned);
    }
    Cow::Borrowed(parts)
}

pub fn handle_xrange(
    parts: &[RedisValueRef],
    storage: &Arc<DashMap<Bytes, ValueEntry>>,
) -> RedisValueRef {
    if parts.len() != 4 {
        return wrong_arity("xrange");
    }
    let (key, start, end) = match (bulk_arg(parts, 1), bulk_arg(parts, 2), bulk_arg(parts, 3)) {
        (Ok(key), Ok(start), Ok(end)) => (key, start, end),
        _ => return crate::command::invalid_argument(),
    };
    let (Some(start), Some(end)) = (parse_range_id(start, 0), parse_range_id(end, u64::MAX)) else {
        return error(ERR_INVALID_ID);
    };

    storage.remove_if(key, |_, entry| entry.is_expired());
    match storage.get(key) {
        Some(entry) => match &entry.data {
            RedisValue::Stream(stream) => RedisValueRef::Array(stream.range(start, end)),
            _ => error(WRONGTYPE),
        },
        None => RedisValueRef::Array(Vec::new()),
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ReadId {
    /// `$`: the stream's top ID when the command arrives.
    Last,
    After(StreamId),
}

pub struct XRead {
    pub keys: Vec<Bytes>,
    pub ids: Vec<ReadId>,
    /// `None` without `BLOCK`, `Some(0)` to wait with no timeout.
    pub block_ms: Option<u64>,
}

pub fn parse_xread(parts: &[RedisValueRef]) -> Result<XRead, RedisValueRef> {
    if parts.len() < 4 {
        return Err(wrong_arity("xread"));
    }
    let mut block_ms = None;
    let mut i = 1;
    let streams_at = loop {
        let Ok(option) = bulk_arg(parts, i) else {
            return Err(error(ERR_SYNTAX));
        };
        if option.eq_ignore_ascii_case(b"STREAMS") {
            break i + 1;
        } else if option.eq_ignore_ascii_case(b"BLOCK") {
            let timeout = bulk_arg(parts, i + 1).map_err(|_| error(ERR_SYNTAX))?;
            let timeout = std::str::from_utf8(timeout)
                .ok()
                .and_then(|s| s.parse::<i64>().ok())
                .ok_or_else(|| error(ERR_TIMEOUT_INVALID))?;
            if timeout < 0 {
                return Err(error(ERR_TIMEOUT_NEGATIVE));
            }
            block_ms = Some(timeout as u64);
            i += 2;
        } else {
            return Err(error(ERR_SYNTAX));
        }
    };

    let rest = &parts[streams_at..];
    if rest.is_empty() || !rest.len().is_multiple_of(2) {
        return Err(error(ERR_UNBALANCED));
    }
    let (keys, ids) = rest.split_at(rest.len() / 2);
    let keys = (0..keys.len())
        .map(|i| bulk_arg(keys, i).cloned())
        .collect::<Result<Vec<_>, _>>()?;
    let ids = (0..ids.len())
        .map(|i| match bulk_arg(ids, i)?.as_ref() {
            b"$" => Ok(ReadId::Last),
            id => parse_range_id(id, 0)
                .filter(|_| id != b"+")
                .map(ReadId::After)
                .ok_or_else(|| error(ERR_INVALID_ID)),
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(XRead {
        keys,
        ids,
        block_ms,
    })
}

/// Parses an `XREAD` that has a `BLOCK` option. Returns `None` for any other
/// command, including an `XREAD` without `BLOCK`.
pub fn parse_blocking_xread(value: &RedisValueRef) -> Option<Result<XRead, RedisValueRef>> {
    let RedisValueRef::Array(parts) = value else {
        return None;
    };
    match parts.first() {
        Some(RedisValueRef::BulkString(name)) if name.eq_ignore_ascii_case(b"XREAD") => {}
        _ => return None,
    }
    match parse_xread(parts) {
        Ok(read) if read.block_ms.is_none() => None,
        result => Some(result),
    }
}

fn with_stream<T>(
    storage: &DashMap<Bytes, ValueEntry>,
    key: &Bytes,
    read: impl FnOnce(Option<&StreamData>) -> T,
) -> Result<T, RedisValueRef> {
    storage.remove_if(key, |_, entry| entry.is_expired());
    match storage.get(key) {
        Some(entry) => match &entry.data {
            RedisValue::Stream(stream) => Ok(read(Some(stream))),
            _ => Err(error(WRONGTYPE)),
        },
        None => Ok(read(None)),
    }
}

/// Replaces each `$` with the stream's current top ID.
pub fn resolve_ids(
    storage: &DashMap<Bytes, ValueEntry>,
    keys: &[Bytes],
    ids: &[ReadId],
) -> Result<Vec<StreamId>, RedisValueRef> {
    keys.iter()
        .zip(ids)
        .map(|(key, id)| match id {
            ReadId::After(id) => with_stream(storage, key, |_| *id),
            ReadId::Last => with_stream(storage, key, |stream| {
                stream.map_or(StreamId::MIN, StreamData::last_id)
            }),
        })
        .collect()
}

/// The `XREAD` reply for entries after each ID, or `None` if there are none.
pub fn read_after(
    storage: &DashMap<Bytes, ValueEntry>,
    keys: &[Bytes],
    ids: &[StreamId],
) -> Result<Option<RedisValueRef>, RedisValueRef> {
    let mut found = Vec::new();
    for (key, id) in keys.iter().zip(ids) {
        let entries = with_stream(storage, key, |stream| {
            stream.map_or_else(Vec::new, |stream| stream.after(*id))
        })?;
        if !entries.is_empty() {
            found.push(RedisValueRef::Array(vec![
                RedisValueRef::BulkString(key.clone()),
                RedisValueRef::Array(entries),
            ]));
        }
    }
    Ok((!found.is_empty()).then_some(RedisValueRef::Array(found)))
}

/// `XREAD` without blocking, as run outside a session or inside `EXEC`.
pub fn handle_xread(
    parts: &[RedisValueRef],
    storage: &Arc<DashMap<Bytes, ValueEntry>>,
) -> RedisValueRef {
    let result = parse_xread(parts).and_then(|read| {
        let ids = resolve_ids(storage, &read.keys, &read.ids)?;
        read_after(storage, &read.keys, &ids)
    });
    match result {
        Ok(Some(reply)) => reply,
        Ok(None) => RedisValueRef::NullArray,
        Err(e) => e,
    }
}

/// Blocked `XREAD` clients, by the stream keys they wait on.
#[derive(Default)]
pub struct StreamWaiters {
    keys: DashMap<Bytes, HashMap<u64, Arc<Notify>>>,
    next_id: AtomicU64,
}

impl StreamWaiters {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(self: &Arc<Self>, keys: &[Bytes]) -> WaitGuard {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let notify = Arc::new(Notify::new());
        let mut unique = keys.to_vec();
        unique.sort();
        unique.dedup();
        for key in &unique {
            self.keys
                .entry(key.clone())
                .or_default()
                .insert(id, Arc::clone(&notify));
        }
        WaitGuard {
            waiters: Arc::clone(self),
            id,
            keys: unique,
            notify,
        }
    }

    /// Wakes every client blocked on `key`. A client that hasn't started
    /// waiting yet keeps the wake-up as a permit.
    pub fn notify(&self, key: &Bytes) {
        let notifies: Vec<Arc<Notify>> = match self.keys.get(key) {
            Some(waiters) => waiters.values().cloned().collect(),
            None => return,
        };
        for notify in notifies {
            notify.notify_one();
        }
    }

    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }
}

pub struct WaitGuard {
    waiters: Arc<StreamWaiters>,
    id: u64,
    keys: Vec<Bytes>,
    notify: Arc<Notify>,
}

impl WaitGuard {
    pub async fn notified(&self) {
        self.notify.notified().await;
    }
}

impl Drop for WaitGuard {
    fn drop(&mut self) {
        for key in &self.keys {
            if let Entry::Occupied(mut entry) = self.waiters.keys.entry(key.clone()) {
                entry.get_mut().remove(&self.id);
                if entry.get().is_empty() {
                    entry.remove();
                }
            }
        }
    }
}

/// A blocking `XREAD` that found nothing on its first check.
pub struct BlockedRead {
    pub keys: Vec<Bytes>,
    pub ids: Vec<StreamId>,
    pub deadline: Option<Instant>,
    pub guard: WaitGuard,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(ms: u64, seq: u64) -> StreamId {
        StreamId::new(ms, seq)
    }

    fn stream_with(top: StreamId) -> StreamData {
        let mut stream = StreamData::new();
        stream.insert(top, Vec::new());
        stream
    }

    #[test]
    fn parses_add_ids() {
        assert_eq!(
            parse_add_id(b"1526919030473-0"),
            Some(AddId::Explicit(id(1526919030473, 0)))
        );
        assert_eq!(parse_add_id(b"5"), Some(AddId::Explicit(id(5, 0))));
        assert_eq!(
            parse_add_id(b"1526919030474-*"),
            Some(AddId::AutoSeq(1526919030474))
        );
        assert_eq!(parse_add_id(b"*"), Some(AddId::Auto));
        for bad in [
            &b""[..],
            b"-",
            b"5-",
            b"-5",
            b"*-1",
            b"a-1",
            b"1-a",
            b"1-2-3",
            b"+1",
        ] {
            assert_eq!(parse_add_id(bad), None, "{:?}", bad);
        }
        assert_eq!(parse_add_id(b"18446744073709551616-0"), None);
    }

    #[test]
    fn parses_range_ids() {
        assert_eq!(parse_range_id(b"-", 0), Some(StreamId::MIN));
        assert_eq!(parse_range_id(b"+", 0), Some(StreamId::MAX));
        assert_eq!(parse_range_id(b"5", 0), Some(id(5, 0)));
        assert_eq!(parse_range_id(b"5", u64::MAX), Some(id(5, u64::MAX)));
        assert_eq!(parse_range_id(b"5-3", u64::MAX), Some(id(5, 3)));
        assert_eq!(parse_range_id(b"5-*", 0), None);
    }

    #[test]
    fn explicit_ids_must_increase() {
        let stream = stream_with(id(5, 5));
        assert_eq!(stream.next_id(AddId::Explicit(id(5, 6)), 0), Ok(id(5, 6)));
        assert_eq!(stream.next_id(AddId::Explicit(id(6, 0)), 0), Ok(id(6, 0)));
        assert_eq!(
            stream.next_id(AddId::Explicit(id(5, 5)), 0),
            Err(ERR_NOT_GREATER)
        );
        assert_eq!(
            stream.next_id(AddId::Explicit(id(4, 9)), 0),
            Err(ERR_NOT_GREATER)
        );
        assert_eq!(
            StreamData::new().next_id(AddId::Explicit(id(0, 0)), 0),
            Err(ERR_ZERO_ID)
        );
    }

    #[test]
    fn auto_sequence_follows_the_top_entry() {
        assert_eq!(
            StreamData::new().next_id(AddId::AutoSeq(0), 0),
            Ok(id(0, 1))
        );
        assert_eq!(
            StreamData::new().next_id(AddId::AutoSeq(7), 0),
            Ok(id(7, 0))
        );
        let stream = stream_with(id(5, 2));
        assert_eq!(stream.next_id(AddId::AutoSeq(5), 0), Ok(id(5, 3)));
        assert_eq!(stream.next_id(AddId::AutoSeq(6), 0), Ok(id(6, 0)));
        assert_eq!(stream.next_id(AddId::AutoSeq(4), 0), Err(ERR_NOT_GREATER));
        assert_eq!(
            stream_with(id(0, 1)).next_id(AddId::AutoSeq(0), 0),
            Ok(id(0, 2))
        );
        assert_eq!(
            stream_with(id(5, u64::MAX)).next_id(AddId::AutoSeq(5), 0),
            Err(ERR_NOT_GREATER)
        );
    }

    #[test]
    fn auto_ids_use_the_clock_but_never_go_back() {
        assert_eq!(StreamData::new().next_id(AddId::Auto, 100), Ok(id(100, 0)));
        assert_eq!(StreamData::new().next_id(AddId::Auto, 0), Ok(id(0, 1)));
        assert_eq!(
            stream_with(id(100, 3)).next_id(AddId::Auto, 100),
            Ok(id(100, 4))
        );
        assert_eq!(
            stream_with(id(100, 3)).next_id(AddId::Auto, 50),
            Ok(id(100, 4))
        );
        assert_eq!(
            stream_with(id(100, 3)).next_id(AddId::Auto, 200),
            Ok(id(200, 0))
        );
        assert_eq!(
            stream_with(id(100, u64::MAX)).next_id(AddId::Auto, 100),
            Ok(id(101, 0))
        );
        assert_eq!(
            stream_with(StreamId::MAX).next_id(AddId::Auto, 0),
            Err(ERR_EXHAUSTED)
        );
    }

    #[test]
    fn ranges_are_inclusive_and_reads_are_exclusive() {
        let mut stream = StreamData::new();
        for (ms, seq) in [(1, 0), (1, 1), (2, 0), (3, 0)] {
            stream.insert(id(ms, seq), Vec::new());
        }
        assert_eq!(stream.range(id(1, 1), id(2, u64::MAX)).len(), 2);
        assert_eq!(stream.range(StreamId::MIN, StreamId::MAX).len(), 4);
        assert!(stream.range(id(3, 0), id(1, 0)).is_empty());
        assert_eq!(stream.after(id(1, 1)).len(), 2);
        assert!(stream.after(id(3, 0)).is_empty());
    }

    #[test]
    fn xread_arguments() {
        let parts = |args: &[&'static str]| -> Vec<RedisValueRef> {
            args.iter()
                .map(|a| RedisValueRef::BulkString(Bytes::from_static(a.as_bytes())))
                .collect()
        };
        let read = parse_xread(&parts(&[
            "XREAD", "BLOCK", "0", "STREAMS", "a", "b", "0-1", "$",
        ]))
        .ok()
        .unwrap();
        assert_eq!(read.keys, vec![Bytes::from("a"), Bytes::from("b")]);
        assert_eq!(read.ids, vec![ReadId::After(id(0, 1)), ReadId::Last]);
        assert_eq!(read.block_ms, Some(0));

        let read = parse_xread(&parts(&["XREAD", "STREAMS", "BLOCK", "5"]))
            .ok()
            .unwrap();
        assert_eq!(read.keys, vec![Bytes::from("BLOCK")]);
        assert_eq!(read.block_ms, None);

        let err = |args| match parse_xread(&parts(args)) {
            Err(RedisValueRef::ErrorMsg(e)) => e,
            _ => panic!("expected an error"),
        };
        assert_eq!(err(&["XREAD", "STREAMS", "a", "b", "0"]), ERR_UNBALANCED);
        assert_eq!(
            err(&["XREAD", "BLOCK", "x", "STREAMS", "a", "0"]),
            ERR_TIMEOUT_INVALID
        );
        assert_eq!(
            err(&["XREAD", "BLOCK", "-1", "STREAMS", "a", "0"]),
            ERR_TIMEOUT_NEGATIVE
        );
        assert_eq!(err(&["XREAD", "COUNT", "1", "a", "0"]), ERR_SYNTAX);
        assert_eq!(err(&["XREAD", "STREAMS", "a", "x"]), ERR_INVALID_ID);
        assert_eq!(err(&["XREAD", "STREAMS", "a", "+"]), ERR_INVALID_ID);
    }

    #[test]
    fn waiters_unregister_on_drop() {
        let waiters = Arc::new(StreamWaiters::new());
        let a = Bytes::from("a");
        let guard = waiters.register(&[a.clone(), a.clone(), Bytes::from("b")]);
        assert!(!waiters.is_empty());
        drop(guard);
        assert!(waiters.is_empty());
    }
}
