use bytes::Bytes;

#[derive(PartialEq, Clone)]
pub enum RedisValueRef {
    BulkString(Bytes),
    SimpleString(Bytes),
    Error(Bytes),
    ErrorMsg(Vec<u8>),
    Int(i64),
    Array(Vec<RedisValueRef>),
    NullArray,
    NullBulkString,
}

pub const NULL_BULK_STRING: &str = "$-1\r\n";
pub const NULL_ARRAY: &str = "*-1\r\n";
pub const EMPTY_ARRAY: &str = "*0\r\n";
