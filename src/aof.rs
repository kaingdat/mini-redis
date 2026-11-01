use std::{
    borrow::Cow,
    fs::{File, OpenOptions},
    io::{self, Read, Write},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use bytes::{Bytes, BytesMut};
use dashmap::DashMap;
use tokio::{
    io::AsyncReadExt,
    sync::{Mutex, Notify, OwnedMutexGuard, watch},
};
use tokio_util::codec::Decoder;

use crate::{
    command::handle_command,
    rdb::{load_into, unix_time_ms},
    resp::{RespParser, encode_command},
    server::{ReplicationState, ServerConfig},
    types::{RedisValueRef, bulk_static},
    value::ValueEntry,
};

#[derive(Clone, Copy, PartialEq, Eq)]
enum Fsync {
    Always,
    EverySec,
    No,
}

impl Fsync {
    fn parse(value: &str) -> Option<Self> {
        match value.to_ascii_lowercase().as_str() {
            "always" => Some(Self::Always),
            "everysec" => Some(Self::EverySec),
            "no" => Some(Self::No),
            _ => None,
        }
    }
}

pub struct Aof {
    log: Option<Log>,
}

struct Log {
    base: Option<PathBuf>,
    incrs: Vec<PathBuf>,
    file: Arc<Mutex<LogFile>>,
    fsync: Fsync,
    sync: Arc<SyncState>,
}

struct LogFile {
    file: File,
    len: u64,
    dirty: bool,
}

struct SyncState {
    file: File,
    written: AtomicU64,
    synced: watch::Sender<u64>,
    wake: Notify,
}

#[derive(Default)]
struct Manifest {
    base: Option<String>,
    incrs: Vec<(u64, String)>,
}

struct Replayed {
    commands: usize,
    valid: u64,
    trailing: usize,
}

impl Aof {
    pub fn disabled() -> Self {
        Self { log: None }
    }

    pub fn open(config: &ServerConfig) -> io::Result<Self> {
        if !config.aof_enabled() {
            return Ok(Self::disabled());
        }
        let fsync = Fsync::parse(&config.appendfsync).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("invalid appendfsync {:?}", config.appendfsync),
            )
        })?;

        let dir = config.aof_dir();
        std::fs::create_dir_all(&dir)?;

        let manifest_path = dir.join(format!("{}.manifest", config.appendfilename));
        let mut manifest = read_manifest(&manifest_path)?.unwrap_or_default();
        if manifest.incrs.is_empty() {
            let name = format!("{}.1.incr.aof", config.appendfilename);
            manifest.incrs.push((1, name));
            std::fs::write(&manifest_path, manifest.render())?;
        }

        let base = manifest.base.map(|name| dir.join(name));
        let incrs: Vec<PathBuf> = manifest
            .incrs
            .into_iter()
            .map(|(_, name)| dir.join(name))
            .collect();
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(incrs.last().unwrap())?;
        let len = file.metadata()?.len();
        let sync = Arc::new(SyncState {
            file: file.try_clone()?,
            written: AtomicU64::new(0),
            synced: watch::Sender::new(0),
            wake: Notify::new(),
        });
        Ok(Self {
            log: Some(Log {
                base,
                incrs,
                file: Arc::new(Mutex::new(LogFile {
                    file,
                    len,
                    dirty: false,
                })),
                fsync,
                sync,
            }),
        })
    }

    // Re-executes every command in the AOF files against `storage`.
    // Replayed commands are neither re-appended nor propagated to replicas.
    pub async fn replay(
        &self,
        storage: &Arc<DashMap<Bytes, ValueEntry>>,
        config: &Arc<ServerConfig>,
    ) -> io::Result<usize> {
        let Some(log) = &self.log else {
            return Ok(0);
        };

        let mut replayed = 0;
        if let Some(base) = &log.base {
            if is_rdb(base)? {
                load_into(base, storage);
            } else {
                replayed += replay_complete_file(base, storage, config).await?;
            }
        }

        let (last, earlier) = log.incrs.split_last().unwrap();
        for path in earlier {
            replayed += replay_complete_file(path, storage, config).await?;
        }

        let tail = replay_file(last, storage, config).await?;
        replayed += tail.commands;
        let mut file = log.file.lock().await;
        if tail.trailing > 0 {
            eprintln!(
                "AOF {} ends with a truncated command after {} commands; dropping its {} bytes",
                last.display(),
                tail.commands,
                tail.trailing
            );
            file.file.set_len(tail.valid)?;
        }
        file.len = tail.valid;
        Ok(replayed)
    }

    pub fn spawn_flusher(&self) {
        let Some(log) = &self.log else {
            return;
        };
        let sync = Arc::clone(&log.sync);
        match log.fsync {
            Fsync::Always => {
                tokio::spawn(async move {
                    loop {
                        sync.wake.notified().await;
                        if let Err(e) = sync.flush().await {
                            eprintln!(
                                "can't recover from AOF fsync error with appendfsync always: {e}"
                            );
                            std::process::exit(1);
                        }
                    }
                });
            }
            Fsync::EverySec => {
                tokio::spawn(async move {
                    let mut interval = tokio::time::interval(Duration::from_secs(1));
                    loop {
                        interval.tick().await;
                        if let Err(e) = sync.flush().await {
                            eprintln!("failed to fsync AOF: {e}");
                        }
                    }
                });
            }
            Fsync::No => {}
        }
    }

    pub async fn apply(
        &self,
        parts: &[RedisValueRef],
        execute: impl FnOnce() -> RedisValueRef,
    ) -> Result<RedisValueRef, RedisValueRef> {
        let (response, log_error) = self
            .execute_logged(|encode| {
                let response = execute();
                let encoded = if !encode || is_error(&response) {
                    Bytes::new()
                } else {
                    encode_command(&logged_form(parts))
                };
                (response, encoded)
            })
            .await?;
        if is_error(&response) {
            return Err(response);
        }
        Ok(log_error.unwrap_or(response))
    }

    pub async fn apply_transaction<'q>(
        &self,
        execute: impl FnOnce() -> (Vec<RedisValueRef>, Vec<&'q [RedisValueRef]>),
    ) -> Result<(RedisValueRef, Vec<&'q [RedisValueRef]>), RedisValueRef> {
        let ((replies, writes), log_error) = self
            .execute_logged(|encode| {
                let (replies, writes) = execute();
                let encoded = if encode {
                    encode_transaction(&writes)
                } else {
                    Bytes::new()
                };
                ((replies, writes), encoded)
            })
            .await?;
        let reply = log_error.unwrap_or(RedisValueRef::Array(replies));
        Ok((reply, writes))
    }

    async fn execute_logged<T>(
        &self,
        execute: impl FnOnce(bool) -> (T, Bytes),
    ) -> Result<(T, Option<RedisValueRef>), RedisValueRef> {
        let Some(log) = &self.log else {
            return Ok((execute(false).0, None));
        };

        let mut file = Arc::clone(&log.file).lock_owned().await;
        if file.dirty {
            let result;
            (file, result) = blocking(file, LogFile::discard_partial_write).await;
            if let Err(e) = result {
                return Err(misconf(&e));
            }
        }
        let (value, encoded) = execute(true);
        if encoded.is_empty() {
            return Ok((value, None));
        }

        let sync = Arc::clone(&log.sync);
        let (file, result) = blocking(file, move |file| {
            file.append(&encoded)?;
            let len = encoded.len() as u64;
            Ok::<_, io::Error>(sync.written.fetch_add(len, Ordering::AcqRel) + len)
        })
        .await;
        drop(file);
        let end = match result {
            Ok(end) => end,
            Err(e) => {
                if log.fsync == Fsync::Always {
                    eprintln!("can't recover from AOF write error with appendfsync always: {e}");
                    std::process::exit(1);
                }
                eprintln!("failed to append to AOF: {e}");
                return Ok((value, Some(misconf(&e))));
            }
        };

        if log.fsync == Fsync::Always {
            log.sync.wake.notify_one();
            let mut synced = log.sync.synced.subscribe();
            let _ = synced.wait_for(|&synced| synced >= end).await;
        }
        Ok((value, None))
    }
}

impl LogFile {
    fn append(&mut self, encoded: &[u8]) -> io::Result<()> {
        if let Err(e) = self.file.write_all(encoded) {
            self.dirty = true;
            let _ = self.discard_partial_write();
            return Err(e);
        }
        self.len += encoded.len() as u64;
        Ok(())
    }

    fn discard_partial_write(&mut self) -> io::Result<()> {
        if self.dirty {
            self.file.set_len(self.len)?;
            self.dirty = false;
        }
        Ok(())
    }
}

async fn blocking<T: Send + 'static>(
    mut file: OwnedMutexGuard<LogFile>,
    f: impl FnOnce(&mut LogFile) -> T + Send + 'static,
) -> (OwnedMutexGuard<LogFile>, T) {
    tokio::task::spawn_blocking(move || {
        let result = f(&mut file);
        (file, result)
    })
    .await
    .expect("AOF file task panicked")
}

fn is_error(response: &RedisValueRef) -> bool {
    matches!(response, RedisValueRef::ErrorMsg(_))
}

fn misconf(e: &io::Error) -> RedisValueRef {
    RedisValueRef::ErrorMsg(format!("MISCONF Errors writing to the AOF file: {e}").into_bytes())
}

fn encode_transaction(writes: &[&[RedisValueRef]]) -> Bytes {
    if writes.is_empty() {
        return Bytes::new();
    }
    let mut out = BytesMut::new();
    out.extend_from_slice(&encode_command(&[bulk_static(b"MULTI")]));
    for parts in writes {
        out.extend_from_slice(&encode_command(&logged_form(parts)));
    }
    out.extend_from_slice(&encode_command(&[bulk_static(b"EXEC")]));
    out.freeze()
}

fn logged_form(parts: &[RedisValueRef]) -> Cow<'_, [RedisValueRef]> {
    if let [
        RedisValueRef::BulkString(cmd),
        key,
        value,
        RedisValueRef::BulkString(option),
        RedisValueRef::BulkString(ms),
    ] = parts
        && cmd.eq_ignore_ascii_case(b"SET")
        && option.eq_ignore_ascii_case(b"PX")
        && let Some(ms) = std::str::from_utf8(ms)
            .ok()
            .and_then(|ms| ms.parse::<u64>().ok())
    {
        let at = unix_time_ms().saturating_add(ms);
        return Cow::Owned(vec![
            parts[0].clone(),
            key.clone(),
            value.clone(),
            RedisValueRef::BulkString(Bytes::from_static(b"PXAT")),
            RedisValueRef::BulkString(Bytes::from(at.to_string())),
        ]);
    }
    Cow::Borrowed(parts)
}

impl SyncState {
    async fn flush(self: &Arc<Self>) -> io::Result<()> {
        let target = self.written.load(Ordering::Acquire);
        if target <= *self.synced.borrow() {
            return Ok(());
        }
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.file.sync_data())
            .await
            .map_err(io::Error::other)??;
        self.synced.send_replace(target);
        Ok(())
    }
}

async fn replay_complete_file(
    path: &Path,
    storage: &Arc<DashMap<Bytes, ValueEntry>>,
    config: &Arc<ServerConfig>,
) -> io::Result<usize> {
    let replayed = replay_file(path, storage, config).await?;
    if replayed.trailing > 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "AOF {} ends with a truncated command but isn't the last file",
                path.display()
            ),
        ));
    }
    Ok(replayed.commands)
}

async fn replay_file(
    path: &Path,
    storage: &Arc<DashMap<Bytes, ValueEntry>>,
    config: &Arc<ServerConfig>,
) -> io::Result<Replayed> {
    let mut file = match tokio::fs::File::open(path).await {
        Ok(file) => file,
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            return Ok(Replayed {
                commands: 0,
                valid: 0,
                trailing: 0,
            });
        }
        Err(e) => return Err(e),
    };
    let mut parser = RespParser::default();
    let mut buf = BytesMut::new();
    let mut read = 0;
    let replication = Arc::new(ReplicationState::new());
    let no_aof = Arc::new(Aof::disabled());

    let corrupt = |commands: usize, detail: String| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "AOF {} is corrupt after {commands} commands: {detail}",
                path.display()
            ),
        )
    };

    let mut commands = 0;
    let mut transaction: Option<(Vec<RedisValueRef>, u64)> = None;
    loop {
        let start = read - buf.len() as u64;
        match parser.decode(&mut buf) {
            Ok(Some(value)) => {
                match (command_name(&value), &mut transaction) {
                    (Some(b"MULTI"), Some(_)) => {
                        return Err(corrupt(commands, "nested MULTI".into()));
                    }
                    (Some(b"MULTI"), None) => transaction = Some((Vec::new(), start)),
                    (Some(b"EXEC"), None) => {
                        return Err(corrupt(commands, "EXEC without MULTI".into()));
                    }
                    (Some(b"EXEC"), Some(_)) => {
                        let (queued, _) = transaction.take().unwrap();
                        for value in &queued {
                            handle_command(value, storage, config, &replication, &no_aof).await;
                        }
                        commands += queued.len();
                    }
                    (_, Some((queued, _))) => queued.push(value),
                    (_, None) => {
                        handle_command(&value, storage, config, &replication, &no_aof).await;
                        commands += 1;
                    }
                }
                continue;
            }
            Ok(None) => {}
            Err(e) => return Err(corrupt(commands, format!("{e:?}"))),
        }
        buf.reserve(64 * 1024);
        match file.read_buf(&mut buf).await? {
            0 => break,
            n => read += n as u64,
        }
    }

    let valid = match transaction {
        Some((_, start)) => start,
        None => read - buf.len() as u64,
    };
    Ok(Replayed {
        commands,
        valid,
        trailing: (read - valid) as usize,
    })
}

fn command_name(value: &RedisValueRef) -> Option<&'static [u8]> {
    let RedisValueRef::Array(parts) = value else {
        return None;
    };
    let Some(RedisValueRef::BulkString(name)) = parts.first() else {
        return None;
    };
    [b"MULTI".as_slice(), b"EXEC"]
        .into_iter()
        .find(|known| name.eq_ignore_ascii_case(known))
}

fn is_rdb(path: &Path) -> io::Result<bool> {
    let mut magic = [0; 5];
    match File::open(path)?.read_exact(&mut magic) {
        Ok(()) => Ok(&magic == b"REDIS"),
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => Ok(false),
        Err(e) => Err(e),
    }
}

impl Manifest {
    fn render(&self) -> String {
        let mut out = String::new();
        if let Some(base) = &self.base {
            out.push_str(&format!("file {base} seq 1 type b\n"));
        }
        for (seq, name) in &self.incrs {
            out.push_str(&format!("file {name} seq {seq} type i\n"));
        }
        out
    }
}

fn read_manifest(path: &Path) -> io::Result<Option<Manifest>> {
    let contents = match std::fs::read_to_string(path) {
        Ok(contents) => contents,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    let invalid = |message: String| io::Error::new(io::ErrorKind::InvalidData, message);

    let mut manifest = Manifest::default();
    for line in contents.lines() {
        let tokens: Vec<&str> = line.split_whitespace().collect();
        if tokens.is_empty() || tokens[0].starts_with('#') {
            continue;
        }
        let field = |key: &str| {
            tokens
                .chunks(2)
                .find(|pair| pair[0] == key)
                .and_then(|pair| pair.get(1).copied())
        };
        let name = field("file")
            .ok_or_else(|| invalid(format!("manifest line has no file: {line:?}")))?
            .to_string();
        match field("type") {
            Some("b") => manifest.base = Some(name),
            Some("i") => {
                let seq = field("seq")
                    .and_then(|seq| seq.parse().ok())
                    .ok_or_else(|| invalid(format!("manifest line has no valid seq: {line:?}")))?;
                manifest.incrs.push((seq, name));
            }
            Some("h") => {}
            _ => {
                return Err(invalid(format!(
                    "manifest line has no valid type: {line:?}"
                )));
            }
        }
    }
    if manifest.base.is_none() && manifest.incrs.is_empty() {
        return Err(invalid("manifest lists no AOF files".to_string()));
    }
    manifest.incrs.sort_by_key(|&(seq, _)| seq);
    Ok(Some(manifest))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manifest_lists_base_and_incrs_in_seq_order() {
        let path = std::env::temp_dir().join(format!("aof-test-{}.manifest", std::process::id()));
        std::fs::write(
            &path,
            "file a.2.incr.aof seq 2 type i\nfile a.1.base.rdb seq 1 type b\nfile old.aof seq 1 type h\nfile a.1.incr.aof seq 1 type i\n",
        )
        .unwrap();
        let manifest = read_manifest(&path).unwrap().unwrap();
        std::fs::remove_file(&path).unwrap();
        assert_eq!(manifest.base.as_deref(), Some("a.1.base.rdb"));
        assert_eq!(
            manifest.incrs,
            [
                (1, "a.1.incr.aof".to_string()),
                (2, "a.2.incr.aof".to_string())
            ]
        );
    }

    fn temp_config(name: &str, appendfsync: &str) -> (ServerConfig, PathBuf) {
        let dir = std::env::temp_dir().join(format!("aof-{}-{}", name, std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let config = ServerConfig {
            port: 0,
            role: crate::server::Role::Master,
            replid: String::new(),
            dir: dir.to_string_lossy().into_owned(),
            dbfilename: "dump.rdb".into(),
            appendonly: "yes".into(),
            appenddirname: "appendonlydir".into(),
            appendfilename: "appendonly.aof".into(),
            appendfsync: appendfsync.into(),
        };
        (config, dir)
    }

    fn open_temp(name: &str, appendfsync: &str) -> (Aof, PathBuf) {
        let (config, dir) = temp_config(name, appendfsync);
        (Aof::open(&config).unwrap(), dir)
    }

    fn command(parts: &[&[u8]]) -> Vec<RedisValueRef> {
        parts
            .iter()
            .map(|part| RedisValueRef::BulkString(Bytes::copy_from_slice(part)))
            .collect()
    }

    fn set(key: &str) -> Vec<RedisValueRef> {
        command(&[b"SET", key.as_bytes(), b"v"])
    }

    async fn append(aof: &Aof, parts: &[RedisValueRef]) {
        let ok = || RedisValueRef::SimpleString(Bytes::from_static(b"OK"));
        assert!(aof.apply(parts, ok).await.is_ok());
    }

    fn replay_config() -> Arc<ServerConfig> {
        Arc::new(ServerConfig {
            port: 0,
            role: crate::server::Role::Master,
            replid: String::new(),
            dir: String::new(),
            dbfilename: String::new(),
            appendonly: "no".into(),
            appenddirname: String::new(),
            appendfilename: String::new(),
            appendfsync: String::new(),
        })
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn always_waits_for_a_covering_fsync() {
        let (aof, dir) = open_temp("always", "always");
        let aof = Arc::new(aof);
        aof.spawn_flusher();

        let tasks: Vec<_> = (0..50)
            .map(|i| {
                let aof = Arc::clone(&aof);
                tokio::spawn(async move { append(&aof, &set(&format!("k{i}"))).await })
            })
            .collect();
        for task in tasks {
            task.await.unwrap();
        }

        let sync = &aof.log.as_ref().unwrap().sync;
        assert_eq!(*sync.synced.borrow(), sync.written.load(Ordering::Acquire));
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn appends_are_written_in_order() {
        let (aof, dir) = open_temp("order", "everysec");
        append(&aof, &set("a")).await;
        append(&aof, &set("b")).await;

        let contents = std::fs::read(dir.join("appendonlydir/appendonly.aof.1.incr.aof")).unwrap();
        let mut expected = encode_command(&set("a")).to_vec();
        expected.extend_from_slice(&encode_command(&set("b")));
        assert_eq!(contents, expected);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn replay_restores_every_command() {
        let (aof, dir) = open_temp("replay", "everysec");
        for key in ["a", "b", "c"] {
            append(&aof, &set(key)).await;
        }
        let path = aof.log.as_ref().unwrap().incrs.last().unwrap();
        let mut file = OpenOptions::new().append(true).open(path).unwrap();
        file.write_all(b"*3\r\n$3\r\nSET").unwrap();

        let storage = Arc::new(DashMap::new());
        assert_eq!(aof.replay(&storage, &replay_config()).await.unwrap(), 3);
        for key in ["a", "b", "c"] {
            assert!(storage.contains_key(key.as_bytes()));
        }
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn replay_truncates_a_partial_tail_so_later_appends_survive() {
        let (aof, dir) = open_temp("tail", "everysec");
        append(&aof, &set("a")).await;
        let path = aof.log.as_ref().unwrap().incrs.last().unwrap().clone();
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(b"*3\r\n$3\r\nSET").unwrap();

        let config = replay_config();
        aof.replay(&Arc::new(DashMap::new()), &config)
            .await
            .unwrap();
        append(&aof, &set("b")).await;

        let storage = Arc::new(DashMap::new());
        assert_eq!(aof.replay(&storage, &config).await.unwrap(), 2);
        assert!(storage.contains_key(b"b".as_slice()));
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn replay_drops_a_transaction_without_its_exec() {
        let (aof, dir) = open_temp("txn-tail", "everysec");
        append(&aof, &set("a")).await;
        let path = aof.log.as_ref().unwrap().incrs.last().unwrap().clone();
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(&encode_command(&command(&[b"MULTI"])))
            .unwrap();
        file.write_all(&encode_command(&set("b"))).unwrap();

        let config = replay_config();
        let storage = Arc::new(DashMap::new());
        assert_eq!(aof.replay(&storage, &config).await.unwrap(), 1);
        assert!(!storage.contains_key(b"b".as_slice()));
        assert_eq!(
            std::fs::read(&path).unwrap(),
            encode_command(&set("a")).to_vec(),
            "the unterminated block is truncated away"
        );

        append(&aof, &set("c")).await;
        let storage = Arc::new(DashMap::new());
        assert_eq!(aof.replay(&storage, &config).await.unwrap(), 2);
        assert!(storage.contains_key(b"c".as_slice()));
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn replay_rejects_a_corrupt_command() {
        let (aof, dir) = open_temp("corrupt", "everysec");
        let path = aof.log.as_ref().unwrap().incrs.last().unwrap().clone();
        std::fs::write(&path, b"?garbage\r\n").unwrap();
        let result = aof
            .replay(&Arc::new(DashMap::new()), &replay_config())
            .await;
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::InvalidData);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn leftover_bytes_from_a_failed_write_are_discarded() {
        let (aof, dir) = open_temp("partial", "everysec");
        append(&aof, &set("a")).await;
        {
            let mut file = aof.log.as_ref().unwrap().file.lock().await;
            file.file.write_all(b"*3\r\n$3\r\nSET").unwrap();
            file.dirty = true;
        }
        append(&aof, &set("b")).await;

        let contents = std::fs::read(dir.join("appendonlydir/appendonly.aof.1.incr.aof")).unwrap();
        let mut expected = encode_command(&set("a")).to_vec();
        expected.extend_from_slice(&encode_command(&set("b")));
        assert_eq!(contents, expected);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn relative_expiry_is_logged_as_a_deadline() {
        let (aof, dir) = open_temp("pxat", "everysec");
        let before = unix_time_ms();
        append(&aof, &command(&[b"SET", b"k", b"v", b"PX", b"1000"])).await;

        let contents = std::fs::read(dir.join("appendonlydir/appendonly.aof.1.incr.aof")).unwrap();
        let logged = RespParser::default()
            .decode(&mut BytesMut::from(contents.as_slice()))
            .unwrap()
            .unwrap();
        let RedisValueRef::Array(parts) = logged else {
            panic!("expected an array");
        };
        let RedisValueRef::BulkString(at) = &parts[4] else {
            panic!("expected a deadline");
        };
        assert!(matches!(&parts[3], RedisValueRef::BulkString(o) if o.as_ref() == b"PXAT"));
        let at: u64 = std::str::from_utf8(at).unwrap().parse().unwrap();
        assert!((before + 1000..=unix_time_ms() + 1000).contains(&at));
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn replayed_deadline_in_the_past_leaves_the_key_absent() {
        let (aof, dir) = open_temp("expired", "everysec");
        let past = (unix_time_ms() - 1000).to_string();
        append(
            &aof,
            &command(&[b"SET", b"k", b"v", b"PXAT", past.as_bytes()]),
        )
        .await;

        let storage = Arc::new(DashMap::new());
        aof.replay(&storage, &replay_config()).await.unwrap();
        assert!(!storage.contains_key(b"k".as_slice()));
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn replay_applies_base_then_incrs_and_appends_to_the_last() {
        let (config, dir) = temp_config("multi", "everysec");
        let aof_dir = dir.join("appendonlydir");
        std::fs::create_dir_all(&aof_dir).unwrap();
        std::fs::write(
            aof_dir.join("appendonly.aof.manifest"),
            "file appendonly.aof.1.base.aof seq 1 type b\nfile appendonly.aof.2.incr.aof seq 2 type i\nfile appendonly.aof.1.incr.aof seq 1 type i\n",
        )
        .unwrap();
        let set_value = |value: &[u8]| encode_command(&command(&[b"SET", b"k", value]));
        std::fs::write(
            aof_dir.join("appendonly.aof.1.base.aof"),
            set_value(b"base"),
        )
        .unwrap();
        std::fs::write(aof_dir.join("appendonly.aof.1.incr.aof"), set_value(b"one")).unwrap();
        std::fs::write(aof_dir.join("appendonly.aof.2.incr.aof"), set_value(b"two")).unwrap();

        let aof = Aof::open(&config).unwrap();
        let storage = Arc::new(DashMap::new());
        assert_eq!(aof.replay(&storage, &replay_config()).await.unwrap(), 3);
        assert!(matches!(
            &storage.get(b"k".as_slice()).unwrap().data,
            crate::value::RedisValue::String(v) if v.as_ref() == b"two"
        ));

        append(&aof, &set("new")).await;
        let last = std::fs::read(aof_dir.join("appendonly.aof.2.incr.aof")).unwrap();
        assert!(last.ends_with(&encode_command(&set("new"))));
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn unknown_appendfsync_is_rejected() {
        let (config, dir) = temp_config("fsync", "Always ");
        let error = Aof::open(&config).err().unwrap();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn failed_commands_are_not_applied() {
        let aof = Aof::disabled();
        let error = || RedisValueRef::ErrorMsg(b"WRONGTYPE".to_vec());
        assert!(aof.apply(&set("a"), error).await.is_err());
    }
}
