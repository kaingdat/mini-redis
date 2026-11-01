use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
};

use bytes::Bytes;
use dashmap::{DashMap, mapref::entry::Entry};
use tokio::sync::mpsc;

use crate::{
    command::{invalid_argument, wrong_arity},
    resp::encode_command,
    types::{RedisValueRef, bulk_static},
};

pub const OUTPUT_LIMIT_BYTES: usize = 32 * 1024 * 1024;

enum Delivery {
    Message(Bytes),
    Overflow,
}

struct Mailbox {
    tx: mpsc::UnboundedSender<Delivery>,
    pending: AtomicUsize,
    overflowed: AtomicBool,
}

impl Mailbox {
    fn deliver(&self, frame: &Bytes, limit: usize) -> bool {
        if self.overflowed.load(Ordering::Acquire) {
            return false;
        }
        let queued = self.pending.fetch_add(frame.len(), Ordering::AcqRel) + frame.len();
        if queued > limit {
            if !self.overflowed.swap(true, Ordering::AcqRel) {
                let _ = self.tx.send(Delivery::Overflow);
            }
            return false;
        }
        self.tx.send(Delivery::Message(frame.clone())).is_ok()
    }
}

pub struct PubSub {
    channels: DashMap<Bytes, HashMap<u64, Arc<Mailbox>>>,
    next_id: AtomicU64,
    output_limit: usize,
}

impl Default for PubSub {
    fn default() -> Self {
        Self::with_output_limit(OUTPUT_LIMIT_BYTES)
    }
}

impl PubSub {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_output_limit(output_limit: usize) -> Self {
        Self {
            channels: DashMap::new(),
            next_id: AtomicU64::new(0),
            output_limit,
        }
    }

    pub fn publish(&self, channel: &Bytes, message: &Bytes) -> usize {
        let mailboxes: Vec<Arc<Mailbox>> = match self.channels.get(channel) {
            Some(subscribers) => subscribers.values().cloned().collect(),
            None => return 0,
        };
        let frame = encode_command(&[
            bulk_static(b"message"),
            RedisValueRef::BulkString(channel.clone()),
            RedisValueRef::BulkString(message.clone()),
        ]);
        mailboxes
            .iter()
            .filter(|mailbox| mailbox.deliver(&frame, self.output_limit))
            .count()
    }

    pub fn execute_publish(&self, value: &RedisValueRef) -> Option<RedisValueRef> {
        let RedisValueRef::Array(parts) = value else {
            return None;
        };
        match parts.first() {
            Some(RedisValueRef::BulkString(name)) if name.eq_ignore_ascii_case(b"PUBLISH") => {
                Some(publish(self, &parts[1..]))
            }
            _ => None,
        }
    }

    fn add(&self, channel: Bytes, id: u64, mailbox: Arc<Mailbox>) {
        self.channels
            .entry(channel)
            .or_default()
            .insert(id, mailbox);
    }

    fn remove(&self, channel: &Bytes, id: u64) {
        if let Entry::Occupied(mut entry) = self.channels.entry(channel.clone()) {
            entry.get_mut().remove(&id);
            if entry.get().is_empty() {
                entry.remove();
            }
        }
    }
}

#[derive(Clone, Copy)]
enum Command {
    Subscribe,
    Unsubscribe,
    Ping,
}

const ALLOWED_WHEN_SUBSCRIBED: &[&[u8]] = &[
    b"SUBSCRIBE",
    b"UNSUBSCRIBE",
    b"PSUBSCRIBE",
    b"PUNSUBSCRIBE",
    b"SSUBSCRIBE",
    b"SUNSUBSCRIBE",
    b"PING",
    b"QUIT",
    b"RESET",
];

pub struct Subscriber {
    id: u64,
    pubsub: Arc<PubSub>,
    channels: HashSet<Bytes>,
    mailbox: Arc<Mailbox>,
    rx: mpsc::UnboundedReceiver<Delivery>,
}

impl Subscriber {
    pub fn new(pubsub: Arc<PubSub>) -> Self {
        let id = pubsub.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = mpsc::unbounded_channel();
        Self {
            id,
            pubsub,
            channels: HashSet::new(),
            mailbox: Arc::new(Mailbox {
                tx,
                pending: AtomicUsize::new(0),
                overflowed: AtomicBool::new(false),
            }),
            rx,
        }
    }

    pub fn is_subscribed(&self) -> bool {
        !self.channels.is_empty()
    }

    pub async fn recv(&mut self) -> Option<Bytes> {
        loop {
            if self.mailbox.overflowed.load(Ordering::Acquire) {
                return None;
            }
            match self.rx.recv().await? {
                Delivery::Message(frame) => {
                    self.mailbox
                        .pending
                        .fetch_sub(frame.len(), Ordering::AcqRel);
                    return Some(frame);
                }
                Delivery::Overflow => {}
            }
        }
    }

    pub fn handle(&mut self, value: &RedisValueRef) -> Option<Vec<RedisValueRef>> {
        let RedisValueRef::Array(parts) = value else {
            return None;
        };
        let Some(RedisValueRef::BulkString(name)) = parts.first() else {
            return None;
        };
        let upper = name.to_ascii_uppercase();

        if self.is_subscribed() && !ALLOWED_WHEN_SUBSCRIBED.contains(&upper.as_slice()) {
            return Some(vec![RedisValueRef::ErrorMsg(
                format!(
                    "ERR Can't execute '{}': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context",
                    String::from_utf8_lossy(name).to_lowercase()
                )
                .into_bytes(),
            )]);
        }
        let command = match upper.as_slice() {
            b"SUBSCRIBE" => Command::Subscribe,
            b"UNSUBSCRIBE" => Command::Unsubscribe,
            b"PING" => Command::Ping,
            _ => return None,
        };
        let args = &parts[1..];

        Some(match command {
            Command::Subscribe => self.subscribe(args),
            Command::Unsubscribe => self.unsubscribe(args),
            Command::Ping if self.is_subscribed() => vec![match args {
                [] => RedisValueRef::Array(vec![bulk_static(b"pong"), bulk_static(b"")]),
                [RedisValueRef::BulkString(message)] => RedisValueRef::Array(vec![
                    bulk_static(b"pong"),
                    RedisValueRef::BulkString(message.clone()),
                ]),
                [_] => invalid_argument(),
                _ => wrong_arity("ping"),
            }],
            Command::Ping => return None,
        })
    }

    fn subscribe(&mut self, args: &[RedisValueRef]) -> Vec<RedisValueRef> {
        let channels = match bulk_args(args, "subscribe") {
            Ok(channels) => channels,
            Err(e) => return vec![e],
        };
        channels
            .into_iter()
            .map(|channel| {
                if self.channels.insert(channel.clone()) {
                    self.pubsub
                        .add(channel.clone(), self.id, Arc::clone(&self.mailbox));
                }
                self.reply(b"subscribe", RedisValueRef::BulkString(channel))
            })
            .collect()
    }

    fn unsubscribe(&mut self, args: &[RedisValueRef]) -> Vec<RedisValueRef> {
        let channels = if args.is_empty() {
            if self.channels.is_empty() {
                return vec![self.reply(b"unsubscribe", RedisValueRef::NullBulkString)];
            }
            self.channels.iter().cloned().collect()
        } else {
            match bulk_args(args, "unsubscribe") {
                Ok(channels) => channels,
                Err(e) => return vec![e],
            }
        };
        channels
            .into_iter()
            .map(|channel| {
                if self.channels.remove(&channel) {
                    self.pubsub.remove(&channel, self.id);
                }
                self.reply(b"unsubscribe", RedisValueRef::BulkString(channel))
            })
            .collect()
    }

    pub fn reset(&mut self) {
        for channel in self.channels.drain() {
            self.pubsub.remove(&channel, self.id);
        }
        while let Ok(delivery) = self.rx.try_recv() {
            if let Delivery::Message(frame) = delivery {
                self.mailbox
                    .pending
                    .fetch_sub(frame.len(), Ordering::AcqRel);
            }
        }
    }

    fn reply(&self, kind: &'static [u8], channel: RedisValueRef) -> RedisValueRef {
        RedisValueRef::Array(vec![
            bulk_static(kind),
            channel,
            RedisValueRef::Int(self.channels.len() as i64),
        ])
    }
}

impl Drop for Subscriber {
    fn drop(&mut self) {
        for channel in &self.channels {
            self.pubsub.remove(channel, self.id);
        }
    }
}

fn publish(pubsub: &PubSub, args: &[RedisValueRef]) -> RedisValueRef {
    match args {
        [
            RedisValueRef::BulkString(channel),
            RedisValueRef::BulkString(message),
        ] => RedisValueRef::Int(pubsub.publish(channel, message) as i64),
        [_, _] => invalid_argument(),
        _ => wrong_arity("publish"),
    }
}

fn bulk_args(args: &[RedisValueRef], cmd: &str) -> Result<Vec<Bytes>, RedisValueRef> {
    if args.is_empty() {
        return Err(wrong_arity(cmd));
    }
    args.iter()
        .map(|arg| match arg {
            RedisValueRef::BulkString(b) => Ok(b.clone()),
            _ => Err(invalid_argument()),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cmd(parts: &[&str]) -> RedisValueRef {
        RedisValueRef::Array(
            parts
                .iter()
                .map(|part| RedisValueRef::BulkString(Bytes::copy_from_slice(part.as_bytes())))
                .collect(),
        )
    }

    fn ack(kind: &'static str, channel: &'static str, count: i64) -> RedisValueRef {
        RedisValueRef::Array(vec![
            bulk_static(kind.as_bytes()),
            bulk_static(channel.as_bytes()),
            RedisValueRef::Int(count),
        ])
    }

    #[test]
    fn subscribe_counts_distinct_channels() {
        let mut client = Subscriber::new(Arc::new(PubSub::new()));
        assert!(client.handle(&cmd(&["SUBSCRIBE", "a"])) == Some(vec![ack("subscribe", "a", 1)]));
        assert!(client.handle(&cmd(&["subscribe", "a"])) == Some(vec![ack("subscribe", "a", 1)]));
        assert!(
            client.handle(&cmd(&["SUBSCRIBE", "b", "c"]))
                == Some(vec![ack("subscribe", "b", 2), ack("subscribe", "c", 3)])
        );
    }

    #[test]
    fn subscribed_mode_restricts_commands_and_changes_ping() {
        let mut client = Subscriber::new(Arc::new(PubSub::new()));
        assert!(client.handle(&cmd(&["PING"])).is_none());
        assert!(client.handle(&cmd(&["ECHO", "hey"])).is_none());

        client.handle(&cmd(&["SUBSCRIBE", "foo"]));
        assert!(
            client.handle(&cmd(&["PING"]))
                == Some(vec![RedisValueRef::Array(vec![
                    bulk_static(b"pong"),
                    bulk_static(b"")
                ])])
        );
        assert!(
            client.handle(&cmd(&["ECHO", "hey"]))
                == Some(vec![RedisValueRef::ErrorMsg(
                    b"ERR Can't execute 'echo': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context".to_vec()
                )])
        );

        client.handle(&cmd(&["UNSUBSCRIBE", "foo"]));
        assert!(client.handle(&cmd(&["ECHO", "hey"])).is_none());
    }

    #[tokio::test]
    async fn publish_delivers_to_current_subscribers() {
        let pubsub = Arc::new(PubSub::new());
        let mut first = Subscriber::new(Arc::clone(&pubsub));
        let mut second = Subscriber::new(Arc::clone(&pubsub));
        first.handle(&cmd(&["SUBSCRIBE", "chan"]));
        second.handle(&cmd(&["SUBSCRIBE", "chan"]));

        assert!(
            pubsub.execute_publish(&cmd(&["PUBLISH", "chan", "hello"]))
                == Some(RedisValueRef::Int(2))
        );
        let expected = Bytes::from_static(b"*3\r\n$7\r\nmessage\r\n$4\r\nchan\r\n$5\r\nhello\r\n");
        assert_eq!(first.recv().await, Some(expected.clone()));
        assert_eq!(second.recv().await, Some(expected));

        second.handle(&cmd(&["UNSUBSCRIBE", "chan"]));
        drop(first);
        assert!(
            pubsub.execute_publish(&cmd(&["PUBLISH", "chan", "bye"]))
                == Some(RedisValueRef::Int(0))
        );
        assert!(pubsub.channels.is_empty());
    }

    #[test]
    fn unsubscribe_reports_remaining_count() {
        let mut client = Subscriber::new(Arc::new(PubSub::new()));
        client.handle(&cmd(&["SUBSCRIBE", "foo", "bar"]));
        assert!(
            client.handle(&cmd(&["UNSUBSCRIBE", "baz"]))
                == Some(vec![ack("unsubscribe", "baz", 2)])
        );
        assert!(
            client.handle(&cmd(&["UNSUBSCRIBE", "foo"]))
                == Some(vec![ack("unsubscribe", "foo", 1)])
        );
        assert!(
            client.handle(&cmd(&["UNSUBSCRIBE", "bar"]))
                == Some(vec![ack("unsubscribe", "bar", 0)])
        );
        assert!(
            client.handle(&cmd(&["UNSUBSCRIBE"]))
                == Some(vec![RedisValueRef::Array(vec![
                    bulk_static(b"unsubscribe"),
                    RedisValueRef::NullBulkString,
                    RedisValueRef::Int(0),
                ])])
        );
    }

    #[test]
    fn subscribed_ping_checks_arity() {
        let mut client = Subscriber::new(Arc::new(PubSub::new()));
        client.handle(&cmd(&["SUBSCRIBE", "foo"]));
        assert!(
            client.handle(&cmd(&["PING", "hi"]))
                == Some(vec![RedisValueRef::Array(vec![
                    bulk_static(b"pong"),
                    bulk_static(b"hi")
                ])])
        );
        assert!(client.handle(&cmd(&["PING", "a", "b"])) == Some(vec![wrong_arity("ping")]));
    }

    #[test]
    fn non_bulk_arguments_are_invalid_not_wrong_arity() {
        let mut client = Subscriber::new(Arc::new(PubSub::new()));
        let with_int = |name: &'static str, extra: usize| {
            let mut parts = vec![bulk_static(name.as_bytes())];
            parts.extend((0..extra).map(|_| bulk_static(b"chan")));
            parts.push(RedisValueRef::Int(1));
            RedisValueRef::Array(parts)
        };
        assert!(client.handle(&with_int("SUBSCRIBE", 0)) == Some(vec![invalid_argument()]));
        assert!(client.handle(&with_int("UNSUBSCRIBE", 0)) == Some(vec![invalid_argument()]));
        assert!(client.pubsub.execute_publish(&with_int("PUBLISH", 1)) == Some(invalid_argument()));
        assert!(client.handle(&cmd(&["SUBSCRIBE"])) == Some(vec![wrong_arity("subscribe")]));
    }

    #[tokio::test]
    async fn reset_leaves_every_channel_and_drops_pending_messages() {
        let pubsub = Arc::new(PubSub::new());
        let mut client = Subscriber::new(Arc::clone(&pubsub));
        client.handle(&cmd(&["SUBSCRIBE", "a", "b"]));
        pubsub.publish(&Bytes::from_static(b"a"), &Bytes::from_static(b"x"));

        client.reset();
        assert!(!client.is_subscribed());
        assert!(pubsub.channels.is_empty());
        assert!(client.rx.try_recv().is_err());
        assert!(client.handle(&cmd(&["ECHO", "hey"])).is_none());
    }

    #[tokio::test]
    async fn subscriber_over_output_limit_is_disconnected() {
        let pubsub = Arc::new(PubSub::with_output_limit(60));
        let mut slow = Subscriber::new(Arc::clone(&pubsub));
        let mut fast = Subscriber::new(Arc::clone(&pubsub));
        slow.handle(&cmd(&["SUBSCRIBE", "chan"]));
        fast.handle(&cmd(&["SUBSCRIBE", "chan"]));
        let channel = Bytes::from_static(b"chan");
        let message = Bytes::from_static(b"hello");

        assert_eq!(pubsub.publish(&channel, &message), 2);
        assert!(fast.recv().await.is_some());
        assert_eq!(pubsub.publish(&channel, &message), 1);
        assert!(fast.recv().await.is_some());
        assert_eq!(pubsub.publish(&channel, &message), 1);

        assert_eq!(slow.recv().await, None);
        assert!(fast.recv().await.is_some());
    }
}
