use std::collections::HashMap;

use bytes::Bytes;
use tokio::time::Instant;

use crate::{skiplist::SkipList, stream::StreamData};

#[derive(Clone)]
pub enum RedisValue {
    String(Bytes),
    SortedSet(SortedSetData),
    Stream(StreamData),
}

pub struct ValueEntry {
    pub data: RedisValue,
    pub expires_at: Option<Instant>,
}

impl ValueEntry {
    pub fn new(data: RedisValue, expires_at: Option<Instant>) -> Self {
        Self { data, expires_at }
    }

    pub fn is_expired(&self) -> bool {
        self.expires_at.is_some_and(|exp| Instant::now() >= exp)
    }
}

#[derive(Clone, Default)]
pub struct SortedSetData {
    members: HashMap<Bytes, f64>,
    list: SkipList,
}

impl SortedSetData {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add(&mut self, member: Bytes, score: f64) -> bool {
        match self.members.get(&member) {
            Some(&old) => {
                if old != score {
                    self.list.remove(old, &member);
                    self.list.insert(score, member.clone());
                    self.members.insert(member, score);
                }
                false
            }
            None => {
                self.list.insert(score, member.clone());
                self.members.insert(member, score);
                true
            }
        }
    }

    pub fn len(&self) -> usize {
        self.members.len()
    }

    pub fn is_empty(&self) -> bool {
        self.members.is_empty()
    }

    pub fn rank(&self, member: &Bytes) -> Option<i64> {
        let score = *self.members.get(member)?;
        self.list.rank(score, member).map(|r| r as i64)
    }

    pub fn score(&self, member: &Bytes) -> Option<f64> {
        self.members.get(member).copied()
    }

    pub fn remove(&mut self, member: &Bytes) -> bool {
        match self.members.remove(member) {
            Some(score) => self.list.remove(score, member),
            None => false,
        }
    }

    pub fn range_by_score(
        &self,
        min: f64,
        max_exclusive: f64,
    ) -> impl Iterator<Item = (&Bytes, f64)> {
        self.list
            .iter_from_score(min)
            .take_while(move |&(_, score)| score < max_exclusive)
    }

    pub fn range(&self, start: i64, stop: i64) -> Vec<Bytes> {
        let len = self.list.len() as i64;
        if len == 0 {
            return vec![];
        }
        let resolve = |idx: i64| -> i64 { if idx < 0 { (len + idx).max(0) } else { idx } };
        let start = resolve(start);
        let stop = resolve(stop).min(len - 1);
        if start >= len || start > stop {
            return vec![];
        }
        self.list
            .iter_from(start as usize)
            .take((stop - start + 1) as usize)
            .map(|(m, _)| m.clone())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sorted_set_add() {
        let mut zset = SortedSetData::new();
        assert!(zset.add(Bytes::from("member1"), 1.0));
        assert_eq!(zset.len(), 1);
        assert!(!zset.add(Bytes::from("member1"), 2.0));
        assert_eq!(zset.len(), 1);
    }

    #[test]
    fn rank_and_range_follow_score_then_member() {
        let mut zset = SortedSetData::new();
        for (m, score) in [("c", 2.0), ("a", 1.0), ("b", 1.0), ("d", -1.0)] {
            zset.add(Bytes::from(m), score);
        }
        assert_eq!(zset.rank(&Bytes::from("d")), Some(0));
        assert_eq!(zset.rank(&Bytes::from("b")), Some(2));
        assert_eq!(zset.range(-2, -1), vec![Bytes::from("b"), Bytes::from("c")]);
        assert_eq!(
            zset.range(1, 100),
            vec![Bytes::from("a"), Bytes::from("b"), Bytes::from("c")]
        );
        assert!(zset.range(3, 1).is_empty());
        assert!(zset.range(4, 10).is_empty());
    }

    #[test]
    fn updating_a_score_moves_the_member() {
        let mut zset = SortedSetData::new();
        zset.add(Bytes::from("a"), 1.0);
        zset.add(Bytes::from("b"), 2.0);
        zset.add(Bytes::from("a"), 3.0);
        assert_eq!(zset.rank(&Bytes::from("a")), Some(1));
        assert_eq!(zset.score(&Bytes::from("a")), Some(3.0));
        assert!(zset.remove(&Bytes::from("a")));
        assert!(!zset.remove(&Bytes::from("a")));
        assert_eq!(zset.range(0, -1), vec![Bytes::from("b")]);
    }

    #[test]
    fn negative_zero_ties_with_zero_and_orders_by_member() {
        let mut zset = SortedSetData::new();
        zset.add(Bytes::from("a"), 0.0);
        zset.add(Bytes::from("b"), -0.0);
        assert_eq!(zset.range(0, -1), vec![Bytes::from("a"), Bytes::from("b")]);
    }
}
