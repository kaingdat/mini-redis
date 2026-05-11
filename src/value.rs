use std::collections::{BTreeSet, HashMap};

use bytes::Bytes;

#[derive(Clone)]
pub enum RedisValue {
    String(Bytes),
    SortedSet(SortedSetData),
}

#[derive(Clone)]
pub struct SortedSetData {
    members: HashMap<Bytes, f64>,
    scores: BTreeSet<OrderedScore>,
}

impl SortedSetData {
    pub fn new() -> Self {
        Self {
            members: HashMap::new(),
            scores: BTreeSet::new(),
        }
    }

    pub fn add(&mut self, member: Bytes, score: f64) -> bool {
        if let Some(&old_score) = self.members.get(&member) {
            if old_score != score {
                self.scores
                    .remove(&OrderedScore::new(old_score, member.clone()));
                self.scores.insert(OrderedScore::new(score, member.clone()));
                self.members.insert(member, score);
            }
            false
        } else {
            self.scores.insert(OrderedScore::new(score, member.clone()));
            self.members.insert(member, score);
            true
        }
    }

    pub fn len(&self) -> usize {
        self.members.len()
    }

    pub fn rank(&self, member: &Bytes) -> Option<i64> {
        let score = self.members.get(member)?;
        let target = OrderedScore::new(*score, member.clone());
        Some(self.scores.range(..target).count() as i64)
    }

    pub fn score(&self, member: &Bytes) -> Option<f64> {
        self.members.get(member).copied()
    }

    pub fn range(&self, start: i64, stop: i64) -> Vec<Bytes> {
        let len = self.scores.len() as i64;
        if len == 0 {
            return vec![];
        }
        let resolve = |idx: i64| -> i64 {
            if idx < 0 {
                (len + idx).max(0)
            } else {
                idx
            }
        };
        let start = resolve(start) as usize;
        let stop = resolve(stop).min(len - 1) as usize;
        if start as i64 >= len {
            return vec![];
        }
        if start > stop {
            return vec![];
        }
        self.scores
            .iter()
            .skip(start)
            .take(stop - start + 1)
            .map(|e| e.member.clone())
            .collect()
    }
}

#[derive(PartialEq, Eq, Clone)]
pub struct OrderedScore {
    score_bits: u64,
    member: Bytes,
}

impl OrderedScore {
    pub fn new(score: f64, member: Bytes) -> Self {
        let bits = score.to_bits();
        let score_bits = if (bits >> 63) == 0 {
            bits ^ 0x8000_0000_0000_0000
        } else {
            !bits
        };
        Self { score_bits, member }
    }
}

impl PartialOrd for OrderedScore {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OrderedScore {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match self.score_bits.cmp(&other.score_bits) {
            std::cmp::Ordering::Equal => self.member.cmp(&other.member),
            other => other,
        }
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
        assert!(!zset.add(Bytes::from("member1"), 2.0)); // update
        assert_eq!(zset.len(), 1);
    }

    #[test]
    fn test_ordered_score_sorting() {
        let s1 = OrderedScore::new(1.0, Bytes::from("a"));
        let s2 = OrderedScore::new(2.0, Bytes::from("b"));
        let s3 = OrderedScore::new(-1.0, Bytes::from("c"));

        assert!(s3 < s1); // -1 < 1
        assert!(s1 < s2); // 1 < 2
    }

    #[test]
    fn test_lexicographic_tiebreak() {
        let s1 = OrderedScore::new(1.0, Bytes::from("apple"));
        let s2 = OrderedScore::new(1.0, Bytes::from("banana"));
        let s3 = OrderedScore::new(1.0, Bytes::from("cherry"));

        assert!(s1 < s2);
        assert!(s2 < s3);
    }
}
