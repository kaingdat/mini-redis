use bytes::Bytes;
use rand::{Rng, SeedableRng, rngs::SmallRng};

const MAX_LEVEL: usize = 32;
const PROMOTE_BELOW: u32 = u32::MAX / 4;
const NIL: u32 = u32::MAX;
const HEAD: u32 = 0;

#[derive(Clone, Copy)]
struct Link {
    next: u32,
    span: u32,
    score: f64,
    key: u32,
}

const EMPTY: Link = Link {
    next: NIL,
    span: 0,
    score: 0.0,
    key: NIL,
};

#[derive(Clone)]
struct Meta {
    member: Bytes,
    score: f64,
    base: u32,
    height: u32,
    backward: u32,
}

#[derive(Clone)]
pub struct SkipList {
    links: Vec<Link>,
    metas: Vec<Meta>,
    free_keys: Vec<u32>,
    free_blocks: Vec<Vec<u32>>,
    level: usize,
    len: usize,
    tail: u32,
    rng: SmallRng,
}

impl Default for SkipList {
    fn default() -> Self {
        Self::new()
    }
}

impl SkipList {
    pub fn new() -> Self {
        Self::with_rng(rand::make_rng())
    }

    pub fn with_seed(seed: u64) -> Self {
        Self::with_rng(SmallRng::seed_from_u64(seed))
    }

    fn with_rng(rng: SmallRng) -> Self {
        Self {
            links: vec![EMPTY; MAX_LEVEL],
            metas: Vec::new(),
            free_keys: Vec::new(),
            free_blocks: vec![Vec::new(); MAX_LEVEL + 1],
            level: 1,
            len: 0,
            tail: NIL,
            rng,
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn insert(&mut self, score: f64, member: Bytes) {
        let mut update = [HEAD; MAX_LEVEL];
        let mut rank = [0u32; MAX_LEVEL];
        let mut x = HEAD;
        let mut x_key = NIL;
        for i in (0..self.level).rev() {
            rank[i] = if i + 1 == self.level { 0 } else { rank[i + 1] };
            loop {
                let link = self.links[x as usize + i];
                if link.next != NIL && self.before(&link, score, &member) {
                    rank[i] += link.span;
                    x = link.next;
                    x_key = link.key;
                } else {
                    break;
                }
            }
            update[i] = x;
        }

        let height = self.random_level();
        if height > self.level {
            for i in self.level..height {
                rank[i] = 0;
                update[i] = HEAD;
                self.links[HEAD as usize + i].span = self.len as u32;
            }
            self.level = height;
        }

        let base = self.alloc_block(height);
        let key = self.alloc_key(Meta {
            member,
            score,
            base,
            height: height as u32,
            backward: x_key,
        });

        for i in 0..height {
            let behind = rank[0] - rank[i];
            let prev = &mut self.links[update[i] as usize + i];
            let moved = Link {
                span: prev.span - behind,
                ..*prev
            };
            *prev = Link {
                next: base,
                span: behind + 1,
                score,
                key,
            };
            self.links[base as usize + i] = moved;
        }
        for i in height..self.level {
            self.links[update[i] as usize + i].span += 1;
        }

        match self.links[base as usize] {
            Link { next: NIL, .. } => self.tail = key,
            Link { key: next, .. } => self.metas[next as usize].backward = key,
        }
        self.len += 1;
    }

    pub fn remove(&mut self, score: f64, member: &Bytes) -> bool {
        let mut update = [HEAD; MAX_LEVEL];
        let mut x = HEAD;
        for i in (0..self.level).rev() {
            loop {
                let link = self.links[x as usize + i];
                if link.next != NIL && self.before(&link, score, member) {
                    x = link.next;
                } else {
                    break;
                }
            }
            update[i] = x;
        }

        let found = self.links[x as usize];
        if found.next == NIL
            || found.score != score
            || self.metas[found.key as usize].member != member
        {
            return false;
        }
        let target = found.next;
        let key = found.key;
        let height = self.metas[key as usize].height as usize;

        for (i, &prev) in update.iter().enumerate().take(self.level) {
            let prev = prev as usize + i;
            if i < height {
                let removed = self.links[target as usize + i];
                let span = self.links[prev].span + removed.span - 1;
                self.links[prev] = Link { span, ..removed };
            } else {
                self.links[prev].span -= 1;
            }
        }

        let backward = self.metas[key as usize].backward;
        match self.links[target as usize] {
            Link { next: NIL, .. } => self.tail = backward,
            Link { key: next, .. } => self.metas[next as usize].backward = backward,
        }
        while self.level > 1 && self.links[HEAD as usize + self.level - 1].next == NIL {
            self.level -= 1;
        }
        self.len -= 1;
        self.free_blocks[height].push(target);
        self.metas[key as usize].member = Bytes::new();
        self.free_keys.push(key);
        true
    }

    pub fn rank(&self, score: f64, member: &Bytes) -> Option<usize> {
        let mut rank = 0usize;
        let mut x = HEAD;
        let mut x_key = NIL;
        for i in (0..self.level).rev() {
            loop {
                let link = self.links[x as usize + i];
                if link.next != NIL && !self.after(&link, score, member) {
                    rank += link.span as usize;
                    x = link.next;
                    x_key = link.key;
                } else {
                    break;
                }
            }
            if x_key != NIL && self.metas[x_key as usize].member == member {
                return Some(rank - 1);
            }
        }
        None
    }

    pub fn iter_from(&self, rank: usize) -> Iter<'_> {
        Iter {
            list: self,
            cur: self.key_at(rank),
        }
    }

    fn key_at(&self, rank: usize) -> u32 {
        if rank >= self.len {
            return NIL;
        }
        let target = rank + 1;
        let mut traversed = 0usize;
        let mut x = HEAD;
        let mut x_key = NIL;
        for i in (0..self.level).rev() {
            loop {
                let link = self.links[x as usize + i];
                if link.next != NIL && traversed + link.span as usize <= target {
                    traversed += link.span as usize;
                    x = link.next;
                    x_key = link.key;
                } else {
                    break;
                }
            }
            if traversed == target {
                return x_key;
            }
        }
        NIL
    }

    fn before(&self, link: &Link, score: f64, member: &Bytes) -> bool {
        link.score < score || (link.score == score && self.metas[link.key as usize].member < member)
    }

    fn after(&self, link: &Link, score: f64, member: &Bytes) -> bool {
        link.score > score || (link.score == score && self.metas[link.key as usize].member > member)
    }

    fn random_level(&mut self) -> usize {
        let mut level = 1;
        while level < MAX_LEVEL && self.rng.next_u32() < PROMOTE_BELOW {
            level += 1;
        }
        level
    }

    fn alloc_block(&mut self, height: usize) -> u32 {
        match self.free_blocks[height].pop() {
            Some(base) => base,
            None => {
                let base = self.links.len() as u32;
                self.links.resize(self.links.len() + height, EMPTY);
                base
            }
        }
    }

    fn alloc_key(&mut self, meta: Meta) -> u32 {
        match self.free_keys.pop() {
            Some(key) => {
                self.metas[key as usize] = meta;
                key
            }
            None => {
                self.metas.push(meta);
                (self.metas.len() - 1) as u32
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn check(&self) {
        use std::collections::HashMap;

        let mut pos = HashMap::new();
        let mut live = 0usize;
        let mut prev_key = NIL;
        let mut link = self.links[HEAD as usize];
        while link.next != NIL {
            let meta = &self.metas[link.key as usize];
            assert_eq!(meta.base, link.next, "cached base");
            assert_eq!(meta.score.to_bits(), link.score.to_bits(), "cached score");
            if prev_key != NIL {
                let prev = &self.metas[prev_key as usize];
                assert!(
                    prev.score < meta.score
                        || (prev.score == meta.score && prev.member < meta.member),
                    "order broken"
                );
            }
            assert_eq!(meta.backward, prev_key, "backward link");
            pos.insert(link.next, pos.len() + 1);
            live += meta.height as usize;
            prev_key = link.key;
            link = self.links[meta.base as usize];
        }
        assert_eq!(pos.len(), self.len, "length");
        assert_eq!(self.tail, prev_key, "tail");
        assert_eq!(
            self.free_keys.len(),
            self.metas.len() - self.len,
            "free keys"
        );

        for i in 0..self.level {
            let mut x = HEAD;
            let mut at = 0usize;
            loop {
                let link = self.links[x as usize + i];
                if link.next == NIL {
                    assert_eq!(link.span as usize, self.len - at, "span to end, level {i}");
                    break;
                }
                let meta = &self.metas[link.key as usize];
                assert_eq!(meta.base, link.next, "cached base, level {i}");
                assert_eq!(
                    meta.score.to_bits(),
                    link.score.to_bits(),
                    "cached score, level {i}"
                );
                assert!(
                    meta.height as usize > i,
                    "link above node height, level {i}"
                );
                let next = pos[&link.next];
                assert_eq!(link.span as usize, next - at, "span, level {i}");
                x = link.next;
                at = next;
            }
        }
        assert!(self.level == 1 || self.links[HEAD as usize + self.level - 1].next != NIL);

        let free: usize = self
            .free_blocks
            .iter()
            .enumerate()
            .map(|(h, bases)| h * bases.len())
            .sum();
        assert_eq!(
            MAX_LEVEL + live + free,
            self.links.len(),
            "block accounting"
        );
    }
}

pub struct Iter<'a> {
    list: &'a SkipList,
    cur: u32,
}

impl<'a> Iterator for Iter<'a> {
    type Item = (&'a Bytes, f64);

    fn next(&mut self) -> Option<Self::Item> {
        if self.cur == NIL {
            return None;
        }
        let meta = &self.list.metas[self.cur as usize];
        let link = &self.list.links[meta.base as usize];
        self.cur = if link.next == NIL { NIL } else { link.key };
        Some((&meta.member, meta.score))
    }
}

#[cfg(test)]
mod tests {
    use std::cmp::Ordering;

    use rand::RngExt;

    use super::*;

    fn order(a: &(f64, Bytes), b: &(f64, Bytes)) -> Ordering {
        if a.0 < b.0 {
            Ordering::Less
        } else if a.0 > b.0 {
            Ordering::Greater
        } else {
            a.1.cmp(&b.1)
        }
    }

    fn contents(list: &SkipList) -> Vec<(f64, Bytes)> {
        list.iter_from(0).map(|(m, s)| (s, m.clone())).collect()
    }

    fn b(s: &str) -> Bytes {
        Bytes::copy_from_slice(s.as_bytes())
    }

    #[test]
    fn empty() {
        let list = SkipList::with_seed(1);
        list.check();
        assert_eq!(list.len(), 0);
        assert_eq!(list.rank(1.0, &b("a")), None);
        assert_eq!(list.iter_from(0).count(), 0);
    }

    #[test]
    fn one_member() {
        let mut list = SkipList::with_seed(1);
        list.insert(1.5, b("a"));
        list.check();
        assert_eq!(list.rank(1.5, &b("a")), Some(0));
        assert_eq!(contents(&list), vec![(1.5, b("a"))]);
        assert!(!list.remove(1.0, &b("a")));
        assert!(list.remove(1.5, &b("a")));
        list.check();
        assert!(list.is_empty());
        assert_eq!(list.level, 1);
    }

    #[test]
    fn equal_scores_order_by_member() {
        let mut list = SkipList::with_seed(2);
        for m in ["d", "b", "a", "c"] {
            list.insert(1.0, b(m));
        }
        list.check();
        let members: Vec<_> = list.iter_from(0).map(|(m, _)| m.clone()).collect();
        assert_eq!(members, vec![b("a"), b("b"), b("c"), b("d")]);
        assert_eq!(list.rank(1.0, &b("c")), Some(2));
    }

    #[test]
    fn negative_zero_ties_with_zero() {
        let mut list = SkipList::with_seed(3);
        list.insert(0.0, b("a"));
        list.insert(-0.0, b("b"));
        list.check();
        assert_eq!(list.rank(-0.0, &b("b")), Some(1));
    }

    #[test]
    fn remove_first_last_and_middle() {
        let mut list = SkipList::with_seed(4);
        for i in 0..50 {
            list.insert(f64::from(i), b(&format!("m{i:02}")));
        }
        assert!(list.remove(0.0, &b("m00")));
        list.check();
        assert!(list.remove(49.0, &b("m49")));
        list.check();
        assert!(list.remove(25.0, &b("m25")));
        list.check();
        assert_eq!(list.len(), 47);
        assert_eq!(list.iter_from(0).next().unwrap().0, &b("m01"));
        assert_eq!(list.iter_from(46).next().unwrap().0, &b("m48"));
        assert_eq!(list.iter_from(47).next(), None);
    }

    #[test]
    fn slots_are_reused() {
        let mut list = SkipList::with_seed(5);
        for round in 0..10 {
            for i in 0..100 {
                list.insert(f64::from(i), b(&format!("{round}:{i}")));
            }
            for i in 0..100 {
                assert!(list.remove(f64::from(i), &b(&format!("{round}:{i}"))));
            }
        }
        list.check();
        assert!(list.metas.len() <= 100);
    }

    #[test]
    fn matches_a_sorted_vec() {
        let scores = [
            -2.5,
            -0.0,
            0.0,
            1.0,
            1.0,
            3.0,
            f64::INFINITY,
            f64::NEG_INFINITY,
        ];
        for seed in 0..20 {
            let mut rng = SmallRng::seed_from_u64(seed);
            let mut list = SkipList::with_seed(seed);
            let mut model: Vec<(f64, Bytes)> = Vec::new();

            for step in 0..10_000 {
                let member = b(&format!("m{}", rng.random_range(0..300)));
                let existing = model.iter().position(|(_, m)| *m == member);
                match rng.random_range(0..4) {
                    0 | 1 => {
                        if let Some(at) = existing {
                            let (score, _) = model.remove(at);
                            assert!(list.remove(score, &member));
                        }
                        let score = scores[rng.random_range(0..scores.len())];
                        list.insert(score, member.clone());
                        let item = (score, member);
                        let at = model.partition_point(|e| order(e, &item) == Ordering::Less);
                        model.insert(at, item);
                    }
                    2 => match existing {
                        Some(at) => {
                            let (score, _) = model.remove(at);
                            assert!(list.remove(score, &member));
                        }
                        None => assert!(!list.remove(1.0, &member)),
                    },
                    _ => {
                        let expected = existing.map(|at| (model[at].0, at));
                        let got = expected.and_then(|(score, _)| list.rank(score, &member));
                        assert_eq!(got, expected.map(|(_, at)| at));
                        let start = rng.random_range(0..=model.len());
                        let got: Vec<_> = list
                            .iter_from(start)
                            .take(5)
                            .map(|(m, s)| (s, m.clone()))
                            .collect();
                        let want: Vec<_> = model.iter().skip(start).take(5).cloned().collect();
                        assert_eq!(got.len(), want.len());
                        for (g, w) in got.iter().zip(&want) {
                            assert_eq!(g.1, w.1);
                            assert_eq!(g.0.to_bits(), w.0.to_bits());
                        }
                    }
                }
                assert_eq!(list.len(), model.len());
                if step % 100 == 0 {
                    list.check();
                }
            }
            list.check();
            assert_eq!(contents(&list).len(), model.len());
        }
    }
}
