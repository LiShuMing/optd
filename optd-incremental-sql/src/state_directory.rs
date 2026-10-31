//! Ordered persistent directories for isolated candidates, with actual path-copy work.

use std::cmp::Ordering;
use std::sync::Arc;

use crate::{Error, Result};

type Link<V> = Option<Arc<Node<V>>>;

/// An AVL branch owns one directory entry and shares its unchanged subtrees.
#[derive(Clone, Debug)]
struct Node<V> {
    key: Vec<u8>,
    value: V,
    left: Link<V>,
    right: Link<V>,
    height: usize,
    len: usize,
}

/// Byte-key directory; production values share immutable payloads through Arc.
/// Cloning shares its root and starts a fresh candidate-local copy counter.
#[derive(Debug)]
pub(crate) struct StateDirectory<V> {
    root: Link<V>,
    copies: Option<usize>,
}

impl<V> Clone for StateDirectory<V> {
    fn clone(&self) -> Self {
        Self {
            root: self.root.clone(),
            copies: Some(0),
        }
    }
}

impl<V> Default for StateDirectory<V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<V> StateDirectory<V> {
    /// Start an empty revision without allocating a root node.
    pub(crate) fn new() -> Self {
        Self {
            root: None,
            copies: Some(0),
        }
    }
    /// Return cached subtree cardinality without traversing entries.
    pub(crate) fn len(&self) -> usize {
        length(&self.root)
    }
    /// An absent root represents the only empty revision.
    pub(crate) fn is_empty(&self) -> bool {
        self.root.is_none()
    }

    /// Actual shared node clones; overflow fails the enclosing candidate accounting.
    pub(crate) fn copied_entries(&self) -> Result<usize> {
        self.copies.ok_or(Error::Overflow)
    }

    /// Borrow a payload using exact byte equality, without cloning keys or nodes.
    pub(crate) fn get(&self, key: &[u8]) -> Option<&V> {
        let mut cursor = self.root.as_deref();
        while let Some(node) = cursor {
            match key.cmp(&node.key) {
                Ordering::Less => cursor = node.left.as_deref(),
                Ordering::Greater => cursor = node.right.as_deref(),
                Ordering::Equal => return Some(&node.value),
            }
        }
        None
    }

    /// Compare payloads only when keys have the same tree alignment.
    /// Shared subtrees require no traversal or payload calls. A shape mismatch is
    /// inconclusive: callers with coarser logical identities must use an exact fallback.
    pub(crate) fn aligned_values_equal(
        &self,
        other: &Self,
        mut equal: impl FnMut(&V, &V) -> bool,
    ) -> Option<bool> {
        if !same_key_shape(&self.root, &other.root) {
            return None;
        }
        Some(same_aligned_values(&self.root, &other.root, &mut equal))
    }

    /// Inclusive predecessor used to locate a table leaf without a directory scan.
    pub(crate) fn predecessor(&self, key: &[u8]) -> Option<(&Vec<u8>, &V)> {
        let mut cursor = self.root.as_deref();
        let mut found = None;
        while let Some(node) = cursor {
            if key < node.key.as_slice() {
                cursor = node.left.as_deref();
            } else {
                found = Some((&node.key, &node.value));
                cursor = node.right.as_deref();
            }
        }
        found
    }

    /// Find the smallest boundary for inserts before a table's first leaf.
    pub(crate) fn first_key_value(&self) -> Option<(&Vec<u8>, &V)> {
        let mut node = self.root.as_deref()?;
        while let Some(left) = node.left.as_deref() {
            node = left;
        }
        Some((&node.key, &node.value))
    }

    /// Read the maximum value without walking or collecting unrelated directory entries.
    pub(crate) fn last_key_value(&self) -> Option<(&Vec<u8>, &V)> {
        let mut node = self.root.as_deref()?;
        while let Some(right) = node.right.as_deref() {
            node = right;
        }
        Some((&node.key, &node.value))
    }

    /// Find the next leaf without collecting directory handles or borrowing a cursor stack.
    pub(crate) fn successor(&self, key: &[u8]) -> Option<(&Vec<u8>, &V)> {
        let mut cursor = self.root.as_deref();
        let mut found = None;
        while let Some(node) = cursor {
            if key < node.key.as_slice() {
                found = Some((&node.key, &node.value));
                cursor = node.left.as_deref();
            } else {
                cursor = node.right.as_deref();
            }
        }
        found
    }

    /// Traverse one borrowed immutable revision in ascending byte-key order.
    pub(crate) fn iter(&self) -> DirectoryIter<'_, V> {
        let mut iter = DirectoryIter { stack: Vec::new() };
        iter.push_left(self.root.as_deref());
        iter
    }
    /// Borrow ordered keys for index alignment and affected-key comparisons.
    pub(crate) fn keys(&self) -> impl Iterator<Item = &Vec<u8>> {
        self.iter().map(|(k, _)| k)
    }
    /// Borrow payloads in key order for ordered table scans.
    pub(crate) fn values(&self) -> impl Iterator<Item = &V> {
        self.iter().map(|(_, v)| v)
    }
}

impl<V: Clone> StateDirectory<V> {
    /// Replace one exact key and rebalance only the affected path.
    pub(crate) fn insert(&mut self, key: Vec<u8>, value: V) -> Option<V> {
        let (root, old) = insert(self.root.take(), key, value, &mut self.copies);
        self.root = root;
        old
    }

    /// Remove one exact key; an absent key retains the existing root and counter.
    pub(crate) fn remove(&mut self, key: &[u8]) -> Option<V> {
        // Failed lookups must not clone an otherwise unchanged path.
        self.get(key)?;
        let (root, old) = remove(self.root.take(), key, &mut self.copies);
        self.root = root;
        old
    }
}

impl<V: Clone> FromIterator<(Vec<u8>, V)> for StateDirectory<V> {
    fn from_iter<T: IntoIterator<Item = (Vec<u8>, V)>>(iter: T) -> Self {
        let mut result = Self::new();
        for (key, value) in iter {
            result.insert(key, value);
        }
        result
    }
}

/// Alignment is proven before a payload difference can imply logical inequality.
fn same_key_shape<V>(left: &Link<V>, right: &Link<V>) -> bool {
    match (left, right) {
        (None, None) => true,
        (Some(left), Some(right)) if Arc::ptr_eq(left, right) => true,
        (Some(left), Some(right)) => {
            left.key == right.key
                && same_key_shape(&left.left, &right.left)
                && same_key_shape(&left.right, &right.right)
        }
        _ => false,
    }
}

/// Called only after complete key alignment; shared immutable branches are exact.
fn same_aligned_values<V>(
    left: &Link<V>,
    right: &Link<V>,
    equal: &mut impl FnMut(&V, &V) -> bool,
) -> bool {
    match (left, right) {
        (None, None) => true,
        (Some(left), Some(right)) if Arc::ptr_eq(left, right) => true,
        (Some(left), Some(right)) => {
            equal(&left.value, &right.value)
                && same_aligned_values(&left.left, &right.left, equal)
                && same_aligned_values(&left.right, &right.right, equal)
        }
        _ => false,
    }
}

/// Borrowed ordered traversal retains one revision and at most one AVL path.
pub(crate) struct DirectoryIter<'a, V> {
    stack: Vec<&'a Node<V>>,
}
impl<'a, V> DirectoryIter<'a, V> {
    fn push_left(&mut self, mut node: Option<&'a Node<V>>) {
        while let Some(next) = node {
            self.stack.push(next);
            node = next.left.as_deref();
        }
    }
}
impl<'a, V> Iterator for DirectoryIter<'a, V> {
    type Item = (&'a Vec<u8>, &'a V);
    fn next(&mut self) -> Option<Self::Item> {
        let node = self.stack.pop()?;
        self.push_left(node.right.as_deref());
        Some((&node.key, &node.value))
    }
}

fn height<V>(node: &Link<V>) -> usize {
    node.as_ref().map_or(0, |n| n.height)
}
fn length<V>(node: &Link<V>) -> usize {
    node.as_ref().map_or(0, |n| n.len)
}

impl<V> Node<V> {
    fn update(&mut self) {
        // Each node is a distinct nonzero-sized allocation in an acyclic AVL tree;
        // its length fits addressable memory, and height is bounded by that length.
        self.height = height(&self.left).max(height(&self.right)) + 1;
        self.len = length(&self.left) + length(&self.right) + 1;
    }
}

/// Move exclusive nodes; count each shared old entry immediately before cloning it.
fn own<V: Clone>(node: Arc<Node<V>>, copies: &mut Option<usize>) -> Node<V> {
    match Arc::try_unwrap(node) {
        Ok(node) => node,
        Err(shared) => {
            // Count this explicit clone, even if another owner drops concurrently.
            *copies = copies.and_then(|n| n.checked_add(1));
            shared.as_ref().clone()
        }
    }
}

fn rotate_left<V: Clone>(mut node: Node<V>, copies: &mut Option<usize>) -> Arc<Node<V>> {
    // Only balance-right and double-rotation callers use this; right is nonempty.
    let mut pivot = own(
        node.right.take().expect("AVL right rotation operand"),
        copies,
    );
    node.right = pivot.left.take();
    node.update();
    pivot.left = Some(Arc::new(node));
    pivot.update();
    Arc::new(pivot)
}
fn rotate_right<V: Clone>(mut node: Node<V>, copies: &mut Option<usize>) -> Arc<Node<V>> {
    // Only balance-left and double-rotation callers use this; left is nonempty.
    let mut pivot = own(node.left.take().expect("AVL left rotation operand"), copies);
    node.left = pivot.right.take();
    node.update();
    pivot.right = Some(Arc::new(node));
    pivot.update();
    Arc::new(pivot)
}
fn balance<V: Clone>(mut node: Node<V>, copies: &mut Option<usize>) -> Arc<Node<V>> {
    node.update();
    if height(&node.left) > height(&node.right) + 1 {
        // A height excess of two proves the left child exists.
        let left = node.left.as_ref().expect("AVL left-heavy branch");
        if height(&left.left) < height(&left.right) {
            let left = own(node.left.take().expect("AVL left-heavy branch"), copies);
            node.left = Some(rotate_left(left, copies));
        }
        rotate_right(node, copies)
    } else if height(&node.right) > height(&node.left) + 1 {
        // A height excess of two proves the right child exists.
        let right = node.right.as_ref().expect("AVL right-heavy branch");
        if height(&right.right) < height(&right.left) {
            let right = own(node.right.take().expect("AVL right-heavy branch"), copies);
            node.right = Some(rotate_right(right, copies));
        }
        rotate_left(node, copies)
    } else {
        Arc::new(node)
    }
}
fn insert<V: Clone>(
    link: Link<V>,
    key: Vec<u8>,
    value: V,
    copies: &mut Option<usize>,
) -> (Link<V>, Option<V>) {
    let Some(link) = link else {
        return (
            Some(Arc::new(Node {
                key,
                value,
                left: None,
                right: None,
                height: 1,
                len: 1,
            })),
            None,
        );
    };
    let mut node = own(link, copies);
    let old = match key.cmp(&node.key) {
        Ordering::Less => {
            let (branch, old) = insert(node.left.take(), key, value, copies);
            node.left = branch;
            old
        }
        Ordering::Greater => {
            let (branch, old) = insert(node.right.take(), key, value, copies);
            node.right = branch;
            old
        }
        Ordering::Equal => Some(std::mem::replace(&mut node.value, value)),
    };
    (Some(balance(node, copies)), old)
}

/// Extract the minimum entry by moving its owned node, including delete successors.
fn pop_first<V: Clone>(link: Arc<Node<V>>, copies: &mut Option<usize>) -> (Link<V>, Node<V>) {
    let mut node = own(link, copies);
    let Some(left) = node.left.take() else {
        return (node.right.take(), node);
    };
    let (branch, first) = pop_first(left, copies);
    node.left = branch;
    (Some(balance(node, copies)), first)
}
fn remove<V: Clone>(link: Link<V>, key: &[u8], copies: &mut Option<usize>) -> (Link<V>, Option<V>) {
    let Some(link) = link else {
        return (None, None);
    };
    let mut node = own(link, copies);
    let old = match key.cmp(&node.key) {
        Ordering::Less => {
            let (branch, old) = remove(node.left.take(), key, copies);
            node.left = branch;
            old
        }
        Ordering::Greater => {
            let (branch, old) = remove(node.right.take(), key, copies);
            node.right = branch;
            old
        }
        Ordering::Equal => {
            let old = node.value;
            return match (node.left, node.right) {
                (None, right) => (right, Some(old)),
                (left, None) => (left, Some(old)),
                (left, Some(right)) => {
                    let (remaining, mut successor) = pop_first(right, copies);
                    successor.left = left;
                    successor.right = remaining;
                    (Some(balance(successor, copies)), Some(old))
                }
            };
        }
    };
    (Some(balance(node, copies)), old)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

    struct CountedValue(Arc<AtomicUsize>);

    impl Clone for CountedValue {
        fn clone(&self) -> Self {
            self.0.fetch_add(1, AtomicOrdering::Relaxed);
            Self(Arc::clone(&self.0))
        }
    }

    #[test]
    fn copy_diagnostics_match_actual_value_clones_through_rotations_and_successors() -> Result<()> {
        let clones = Arc::new(AtomicUsize::new(0));
        let mut installed = StateDirectory::new();
        for step in 0u64..2048 {
            let mut candidate = installed.clone();
            let before = clones.load(AtomicOrdering::Relaxed);
            // Monotone inserts force rotations; interior deletes extract successors.
            if step < 1024 {
                candidate.insert(key(step), CountedValue(Arc::clone(&clones)));
            } else {
                candidate.remove(&key((step - 1024) * 397 % 1024));
            }
            assert_eq!(
                candidate.copied_entries()?,
                clones.load(AtomicOrdering::Relaxed) - before
            );
            check(&candidate.root, None, None);
            check(&installed.root, None, None);
            installed = candidate;
        }
        assert!(installed.is_empty());
        Ok(())
    }

    fn key(k: u64) -> Vec<u8> {
        k.to_be_bytes().to_vec()
    }
    fn check<V>(link: &Link<V>, lo: Option<&[u8]>, hi: Option<&[u8]>) -> (usize, usize) {
        let Some(node) = link else {
            return (0, 0);
        };
        assert!(lo.is_none_or(|k| k < node.key.as_slice()));
        assert!(hi.is_none_or(|k| node.key.as_slice() < k));
        let (lh, ll) = check(&node.left, lo, Some(&node.key));
        let (rh, rl) = check(&node.right, Some(&node.key), hi);
        assert!(lh.abs_diff(rh) <= 1);
        assert_eq!(node.height, lh.max(rh) + 1);
        assert_eq!(node.len, ll + rl + 1);
        (node.height, node.len)
    }
    fn assert_model(d: &StateDirectory<Arc<u64>>, m: &BTreeMap<Vec<u8>, Arc<u64>>) {
        check(&d.root, None, None);
        assert_eq!(d.len(), m.len());
        assert_eq!(d.is_empty(), m.is_empty());
        assert!(d.iter().eq(m.iter()));
        assert!(d.keys().eq(m.keys()));
        assert!(d.values().eq(m.values()));
        assert_eq!(d.first_key_value(), m.first_key_value());
        assert_eq!(d.last_key_value(), m.last_key_value());
        for k in [0, 1, 7, 64, 127, 512, 1025, u64::MAX] {
            assert_eq!(d.get(&key(k)), m.get(&key(k)));
            assert_eq!(d.predecessor(&key(k)), m.range(..=key(k)).next_back());
            assert_eq!(
                d.successor(&key(k)),
                m.range((
                    std::ops::Bound::Excluded(key(k)),
                    std::ops::Bound::Unbounded
                ))
                .next()
            );
        }
    }
    #[test]
    fn deterministic_rotations_deletes_and_retained_roots_match_btree() {
        let mut d = StateDirectory::new();
        let mut m = BTreeMap::new();
        let mut retained = Vec::new();
        let mut seed = 0x3278u64;
        for round in 0usize..4000 {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            let k = key((seed >> 32) % 1024);
            if round.is_multiple_of(3) {
                assert_eq!(d.remove(&k), m.remove(&k));
            } else {
                let v = Arc::new(seed);
                assert_eq!(d.insert(k.clone(), v.clone()), m.insert(k, v));
            }
            assert_model(&d, &m);
            if round.is_multiple_of(400) {
                retained.push((d.clone(), m.clone()));
            }
        }
        for (old, model) in &retained {
            assert_model(old, model);
        }
        for k in m.keys().cloned().collect::<Vec<_>>() {
            assert_eq!(d.remove(&k), m.remove(&k));
            assert_model(&d, &m);
        }
        for (old, model) in &retained {
            assert_model(old, model);
        }
    }
    #[test]
    fn clone_and_missing_deletes_copy_zero_and_updates_share_unaffected_payloads() -> Result<()> {
        for n in [256u64, 4096, 65536] {
            let d = (0..n)
                .map(|k| (key(k), Arc::new(k)))
                .collect::<StateDirectory<_>>();
            assert_eq!(d.copied_entries()?, 0); // Exclusive bootstrap moves nodes.
            let mut next = d.clone();
            assert_eq!(next.copied_entries()?, 0);
            assert!(next.remove(&key(n)).is_none());
            assert_eq!(next.copied_entries()?, 0);
            next.insert(key(0), Arc::new(n));
            let path = next.copied_entries()?;
            assert!(path > 0 && path <= d.root.as_ref().unwrap().height);
            assert!(Arc::ptr_eq(
                next.get(&key(n - 1)).unwrap(),
                d.get(&key(n - 1)).unwrap()
            ));
            assert_eq!(**d.get(&key(0)).unwrap(), 0);
            assert_eq!(**next.get(&key(0)).unwrap(), n);
            next.remove(&key(n / 2));
            next.insert(key(n + 1), Arc::new(n + 1));
            assert!(next.copied_entries()? <= 6 * (n.ilog2() as usize + 1));
            check(&next.root, None, None);
            check(&d.root, None, None);
        }
        Ok(())
    }
    #[test]
    fn copy_counter_overflow_is_reported_and_exclusive_moves_are_not_copies() {
        let mut before = StateDirectory::new();
        before.insert(key(1), Arc::new(1));
        let mut next = before.clone();
        next.copies = Some(usize::MAX);
        next.insert(key(1), Arc::new(2));
        assert!(matches!(next.copied_entries(), Err(Error::Overflow)));
        assert_eq!(**before.get(&key(1)).unwrap(), 1);
    }
}

#[cfg(test)]
mod aligned_comparison_tests {
    use super::*;

    #[test]
    fn shared_paths_skip_payload_work_and_shape_changes_are_inconclusive() {
        let before = (0..4096u64)
            .map(|k| (k.to_be_bytes().to_vec(), k))
            .collect::<StateDirectory<_>>();
        let mut calls = 0usize;
        assert_eq!(
            before.aligned_values_equal(&before.clone(), |a, b| {
                calls += 1;
                a == b
            }),
            Some(true)
        );
        assert_eq!(calls, 0);
        let mut equal_revision = before.clone();
        equal_revision.insert(2048u64.to_be_bytes().to_vec(), 2048);
        assert_eq!(
            before.aligned_values_equal(&equal_revision, |a, b| {
                calls += 1;
                a == b
            }),
            Some(true)
        );
        assert!(calls <= 3 * 4096usize.ilog2() as usize);
        let mut changed = before.clone();
        changed.insert(2048u64.to_be_bytes().to_vec(), 99);
        assert_eq!(
            before.aligned_values_equal(&changed, |a, b| a == b),
            Some(false)
        );
        let mut different_keys = before.clone();
        different_keys.insert(5000u64.to_be_bytes().to_vec(), 5000);
        assert_eq!(
            before.aligned_values_equal(&different_keys, |a, b| a == b),
            None
        );
        let empty = StateDirectory::<u64>::new();
        assert_eq!(
            empty.aligned_values_equal(&empty, |a, b| a == b),
            Some(true)
        );
    }
}
