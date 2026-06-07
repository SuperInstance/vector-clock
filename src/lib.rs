//! # Vector Clock
//!
//! A library implementing vector clocks for causal ordering in distributed
//! systems. Vector clocks allow nodes to determine the causal relationships
//! between events without a central coordinator.
//!
//! ## Overview
//!
//! Vector clocks track causal dependencies between events across distributed
//! nodes. Each node maintains a vector of counters, one per known node. When
//! a node performs a local event, it increments its own counter. When nodes
//! communicate, they exchange and merge their vectors.
//!
//! This enables:
//! - **Causal ordering**: Determine if event A happened before event B
//! - **Concurrency detection**: Detect when events are independent
//! - **Conflict resolution**: Identify conflicting updates that need resolution
//!
//! ## Example
//!
//! ```
//! use vector_clock::VectorClock;
//!
//! let mut vc1 = VectorClock::new("node-1");
//! let mut vc2 = VectorClock::new("node-2");
//!
//! vc1.increment();
//! vc1.increment();
//!
//! // node-2 receives from node-1
//! vc2.merge_in_place(&vc1);
//! vc2.increment();
//!
//! assert!(vc1.happened_before(&vc2));
//! assert!(!vc2.happened_before(&vc1));
//! ```

use std::collections::HashMap;
use std::fmt;

/// A vector clock tracking causal timestamps across distributed nodes.
///
/// Each entry maps a node ID to a counter representing the number of events
/// that node knows about. The vector clock defines a partial order over events.
///
/// ## Properties
///
/// - **Reflexive**: VC ≤ VC (every clock is comparable to itself)
/// - **Antisymmetric**: if A ≤ B and B ≤ A, then A = B
/// - **Transitive**: if A ≤ B and B ≤ C, then A ≤ C
///
/// Together these form a **partial order** suitable for causal reasoning.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VectorClock {
    /// The clock vector: node_id → counter.
    clock: HashMap<String, u64>,
    /// This node's identifier.
    node_id: String,
}

impl VectorClock {
    /// Create a new vector clock for the given node.
    pub fn new(node_id: &str) -> Self {
        let mut clock = HashMap::new();
        clock.insert(node_id.to_string(), 0);
        Self {
            clock,
            node_id: node_id.to_string(),
        }
    }

    /// Create a vector clock from a raw map (for testing/deserialization).
    pub fn from_map(clock: HashMap<String, u64>) -> Self {
        Self {
            clock,
            node_id: String::new(),
        }
    }

    /// Get this node's ID.
    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    /// Increment this node's counter (local event).
    pub fn increment(&mut self) -> u64 {
        let count = self.clock.entry(self.node_id.clone()).or_insert(0);
        *count += 1;
        *count
    }

    /// Increment a specific node's counter (for testing / external events).
    pub fn increment_node(&mut self, node_id: &str) -> u64 {
        let count = self.clock.entry(node_id.to_string()).or_insert(0);
        *count += 1;
        *count
    }

    /// Get the counter value for a specific node.
    pub fn get(&self, node_id: &str) -> u64 {
        self.clock.get(node_id).copied().unwrap_or(0)
    }

    /// Get all node IDs in this clock.
    pub fn nodes(&self) -> Vec<String> {
        self.clock.keys().cloned().collect()
    }

    /// Number of nodes tracked by this clock.
    pub fn node_count(&self) -> usize {
        self.clock.len()
    }

    /// Check if this clock happened before another (strict partial order).
    ///
    /// A happened-before B iff:
    /// - For every node i: A[i] ≤ B[i]
    /// - AND there exists at least one node j: A[j] < B[j]
    ///
    /// This implements Lamport's happened-before relation (→) using vector clocks.
    pub fn happened_before(&self, other: &VectorClock) -> bool {
        let all_nodes: std::collections::HashSet<&String> =
            self.clock.keys().chain(other.clock.keys()).collect();

        let mut at_least_one_less = false;
        for node in &all_nodes {
            let a = self.clock.get(*node).copied().unwrap_or(0);
            let b = other.clock.get(*node).copied().unwrap_or(0);
            if a > b {
                return false;
            }
            if a < b {
                at_least_one_less = true;
            }
        }
        at_least_one_less
    }

    /// Check if two clocks are concurrent (neither happened before the other).
    ///
    /// A ∥ B iff ¬(A → B) ∧ ¬(B → A)
    ///
    /// Concurrent events cannot be causally ordered and may represent conflicts.
    pub fn is_concurrent(&self, other: &VectorClock) -> bool {
        !self.happened_before(other) && !other.happened_before(self) && self != other
    }

    /// Merge this clock with another (component-wise maximum).
    ///
    /// After merge, the result dominates both inputs:
    /// - For all i: result[i] = max(self[i], other[i])
    ///
    /// This represents "I have seen everything you have seen, and vice versa."
    /// Returns a new merged clock.
    pub fn merge(&self, other: &VectorClock) -> VectorClock {
        let mut merged = self.clock.clone();
        for (node, &count) in &other.clock {
            merged
                .entry(node.clone())
                .and_modify(|c| *c = (*c).max(count))
                .or_insert(count);
        }
        VectorClock {
            clock: merged,
            node_id: self.node_id.clone(),
        }
    }

    /// Merge in-place with another clock.
    pub fn merge_in_place(&mut self, other: &VectorClock) {
        for (node, &count) in &other.clock {
            self.clock
                .entry(node.clone())
                .and_modify(|c| *c = (*c).max(count))
                .or_insert(count);
        }
    }

    /// Check if this clock dominates another (is strictly greater in all components).
    pub fn dominates(&self, other: &VectorClock) -> bool {
        other.happened_before(self)
    }

    /// Check if this clock is dominated by or equal to another.
    pub fn dominated_by_or_equal(&self, other: &VectorClock) -> bool {
        !self.happened_before(other) || self == other
    }

    /// Compute the difference: components where self > other.
    pub fn diff(&self, other: &VectorClock) -> HashMap<String, u64> {
        let mut result = HashMap::new();
        for (node, &count) in &self.clock {
            let other_count = other.clock.get(node).copied().unwrap_or(0);
            if count > other_count {
                result.insert(node.clone(), count - other_count);
            }
        }
        result
    }

    /// Get the sum of all counters (total event count).
    pub fn total(&self) -> u64 {
        self.clock.values().sum()
    }

    /// Check if this is an empty clock (all zeros).
    pub fn is_empty(&self) -> bool {
        self.clock.values().all(|&c| c == 0)
    }

    /// Create a copy of this clock with a different node_id.
    pub fn with_node_id(&self, node_id: &str) -> VectorClock {
        VectorClock {
            clock: self.clock.clone(),
            node_id: node_id.to_string(),
        }
    }

    /// Get a reference to the internal map.
    pub fn as_map(&self) -> &HashMap<String, u64> {
        &self.clock
    }
}

impl fmt::Display for VectorClock {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut entries: Vec<_> = self.clock.iter().collect();
        entries.sort_by_key(|(k, _)| *k);
        write!(f, "{{")?;
        let items: Vec<String> = entries
            .iter()
            .map(|(k, v)| format!("{}: {}", k, v))
            .collect();
        write!(f, "{}", items.join(", "))?;
        write!(f, "}}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_vector_clock() {
        let vc = VectorClock::new("n1");
        assert_eq!(vc.get("n1"), 0);
        assert_eq!(vc.node_count(), 1);
    }

    #[test]
    fn test_increment() {
        let mut vc = VectorClock::new("n1");
        assert_eq!(vc.increment(), 1);
        assert_eq!(vc.increment(), 2);
        assert_eq!(vc.get("n1"), 2);
    }

    #[test]
    fn test_increment_node() {
        let mut vc = VectorClock::new("n1");
        vc.increment_node("n2");
        vc.increment_node("n2");
        assert_eq!(vc.get("n2"), 2);
    }

    #[test]
    fn test_happened_before_true() {
        let mut vc1 = VectorClock::new("n1");
        let mut vc2 = VectorClock::new("n2");
        vc1.increment();
        vc2.merge_in_place(&vc1);
        vc2.increment();
        assert!(vc1.happened_before(&vc2));
    }

    #[test]
    fn test_happened_before_false() {
        let mut vc1 = VectorClock::new("n1");
        let mut vc2 = VectorClock::new("n2");
        vc1.increment();
        vc2.increment();
        assert!(!vc1.happened_before(&vc2));
    }

    #[test]
    fn test_happened_before_self() {
        let vc = VectorClock::new("n1");
        assert!(!vc.happened_before(&vc)); // not strictly before itself
    }

    #[test]
    fn test_concurrent() {
        let mut vc1 = VectorClock::new("n1");
        let mut vc2 = VectorClock::new("n2");
        vc1.increment();
        vc2.increment();
        assert!(vc1.is_concurrent(&vc2));
        assert!(vc2.is_concurrent(&vc1));
    }

    #[test]
    fn test_not_concurrent_when_ordered() {
        let mut vc1 = VectorClock::new("n1");
        let mut vc2 = VectorClock::new("n2");
        vc1.increment();
        vc2.merge_in_place(&vc1);
        vc2.increment();
        assert!(!vc1.is_concurrent(&vc2));
    }

    #[test]
    fn test_merge() {
        let mut vc1 = VectorClock::new("n1");
        let mut vc2 = VectorClock::new("n2");
        vc1.increment();
        vc1.increment();
        vc2.increment();
        let merged = vc1.merge(&vc2);
        assert_eq!(merged.get("n1"), 2);
        assert_eq!(merged.get("n2"), 1);
    }

    #[test]
    fn test_merge_idempotent() {
        let mut vc = VectorClock::new("n1");
        vc.increment();
        let merged = vc.merge(&vc);
        assert_eq!(merged, vc);
    }

    #[test]
    fn test_merge_commutative() {
        let mut vc1 = VectorClock::new("n1");
        let mut vc2 = VectorClock::new("n2");
        vc1.increment();
        vc1.increment();
        vc2.increment();
        let m1 = vc1.merge(&vc2);
        let m2 = vc2.merge(&vc1);
        // Compare by clock values
        assert_eq!(m1.get("n1"), m2.get("n1"));
        assert_eq!(m1.get("n2"), m2.get("n2"));
    }

    #[test]
    fn test_merge_in_place() {
        let mut vc1 = VectorClock::new("n1");
        let mut vc2 = VectorClock::new("n2");
        vc2.increment();
        vc2.increment();
        vc1.merge_in_place(&vc2);
        assert_eq!(vc1.get("n2"), 2);
        assert_eq!(vc1.get("n1"), 0);
    }

    #[test]
    fn test_dominates() {
        let mut vc1 = VectorClock::new("n1");
        let mut vc2 = VectorClock::new("n2");
        vc1.increment();
        vc2.merge_in_place(&vc1);
        vc2.increment();
        assert!(vc2.dominates(&vc1));
        assert!(!vc1.dominates(&vc2));
    }

    #[test]
    fn test_diff() {
        let mut vc1 = VectorClock::new("n1");
        let mut vc2 = VectorClock::new("n2");
        vc1.increment();
        vc1.increment();
        let diff = vc1.diff(&vc2);
        assert_eq!(diff.get("n1"), Some(&2));
    }

    #[test]
    fn test_total() {
        let mut vc = VectorClock::new("n1");
        vc.increment();
        vc.increment();
        vc.increment_node("n2");
        assert_eq!(vc.total(), 3);
    }

    #[test]
    fn test_display() {
        let mut vc = VectorClock::new("n1");
        vc.increment();
        let s = format!("{}", vc);
        assert!(s.contains("n1: 1"));
    }

    #[test]
    fn test_from_map() {
        let mut map = HashMap::new();
        map.insert("a".to_string(), 1);
        map.insert("b".to_string(), 2);
        let vc = VectorClock::from_map(map);
        assert_eq!(vc.get("a"), 1);
        assert_eq!(vc.get("b"), 2);
    }
}
