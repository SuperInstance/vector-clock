# Vector Clock

A Rust library implementing vector clocks for causal ordering in distributed systems. Provides happened-before relation, concurrency detection, and merge operations for reasoning about event ordering without centralized coordination.

## Why This Matters

In distributed systems, events happen concurrently across multiple nodes. Without a global clock, determining the order of events is impossible using physical timestamps alone. Vector clocks solve this by tracking causal dependencies — which events influenced which other events.

Vector clocks were introduced by Colin Fidge (1988) and Friedemann Mattern (1988) as a practical implementation of Leslie Lamport's happened-before relation. They are fundamental building blocks for:

- **Conflict detection** in distributed databases (DynamoDB, Riak, Cassandra)
- **Eventual consistency** in collaborative editing (Google Docs, Figma)
- **Debugging** distributed systems (causal tracing)
- **Version vectors** for replicated data (tracking which replicas have which updates)

## Architecture

### VectorClock

A `VectorClock` maps node IDs to monotonically increasing counters:

```
Node A: {A: 3, B: 1, C: 2}
Node B: {A: 2, B: 4, C: 2}
```

Each counter represents "the number of events from that node that this node knows about."

### Core Operations

#### Increment
When a node performs a local event, it increments its own counter:
```
Node A does work → {A: 4, B: 1, C: 2}
```

#### Merge
When nodes communicate, they merge clocks by taking component-wise maximum:
```
merge({A: 3, B: 1}, {A: 2, B: 4}) = {A: 3, B: 4}
```
After merge, both nodes know about all events the other knows about.

#### Happened-Before (→)
Clock A happened-before clock B iff:
- For every node i: A[i] ≤ B[i]
- AND there exists some node j: A[j] < B[j]

This means B has seen everything A has seen, and strictly more.

#### Concurrency (∥)
Two clocks are concurrent if neither happened before the other:
```
{A: 3, B: 1} ∥ {A: 1, B: 4}
```
Concurrent events are causally independent — neither caused or was caused by the other.

## Usage

```rust
use vector_clock::VectorClock;

// Two nodes with independent clocks
let mut vc_a = VectorClock::new("node-a");
let mut vc_b = VectorClock::new("node-b");

// Node A does some work
vc_a.increment(); // A: {a: 1}
vc_a.increment(); // A: {a: 2}

// Node B does some work (concurrently)
vc_b.increment(); // B: {b: 1}

// These are concurrent — neither caused the other
assert!(vc_a.is_concurrent(&vc_b));

// Node A sends a message to Node B
vc_b.merge_in_place(&vc_a);
vc_b.increment(); // B processes the message

// Now A happened-before B
assert!(vc_a.happened_before(&vc_b));
assert!(!vc_b.happened_before(&vc_a));
assert!(vc_b.dominates(&vc_a));

// Display
println!("Clock A: {}", vc_a); // {a: 2}
println!("Clock B: {}", vc_b); // {a: 2, b: 2}
```

## Mathematical Background

### Partial Order Theory

A vector clock VC defines a partial order (≤) on the set of events:

**Definition**: VC₁ ≤ VC₂ iff ∀i: VC₁[i] ≤ VC₂[i]

This is a partial order because it satisfies:
1. **Reflexivity**: ∀VC: VC ≤ VC
2. **Antisymmetry**: VC₁ ≤ VC₂ ∧ VC₂ ≤ VC₁ → VC₁ = VC₂
3. **Transitivity**: VC₁ ≤ VC₂ ∧ VC₂ ≤ VC₃ → VC₁ ≤ VC₃

The strict order (happened-before) is: VC₁ < VC₂ iff VC₁ ≤ VC₂ ∧ VC₁ ≠ VC₂

### Lamport's Happened-Before Relation

The happened-before relation (→) is the smallest relation satisfying:
1. If a and b are events in the same process, and a comes before b: a → b
2. If a is sending a message and b is receiving it: a → b
3. If a → b and b → c: a → c (transitivity)

Vector clocks capture this: event a → event b iff VC(a) < VC(b).

### Concurrency

Events a and b are concurrent (a ∥ b) iff ¬(a → b) ∧ ¬(b → a).

In terms of vector clocks: a ∥ b iff ∃i,j: VC(a)[i] < VC(b)[i] ∧ VC(a)[j] > VC(b)[j].

### Theorem: Vector Clocks Correctly Capture Causality

**Claim**: VC(a) < VC(b) iff a → b.

**Proof sketch**:
- (→) By induction on the definition of →. Local events increment the local counter. Send/receive triggers merge (component-wise max) + increment. Both operations maintain the invariant.
- (←) If VC(a) < VC(b), then b's vector dominates a's in every component. This can only happen if there is a chain of events and messages from a to b.

### Space and Time Complexity

| Property | Value |
|----------|-------|
| Space per clock | O(n) for n nodes |
| Increment time | O(1) |
| Merge time | O(n) |
| Comparison time | O(n) |
| Total events representable | O(k^n) for counter range k |

### Optimizations

In practice, vector clocks can be optimized:
- **Sparse representation**: Only store non-zero entries (used in this crate)
- **Dotted version vectors**: Separate event identity from causal context
- **Interval tree clocks**: Dynamic allocation/deallocation of node IDs
- **Exception lists**: Only track nodes where VC₁ ≠ VC₂

## Comparison with Alternatives

| Approach | Causal Precision | Space | Update Cost |
|----------|-----------------|-------|-------------|
| Lamport timestamps | Partial (lossy) | O(1) | O(1) |
| Vector clocks | Full partial order | O(n) | O(n) |
| Matrix clocks | Full + future knowledge | O(n²) | O(n²) |
| Bloom clocks | Probabilistic | O(b) << O(n) | O(b) |

## License

MIT
