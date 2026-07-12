# EOS diagrams (current design)

> Canonical write-up: [eos.md](eos.md).  
> This file keeps diagrams only. Historical JDBC_FINISHED-first designs are omitted.

## Per-worker expected counts

```mermaid
flowchart LR
    subgraph TD[Teradata]
      A1[AMP routing_id]
      A2[AMP routing_id]
      A3[AMP routing_id]
    end

    subgraph Coord[Coordinator]
      C1[Collect routing PIDs]
      C2["expected[w] = count(unsigned(pid) % N == w)"]
      C3[Broadcast EXPECTED_TERADATA_SIGNALS]
      C1 --> C2 --> C3
    end

    subgraph W0[Worker 0]
      B0[Bridge receive]
      E0{received == expected0?}
      B0 --> E0
      E0 -->|yes| EOS0[Signal EOS]
    end

    subgraph W1[Worker 1]
      B1[Bridge receive]
      E1{received == expected1?}
      B1 --> E1
      E1 -->|yes| EOS1[Signal EOS]
    end

    A1 -->|idx%N==0| B0
    A2 -->|idx%N==1| B1
    A3 -->|idx%N==0| B0
    C3 --> E0
    C3 --> E1
```

## Worker-local state

```mermaid
stateDiagram-v2
    [*] --> Accepting
    Accepting --> Buffering: data batch
    Buffering --> Accepting: more data
    Accepting --> CountingFinished: AMP finished / conn closed
    CountingFinished --> WaitingExpected: expected not yet known
    WaitingExpected --> Checking: expected count received
    CountingFinished --> Checking: expected already known
    Checking --> Checking: received < expected
    Checking --> Complete: received >= expected and buffers drained
    Complete --> [*]

    note right of WaitingExpected
      Fallback timeout (~60s) may
      complete without expected
      if broadcast is lost
    end note
```

## Control command ids

| Id | Name | Current behavior |
|----|------|------------------|
| 1 | AMP / external finished | Increments received finished count |
| 2 | JDBC_FINISHED | **Ignored** (legacy) |
| 3 | EXPECTED_TERADATA_SIGNALS | Sets expected count for this worker |

## Ordering invariant

```text
receive batch → decompress → parse Page → enqueue buffer
                                              ↓
                                    then mark connection finished
                                              ↓
                                    then evaluate EOS predicate
```

Never mark finished before the last page for that connection is enqueued.
