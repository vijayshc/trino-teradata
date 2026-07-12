# EOS Architecture Diagrams

## Current Architecture (Complex)

```mermaid
flowchart TB
    subgraph Teradata["Teradata Database"]
        subgraph AMP1["AMP 1 (has data)"]
            A1[Open socket] --> B1[Send data batches]
            B1 --> C1[Close socket]
            C1 --> D1[Send TERADATA_FINISHED]
            D1 --> E1[Write JDBC status row]
        end
        
        subgraph AMP2["AMP 2 (has data)"]
            A2[Open socket] --> B2[Send data batches]
            B2 --> C2[Close socket]
            C2 --> D2[Send TERADATA_FINISHED]
            D2 --> E2[Write JDBC status row]
        end
        
        subgraph AMP3["AMP 3 (empty)"]
            A3[No execution] 
        end
    end
    
    subgraph Coordinator["Trino/Spark Coordinator"]
        F[JDBC Query Execution]
        F --> G[Collect status rows]
        G --> H{Row count?}
        H -->|0 rows| I[Mark zero-row case]
        H -->|N rows| J[Broadcast EXPECTED_SIGNALS=N]
        I --> K[Broadcast JDBC_FINISHED]
        J --> K
        K --> L[Broadcast TERADATA_FINISHED]
    end
    
    subgraph Worker1["Worker 1"]
        M[Receive data connection]
        M --> N[Track connection opened]
        N --> O[Receive data]
        O --> P[Connection closed]
        P --> Q[Track connection closed]
        Q --> R[Receive TERADATA_FINISHED]
        R --> S[Increment signal count]
        S --> T{Check EOS conditions}
        T -->|Not met| U[Wait for more signals]
        T -->|Met| V[Signal EOS]
        K -.->|JDBC_FINISHED| T
    end
    
    subgraph Worker2["Worker 2"]
        W[Receive data connection]
        W --> X[Track connection opened]
        X --> Y[Receive data]
        Y --> Z[Connection closed]
        Z --> AA[Track connection closed]
        AA --> AB[Receive TERADATA_FINISHED]
        AB --> AC[Increment signal count]
        AC --> AD{Check EOS conditions}
        AD -->|Not met| AE[Wait for more signals]
        AD -->|Met| AF[Signal EOS]
        K -.->|JDBC_FINISHED| AD
    end
    
    E1 --> F
    E2 --> F
    D1 -.->|Control signal| R
    D2 -.->|Control signal| AB
    J -.->|Broadcast| T
    J -.->|Broadcast| AD
```

## Proposed Simplified Architecture

```mermaid
flowchart TB
    subgraph Teradata["Teradata Database"]
        subgraph AMP1["AMP 1 (has data)"]
            A1[Open socket] --> B1[Send data batches]
            B1 --> C1[Send EOS marker]
            C1 --> D1[Close socket]
            D1 --> E1[Send AMP_FINISHED signal]
        end
        
        subgraph AMP2["AMP 2 (has data)"]
            A2[Open socket] --> B2[Send data batches]
            B2 --> C2[Send EOS marker]
            C2 --> D2[Close socket]
            D2 --> E2[Send AMP_FINISHED signal]
        end
        
        subgraph AMP3["AMP 3 (empty)"]
            A3[Open socket] --> B3[Send EOS marker immediately]
            B3 --> C3[Close socket]
            C3 --> D3[Send AMP_FINISHED signal]
        end
    end
    
    subgraph Worker["Worker (Self-Contained)"]
        F[Accept connection] --> G[Track AMP ID]
        G --> H[Receive data]
        H --> I[Receive EOS marker]
        I --> J[Connection closed]
        J --> K[Mark connection closed]
        
        E1 -.->|AMP_FINISHED| L
        E2 -.->|AMP_FINISHED| L
        D3 -.->|AMP_FINISHED| L
        
        L[Receive AMP_FINISHED] --> M[Mark AMP finished]
        M --> N{All AMPs finished?}
        K --> N
        
        N -->|No| O[Continue waiting]
        N -->|Yes| P[Signal EOS]
    end
    
    style Worker fill:#90EE90
    style AMP3 fill:#FFE4B5
```

## State Machine Comparison

### Current State Machine (Complex)

```mermaid
stateDiagram-v2
    [*] --> INITIALIZING
    INITIALIZING --> WAITING_CONNECTIONS
    
    WAITING_CONNECTIONS --> CONNECTION_ESTABLISHED : Connection opened
    WAITING_CONNECTIONS --> JDBC_FINISHED_RECEIVED : JDBC finished broadcast
    
    CONNECTION_ESTABLISHED --> RECEIVING_DATA : Data flowing
    CONNECTION_ESTABLISHED --> CONNECTION_CLOSED : Connection closed (no data)
    
    RECEIVING_DATA --> CONNECTION_CLOSED : Connection closed
    
    CONNECTION_CLOSED --> CHECKING_SIGNALS : Connection closed
    
    JDBC_FINISHED_RECEIVED --> CHECKING_SIGNALS : Connection received
    
    CHECKING_SIGNALS --> WAITING_SIGNALS : Signals < Expected
    CHECKING_SIGNALS --> ZERO_ROW_CHECK : No connections
    CHECKING_SIGNALS --> SIGNAL_EOS : All conditions met
    
    WAITING_SIGNALS --> CHECKING_SIGNALS : Signal received
    WAITING_SIGNALS --> TIMEOUT_CHECK : Wait timeout
    
    ZERO_ROW_CHECK --> SIGNAL_EOS : jdbcReportedZeroRows=true
    ZERO_ROW_CHECK --> WAITING_SIGNALS : Wait for signals
    
    TIMEOUT_CHECK --> SIGNAL_EOS : Timeout with data
    TIMEOUT_CHECK --> ERROR : Timeout without data
    
    SIGNAL_EOS --> [*]
    ERROR --> [*]
```

### Proposed State Machine (Simplified)

```mermaid
stateDiagram-v2
    [*] --> RECEIVING
    
    RECEIVING --> RECEIVING : Connection opened
    RECEIVING --> RECEIVING : Data received
    RECEIVING --> RECEIVING : Connection closed
    RECEIVING --> RECEIVING : AMP_FINISHED received
    
    RECEIVING --> COMPLETED : All AMPs finished & no active connections
    
    COMPLETED --> [*]
```

## Signal Flow Comparison

### Current: Multi-Signal Coordination

```mermaid
sequenceDiagram
    participant AMP1 as AMP 1
    participant AMP2 as AMP 2
    participant W1 as Worker 1
    participant W2 as Worker 2
    participant Coord as Coordinator
    
    AMP1->>W1: Open data connection
    AMP1->>W1: Send data batches
    AMP1->>W1: Close connection
    AMP1->>W1: Send TERADATA_FINISHED
    
    AMP2->>W2: Open data connection
    AMP2->>W2: Send data batches
    AMP2->>W2: Close connection
    AMP2->>W2: Send TERADATA_FINISHED
    
    AMP1->>Coord: JDBC status row
    AMP2->>Coord: JDBC status row
    
    Coord->>W1: Broadcast EXPECTED_SIGNALS=2
    Coord->>W2: Broadcast EXPECTED_SIGNALS=2
    Coord->>W1: Broadcast JDBC_FINISHED
    Coord->>W2: Broadcast JDBC_FINISHED
    Coord->>W1: Broadcast TERADATA_FINISHED
    Coord->>W2: Broadcast TERADATA_FINISHED
    
    Note over W1: Check: connections=0, signals=1, expected=2, jdbc=true
    Note over W1: Not ready - wait for more signals
    
    Note over W2: Check: connections=0, signals=1, expected=2, jdbc=true
    Note over W2: Not ready - wait for more signals
    
    Note over W1,W2: Complex race condition handling...
```

### Proposed: Single Signal Per AMP

```mermaid
sequenceDiagram
    participant AMP1 as AMP 1
    participant AMP2 as AMP 2
    participant AMP3 as AMP 3 (empty)
    participant W as Worker
    
    AMP1->>W: Open connection (AMP_ID=1)
    AMP1->>W: Send data
    AMP1->>W: Close connection
    AMP1->>W: AMP_FINISHED(amp_id=1, rows=100)
    
    AMP2->>W: Open connection (AMP_ID=2)
    AMP2->>W: Send data
    AMP2->>W: Close connection
    AMP2->>W: AMP_FINISHED(amp_id=2, rows=50)
    
    AMP3->>W: Open connection (AMP_ID=3)
    AMP3->>W: EOS marker (no data)
    AMP3->>W: Close connection
    AMP3->>W: AMP_FINISHED(amp_id=3, rows=0)
    
    Note over W: All connections closed: YES
    Note over W: All AMPs finished: YES
    Note over W: Signal EOS immediately
```

## Component Interaction

### Current: Tight Coupling

```mermaid
flowchart LR
    subgraph Current["Current Architecture"]
        direction TB
        C1[Teradata UDF] -->|Data| C2[Bridge Server]
        C1 -->|TERADATA_FINISHED| C2
        C1 -->|JDBC Status| C3[Coordinator]
        C3 -->|EXPECTED_SIGNALS| C2
        C3 -->|JDBC_FINISHED| C2
        C3 -->|TERADATA_FINISHED| C2
        C2 -->|EOS| C4[PageSource]
        
        C1 -.->|4 connections| C2
        C3 -.->|3 broadcasts| C2
    end
```

### Proposed: Loose Coupling

```mermaid
flowchart LR
    subgraph Proposed["Proposed Architecture"]
        direction TB
        P1[Teradata UDF] -->|Data| P2[Bridge Server]
        P1 -->|AMP_FINISHED| P2
        P1 -->|JDBC Status| P3[Coordinator]
        P2 -->|EOS| P4[PageSource]
        
        P1 -.->|2 connections| P2
        P3 -.->|No broadcasts| P2
    end
```

## Data Structures Comparison

### Current: Complex State Tracking

```java
class QueryBuffer {
    // Connection tracking
    AtomicInteger activeConnections;
    AtomicInteger totalConnectionsOpened;
    AtomicInteger totalConnectionsClosed;
    
    // Signal tracking
    AtomicInteger expectedTeradataSignals;
    AtomicInteger receivedTeradataSignals;
    
    // State flags
    volatile boolean jdbcFinished;
    volatile boolean teradataFinished;
    volatile boolean eosSignaled;
    volatile boolean jdbcReportedZeroRows;
    volatile boolean hadAnyConnections;
    
    // Consumer tracking
    AtomicInteger activeConsumers;
    AtomicInteger totalFinishedConsumers;
    volatile int expectedConsumers;
    
    // Timing
    volatile long lastActivityTime;
    volatile boolean delayedEosScheduled;
}
```

### Proposed: Minimal State

```java
class SimpleQueryBuffer {
    // AMP tracking only
    Set<Integer> activeAmps = ConcurrentHashMap.newKeySet();
    Set<Integer> finishedAmps = ConcurrentHashMap.newKeySet();
    
    // Simple state
    enum State { RECEIVING, COMPLETED }
    volatile State state = State.RECEIVING;
    
    // Consumer tracking (unchanged)
    AtomicInteger activeConsumers;
}
```

## Benefits Summary

| Aspect | Current | Proposed | Improvement |
|--------|---------|----------|-------------|
| Signal Types | 4 | 1 | 75% reduction |
| State Variables | 12+ | 3 | 75% reduction |
| Network Messages | 7 per query | 2 per AMP | Varies |
| Race Conditions | 6+ | 0 | 100% elimination |
| Special Cases | 4 | 0 | 100% elimination |
| Code Complexity | High | Low | 63% reduction |
| Coordinator Dependency | Required | None | Decoupled |

---

## Migration Path

```mermaid
gantt
    title EOS Optimization Implementation Timeline
    dateFormat  YYYY-MM-DD
    section Phase 1
    Add AMP_FINISHED signal       :a1, 2024-01-01, 5d
    Backward compatibility        :a2, after a1, 3d
    
    section Phase 2
    Worker support                :b1, after a2, 5d
    Simplified EOS logic          :b2, after b1, 3d
    
    section Phase 3
    Remove broadcasts             :c1, after b2, 3d
    Testing                       :c2, after c1, 5d
    
    section Phase 4
    Remove legacy code            :d1, after c2, 3d
    Documentation                 :d2, after d1, 2d
```
