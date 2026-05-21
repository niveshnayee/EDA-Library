# pyeda vs Kafka vs Celery — Diagram Heavy Architecture Guide

---

# 1. High-Level Architecture Overview

## Unified View

```mermaid
flowchart LR
    A[Application] --> B[Event / Task Produced]

    B --> C1[pyeda EventBus]
    B --> C2[Kafka Cluster]
    B --> C3[Celery Broker]

    C1 --> D1[Async Handler Queues]
    C2 --> D2[Topics + Partitions]
    C3 --> D3[Worker Queues]

    D1 --> E1[Async Workers]
    D2 --> E2[Consumer Services]
    D3 --> E3[Celery Workers]
```

---

# 2. pyeda Architecture (In-Process Event System)

## Core Flow

```mermaid
flowchart TD
    A[Event Emitted] --> B[EventBus]
    B --> C1[Queue: download_video]
    B --> C2[Queue: extract_audio]
    B --> C3[Queue: transcribe]

    C1 --> D1[Worker Task]
    C2 --> D2[Worker Task]
    C3 --> D3[Worker Task]
```

## Internal Mechanism

```text
ONE EVENT → COPIED INTO MULTIPLE QUEUES

EventBus
   |
   |-- Queue per handler
   |-- asyncio Workers
   |-- async dispatch tasks
```

## Example Flow

```mermaid
sequenceDiagram
    participant App
    participant Bus
    participant Q as Handler Queue
    participant W as Worker
    participant H as Handler

    App->>Bus: emit(VideoUploaded)
    Bus->>Q: push event
    W->>Q: get event
    W->>W: create asyncio task
    W->>H: execute handler
    H->>Bus: emit next event
```

---

# 3. Kafka Architecture (Distributed Event Log)

## Core Flow

```mermaid
flowchart TD
    P[Producer] --> T[Kafka Topic Log]
    T --> C1[Consumer Group A]
    T --> C2[Consumer Group B]
    T --> C3[Consumer Group C]

    C1 --> S1[Service A]
    C2 --> S2[Service B]
    C3 --> S3[Service C]
```

## Internal Storage Model

```text
Kafka Topic = Append-only Log

Offset 100 → VideoUploaded
Offset 101 → VideoDownloaded
Offset 102 → AudioExtracted
```

## Consumer Offset Tracking

```mermaid
flowchart LR
    A[Kafka Topic] --> B[Consumer]
    B --> C[Process Event]
    C --> D[Commit Offset]
    D --> B
```

---

# 4. Celery Architecture (Distributed Task Queue)

## Core Flow

```mermaid
flowchart TD
    A[Application] --> B[Broker: Redis/RabbitMQ]

    B --> C1[Worker 1]
    B --> C2[Worker 2]
    B --> C3[Worker 3]

    C1 --> D[Task Execution]
    C2 --> D
    C3 --> D
```

## Task Execution Model

```mermaid
sequenceDiagram
    participant App
    participant Broker
    participant Worker

    App->>Broker: enqueue task
    Worker->>Broker: fetch task
    Worker->>Worker: execute
    Worker->>Broker: ack task
```

---

# 5. Video Pipeline Comparison (Same Use Case)

## pyeda Flow (Fan-Out Event System)

```mermaid
flowchart TD
    A[VideoUploaded] --> B[download_video handler]
    B --> C[VideoDownloaded]

    C --> D[extract_audio handler]
    D --> E[AudioExtracted]

    E --> F[transcribe handler]
    F --> G[AudioTranscribed]

    G --> H[multiple handlers]
```

### Key Idea
```text
EVENT TRIGGERS NEXT EVENT (CHAIN)
```

---

## Kafka Flow (Event Log + Consumers)

```mermaid
flowchart TD
    A[video-uploaded Topic] --> B[Download Service]
    B --> C[video-downloaded Topic]

    C --> D[Extract Service]
    D --> E[audio-extracted Topic]

    E --> F[Transcribe Service]
```

### Key Idea
```text
SERVICES READ LOG + PRODUCE NEW EVENTS
```

---

## Celery Flow (Task Execution)

```mermaid
flowchart TD
    A[Task: download_video] --> B[Worker]
    B --> C[Task: extract_audio]
    C --> D[Worker]
    D --> E[Task: transcribe]
```

### Key Idea
```text
TASKS FLOW THROUGH BROKER QUEUES
```

---

# 6. Core Architectural Differences

## 6.1 Event Delivery Model

```text
pyeda   → Push into handler queues
Kafka   → Pull from event log (offsets)
Celery  → Pull tasks from broker queue
```

---

## 6.2 Fan-Out Strategy

```mermaid
flowchart LR
    A[Event]

    A --> B1[pyeda: copied to all queues]
    A --> B2[Kafka: multiple consumers read same log]
    A --> B3[Celery: usually one worker per task]
```

---

## 6.3 Storage Model

```text
pyeda   → in-memory queues (+ optional SQLite)
Kafka   → durable distributed log
Celery  → broker-backed queue storage
```

---

## 6.4 Execution Model

```mermaid
flowchart TD
    A[Event Arrives]

    A --> B1[pyeda: asyncio tasks]
    A --> B2[Kafka: external services]
    A --> B3[Celery: worker processes]
```

---

# 7. Mental Model Diagrams

## pyeda (Mail Copy System)

```text
Event = letter
Handlers = people
Bus = post office

Post office makes copies and delivers to everyone
```

---

## Kafka (Shared Notebook)

```text
Everyone reads same notebook page
Each reader tracks page number (offset)
```

---

## Celery (Job Queue)

```text
Jobs placed in queue
Workers pick one job at a time
```

---

# 8. Summary View

```mermaid
mindmap
  root((Event Systems))
    pyeda
      in-process
      fan-out queues
      asyncio workers
    Kafka
      distributed log
      offsets
      consumer groups
    Celery
      task queue
      broker
      worker pool
```

---

# END
