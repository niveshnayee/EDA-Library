# pyeda vs Kafka vs Celery

## Overview

| System | Primary Purpose |
|---|---|
| pyeda | In-process async event-driven workflow engine |
| Kafka | Distributed durable event streaming platform |
| Celery | Distributed background task queue |

---

# pyeda

`pyeda` is an in-process async event bus built using Python `asyncio`.

## Core Architecture

```
emit(event)
    ↓
EventBus
    ↓
Handler Queues
    ↓
Workers
    ↓
Async Handlers
```

## Characteristics

- Runs inside a single Python process
- Uses `asyncio.Queue`
- One queue per handler
- Fan-out event delivery
- Handlers run asynchronously
- Optional SQLite durability
- Lightweight and fast

## Best For

- AI pipelines
- Video processing workflows
- Internal application events
- Modular monoliths
- Async orchestration

---

# Kafka

Kafka is a distributed event streaming platform based on durable append-only logs.

## Core Architecture

```
Producer
    ↓
Kafka Topic Log
    ↓
Consumers pull using offsets
```

## Characteristics

- Distributed across multiple machines
- Events stored durably on disk
- Consumers track offsets
- Pull-based consumption
- High scalability and replayability
- Supports consumer groups and partitions

## Best For

- Microservices
- Large-scale event streaming
- Event replay
- Distributed systems
- Analytics pipelines

---

# Celery

Celery is a distributed task queue system.

## Core Architecture

```
Application
    ↓
Broker Queue (Redis/RabbitMQ)
    ↓
Workers execute tasks
```

## Characteristics

- Distributed task execution
- Usually backed by Redis or RabbitMQ
- One worker consumes one task
- Task-oriented architecture
- Supports retries and scheduling

## Best For

- Background jobs
- Scheduled tasks
- Distributed processing
- Async task execution

---

# Main Difference

## pyeda

```
EventBus actively pushes events to handler queues
```

## Kafka

```
Kafka stores events; consumers pull using offsets
```

## Celery

```
Broker distributes tasks to workers
```

---

# Event Flow Comparison

## pyeda

```
Event emitted
    ↓
Copied into all handler queues
    ↓
All subscribed handlers run
```

## Kafka

```
Event written to topic
    ↓
Consumers independently read topic
```

## Celery

```
Task pushed to broker
    ↓
One worker executes task
```

---

# Queue Behavior

| System | Queue Behavior |
|---|---|
| pyeda | One queue per handler |
| Kafka | Shared topic log |
| Celery | Shared broker queue |

---

# Fan-Out Behavior

| System | Fan-Out |
|---|---|
| pyeda | Event copied to all handler queues |
| Kafka | Multiple consumers read same topic |
| Celery | Usually one worker processes one task |

---

# Persistence

| System | Persistence |
|---|---|
| pyeda | Memory + optional SQLite journal |
| Kafka | Durable distributed log |
| Celery | Broker persistence (Redis/RabbitMQ) |

---

# Concurrency Model

| System | Concurrency |
|---|---|
| pyeda | asyncio tasks |
| Kafka | Distributed consumers |
| Celery | Worker processes/threads |

---

# Simple Mental Models

## pyeda

```
Photocopy event and give copy to every subscriber
```

## Kafka

```
Write event into shared event log with offsets
```

## Celery

```
Put task into work queue for workers
```
