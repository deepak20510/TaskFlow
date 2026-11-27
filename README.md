# TaskFlow: Production-Grade Distributed Job Queue System

TaskFlow is a high-throughput, fault-tolerant, custom distributed job queue and background processing engine built with **Node.js**, **TypeScript**, **Express**, **Redis Streams**, **PostgreSQL**, **Prisma ORM**, and **Prometheus**.

---

## 1. System Architecture

```mermaid
flowchart TD
    Client["Client / API Consumers"] -->|HTTP / JSON + Auth| API["TaskFlow API Service (:5000)"]
    
    subgraph Storage ["Persistence & Queue Layer"]
        DB[("PostgreSQL\n(Jobs & Outbox)")]
        RedisStreams[("Redis Streams\n(12 Partitioned Streams)")]
        RedisKeys[("Redis Key-Value / Sets\n(Locks, Heartbeats, Token Bucket)")]
    end

    API -->|1. Tx: Job + OutboxEvent| DB
    API -->|2. Immediate Async Dispatch| RedisStreams
    
    subgraph Background ["Reliability Daemons"]
        OutboxDaemon["Outbox Publisher Daemon"]
        Scanner["Recovery Scanner Service"]
        AlertService["Alert Monitoring Service"]
    end
    
    OutboxDaemon -->|Polls Pending Outbox| DB
    OutboxDaemon -->|XADD Stream| RedisStreams
    Scanner -->|XAUTOCLAIM & Stuck Job Recovery| RedisStreams
    Scanner -->|Reconcile Dead Worker Jobs| DB

    subgraph Workers ["Distributed Worker Cluster"]
        W1["Worker Node 1\n(:3001 Metrics)"]
        W2["Worker Node 2\n(:3002 Metrics)"]
    end

    RedisStreams -->|XREADGROUP Consumer Group| W1
    RedisStreams -->|XREADGROUP Consumer Group| W2
    W1 -->|Atomic Lua Lock & Heartbeat| RedisKeys
    W2 -->|Atomic Lua Lock & Heartbeat| RedisKeys
    W1 -->|State Transitions: COMPLETED / DLQ| DB
    W2 -->|State Transitions: COMPLETED / DLQ| DB
    W1 -->|XACK & XDEL| RedisStreams
    W2 -->|XACK & XDEL| RedisStreams

    subgraph Monitoring ["Observability"]
        Prometheus["Prometheus Server (:9090)"]
        Grafana["Grafana Dashboards (:3000)"]
    end

    Prometheus -->|Scrapes /metrics| API
    Prometheus -->|Scrapes :3001/metrics| W1
    Grafana -->|PromQL Queries| Prometheus
```

---

## 2. Core Architectural Pillars

### A. Reliable Job Delivery (Redis Streams)
- **Zero Message Loss**: Eliminates the `BLPOP` message-loss window by utilizing **Redis Streams** (`XADD`) with Consumer Groups (`XREADGROUP`).
- **PEL & Crash Recovery**: Messages remain in the Redis Pending Entries List (PEL) until explicitly acknowledged with `XACK`. If a worker crashes mid-execution, surviving workers automatically claim abandoned messages via `XAUTOCLAIM` after a 30-second inactivity window.
- **Priority Partitioning**: 12 independent streams maintain strict user partitioning and priority isolation:
  - `high_priority_stream:0..3`
  - `medium_priority_stream:0..3`
  - `low_priority_stream:0..3`

### B. Distributed Locking with Unique Ownership & Heartbeat
- **Unique Tokens**: Every lock acquisition generates a cryptographically random UUID (`workerId:uuid`).
- **Atomic Lua Release**: Safe compare-and-delete script ensures a worker never accidentally deletes another worker's expired lock.
- **Auto-Extending Heartbeat**: Long-running jobs run an internal timer that safely renews the lock TTL at half-life, preventing false timeouts during intensive operations.

### C. Transactional Outbox Pattern
- **Dual-Write Safety**: Job creation and Outbox Event emission occur within the same PostgreSQL transaction (`prisma.$transaction`).
- **At-Least-Once Delivery**: Even if Redis is momentarily unavailable when an API request arrives, the Outbox Publisher daemon continually publishes pending events once connectivity restores.

### D. Centralized Job State Machine
- **Strict Conditional Transitions**: All job status updates (`QUEUED` $\rightarrow$ `PROCESSING` $\rightarrow$ `COMPLETED` / `DLQ`) are managed centrally via `JobStateService`.
- **Concurrency Protection**: Atomic SQL updates ensure a job can only be claimed once, cannot be moved back to `PROCESSING` once `COMPLETED`, and invalid transitions are rejected.

### E. Worker Coordination & Fair Scheduling
- **Weighted Priority Scheduling**: Workers pull jobs using a 5:2:1 weighted round-robin distribution (5 High : 2 Medium : 1 Low).
- **Starvation Prevention**: Empty poll backoff temporarily yields priority when queues are empty, ensuring low-priority jobs consistently make progress.
- **Cluster Rebalancing**: Active workers maintain heartbeat leases; if a worker joins or leaves, partition ownership rebalances fairly.

### F. Non-Blocking Retries & Dead Letter Queue (DLQ)
- **Exponential Backoff with Full Jitter**:
  $$\text{delay} = \min(\text{maxDelay}, \text{baseDelay} \times 2^{\text{attempt} - 1}) + \text{jitter}$$
- **Non-Blocking Delayed Queue**: Retries are stored in a Redis Sorted Set (`queue:delayed_jobs`) indexed by target execution timestamp. The worker never blocks its execution loop waiting for backoff timers.
- **Error Classification**: Permanent validation or payload errors throw `NonRetryableError` and route directly to `DLQ` without burning useless retry attempts.

---

## 3. Technology Stack

- **Runtime**: Node.js (v18+ or v20+ LTS)
- **Language**: TypeScript (Strict Mode)
- **HTTP Server**: Express.js
- **Validation**: Zod
- **Authentication**: JWT & API Keys
- **Primary Database**: PostgreSQL via Prisma ORM
- **Queue & Distributed Cache**: Redis (Streams, Sorted Sets, Hashes)
- **Metrics & Observability**: Prometheus (`prom-client`) & Grafana
- **Testing**: Vitest, Supertest

---

## 4. Environment Variables

Create a `.env` file in the root directory (see [.env.example](file:///.env.example)):

```env
# PostgreSQL Database Connection String (Prisma)
DATABASE_URL="postgresql://user:password@host:port/neondb?sslmode=require"

# Redis Server Connection URL
REDIS_URL="redis://localhost:6379"

# API Port
PORT=5000

# Worker Metrics Port
WORKER_METRICS_PORT=3001

# Master API Key
API_KEY="taskflow-dev-api-key"

# JWT Secret
JWT_SECRET="taskflow-development-secret-change-in-production"

# Rate Limiter
RATE_LIMIT_CAPACITY=100
RATE_LIMIT_REFILL_RATE=10
RATE_LIMIT_FAIL_CLOSED=false

# Backpressure Limits
BACKPRESSURE_SOFT_LIMIT=5000
BACKPRESSURE_HARD_LIMIT=8000
```

---

## 5. Local Setup & Running Instructions

### 1. Install Dependencies
```bash
npm install
```

### 2. Generate Prisma Client & Sync Database
```bash
npm run prisma:generate
npm run prisma:push
```

### 3. Run the Services

#### Option A: Start API Server
```bash
npm run dev
# Running on http://localhost:5000
```

#### Option B: Start Background Worker
```bash
npm run worker
# Metrics exposed on http://localhost:3001/metrics
```

#### Option C: Start Recovery Scanner
```bash
npm run start:recovery
```

#### Option D: Start Alert Monitor
```bash
npm run start:alert
```

---

## 6. API Reference

### Authentication
All job endpoints require authentication via either:
- **Header**: `x-api-key: taskflow-dev-api-key`
- **Header**: `Authorization: Bearer <jwt-token>`

*(In development mode, `x-user-id` or request body `userId` can be passed as fallback).*

### Endpoints

#### 1. Create Single Job
- **POST** `/jobs`
- **Request Body**:
  ```json
  {
    "type": "EMAIL",
    "priority": "high",
    "payload": {
      "to": "user@example.com"
    },
    "maxRetries": 3
  }
  ```
- **Response** (201 Created):
  ```json
  {
    "success": true,
    "data": {
      "jobId": "49198271-0499-404c-8ae1-230c7ba06f78",
      "status": "QUEUED",
      "userId": "user-123",
      "priority": "high"
    }
  }
  ```

#### 2. Create Batch Jobs
- **POST** `/jobs/batch`
- **Request Body**:
  ```json
  {
    "jobs": [
      { "type": "EMAIL", "priority": "high", "payload": { "to": "a@test.com" } },
      { "type": "IMAGE", "priority": "medium", "payload": { "file": "avatar.png" } }
    ]
  }
  ```
- **Response** (201 Created):
  ```json
  {
    "success": true,
    "message": "Batch submission completed successfully",
    "totalSubmitted": 2,
    "jobs": [
      { "jobId": "uuid-1", "priority": "high" },
      { "jobId": "uuid-2", "priority": "medium" }
    ]
  }
  ```

#### 3. Get Job Details (Tenant-Isolated)
- **GET** `/jobs/:id`

#### 4. List Jobs
- **GET** `/jobs?page=1&limit=20&status=COMPLETED`

#### 5. Replay DLQ Job
- **POST** `/jobs/:id/replay`

---

## 7. Automated Testing Suite

TaskFlow uses **Vitest** for fast unit and integration testing.

```bash
# Run all unit and integration tests
npm test

# Run unit tests only
npm run test:unit

# Run integration tests only
npm run test:integration

# Run TypeScript strict type-check
npm run typecheck
```

---

## 8. Benchmarking

Run high-throughput automated load tests with latency percentiles:

```bash
npm run benchmark
```

Configure custom benchmark scale via environment variables:
```powershell
$env:BENCHMARK_JOBS = "1000"; npm run benchmark
```
Outputs comprehensive statistics:
- End-to-End Throughput (jobs/sec)
- Queue-Drain Throughput (jobs/sec)
- Latency percentiles: p50, p95, p99
- Average queue wait time vs execution duration
- Success, failure, and retry rates
