# Distributed Resilient Job Processing Engine
A production-style distributed job processing system built with Node.js, Redis, and multi-worker architecture.

This system demonstrates priority-based scheduling, retry mechanisms, circuit breaker protection, rate limiting, visibility timeouts, dead-letter handling, and graceful shutdown — similar to real-world background processing engines.

## Architecture Overview

CLIENT → API SERVER → REDIS <- WORKERS

Redis Structures Used

- queue:high → High priority jobs
- queue:normal → Normal priority jobs
- queue:low → Low priority jobs
- queue:processing → In-progress jobs
- queue:delayed → Scheduled retries (sorted set)
- job:<id> → Job metadata (hash)

## Features

### Priority Queue
Jobs are processed in order
 1. HIGH
 2. NORMAL
 3. LOW

### Retry with Exponential Backoff
Failed jobs retry automatically using:
```delay = 2^attempts * 1000ms```

### Dead Letter Handling
If max attempts exceeded → job moves to DEAD.
Dead jobs can be retried manually.

### Circuit Breaker
Prevents system overload during repeated failures.
States:
- CLOSED
- OPEN
- HALF_OPEN
Automatically resets after cooldown.

### Distributed Rate Limiting
Global rate limit across all workers using Redis counters.
Prevents API flooding.

### Visibility Timeout
If worker crashes during execution:
- Job automatically requeued
- No job loss

### TTL Cleanup
Completed jobs auto-expire after 24 hours to prevent memory bloat.

### Metrics Tracking
Tracks:
- processed
- failed
- retried
- dead
- average processing time

### Graceful Shutdown
Workers handle:
- SIGINT
- SIGTERM
Ensures in-progress jobs complete safely.

### API Endpoints
Create Job
```POST /jobs```

Example:

{
  "type": "SEND_EMAIL",
  "priority": "HIGH",
  "payload": {
    "to": "user@example.com"
  }
}

Get Jobs (Filterable)
```GET /jobs```
```GET /jobs?status=FAILED```
```GET /jobs?priority=HIGH```

Cancel Job
```DELETE /jobs/:id```

Retry Dead Job
```POST /jobs/:id/retry```

Metrics
```GET /metrics```
Returns system performance statistics.


## Tech Stack
- Node.js
- Express
- Redis (ioredis)
- Docker
- Multi-worker architecture

## How it works
1. API enqueues job into priority queue
2. Worker atomically reserves job using RPOPLPUSH
3. Job moves to processing queue
4. Worker executes handler
5. On success → mark COMPLETED
6. On failure → schedule retry via sorted set
7. Visibility monitor ensures no stuck jobs
8. Circuit breaker protects from failure storms

## Running Locally
1. Install dependencies
```npm install```
2. Start Redis (Docker)
```docker run -p 6379:6379 redis```
3. Start API
```npm start```
4. Start Worker
```npm run worker```

## Deployment
Designed to run on:
- Render (Web Service + Background Worker)
- Docker Compose
- VPS environments

Environment variables required:
```PORT=3000```
```REDIS_URL=redis://...```

## Design Goals
- Fault tolerance
- Distributed safety
- Idempotent execution
- Horizontal scalability
- Production-ready patterns

## Future Enhancements
- WebSocket monitoring dashboard
- Multi-tenant support
- Token bucket rate limiting
- Prometheus integration

## Author
Shivashankar Raje Urs
