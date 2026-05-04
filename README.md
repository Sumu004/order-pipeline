# Adaptive Rate-Limited Order Pipeline

A serverless, event-driven order processing pipeline with per-tenant rate limiting, exactly-once semantics, and saga-pattern compensation.

## Architecture

```
                          ┌─────────────────────────────────────────┐
                          │            AWS Infrastructure           │
┌────────┐   POST /orders │  ┌──────────┐    ┌──────────────────┐   │
│ Client ├───────────────►│  │  Ingest   │───►│ SQS (Inventory)  │   │
└────────┘                │  │  Lambda   │    └────────┬─────────┘   │
                          │  └──────────┘             │              │
                          │       │                   ▼              │
                          │  ┌────┴─────┐    ┌──────────────────┐   │
                          │  │ DynamoDB  │    │   Inventory      │   │
                          │  │ (idempot  │    │   Lambda         │   │
                          │  │  + rate)  │    └────────┬─────────┘   │
                          │  └──────────┘             │              │
                          │                           ▼              │
                          │                  ┌──────────────────┐   │
                          │                  │ SQS (Payment)    │   │
                          │                  └────────┬─────────┘   │
                          │                           │              │
                          │                           ▼              │
                          │                  ┌──────────────────┐   │
                          │          ┌──────►│  Payment Lambda  │   │
                          │          │ retry │  (saga pattern)  │   │
                          │          │       └────────┬─────────┘   │
                          │          │                │  │           │
                          │          │         success│  │fail       │
                          │          │                ▼  ▼           │
                          │   ┌──────┴──┐    ┌────────────────┐    │
                          │   │   DLQ   │    │SQS (Fulfillment)│    │
                          │   └─────────┘    └───────┬────────┘    │
                          │                          │              │
                          │                          ▼              │
                          │                 ┌────────────────┐     │
                          │                 │  Fulfillment   │     │
                          │                 │  Lambda        ├──┐  │
                          │                 └────────────────┘  │  │
                          │                                     │  │
                          │  ┌───────────┐    ┌──────────────┐  │  │
                          │  │PostgreSQL │    │ S3 (receipts)│◄─┘  │
                          │  │(partitned)│    └──────────────┘     │
                          │  └───────────┘                         │
                          └─────────────────────────────────────────┘
```

## Design Decisions

### Why separate Lambda functions?
Each pipeline stage runs as an independent Lambda with its own memory, timeout, and concurrency limits. A bug in payment logic can't crash inventory processing. Cold-start time is optimised per stage.

### Why an atomic rate limiter?
The token bucket uses a single DynamoDB `UpdateExpression` for the entire refill-then-decrement. This eliminates the TOCTOU race condition inherent in get→compute→conditional-update patterns. Two concurrent requests **cannot** both consume the last token.

### Why the saga pattern?
If the payment stage charges a customer but fails to enqueue the fulfillment message, the charge is automatically reversed (compensating transaction). The message is retried up to 3 times before routing to the DLQ. This prevents the "charged but never fulfilled" failure mode.

### Why idempotency TTLs?
Keys expire after 7 days via DynamoDB TTL. Without this, the idempotency table grows without bound — a cost and performance issue at scale.

### Why partitioned PostgreSQL?
Orders are partitioned by `created_at` month. Time-range queries (the most common pattern for order systems) scan only the relevant partitions. A `create_monthly_partitions()` function auto-creates partitions 3 months ahead.

## Features

- **Atomic Token Bucket Rate Limiter** — DynamoDB conditional writes, no TOCTOU race
- **Exactly-Once Semantics** — idempotency keys with 7-day TTL
- **Saga Pattern** — compensating transactions with automatic retry
- **Event-Driven** — decoupled Lambda functions per stage via SQS queues
- **Back-Pressure** — CloudWatch queue-depth check rejects requests during overload (503)
- **Dead Letter Queue** — failed messages routed to DLQ after 3 retries
- **Structured Logging** — JSON logs with correlation IDs across all stages
- **Partitioned PostgreSQL** — auto-created monthly partitions
- **Least-Privilege IAM** — scoped to exact resources and actions

## Tech Stack

- AWS Lambda (Python 3.11) — 5 independent functions
- AWS SQS + DLQ
- DynamoDB (idempotency, rate limiting)
- PostgreSQL 15 (partitioned by month)
- S3 (receipt storage)
- Terraform (infrastructure-as-code)
- GitHub Actions (CI/CD with coverage + linting)

## Project Structure

```
order-pipeline/
├── lambda_handlers/
│   ├── ingest.py              # POST /orders entry point
│   ├── inventory.py           # Inventory validation
│   ├── payment.py             # Payment + saga compensation
│   ├── fulfillment.py         # Receipt generation
│   ├── notification.py        # Status notifications
│   ├── handler.py             # Legacy monolith (kept for reference)
│   └── shared/
│       ├── config.py          # AWS clients + env config
│       ├── idempotency.py     # Exactly-once key management
│       ├── rate_limiter.py    # Atomic token bucket
│       ├── queue.py           # SQS helpers
│       ├── receipts.py        # S3 receipt writer
│       ├── backpressure.py    # Queue depth monitoring
│       └── logging.py         # Structured JSON logger
├── docker/
│   ├── docker-compose.yml     # PostgreSQL + LocalStack
│   └── init.sql               # Schema + indexes + partitions
├── tests/
│   ├── test_handler.py        # Unit + integration tests
│   └── locustfile.py          # Load test (burst + sustained)
├── scripts/
│   ├── demo.py                # Visual demo
│   └── benchmark_partitions.py # Partition performance benchmark
├── terraform/
│   └── main.tf                # AWS infrastructure (least-privilege IAM)
└── .github/workflows/
    └── ci.yml                 # CI/CD pipeline
```

## Quick Start

```bash
# Start services
cd docker
docker compose up -d

# Run tests
pip install -r requirements.txt
pytest tests/test_handler.py -v

# Run demo
python scripts/demo.py
```

## API Usage

```bash
# Place order
curl -X POST http://localhost:9000/orders \
  -H "Content-Type: application/json" \
  -d '{
    "action": "place_order",
    "customer_id": "cust-123",
    "items": [{"product_id": "PROD-001", "quantity": 2, "price": 10.0}],
    "tenant_id": "default",
    "idempotency_key": "idem-123"
  }'

# Check rate limit
curl -X POST http://localhost:9000/orders \
  -H "Content-Type: application/json" \
  -d '{"action": "check_rate_limit", "tenant_id": "default"}'
```

## Testing

```bash
# Unit + integration tests
pytest tests/test_handler.py -v --cov=lambda_handlers

# Load test
locust -f tests/locustfile.py --host=http://localhost:9000
```

## License

MIT