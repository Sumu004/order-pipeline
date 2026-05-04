# Adaptive Rate-Limited Order Pipeline

A serverless, event-driven order processing pipeline with custom rate limiting and exactly-once semantics.

## Architecture

```
Client → Lambda → SQS (Inventory) → Lambda → SQS (Payment) → Lambda → SQS (Fulfillment)
                    ↓                                      ↓
              DynamoDB (idempotency)                    S3 (receipts)
```

## Features

- **Token Bucket Rate Limiter**: Per-tenant rate limiting using DynamoDB conditional writes
- **Exactly-Once Semantics**: Idempotency keys prevent duplicate charges
- **Event-Driven**: Decoupled Lambda functions via SQS queues
- **Dead Letter Queue**: Failed messages routed to DLQ
- **PostgreSQL**: Partitioned orders table by month

## Tech Stack

- AWS Lambda (Python 3.11)
- AWS SQS + DLQ
- DynamoDB (idempotency, rate limiting)
- PostgreSQL 15 (partitioned)
- S3 (receipts)
- GitHub Actions

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
# Unit tests
pytest tests/test_handler.py -v

# Load test
locust -f tests/locustfile.py --host=http://localhost:9000
```

## Project Structure

```
order-pipeline/
├── docker/               # Docker Compose + PostgreSQL schema
├── lambda_handlers/     # Lambda functions
├── tests/               # Unit + load tests
├── scripts/            # Demo + benchmarks
├── terraform/           # AWS infrastructure
└── .github/workflows/  # CI/CD
```

## License

MIT