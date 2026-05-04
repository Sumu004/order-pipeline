"""
Ingest Lambda — handles POST /orders (place_order action).

Responsibilities:
  1. Check back-pressure (SQS queue depth).
  2. Enforce per-tenant rate limit.
  3. Deduplicate via idempotency key.
  4. Enqueue to inventory queue.
"""

import json
import uuid
from datetime import datetime, timezone
from typing import Any, Dict

from lambda_handlers.shared import idempotency, rate_limiter, backpressure
from lambda_handlers.shared.config import INVENTORY_QUEUE
from lambda_handlers.shared.queue import enqueue
from lambda_handlers.shared.logging import get_logger

log = get_logger('ingest')


def lambda_handler(event, context):
    """Entry-point for the ingest Lambda (API Gateway trigger)."""
    http_method = (event.get('httpMethod')
                   or event.get('requestContext', {}).get('http', {}).get('method'))

    if event.get('path') == '/health' or http_method == 'GET':
        return {
            'statusCode': 200,
            'body': json.dumps({
                'status': 'healthy',
                'timestamp': datetime.now(timezone.utc).isoformat(),
            }),
        }

    body = json.loads(event.get('body', '{}')) if http_method == 'POST' else event
    action = body.get('action', 'place_order')

    if action == 'place_order':
        return handle_place_order(body)
    elif action == 'check_rate_limit':
        return handle_rate_limit_check(body)
    elif action == 'check_idempotency':
        return handle_idempotency_check(body)

    return {'statusCode': 400, 'body': json.dumps({'error': 'Unknown action'})}


def handle_place_order(body: Dict[str, Any]) -> Dict[str, Any]:
    """Place a new order."""
    customer_id = body.get('customer_id')
    items = body.get('items', [])
    tenant_id = body.get('tenant_id', 'default')
    idempotency_key = body.get('idempotency_key', str(uuid.uuid4()))
    correlation_id = idempotency_key  # use idem key as natural trace ID

    logger = log.bind(correlation_id=correlation_id, tenant_id=tenant_id)

    if not customer_id or not items:
        return {'statusCode': 400, 'body': json.dumps({'error': 'Missing required fields'})}

    # 1. Back-pressure guard
    if backpressure.is_overloaded():
        depth = backpressure.get_queue_depth()
        logger.warn('back_pressure_triggered', queue_depth=depth)
        return {'statusCode': 503, 'body': json.dumps({
            'error': 'Service temporarily unavailable — queue backlog too large',
            'queue_depth': depth,
            'retry_after': '5 seconds',
        })}

    # 2. Per-tenant rate limit
    rl = rate_limiter.check(tenant_id)
    if not rl.get('allowed'):
        logger.warn('rate_limited', tenant_id=tenant_id)
        return {'statusCode': 429, 'body': json.dumps({
            'error': 'Rate limit exceeded',
            'retry_after': '1 second',
        })}

    # 3. Exactly-once: reject duplicate idempotency keys
    idem = idempotency.check(idempotency_key)
    if idem.get('exists'):
        logger.info('duplicate_blocked', existing_order=idem.get('order_id'))
        return {'statusCode': 409, 'body': json.dumps({
            'error': 'Duplicate order',
            'existing_order_id': idem.get('order_id'),
            'status': idem.get('status'),
        })}

    # 4. Create order, write idempotency key, enqueue to inventory
    order_id = str(uuid.uuid4())
    total = sum(item.get('quantity', 1) * item.get('price', 0) for item in items)

    idempotency.write(idempotency_key, order_id, 'pending')

    message = {
        'order_id': order_id,
        'customer_id': customer_id,
        'items': items,
        'total': str(total),
        'idempotency_key': idempotency_key,
        'correlation_id': correlation_id,
        'status': 'placed',
    }
    enqueue(INVENTORY_QUEUE, message)

    logger.info('order_placed', order_id=order_id, total=total)

    return {'statusCode': 200, 'body': json.dumps({
        'order_id': order_id,
        'status': 'placed',
        'total': str(total),
        'idempotency_key': idempotency_key,
        'tokens_remaining': rl.get('tokens'),
    })}


def handle_rate_limit_check(body: Dict[str, Any]) -> Dict[str, Any]:
    """Check rate limit endpoint."""
    tenant_id = body.get('tenant_id', 'default')
    result = rate_limiter.check(tenant_id)
    return {
        'statusCode': 200 if result.get('allowed') else 429,
        'body': json.dumps(result),
    }


def handle_idempotency_check(body: Dict[str, Any]) -> Dict[str, Any]:
    """Check idempotency endpoint."""
    key = body.get('idempotency_key')
    if not key:
        return {'statusCode': 400, 'body': json.dumps({'error': 'Missing idempotency_key'})}
    result = idempotency.check(key)
    return {'statusCode': 200, 'body': json.dumps(result)}
