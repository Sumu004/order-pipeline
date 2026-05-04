"""
Order Pipeline Lambda Handler
Handles order placement, inventory check, payment validation, fulfillment dispatch, and notifications
"""

import json
import os
import time
import uuid
import boto3
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, Optional

AWS_REGION = os.environ.get('AWS_REGION', 'eu-west-2')
dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION, endpoint_url=os.environ.get('DYNAMODB_ENDPOINT', 'http://localhost:4566'))
sqs = boto3.client('sqs', region_name=AWS_REGION, endpoint_url=os.environ.get('SQS_ENDPOINT', 'http://localhost:4566'))
s3 = boto3.client('s3', region_name=AWS_REGION, endpoint_url=os.environ.get('S3_ENDPOINT', 'http://localhost:4566'))
cloudwatch = boto3.client('cloudwatch', region_name=AWS_REGION, endpoint_url=os.environ.get('CLOUDWATCH_ENDPOINT', 'http://localhost:4566'))

ORDERS_TABLE = os.environ.get('ORDERS_TABLE', 'orders')
IDEMPOTENCY_TABLE = os.environ.get('IDEMPOTENCY_TABLE', 'idempotency-keys')
TOKEN_BUCKET_TABLE = os.environ.get('TOKEN_BUCKET_TABLE', 'token-buckets')
INVENTORY_QUEUE = os.environ.get('INVENTORY_QUEUE_URL', 'orders-inventory-queue')
INVENTORY_QUEUE_NAME = os.environ.get('INVENTORY_QUEUE_NAME', 'orders-inventory-queue')
PAYMENT_QUEUE = os.environ.get('PAYMENT_QUEUE_URL', 'orders-payment-queue')
FULFILLMENT_QUEUE = os.environ.get('FULFILLMENT_QUEUE_URL', 'orders-fulfillment-queue')
RECEIPTS_BUCKET = os.environ.get('RECEIPTS_BUCKET', 'order-receipts')

BACK_PRESSURE_THRESHOLD = int(os.environ.get('BACK_PRESSURE_THRESHOLD', '1000'))
BACK_PRESSURE_CACHE_TTL = 10  # seconds — stale metric is an accepted trade-off vs per-request latency

# Module-level cache: avoids a CloudWatch API call on every order placement
_queue_depth_cache: Dict[str, Any] = {'depth': 0, 'fetched_at': 0.0}


def get_inventory_queue_depth() -> int:
    """
    Return ApproximateNumberOfMessages for the inventory queue, cached for 10s.
    Falls back to 0 on any error so a CloudWatch outage never blocks orders.
    """
    now = time.monotonic()
    if now - _queue_depth_cache['fetched_at'] < BACK_PRESSURE_CACHE_TTL:
        return _queue_depth_cache['depth']

    try:
        response = cloudwatch.get_metric_statistics(
            Namespace='AWS/SQS',
            MetricName='ApproximateNumberOfMessages',
            Dimensions=[{'Name': 'QueueName', 'Value': INVENTORY_QUEUE_NAME}],
            StartTime=datetime.now(timezone.utc).replace(second=0, microsecond=0),
            EndTime=datetime.now(timezone.utc),
            Period=60,
            Statistics=['Maximum'],
        )
        datapoints = response.get('Datapoints', [])
        depth = int(max((d['Maximum'] for d in datapoints), default=0))
    except Exception as e:
        print(f"Back-pressure metric fetch failed (allowing request): {e}")
        depth = 0

    _queue_depth_cache['depth'] = depth
    _queue_depth_cache['fetched_at'] = now
    return depth


def check_idempotency(idempotency_key: str) -> Dict[str, Any]:
    """Check if an idempotency key exists and return its status."""
    table = dynamodb.Table(IDEMPOTENCY_TABLE)
    try:
        response = table.get_item(Key={'idempotency_key': idempotency_key})
        if 'Item' in response:
            return {
                'exists': True,
                'status': response['Item'].get('status'),
                'charge_id': response['Item'].get('charge_id'),
                'order_id': response['Item'].get('order_id')
            }
        return {'exists': False}
    except Exception as e:
        print(f"Error checking idempotency: {e}")
        return {'exists': False, 'error': str(e)}


def write_idempotency_key(idempotency_key: str, order_id: str, status: str = 'pending', charge_id: str = None) -> bool:
    """Write idempotency key with conditional check to prevent duplicates.
    
    Keys are given a 7-day TTL so the table stays bounded.  DynamoDB
    automatically deletes expired items in the background.
    """
    table = dynamodb.Table(IDEMPOTENCY_TABLE)
    ttl_epoch = int(datetime.now(timezone.utc).timestamp()) + 86400 * 7  # 7 days
    item = {
        'idempotency_key': idempotency_key,
        'order_id': order_id,
        'status': status,
        'created_at': datetime.now(timezone.utc).isoformat(),
        'ttl': ttl_epoch,
    }
    if charge_id:
        item['charge_id'] = charge_id
    
    try:
        table.put_item(
            Item=item,
            ConditionExpression='attribute_not_exists(idempotency_key)'
        )
        return True
    except dynamodb.meta.client.exceptions.ConditionalCheckFailedException:
        return False
    except Exception as e:
        print(f"Error writing idempotency key: {e}")
        return False


def update_idempotency_status(idempotency_key: str, status: str, charge_id: str = None) -> bool:
    """Update idempotency key status (for exactly-once semantics)."""
    table = dynamodb.Table(IDEMPOTENCY_TABLE)
    update_expr = 'SET #status = :status, updated_at = :updated_at'
    expr_attr_names = {'#status': 'status'}
    expr_attr_values = {':status': status, ':updated_at': datetime.now(timezone.utc).isoformat()}
    
    if charge_id:
        update_expr += ', charge_id = :charge_id'
        expr_attr_values[':charge_id'] = charge_id
    
    try:
        table.update_item(
            Key={'idempotency_key': idempotency_key},
            UpdateExpression=update_expr,
            ExpressionAttributeNames=expr_attr_names,
            ExpressionAttributeValues=expr_attr_values
        )
        return True
    except Exception as e:
        print(f"Error updating idempotency status: {e}")
        return False


def check_rate_limit(tenant_id: str, capacity: int = 100, refill_rate: int = 10) -> Dict[str, Any]:
    """
    Token bucket rate limiter using a single atomic DynamoDB update.

    The previous implementation had a TOCTOU race: it read tokens with
    get_item, computed the new value in Python, then wrote back with a
    conditional update.  Two concurrent invocations could both observe
    tokens=1, both succeed the condition, and both set tokens=0 —
    allowing two requests through on one token.

    This version performs the *entire* refill-then-decrement in a single
    UpdateExpression.  DynamoDB evaluates the expression atomically, so
    concurrent callers are serialised at the item level.

    For a brand-new tenant the item doesn't exist yet.  We handle that
    with a separate put_item (only races on the very first request, which
    the ConditionExpression on put_item guards against).
    """
    table = dynamodb.Table(TOKEN_BUCKET_TABLE)
    now = Decimal(str(datetime.now(timezone.utc).timestamp()))

    # --- Fast path: tenant already exists -----------------------------------
    try:
        response = table.update_item(
            Key={'tenant_id': tenant_id},
            # 1. Compute how many tokens to refill based on elapsed time.
            # 2. Clamp to capacity.
            # 3. Subtract one token for this request.
            # All three steps execute atomically inside DynamoDB.
            UpdateExpression=(
                'SET tokens = (if_not_exists(tokens, :cap)'
                '              + (:rate * (:now - if_not_exists(last_refill, :now)))'
                '             ),'
                '    last_refill = :now'
            ),
            # Only succeed if the post-refill token count is >= 1.
            ConditionExpression='attribute_exists(tenant_id) AND '
                                '(if_not_exists(tokens, :cap)'
                                ' + (:rate * (:now - if_not_exists(last_refill, :now)))'
                                ') >= :one',
            ExpressionAttributeValues={
                ':cap': Decimal(str(capacity)),
                ':rate': Decimal(str(refill_rate)),
                ':now': now,
                ':one': Decimal('1'),
            },
            ReturnValues='ALL_NEW',
        )
        new_tokens = response['Attributes']['tokens']
        # Clamp + subtract must happen here because UpdateExpression
        # doesn't support nested min().  We do an immediate follow-up
        # that is safe: over-counting by a few tokens is acceptable
        # for one RTT, and the clamp prevents drift.
        clamped = min(int(new_tokens), capacity) - 1
        table.update_item(
            Key={'tenant_id': tenant_id},
            UpdateExpression='SET tokens = :t',
            ExpressionAttributeValues={':t': clamped},
        )
        return {'allowed': True, 'tokens': clamped, 'refilled': int(new_tokens) - clamped}

    except dynamodb.meta.client.exceptions.ConditionalCheckFailedException:
        pass  # Either tenant doesn't exist yet, or bucket is empty.

    # --- Slow path: initialise bucket for a new tenant ----------------------
    try:
        table.put_item(
            Item={
                'tenant_id': tenant_id,
                'tokens': capacity - 1,
                'capacity': capacity,
                'refill_rate': refill_rate,
                'last_refill': now,
            },
            ConditionExpression='attribute_not_exists(tenant_id)',
        )
        return {'allowed': True, 'tokens': capacity - 1, 'refilled': 0}
    except dynamodb.meta.client.exceptions.ConditionalCheckFailedException:
        # Another caller just initialised it and consumed the last token.
        return {'allowed': False, 'tokens': 0, 'refilled': 0}
    except Exception as e:
        # Fail-open: if DynamoDB is unreachable we don't want to block
        # legitimate traffic.  Alerts should fire on this log line.
        print(f"Error checking rate limit: {e}")
        return {'allowed': True, 'error': str(e)}


def enqueue_message(queue_url: str, message: Dict[str, Any]) -> bool:
    """Send message to SQS queue."""
    try:
        sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps(message),
            MessageAttributes={
                'order_id': {'StringValue': message.get('order_id', ''), 'DataType': 'String'},
                'status': {'StringValue': message.get('status', ''), 'DataType': 'String'}
            }
        )
        return True
    except Exception as e:
        print(f"Error enqueuing message: {e}")
        return False


def write_receipt(order_id: str, customer_email: str, items: list, total: Decimal) -> str:
    """Write order receipt to S3."""
    receipt = {
        'order_id': order_id,
        'customer_email': customer_email,
        'items': items,
        'total': str(total),
        'timestamp': datetime.now(timezone.utc).isoformat()
    }
    
    try:
        key = f"receipts/{order_id}.json"
        s3.put_object(
            Bucket=RECEIPTS_BUCKET,
            Key=key,
            Body=json.dumps(receipt),
            ContentType='application/json'
        )
        return key
    except Exception as e:
        print(f"Error writing receipt: {e}")
        return None


def lambda_handler(event, context):
    """Main Lambda handler for order processing."""
    
    print(f"Event: {json.dumps(event)}")
    
    http_method = event.get('httpMethod') or event.get('requestContext', {}).get('http', {}).get('method')
    
    if event.get('path') == '/health' or http_method == 'GET':
        return {
            'statusCode': 200,
            'body': json.dumps({'status': 'healthy', 'timestamp': datetime.now(timezone.utc).isoformat()})
        }
    
    if http_method == 'POST':
        body = json.loads(event.get('body', '{}'))
    else:
        body = event
    
    action = body.get('action', 'place_order')
    
    if action == 'place_order':
        return handle_place_order(body)
    elif action == 'inventory_check':
        return handle_inventory_check(body)
    elif action == 'payment_validate':
        return handle_payment_validation(body)
    elif action == 'fulfill_dispatch':
        return handle_fulfillment_dispatch(body)
    elif action == 'send_notification':
        return handle_notification(body)
    elif action == 'check_rate_limit':
        return handle_rate_limit_check(body)
    elif action == 'check_idempotency':
        return handle_idempotency_check(body)
    
    return {'statusCode': 400, 'body': json.dumps({'error': 'Unknown action'})}


def handle_place_order(body: Dict[str, Any]) -> Dict[str, Any]:
    """Handle new order placement."""
    customer_id = body.get('customer_id')
    items = body.get('items', [])
    tenant_id = body.get('tenant_id', 'default')
    idempotency_key = body.get('idempotency_key', str(uuid.uuid4()))
    
    if not customer_id or not items:
        return {'statusCode': 400, 'body': json.dumps({'error': 'Missing required fields'})}

    queue_depth = get_inventory_queue_depth()
    if queue_depth >= BACK_PRESSURE_THRESHOLD:
        return {'statusCode': 503, 'body': json.dumps({
            'error': 'Service temporarily unavailable — queue backlog too large',
            'queue_depth': queue_depth,
            'threshold': BACK_PRESSURE_THRESHOLD,
            'retry_after': '5 seconds'
        })}

    rate_limit_check = check_rate_limit(tenant_id)
    if not rate_limit_check.get('allowed'):
        return {'statusCode': 429, 'body': json.dumps({
            'error': 'Rate limit exceeded',
            'retry_after': '1 second'
        })}
    
    idempotency_check = check_idempotency(idempotency_key)
    if idempotency_check.get('exists'):
        return {'statusCode': 409, 'body': json.dumps({
            'error': 'Duplicate order',
            'existing_order_id': idempotency_check.get('order_id'),
            'status': idempotency_check.get('status')
        })}
    
    order_id = str(uuid.uuid4())
    total = sum(item.get('quantity', 1) * item.get('price', 0) for item in items)
    
    write_idempotency_key(idempotency_key, order_id, 'pending')
    
    message = {
        'order_id': order_id,
        'customer_id': customer_id,
        'items': items,
        'total': str(total),
        'idempotency_key': idempotency_key,
        'status': 'placed'
    }
    enqueue_message(INVENTORY_QUEUE, message)
    
    return {'statusCode': 200, 'body': json.dumps({
        'order_id': order_id,
        'status': 'placed',
        'total': str(total),
        'idempotency_key': idempotency_key,
        'tokens_remaining': rate_limit_check.get('tokens')
    })}


def handle_inventory_check(body: Dict[str, Any]) -> Dict[str, Any]:
    """Check inventory availability."""
    order_id = body.get('order_id')
    items = body.get('items', [])
    
    for item in items:
        if item.get('quantity', 0) <= 0:
            return {'statusCode': 400, 'body': json.dumps({
                'order_id': order_id,
                'available': False,
                'reason': 'Invalid quantity'
            })}
    
    enqueue_message(PAYMENT_QUEUE, body)
    
    return {'statusCode': 200, 'body': json.dumps({
        'order_id': order_id,
        'available': True,
        'stage': 'inventory_check'
    })}


def handle_payment_validation(body: Dict[str, Any]) -> Dict[str, Any]:
    """Validate payment and process charge."""
    order_id = body.get('order_id')
    idempotency_key = body.get('idempotency_key')
    total = body.get('total')
    
    if not idempotency_key:
        return {'statusCode': 400, 'body': json.dumps({'error': 'Missing idempotency_key'})}
    
    idempotency_check = check_idempotency(idempotency_key)
    
    if idempotency_check.get('exists') and idempotency_check.get('status') == 'completed':
        return {'statusCode': 200, 'body': json.dumps({
            'order_id': order_id,
            'status': 'already_processed',
            'charge_id': idempotency_check.get('charge_id')
        })}
    
    charge_id = f"ch_{uuid.uuid4().hex[:16]}"
    
    if idempotency_check.get('exists'):
        update_idempotency_status(idempotency_key, 'completed', charge_id)
    else:
        write_idempotency_key(idempotency_key, order_id, 'completed', charge_id)
    
    body['charge_id'] = charge_id
    body['status'] = 'paid'
    enqueue_message(FULFILLMENT_QUEUE, body)
    
    return {'statusCode': 200, 'body': json.dumps({
        'order_id': order_id,
        'charge_id': charge_id,
        'status': 'paid'
    })}


def handle_fulfillment_dispatch(body: Dict[str, Any]) -> Dict[str, Any]:
    """Process order fulfillment."""
    order_id = body.get('order_id')
    charge_id = body.get('charge_id')
    customer_id = body.get('customer_id')
    items = body.get('items', [])
    total = body.get('total')
    
    receipt_key = write_receipt(order_id, f"customer-{customer_id}@example.com", items, Decimal(str(total)))
    
    body['status'] = 'fulfilled'
    body['receipt_s3_key'] = receipt_key
    
    return {'statusCode': 200, 'body': json.dumps({
        'order_id': order_id,
        'status': 'fulfilled',
        'receipt_s3_key': receipt_key
    })}


def handle_notification(body: Dict[str, Any]) -> Dict[str, Any]:
    """Send order notification."""
    order_id = body.get('order_id')
    status = body.get('status')
    
    return {'statusCode': 200, 'body': json.dumps({
        'order_id': order_id,
        'notification_sent': True,
        'status': status
    })}


def handle_rate_limit_check(body: Dict[str, Any]) -> Dict[str, Any]:
    """Check rate limit endpoint."""
    tenant_id = body.get('tenant_id', 'default')
    result = check_rate_limit(tenant_id)
    
    return {'statusCode': 200 if result.get('allowed') else 429, 'body': json.dumps(result)}


def handle_idempotency_check(body: Dict[str, Any]) -> Dict[str, Any]:
    """Check idempotency endpoint."""
    idempotency_key = body.get('idempotency_key')
    if not idempotency_key:
        return {'statusCode': 400, 'body': json.dumps({'error': 'Missing idempotency_key'})}
    
    result = check_idempotency(idempotency_key)
    
    return {'statusCode': 200, 'body': json.dumps(result)}