"""
Idempotency key operations for exactly-once semantics.

Keys are written with a 7-day DynamoDB TTL so the table stays bounded.
The conditional PutItem prevents duplicate creation even under
concurrent Lambda invocations.
"""

from datetime import datetime, timezone
from typing import Dict, Any

from lambda_handlers.shared.config import dynamodb, IDEMPOTENCY_TABLE
from lambda_handlers.shared.logging import get_logger

log = get_logger('idempotency')


def check(idempotency_key: str) -> Dict[str, Any]:
    """Check if an idempotency key exists and return its status."""
    table = dynamodb.Table(IDEMPOTENCY_TABLE)
    try:
        response = table.get_item(Key={'idempotency_key': idempotency_key})
        if 'Item' in response:
            return {
                'exists': True,
                'status': response['Item'].get('status'),
                'charge_id': response['Item'].get('charge_id'),
                'order_id': response['Item'].get('order_id'),
            }
        return {'exists': False}
    except Exception as e:
        log.error('check_failed', idempotency_key=idempotency_key, error=str(e))
        return {'exists': False, 'error': str(e)}


def write(idempotency_key: str, order_id: str, status: str = 'pending',
          charge_id: str = None) -> bool:
    """Write idempotency key with conditional check to prevent duplicates.

    Keys are given a 7-day TTL so the table stays bounded.  DynamoDB
    automatically deletes expired items in the background.
    """
    table = dynamodb.Table(IDEMPOTENCY_TABLE)
    ttl_epoch = int(datetime.now(timezone.utc).timestamp()) + 86400 * 7
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
            ConditionExpression='attribute_not_exists(idempotency_key)',
        )
        return True
    except dynamodb.meta.client.exceptions.ConditionalCheckFailedException:
        return False
    except Exception as e:
        log.error('write_failed', idempotency_key=idempotency_key, error=str(e))
        return False


def update_status(idempotency_key: str, status: str,
                  charge_id: str = None) -> bool:
    """Update idempotency key status (for exactly-once semantics)."""
    table = dynamodb.Table(IDEMPOTENCY_TABLE)
    update_expr = 'SET #status = :status, updated_at = :updated_at'
    names = {'#status': 'status'}
    values = {
        ':status': status,
        ':updated_at': datetime.now(timezone.utc).isoformat(),
    }

    if charge_id:
        update_expr += ', charge_id = :charge_id'
        values[':charge_id'] = charge_id

    try:
        table.update_item(
            Key={'idempotency_key': idempotency_key},
            UpdateExpression=update_expr,
            ExpressionAttributeNames=names,
            ExpressionAttributeValues=values,
        )
        return True
    except Exception as e:
        log.error('update_status_failed', idempotency_key=idempotency_key, error=str(e))
        return False
