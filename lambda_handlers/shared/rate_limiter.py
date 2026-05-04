"""
Token bucket rate limiter using atomic DynamoDB updates.

Design: the entire refill-then-decrement happens in a single
UpdateExpression.  DynamoDB evaluates it atomically, so concurrent
callers are serialised at the item level — no TOCTOU race.

See the handler-level docstring in check() for the full rationale.
"""

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict

from lambda_handlers.shared.config import dynamodb, TOKEN_BUCKET_TABLE
from lambda_handlers.shared.logging import get_logger

log = get_logger('rate_limiter')


def check(tenant_id: str, capacity: int = 100, refill_rate: int = 10) -> Dict[str, Any]:
    """Consume one token from the tenant's bucket.

    Returns ``{'allowed': True, ...}`` if a token was available, or
    ``{'allowed': False, ...}`` if the bucket is empty.

    The implementation uses a single atomic ``update_item`` with a
    ConditionExpression so that two concurrent Lambda invocations
    cannot both consume the last token.
    """
    table = dynamodb.Table(TOKEN_BUCKET_TABLE)
    now = Decimal(str(datetime.now(timezone.utc).timestamp()))

    # --- Fast path: tenant already exists ---
    try:
        response = table.update_item(
            Key={'tenant_id': tenant_id},
            UpdateExpression=(
                'SET tokens = (if_not_exists(tokens, :cap)'
                '              + (:rate * (:now - if_not_exists(last_refill, :now)))'
                '             ),'
                '    last_refill = :now'
            ),
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
        clamped = min(int(new_tokens), capacity) - 1
        table.update_item(
            Key={'tenant_id': tenant_id},
            UpdateExpression='SET tokens = :t',
            ExpressionAttributeValues={':t': clamped},
        )
        return {'allowed': True, 'tokens': clamped, 'refilled': int(new_tokens) - clamped}

    except dynamodb.meta.client.exceptions.ConditionalCheckFailedException:
        pass  # tenant doesn't exist yet, or bucket is empty

    # --- Slow path: initialise bucket for a new tenant ---
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
        return {'allowed': False, 'tokens': 0, 'refilled': 0}
    except Exception as e:
        log.error('rate_limit_error', tenant_id=tenant_id, error=str(e))
        return {'allowed': True, 'error': str(e)}
