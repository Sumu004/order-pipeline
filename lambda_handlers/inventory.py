"""
Inventory Lambda — triggered by the inventory SQS queue.

Validates item quantities and forwards the order to the payment queue.
"""

import json
from typing import Any, Dict

from lambda_handlers.shared.config import PAYMENT_QUEUE
from lambda_handlers.shared.queue import enqueue
from lambda_handlers.shared.logging import get_logger

log = get_logger('inventory')


def lambda_handler(event, context):
    """Entry-point for inventory Lambda (SQS trigger)."""
    for record in event.get('Records', [event]):
        body = json.loads(record.get('body', '{}')) if 'body' in record else record
        handle_inventory_check(body)


def handle_inventory_check(body: Dict[str, Any]) -> Dict[str, Any]:
    """Check inventory availability."""
    order_id = body.get('order_id')
    items = body.get('items', [])
    correlation_id = body.get('correlation_id', 'unknown')

    logger = log.bind(correlation_id=correlation_id, order_id=order_id)

    for item in items:
        if item.get('quantity', 0) <= 0:
            logger.warn('invalid_quantity', product_id=item.get('product_id'))
            return {'statusCode': 400, 'body': json.dumps({
                'order_id': order_id,
                'available': False,
                'reason': 'Invalid quantity',
            })}

    # Forward to payment stage.
    body['status'] = 'inventory_checked'
    enqueue(PAYMENT_QUEUE, body)

    logger.info('inventory_passed', num_items=len(items))

    return {'statusCode': 200, 'body': json.dumps({
        'order_id': order_id,
        'available': True,
        'stage': 'inventory_check',
    })}
