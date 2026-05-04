"""
Fulfillment Lambda — triggered by the fulfillment SQS queue.

Writes the receipt to S3 and marks the order as fulfilled.
"""

import json
from decimal import Decimal
from typing import Any, Dict

from lambda_handlers.shared.receipts import write as write_receipt
from lambda_handlers.shared.logging import get_logger

log = get_logger('fulfillment')


def lambda_handler(event, context):
    """Entry-point for fulfillment Lambda (SQS trigger)."""
    for record in event.get('Records', [event]):
        body = json.loads(record.get('body', '{}')) if 'body' in record else record
        handle_fulfillment_dispatch(body)


def handle_fulfillment_dispatch(body: Dict[str, Any]) -> Dict[str, Any]:
    """Process order fulfillment."""
    order_id = body.get('order_id')
    charge_id = body.get('charge_id')
    customer_id = body.get('customer_id')
    items = body.get('items', [])
    total = body.get('total')
    correlation_id = body.get('correlation_id', 'unknown')

    logger = log.bind(correlation_id=correlation_id, order_id=order_id)

    receipt_key = write_receipt(
        order_id,
        f"customer-{customer_id}@example.com",
        items,
        Decimal(str(total)),
    )

    logger.info('order_fulfilled', receipt_s3_key=receipt_key, charge_id=charge_id)

    return {'statusCode': 200, 'body': json.dumps({
        'order_id': order_id,
        'status': 'fulfilled',
        'receipt_s3_key': receipt_key,
    })}
