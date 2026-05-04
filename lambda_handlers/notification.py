"""
Notification Lambda — triggered after fulfillment.

Sends order status notifications.  In production this would integrate
with SES, SNS, or a third-party provider.
"""

import json
from typing import Any, Dict

from lambda_handlers.shared.logging import get_logger

log = get_logger('notification')


def lambda_handler(event, context):
    """Entry-point for notification Lambda (SQS trigger)."""
    for record in event.get('Records', [event]):
        body = json.loads(record.get('body', '{}')) if 'body' in record else record
        handle_notification(body)


def handle_notification(body: Dict[str, Any]) -> Dict[str, Any]:
    """Send order notification."""
    order_id = body.get('order_id')
    status = body.get('status')
    correlation_id = body.get('correlation_id', 'unknown')

    logger = log.bind(correlation_id=correlation_id, order_id=order_id)
    logger.info('notification_sent', status=status)

    return {'statusCode': 200, 'body': json.dumps({
        'order_id': order_id,
        'notification_sent': True,
        'status': status,
    })}
