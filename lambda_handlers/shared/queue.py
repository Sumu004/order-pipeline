"""SQS message helpers."""

import json
from typing import Any, Dict

from lambda_handlers.shared.config import sqs
from lambda_handlers.shared.logging import get_logger

log = get_logger('queue')


def enqueue(queue_url: str, message: Dict[str, Any]) -> bool:
    """Send a message to an SQS queue.

    The correlation_id (if present) is passed as a MessageAttribute so
    that downstream consumers can continue the trace without parsing the
    body.
    """
    try:
        attributes = {
            'order_id': {
                'StringValue': message.get('order_id', ''),
                'DataType': 'String',
            },
            'status': {
                'StringValue': message.get('status', ''),
                'DataType': 'String',
            },
        }
        if message.get('correlation_id'):
            attributes['correlation_id'] = {
                'StringValue': message['correlation_id'],
                'DataType': 'String',
            }

        sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps(message),
            MessageAttributes=attributes,
        )
        return True
    except Exception as e:
        log.error('enqueue_failed', queue_url=queue_url, error=str(e))
        return False
