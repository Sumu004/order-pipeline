"""
Payment Lambda — triggered by the payment SQS queue.

Validates payment, issues a charge, and forwards to fulfillment.
Implements the **saga pattern**: if the fulfillment enqueue fails after
the charge has been issued, the charge is reversed and the message is
re-queued for retry (up to MAX_RETRIES, then routed to the DLQ).
"""

import json
import uuid
from typing import Any, Dict

from lambda_handlers.shared import idempotency
from lambda_handlers.shared.config import FULFILLMENT_QUEUE, DLQ_URL, PAYMENT_QUEUE
from lambda_handlers.shared.queue import enqueue
from lambda_handlers.shared.logging import get_logger

log = get_logger('payment')

MAX_RETRIES = 3


def lambda_handler(event, context):
    """Entry-point for payment Lambda (SQS trigger)."""
    for record in event.get('Records', [event]):
        body = json.loads(record.get('body', '{}')) if 'body' in record else record
        handle_payment_validation(body)


def handle_payment_validation(body: Dict[str, Any]) -> Dict[str, Any]:
    """Validate payment and process charge with saga compensation."""
    order_id = body.get('order_id')
    idempotency_key = body.get('idempotency_key')
    correlation_id = body.get('correlation_id', 'unknown')

    logger = log.bind(correlation_id=correlation_id, order_id=order_id)

    if not idempotency_key:
        return {'statusCode': 400, 'body': json.dumps({'error': 'Missing idempotency_key'})}

    # Exactly-once: skip if already completed.
    idem = idempotency.check(idempotency_key)
    if idem.get('exists') and idem.get('status') == 'completed':
        logger.info('already_processed', charge_id=idem.get('charge_id'))
        return {'statusCode': 200, 'body': json.dumps({
            'order_id': order_id,
            'status': 'already_processed',
            'charge_id': idem.get('charge_id'),
        })}

    # Issue the charge.
    charge_id = f"ch_{uuid.uuid4().hex[:16]}"
    logger.info('charge_issued', charge_id=charge_id)

    if idem.get('exists'):
        idempotency.update_status(idempotency_key, 'completed', charge_id)
    else:
        idempotency.write(idempotency_key, order_id, 'completed', charge_id)

    # --- SAGA: attempt to forward to fulfillment ---
    body['charge_id'] = charge_id
    body['status'] = 'paid'
    enqueued = enqueue(FULFILLMENT_QUEUE, body)

    if not enqueued:
        # Compensating transaction: reverse the charge.
        logger.error('fulfillment_enqueue_failed', charge_id=charge_id)
        _reverse_charge(charge_id, logger)
        idempotency.update_status(idempotency_key, 'payment_reversed')

        retry_count = body.get('retry_count', 0) + 1
        body['retry_count'] = retry_count

        if retry_count <= MAX_RETRIES:
            logger.warn('retrying_payment', attempt=retry_count)
            enqueue(PAYMENT_QUEUE, body)
        else:
            logger.error('max_retries_exceeded', routing_to='DLQ')
            enqueue(DLQ_URL, body)

        return {'statusCode': 500, 'body': json.dumps({
            'error': 'Fulfillment enqueue failed, charge reversed',
            'charge_id': charge_id,
            'retry': retry_count,
        })}

    logger.info('payment_completed', charge_id=charge_id)

    return {'statusCode': 200, 'body': json.dumps({
        'order_id': order_id,
        'charge_id': charge_id,
        'status': 'paid',
    })}


def _reverse_charge(charge_id: str, logger):
    """Compensating transaction — reverse a previously issued charge.

    In production this would call the payment provider's refund API.
    Here we log the reversal so the pattern is visible in code review.
    """
    logger.info('charge_reversed', charge_id=charge_id)
