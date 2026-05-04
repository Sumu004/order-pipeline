"""S3 receipt writer."""

import json
from datetime import datetime, timezone
from decimal import Decimal
from typing import List, Optional

from lambda_handlers.shared.config import s3, RECEIPTS_BUCKET
from lambda_handlers.shared.logging import get_logger

log = get_logger('receipts')


def write(order_id: str, customer_email: str, items: List,
          total: Decimal) -> Optional[str]:
    """Write order receipt to S3 and return the object key."""
    receipt = {
        'order_id': order_id,
        'customer_email': customer_email,
        'items': items,
        'total': str(total),
        'timestamp': datetime.now(timezone.utc).isoformat(),
    }

    try:
        key = f"receipts/{order_id}.json"
        s3.put_object(
            Bucket=RECEIPTS_BUCKET,
            Key=key,
            Body=json.dumps(receipt),
            ContentType='application/json',
        )
        return key
    except Exception as e:
        log.error('write_receipt_failed', order_id=order_id, error=str(e))
        return None
