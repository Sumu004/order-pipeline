"""
Back-pressure detection via SQS queue depth.

Caches the CloudWatch metric for 10 seconds to avoid adding latency to
every request.  Falls back to 0 on any error so a CloudWatch outage
never blocks orders.
"""

import time
from datetime import datetime, timezone
from typing import Any, Dict

from lambda_handlers.shared.config import cloudwatch, INVENTORY_QUEUE_NAME, BACK_PRESSURE_THRESHOLD

_cache: Dict[str, Any] = {'depth': 0, 'fetched_at': 0.0}
_CACHE_TTL = 10  # seconds


def get_queue_depth() -> int:
    """Return ApproximateNumberOfMessages, cached for 10s."""
    now = time.monotonic()
    if now - _cache['fetched_at'] < _CACHE_TTL:
        return _cache['depth']

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

    _cache['depth'] = depth
    _cache['fetched_at'] = now
    return depth


def is_overloaded() -> bool:
    """Return True if the inventory queue depth exceeds the threshold."""
    return get_queue_depth() >= BACK_PRESSURE_THRESHOLD
