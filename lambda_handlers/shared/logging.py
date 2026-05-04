"""
Structured JSON logger with correlation ID threading.

Every log line is a JSON object containing:
  - timestamp, level, stage, message
  - correlation_id (threaded through SQS message attributes)
  - order_id, tenant_id when available

This makes logs grep-friendly in CloudWatch Logs Insights and
trivially parseable by any log-aggregation pipeline.
"""

import json
from datetime import datetime, timezone
from typing import Dict, Any, Optional


class StructuredLogger:
    """JSON logger that threads a correlation_id through all stages."""

    def __init__(self, stage: str):
        self.stage = stage
        self._correlation_id: Optional[str] = None
        self._context: Dict[str, Any] = {}

    def bind(self, **kwargs) -> 'StructuredLogger':
        """Return a new logger instance with extra context fields."""
        new = StructuredLogger(self.stage)
        new._correlation_id = self._correlation_id
        new._context = {**self._context, **kwargs}
        return new

    def set_correlation_id(self, correlation_id: str):
        self._correlation_id = correlation_id

    def _emit(self, level: str, message: str, **extra):
        record = {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'level': level,
            'stage': self.stage,
            'correlation_id': self._correlation_id or 'none',
            'message': message,
            **self._context,
            **extra,
        }
        # CloudWatch ingests stdout as structured JSON automatically.
        print(json.dumps(record, default=str))

    def info(self, message: str, **extra):
        self._emit('INFO', message, **extra)

    def warn(self, message: str, **extra):
        self._emit('WARN', message, **extra)

    def error(self, message: str, **extra):
        self._emit('ERROR', message, **extra)


def get_logger(stage: str) -> StructuredLogger:
    """Factory — one logger per Lambda handler module."""
    return StructuredLogger(stage)
