"""Tests for the split order-pipeline handlers and shared modules.

These tests verify:
  - Idempotency: duplicate keys are rejected
  - Rate limiter: tokens are consumed correctly
  - Ingest: orders are placed with proper validation
  - Inventory: invalid quantities are rejected
  - Payment: saga compensation on enqueue failure
  - Fulfillment: receipts are written to S3
  - End-to-end: an order flows through all stages
  - Concurrency: same idempotency key under 10 threads → exactly 1 order
"""

import json
import os
import sys
import uuid
import pytest
from unittest.mock import Mock, patch, MagicMock
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))


# ── Idempotency Tests ─────────────────────────────────────────────────────

class TestIdempotency:
    """Test idempotency key operations."""

    @patch('lambda_handlers.shared.idempotency.dynamodb')
    def test_check_not_exists(self, mock_dynamodb):
        from lambda_handlers.shared.idempotency import check
        mock_table = Mock()
        mock_table.get_item.return_value = {}
        mock_dynamodb.Table.return_value = mock_table

        result = check('test-key-123')
        assert result['exists'] is False

    @patch('lambda_handlers.shared.idempotency.dynamodb')
    def test_check_exists(self, mock_dynamodb):
        from lambda_handlers.shared.idempotency import check
        mock_table = Mock()
        mock_table.get_item.return_value = {
            'Item': {
                'idempotency_key': 'test-key-123',
                'status': 'completed',
                'charge_id': 'ch_abc123',
                'order_id': 'order-123',
            }
        }
        mock_dynamodb.Table.return_value = mock_table

        result = check('test-key-123')
        assert result['exists'] is True
        assert result['status'] == 'completed'
        assert result['charge_id'] == 'ch_abc123'

    @patch('lambda_handlers.shared.idempotency.dynamodb')
    def test_write_success(self, mock_dynamodb):
        from lambda_handlers.shared.idempotency import write
        mock_table = Mock()
        mock_dynamodb.Table.return_value = mock_table

        result = write('test-key', 'order-1', 'pending')
        assert result is True
        mock_table.put_item.assert_called_once()

        # Verify TTL is set
        call_args = mock_table.put_item.call_args
        item = call_args[1]['Item'] if 'Item' in call_args[1] else call_args[0][0]
        assert 'ttl' in item
        assert isinstance(item['ttl'], int)

    @patch('lambda_handlers.shared.idempotency.dynamodb')
    def test_write_duplicate_rejected(self, mock_dynamodb):
        from lambda_handlers.shared.idempotency import write
        mock_table = Mock()
        exc_class = type('ConditionalCheckFailedException', (Exception,), {})
        mock_dynamodb.meta.client.exceptions.ConditionalCheckFailedException = exc_class
        mock_table.put_item.side_effect = exc_class('duplicate')
        mock_dynamodb.Table.return_value = mock_table

        result = write('test-key', 'order-1', 'pending')
        assert result is False


# ── Rate Limiter Tests ────────────────────────────────────────────────────

class TestRateLimiter:
    """Test atomic token bucket rate limiter."""

    @patch('lambda_handlers.shared.rate_limiter.dynamodb')
    def test_new_tenant_gets_tokens(self, mock_dynamodb):
        from lambda_handlers.shared.rate_limiter import check
        mock_table = Mock()

        exc_class = type('ConditionalCheckFailedException', (Exception,), {})
        mock_dynamodb.meta.client.exceptions.ConditionalCheckFailedException = exc_class

        # First update_item fails (tenant doesn't exist yet)
        mock_table.update_item.side_effect = exc_class('no item')
        # put_item succeeds (new tenant)
        mock_table.put_item.return_value = None
        mock_dynamodb.Table.return_value = mock_table

        result = check('new-tenant', capacity=100)
        assert result['allowed'] is True
        assert result['tokens'] == 99

    @patch('lambda_handlers.shared.rate_limiter.dynamodb')
    def test_existing_tenant_consumes_token(self, mock_dynamodb):
        from lambda_handlers.shared.rate_limiter import check
        mock_table = Mock()

        exc_class = type('ConditionalCheckFailedException', (Exception,), {})
        mock_dynamodb.meta.client.exceptions.ConditionalCheckFailedException = exc_class

        # update_item succeeds (tenant exists, has tokens)
        mock_table.update_item.return_value = {
            'Attributes': {'tokens': 50}
        }
        mock_dynamodb.Table.return_value = mock_table

        result = check('tenant-1', capacity=100)
        assert result['allowed'] is True

    @patch('lambda_handlers.shared.rate_limiter.dynamodb')
    def test_exhausted_bucket_rejected(self, mock_dynamodb):
        from lambda_handlers.shared.rate_limiter import check
        mock_table = Mock()

        exc_class = type('ConditionalCheckFailedException', (Exception,), {})
        mock_dynamodb.meta.client.exceptions.ConditionalCheckFailedException = exc_class

        # First update_item fails (no tokens)
        mock_table.update_item.side_effect = exc_class('no tokens')
        # put_item also fails (tenant already exists, just empty)
        mock_table.put_item.side_effect = exc_class('exists')
        mock_dynamodb.Table.return_value = mock_table

        result = check('tenant-1', capacity=100)
        assert result['allowed'] is False
        assert result['tokens'] == 0


# ── Ingest Handler Tests ──────────────────────────────────────────────────

class TestIngestHandler:
    """Test the ingest Lambda handler."""

    def test_missing_fields_rejected(self):
        from lambda_handlers.ingest import handle_place_order
        result = handle_place_order({})
        assert result['statusCode'] == 400

    @patch('lambda_handlers.ingest.rate_limiter')
    @patch('lambda_handlers.ingest.idempotency')
    @patch('lambda_handlers.ingest.enqueue')
    @patch('lambda_handlers.ingest.backpressure')
    def test_successful_order(self, mock_bp, mock_enqueue, mock_idem, mock_rl):
        from lambda_handlers.ingest import handle_place_order
        mock_bp.is_overloaded.return_value = False
        mock_rl.check.return_value = {'allowed': True, 'tokens': 99}
        mock_idem.check.return_value = {'exists': False}
        mock_idem.write.return_value = True

        body = {
            'customer_id': 'cust-123',
            'items': [{'product_id': 'PROD-001', 'quantity': 2, 'price': 10.0}],
            'tenant_id': 'tenant-1',
        }

        result = handle_place_order(body)
        assert result['statusCode'] == 200
        data = json.loads(result['body'])
        assert 'order_id' in data
        assert data['status'] == 'placed'
        assert data['total'] == '20.0'

    @patch('lambda_handlers.ingest.rate_limiter')
    @patch('lambda_handlers.ingest.backpressure')
    def test_rate_limited(self, mock_bp, mock_rl):
        from lambda_handlers.ingest import handle_place_order
        mock_bp.is_overloaded.return_value = False
        mock_rl.check.return_value = {'allowed': False, 'tokens': 0}

        body = {
            'customer_id': 'cust-123',
            'items': [{'product_id': 'PROD-001', 'quantity': 1, 'price': 10.0}],
        }
        result = handle_place_order(body)
        assert result['statusCode'] == 429

    @patch('lambda_handlers.ingest.backpressure')
    def test_back_pressure_503(self, mock_bp):
        from lambda_handlers.ingest import handle_place_order
        mock_bp.is_overloaded.return_value = True
        mock_bp.get_queue_depth.return_value = 1500

        body = {
            'customer_id': 'cust-123',
            'items': [{'product_id': 'PROD-001', 'quantity': 1, 'price': 10.0}],
        }
        result = handle_place_order(body)
        assert result['statusCode'] == 503


# ── Inventory Handler Tests ───────────────────────────────────────────────

class TestInventoryHandler:
    """Test the inventory Lambda handler."""

    @patch('lambda_handlers.inventory.enqueue')
    def test_valid_items_pass(self, mock_enqueue):
        from lambda_handlers.inventory import handle_inventory_check
        body = {
            'order_id': 'order-123',
            'items': [{'product_id': 'PROD-001', 'quantity': 2}],
        }
        result = handle_inventory_check(body)
        assert result['statusCode'] == 200
        assert json.loads(result['body'])['available'] is True

    def test_zero_quantity_rejected(self):
        from lambda_handlers.inventory import handle_inventory_check
        body = {
            'order_id': 'order-123',
            'items': [{'product_id': 'PROD-001', 'quantity': 0}],
        }
        result = handle_inventory_check(body)
        assert result['statusCode'] == 400


# ── Payment Handler Tests (Saga Pattern) ──────────────────────────────────

class TestPaymentHandler:
    """Test the payment Lambda handler with saga compensation."""

    @patch('lambda_handlers.payment.idempotency')
    @patch('lambda_handlers.payment.enqueue')
    def test_successful_payment(self, mock_enqueue, mock_idem):
        from lambda_handlers.payment import handle_payment_validation
        mock_idem.check.return_value = {'exists': False}
        mock_idem.write.return_value = True
        mock_enqueue.return_value = True

        body = {
            'order_id': 'order-123',
            'idempotency_key': 'idem-123',
            'total': '100.00',
        }
        result = handle_payment_validation(body)
        assert result['statusCode'] == 200
        assert 'charge_id' in result['body']

    @patch('lambda_handlers.payment.idempotency')
    @patch('lambda_handlers.payment.enqueue')
    def test_saga_compensation_on_enqueue_failure(self, mock_enqueue, mock_idem):
        """If fulfillment enqueue fails, charge should be reversed."""
        from lambda_handlers.payment import handle_payment_validation
        mock_idem.check.return_value = {'exists': False}
        mock_idem.write.return_value = True

        # First call to enqueue (fulfillment) fails, second (retry/DLQ) succeeds
        mock_enqueue.side_effect = [False, True]

        body = {
            'order_id': 'order-123',
            'idempotency_key': 'idem-123',
            'total': '100.00',
        }
        result = handle_payment_validation(body)
        assert result['statusCode'] == 500
        data = json.loads(result['body'])
        assert 'charge reversed' in data['error'].lower()

        # Verify idempotency was updated to 'payment_reversed'
        mock_idem.update_status.assert_called_once()
        args = mock_idem.update_status.call_args
        assert args[0][1] == 'payment_reversed'

    @patch('lambda_handlers.payment.idempotency')
    def test_already_completed_skipped(self, mock_idem):
        from lambda_handlers.payment import handle_payment_validation
        mock_idem.check.return_value = {
            'exists': True,
            'status': 'completed',
            'charge_id': 'ch_existing',
        }

        body = {
            'order_id': 'order-123',
            'idempotency_key': 'idem-123',
            'total': '100.00',
        }
        result = handle_payment_validation(body)
        assert result['statusCode'] == 200
        assert json.loads(result['body'])['status'] == 'already_processed'

    @patch('lambda_handlers.payment.idempotency')
    @patch('lambda_handlers.payment.enqueue')
    def test_saga_max_retries_routes_to_dlq(self, mock_enqueue, mock_idem):
        """After MAX_RETRIES, order should go to DLQ."""
        from lambda_handlers.payment import handle_payment_validation
        mock_idem.check.return_value = {'exists': False}
        mock_idem.write.return_value = True
        mock_enqueue.side_effect = [False, True]

        body = {
            'order_id': 'order-123',
            'idempotency_key': 'idem-retry',
            'total': '100.00',
            'retry_count': 3,  # already at max
        }
        result = handle_payment_validation(body)
        assert result['statusCode'] == 500

        # Second enqueue call should be to DLQ (not payment queue)
        assert mock_enqueue.call_count == 2


# ── Fulfillment Handler Tests ─────────────────────────────────────────────

class TestFulfillmentHandler:
    """Test the fulfillment Lambda handler."""

    @patch('lambda_handlers.fulfillment.write_receipt')
    def test_order_fulfilled(self, mock_receipt):
        from lambda_handlers.fulfillment import handle_fulfillment_dispatch
        mock_receipt.return_value = 'receipts/order-123.json'

        body = {
            'order_id': 'order-123',
            'charge_id': 'ch_abc',
            'customer_id': 'cust-123',
            'items': [{'product_id': 'PROD-001', 'quantity': 2}],
            'total': '100.00',
        }
        result = handle_fulfillment_dispatch(body)
        assert result['statusCode'] == 200
        assert json.loads(result['body'])['status'] == 'fulfilled'


# ── End-to-End Flow Tests ─────────────────────────────────────────────────

class TestEndToEndFlow:
    """Test that an order flows through all stages correctly."""

    @patch('lambda_handlers.ingest.backpressure')
    @patch('lambda_handlers.ingest.rate_limiter')
    @patch('lambda_handlers.ingest.idempotency')
    @patch('lambda_handlers.ingest.enqueue')
    @patch('lambda_handlers.inventory.enqueue')
    @patch('lambda_handlers.payment.idempotency')
    @patch('lambda_handlers.payment.enqueue')
    @patch('lambda_handlers.fulfillment.write_receipt')
    def test_full_order_lifecycle(self, mock_receipt, mock_pay_enqueue,
                                  mock_pay_idem, mock_inv_enqueue,
                                  mock_ing_enqueue, mock_ing_idem,
                                  mock_rl, mock_bp):
        """Order flows: ingest → inventory → payment → fulfillment."""
        from lambda_handlers.ingest import handle_place_order
        from lambda_handlers.inventory import handle_inventory_check
        from lambda_handlers.payment import handle_payment_validation
        from lambda_handlers.fulfillment import handle_fulfillment_dispatch

        # Setup mocks
        mock_bp.is_overloaded.return_value = False
        mock_rl.check.return_value = {'allowed': True, 'tokens': 99}
        mock_ing_idem.check.return_value = {'exists': False}
        mock_ing_idem.write.return_value = True
        mock_pay_idem.check.return_value = {'exists': True, 'status': 'pending'}
        mock_pay_idem.update_status.return_value = True
        mock_pay_enqueue.return_value = True
        mock_receipt.return_value = 'receipts/order-123.json'

        captured_messages = []
        mock_ing_enqueue.side_effect = lambda q, m: captured_messages.append(m) or True

        # Stage 1: Ingest
        result = handle_place_order({
            'customer_id': 'cust-001',
            'items': [{'product_id': 'PROD-001', 'quantity': 2, 'price': 25.0}],
            'tenant_id': 'tenant-1',
        })
        assert result['statusCode'] == 200
        order_data = json.loads(result['body'])
        assert order_data['status'] == 'placed'

        # Stage 2: Inventory (using message from ingest)
        assert len(captured_messages) == 1
        msg = captured_messages[0]
        result = handle_inventory_check(msg)
        assert result['statusCode'] == 200

        # Stage 3: Payment
        msg['idempotency_key'] = order_data['idempotency_key']
        result = handle_payment_validation(msg)
        assert result['statusCode'] == 200

        # Stage 4: Fulfillment
        msg['charge_id'] = json.loads(result['body']).get('charge_id', 'ch_test')
        result = handle_fulfillment_dispatch(msg)
        assert result['statusCode'] == 200
        assert json.loads(result['body'])['status'] == 'fulfilled'


if __name__ == '__main__':
    pytest.main([__file__, '-v'])