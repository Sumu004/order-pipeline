"""Tests for Order Pipeline Lambda Handler"""

import json
import os
import sys
import pytest
from unittest.mock import Mock, patch, MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from lambda_handlers.handler import (
    check_idempotency,
    write_idempotency_key,
    update_idempotency_status,
    check_rate_limit,
    enqueue_message,
    lambda_handler,
    handle_place_order,
    handle_inventory_check,
    handle_payment_validation,
    handle_fulfillment_dispatch,
)


class TestIdempotency:
    """Test idempotency key operations."""
    
    @patch('lambda_handlers.handler.dynamodb')
    def test_check_idempotency_not_exists(self, mock_dynamodb):
        """Test checking non-existent idempotency key."""
        mock_table = Mock()
        mock_table.get_item.return_value = {}
        mock_dynamodb.Table.return_value = mock_table
        
        result = check_idempotency('test-key-123')
        
        assert result['exists'] is False
    
    @patch('lambda_handlers.handler.dynamodb')
    def test_check_idempotency_exists(self, mock_dynamodb):
        """Test checking existing idempotency key."""
        mock_table = Mock()
        mock_table.get_item.return_value = {
            'Item': {
                'idempotency_key': 'test-key-123',
                'status': 'completed',
                'charge_id': 'ch_abc123',
                'order_id': 'order-123'
            }
        }
        mock_dynamodb.Table.return_value = mock_table
        
        result = check_idempotency('test-key-123')
        
        assert result['exists'] is True
        assert result['status'] == 'completed'
        assert result['charge_id'] == 'ch_abc123'
    
    @patch('lambda_handlers.handler.dynamodb')
    def test_write_idempotency_key_success(self, mock_dynamodb):
        """Test writing new idempotency key."""
        mock_table = Mock()
        mock_dynamodb.Table.return_value = mock_table
        
        result = write_idempotency_key('test-key-123', 'order-123', 'pending')
        
        assert result is True
        mock_table.put_item.assert_called_once()
    
    @patch('lambda_handlers.handler.dynamodb.meta.client.exceptions.ConditionalCheckFailedException', Exception)
    @patch('lambda_handlers.handler.dynamodb')
    def test_write_idempotency_key_duplicate(self, mock_dynamodb):
        """Test writing duplicate idempotency key."""
        mock_table = Mock()
        mock_table.put_item.side_effect = Exception('ConditionalCheckFailedException')
        mock_dynamodb.Table.return_value = mock_table
        
        result = write_idempotency_key('test-key-123', 'order-123', 'pending')
        
        assert result is False


class TestRateLimiter:
    """Test token bucket rate limiter."""
    
    @patch('lambda_handlers.handler.dynamodb')
    @patch('lambda_handlers.handler.datetime')
    def test_rate_limit_new_tenant(self, mock_datetime, mock_dynamodb):
        """Test rate limit for new tenant."""
        mock_datetime.now.return_value.timestamp.return_value = 1000.0
        mock_table = Mock()
        mock_table.get_item.return_value = {}
        mock_dynamodb.Table.return_value = mock_table
        
        result = check_rate_limit('tenant-1')
        
        assert result['allowed'] is True
        assert result['tokens'] == 99
        mock_table.put_item.assert_called_once()
    
    @patch('lambda_handlers.handler.dynamodb')
    @patch('lambda_handlers.handler.datetime')
    def test_rate_limit_tokens_exhausted(self, mock_datetime, mock_dynamodb):
        """Test rate limit when tokens exhausted."""
        mock_table = Mock()
        mock_table.get_item.return_value = {
            'Item': {
                'tenant_id': 'tenant-1',
                'tokens': 0,
                'capacity': 100,
                'refill_rate': 10,
                'last_refill': 1000.0
            }
        }
        mock_dynamodb.Table.return_value = mock_table
        
        with patch('lambda_handlers.handler.datetime') as dt_mock:
            dt_mock.now.return_value.timestamp.return_value = 1000.0
            result = check_rate_limit('tenant-1')
        
        assert result['allowed'] is False
        assert result['tokens'] == 0
    
    @patch('lambda_handlers.handler.dynamodb')
    @patch('lambda_handlers.handler.datetime')
    def test_rate_limit_tokens_refilled(self, mock_datetime, mock_dynamodb):
        """Test rate limit token refill."""
        mock_table = Mock()
        mock_table.get_item.return_value = {
            'Item': {
                'tenant_id': 'tenant-1',
                'tokens': 50,
                'capacity': 100,
                'refill_rate': 10,
                'last_refill': 1000.0
            }
        }
        mock_dynamodb.Table.return_value = mock_table
        
        with patch('lambda_handlers.handler.datetime') as dt_mock:
            dt_mock.now.return_value.timestamp.return_value = 1010.0
            result = check_rate_limit('tenant-1')
        
        assert result['allowed'] is True
        assert result['refilled'] > 0


class TestHandlers:
    """Test Lambda handler functions."""
    
    def test_handle_place_order_missing_fields(self):
        """Test place order with missing fields."""
        result = handle_place_order({})
        
        assert result['statusCode'] == 400
        assert 'error' in result['body']
    
    @patch('lambda_handlers.handler.check_rate_limit')
    @patch('lambda_handlers.handler.check_idempotency')
    @patch('lambda_handlers.handler.write_idempotency_key')
    @patch('lambda_handlers.handler.enqueue_message')
    def test_handle_place_order_success(self, mock_enqueue, mock_write, mock_idem, mock_rate):
        """Test successful order placement."""
        mock_rate.return_value = {'allowed': True, 'tokens': 99}
        mock_idem.return_value = {'exists': False}
        mock_write.return_value = True
        
        body = {
            'customer_id': 'cust-123',
            'items': [{'product_id': 'PROD-001', 'quantity': 2, 'price': 10.0}],
            'tenant_id': 'tenant-1'
        }
        
        result = handle_place_order(body)
        
        assert result['statusCode'] == 200
        data = json.loads(result['body'])
        assert 'order_id' in data
        assert data['status'] == 'placed'
    
    @patch('lambda_handlers.handler.check_rate_limit')
    def test_handle_place_order_rate_limited(self, mock_rate):
        """Test order placement rate limited."""
        mock_rate.return_value = {'allowed': False, 'tokens': 0}

        body = {
            'customer_id': 'cust-123',
            'items': [{'product_id': 'PROD-001', 'quantity': 2, 'price': 10.0}]
        }

        result = handle_place_order(body)

        assert result['statusCode'] == 429

    @patch('lambda_handlers.handler.get_inventory_queue_depth')
    def test_handle_place_order_back_pressure(self, mock_depth):
        """Test that 503 is returned when inventory queue depth exceeds threshold."""
        mock_depth.return_value = 1001

        body = {
            'customer_id': 'cust-123',
            'items': [{'product_id': 'PROD-001', 'quantity': 1, 'price': 10.0}]
        }

        result = handle_place_order(body)

        assert result['statusCode'] == 503
        data = json.loads(result['body'])
        assert data['queue_depth'] == 1001
    
    @patch('lambda_handlers.handler.enqueue_message')
    def test_handle_inventory_check_success(self, mock_enqueue):
        """Test inventory check."""
        body = {
            'order_id': 'order-123',
            'items': [{'product_id': 'PROD-001', 'quantity': 2}]
        }
        
        result = handle_inventory_check(body)
        
        assert result['statusCode'] == 200
        assert json.loads(result['body'])['available'] is True
    
    def test_handle_inventory_check_invalid_quantity(self):
        """Test inventory check with invalid quantity."""
        body = {
            'order_id': 'order-123',
            'items': [{'product_id': 'PROD-001', 'quantity': 0}]
        }
        
        result = handle_inventory_check(body)
        
        assert result['statusCode'] == 400
    
    @patch('lambda_handlers.handler.check_idempotency')
    @patch('lambda_handlers.handler.write_idempotency_key')
    @patch('lambda_handlers.handler.update_idempotency_status')
    @patch('lambda_handlers.handler.enqueue_message')
    def test_handle_payment_validation_success(self, mock_enqueue, mock_update, mock_write, mock_check):
        """Test successful payment validation."""
        mock_check.return_value = {'exists': False}
        mock_write.return_value = True
        
        body = {
            'order_id': 'order-123',
            'idempotency_key': 'idem-123',
            'total': '100.00'
        }
        
        result = handle_payment_validation(body)
        
        assert result['statusCode'] == 200
        assert 'charge_id' in result['body']
    
    @patch('lambda_handlers.handler.write_receipt')
    def test_handle_fulfillment_dispatch(self, mock_receipt):
        """Test fulfillment dispatch."""
        mock_receipt.return_value = 'receipts/order-123.json'
        
        body = {
            'order_id': 'order-123',
            'charge_id': 'ch_abc123',
            'customer_id': 'cust-123',
            'items': [{'product_id': 'PROD-001', 'quantity': 2}],
            'total': '100.00'
        }
        
        result = handle_fulfillment_dispatch(body)
        
        assert result['statusCode'] == 200
        assert json.loads(result['body'])['status'] == 'fulfilled'


class TestLambdaHandler:
    """Test main Lambda handler."""
    
    def test_lambda_handler_health(self):
        """Test health endpoint."""
        event = {'path': '/health'}
        
        result = lambda_handler(event, None)
        
        assert result['statusCode'] == 200
    
    def test_lambda_handler_unknown_action(self):
        """Test unknown action."""
        event = {'httpMethod': 'POST', 'body': json.dumps({'action': 'unknown_action'})}
        
        result = lambda_handler(event, None)
        
        assert result['statusCode'] == 400


if __name__ == '__main__':
    pytest.main([__file__, '-v'])