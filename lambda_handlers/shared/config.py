"""
AWS service clients and configuration shared across all handlers.

Centralises endpoint URLs, table names, queue URLs, and boto3 client
construction so each handler module doesn't repeat boilerplate.
"""

import os
import boto3

AWS_REGION = os.environ.get('AWS_REGION', 'eu-west-2')

# Endpoint overrides (LocalStack for local dev, absent in production).
_DDB_ENDPOINT = os.environ.get('DYNAMODB_ENDPOINT')
_SQS_ENDPOINT = os.environ.get('SQS_ENDPOINT')
_S3_ENDPOINT = os.environ.get('S3_ENDPOINT')
_CW_ENDPOINT = os.environ.get('CLOUDWATCH_ENDPOINT')

# DynamoDB
dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION,
                          **({'endpoint_url': _DDB_ENDPOINT} if _DDB_ENDPOINT else {}))

# SQS
sqs = boto3.client('sqs', region_name=AWS_REGION,
                    **({'endpoint_url': _SQS_ENDPOINT} if _SQS_ENDPOINT else {}))

# S3
s3 = boto3.client('s3', region_name=AWS_REGION,
                   **({'endpoint_url': _S3_ENDPOINT} if _S3_ENDPOINT else {}))

# CloudWatch
cloudwatch = boto3.client('cloudwatch', region_name=AWS_REGION,
                          **({'endpoint_url': _CW_ENDPOINT} if _CW_ENDPOINT else {}))

# Table names
ORDERS_TABLE = os.environ.get('ORDERS_TABLE', 'orders')
IDEMPOTENCY_TABLE = os.environ.get('IDEMPOTENCY_TABLE', 'idempotency-keys')
TOKEN_BUCKET_TABLE = os.environ.get('TOKEN_BUCKET_TABLE', 'token-buckets')

# Queue URLs
INVENTORY_QUEUE = os.environ.get('INVENTORY_QUEUE_URL', 'orders-inventory-queue')
INVENTORY_QUEUE_NAME = os.environ.get('INVENTORY_QUEUE_NAME', 'orders-inventory-queue')
PAYMENT_QUEUE = os.environ.get('PAYMENT_QUEUE_URL', 'orders-payment-queue')
FULFILLMENT_QUEUE = os.environ.get('FULFILLMENT_QUEUE_URL', 'orders-fulfillment-queue')
DLQ_URL = os.environ.get('DLQ_URL', 'orders-dlq')

# S3 bucket
RECEIPTS_BUCKET = os.environ.get('RECEIPTS_BUCKET', 'order-receipts')

# Back-pressure
BACK_PRESSURE_THRESHOLD = int(os.environ.get('BACK_PRESSURE_THRESHOLD', '1000'))
