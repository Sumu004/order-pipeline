#!/bin/bash
set -e

echo "Setting up AWS resources in LocalStack..."

awslocal lambda create-function --function-name order-handler \
    --runtime python3.11 \
    --role arn:aws:iam::123456789012:role/lambda-role \
    --handler lambda_handlers.handler.lambda_handler \
    --zip-file fileb://./deployment.zip \
    --timeout 30 \
    --memory-size 256 2>/dev/null || echo "Lambda function already exists or ZIP not ready"

awslocal sqs create-queue --queue-name orders-inventory-queue
awslocal sqs create-queue --queue-name orders-payment-queue
awslocal sqs create-queue --queue-name orders-fulfillment-queue
awslocal sqs create-queue --queue-name orders-dlq

awslocal dynamodb create-table --table-name orders \
    --attribute-definitions AttributeName=order_id,AttributeType=S \
    --key-schema AttributeName=order_id,KeyType=HASH \
    --billing-mode PAY_PER_REQUEST

awslocal dynamodb create-table --table-name idempotency-keys \
    --attribute-definitions AttributeName=idempotency_key,AttributeType=S \
    --key-schema AttributeName=idempotency_key,KeyType=HASH \
    --billing-mode PAY_PER_REQUEST

awslocal dynamodb create-table --table-name token-buckets \
    --attribute-definitions AttributeName=tenant_id,AttributeType=S \
    --key-schema AttributeName=tenant_id,KeyType=HASH \
    --billing-mode PAY_PER_REQUEST

awslocal s3 mb s3://order-receipts

echo "AWS resources created successfully"
echo "Queue URLs:"
awslocal sqs get-queue-url --queue-name orders-inventory-queue
awslocal sqs get-queue-url --queue-name orders-payment-queue
awslocal sqs get-queue-url --queue-name orders-fulfillment-queue
awslocal sqs get-queue-url --queue-name orders-dlq