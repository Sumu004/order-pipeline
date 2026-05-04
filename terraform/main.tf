# Terraform configuration for AWS resources
# Deploys Lambda, SQS, DynamoDB, S3 for Order Pipeline

terraform {
  required_version = ">= 1.0"
  required_providers {
    aws {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
  backend "local" {}
}

provider "aws" {
  region = var.aws_region
}

variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "eu-west-2"
}

variable "environment" {
  description = "Environment name"
  type        = string
  default     = "dev"
}

locals {
  project_name = "order-pipeline"
  tags = {
    Project     = local.project_name
    Environment = var.environment
    ManagedBy  = "terraform"
  }
}

# SQS Queues
resource "aws_sqs_queue" "inventory" {
  name                       = "${local.project_name}-inventory-queue-${var.environment}"
  max_message_size           = 262144
  message_retention_seconds    = 345600
  receive_wait_time_seconds  = 10
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.dlq.arn
    maxReceiveCount  = 3
  })
  tags = local.tags
}

resource "aws_sqs_queue" "payment" {
  name                       = "${local.project_name}-payment-queue-${var.environment}"
  max_message_size           = 262144
  message_retention_seconds    = 345600
  receive_wait_time_seconds  = 10
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.dlq.arn
    maxReceiveCount  = 3
  })
  tags = local.tags
}

resource "aws_sqs_queue" "fulfillment" {
  name                       = "${local.project_name}-fulfillment-queue-${var.environment}"
  max_message_size           = 262144
  message_retention_seconds    = 345600
  receive_wait_time_seconds  = 10
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.dlq.arn
    maxReceiveCount  = 3
  })
  tags = local.tags
}

resource "aws_sqs_queue" "dlq" {
  name                       = "${local.project_name}-dlq-${var.environment}"
  max_message_size           = 262144
  message_retention_seconds  = 1209600
  tags = local.tags
}

# S3 Bucket for receipts
resource "aws_s3_bucket" "receipts" {
  bucket = "${local.project_name}-receipts-${var.environment}"
  tags   = local.tags
}

resource "aws_s3_bucket_server_side_encryption_configuration" "receipts" {
  bucket = aws_s3_bucket.receipts.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "receipts" {
  bucket = aws_s3_bucket.receipts.id
  block_public_acls       = true
  block_public_policy    = true
  ignore_public_acls   = true
  restrict_public_buckets = true
}

# DynamoDB Tables
resource "aws_dynamodb_table" "orders" {
  name           = "${local.project_name}-orders-${var.environment}"
  billing_mode  = "PAY_PER_REQUEST"
  hash_key      = "order_id"
  range_key     = "created_at"
  
  attribute {
    name = "order_id"
    type = "S"
  }
  
  attribute {
    name = "created_at"
    type = "S"
  }
  
  attribute {
    name = "customer_id"
    type = "S"
  }
  
  attribute {
    name = "idempotency_key"
    type = "S"
  }
  
  global_secondary_index {
    name            = "customer_id-index"
    hash_key       = "customer_id"
    projection_type = "KEYS_ONLY"
  }
  
  global_secondary_index {
    name            = "idempotency_key-index"
    hash_key       = "idempotency_key"
    projection_type = "KEYS_ONLY"
  }
  
  ttl {
    attribute_name = "ttl"
    enabled      = true
  }
  
  tags = local.tags
}

resource "aws_dynamodb_table" "idempotency_keys" {
  name           = "${local.project_name}-idempotency-keys-${var.environment}"
  billing_mode  = "PAY_PER_REQUEST"
  hash_key      = "idempotency_key"
  
  attribute {
    name = "idempotency_key"
    type = "S"
  }
  
  ttl {
    attribute_name = "ttl"
    enabled      = false
  }
  
  tags = local.tags
}

resource "aws_dynamodb_table" "token_buckets" {
  name           = "${local.project_name}-token-buckets-${var.environment}"
  billing_mode  = "PAY_PER_REQUEST"
  hash_key      = "tenant_id"
  
  attribute {
    name = "tenant_id"
    type = "S"
  }
  
  tags = local.tags
}

# IAM Role for Lambda
resource "aws_iam_role" "lambda_exec" {
  name = "${local.project_name}-lambda-role-${var.environment}"
  
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "lambda.amazonaws.com"
      }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_basic" {
  role       = aws_iam_role.lambda_exec.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy_attachment" "lambda_sqs" {
  role       = aws_iam_role.lambda_exec.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSQSFullAccess"
}

resource "aws_iam_role_policy_attachment" "lambda_s3" {
  role       = aws_iam_role.lambda_exec.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3FullAccess"
}

resource "aws_iam_role_policy_attachment" "lambda_dynamodb" {
  role       = aws_iam_role.lambda_exec.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonDynamoDBFullAccess"
}

# Lambda Function
data "archive_file" "lambda_zip" {
  type        = "zip"
  source_file = "${path.package}/../lambda_handlers/handler.py"
  output_path = "${path.root}/lambda.zip"
}

resource "aws_lambda_function" "order_handler" {
  filename         = data.archive_file.lambda_zip.output_path
  function_name   = "${local.project_name}-handler-${var.environment}"
  role           = aws_iam_role.lambda_exec.arn
  runtime        = "python3.11"
  handler        = "lambda_handlers.handler.lambda_handler"
  timeout        = 30
  memory_size    = 256
  source_code_hash = data.archive_file.lambda_zip.output_base64sha256
  
  environment {
    variables = {
      ORDERS_TABLE         = aws_dynamodb_table.orders.name
      IDEMPOTENCY_TABLE    = aws_dynamodb_table.idempotency_keys.name
      TOKEN_BUCKET_TABLE  = aws_dynamodb_table.token_buckets.name
      INVENTORY_QUEUE_URL = aws_sqs_queue.inventory.url
      PAYMENT_QUEUE_URL   = aws_sqs_queue.payment.url
      FULFILLMENT_QUEUE_URL = aws_sqs_queue.fulfillment.url
      RECEIPTS_BUCKET     = aws_s3_bucket.receipts.id
    }
  }
  
  depends_on = [
    aws_iam_role_policy_attachment.lambda_basic,
    aws_sqs_queue.inventory,
    aws_sqs_queue.payment,
    aws_sqs_queue.fulfillment,
    aws_s3_bucket.receipts
  ]
  
  tags = local.tags
}

# Lambda Event Source Mappings (SQS triggers)
resource "aws_lambda_event_source_mapping" "inventory_trigger" {
  event_source_arn = aws_sqs_queue.inventory.arn
  function_name   = aws_lambda_function.order_handler.arn
  batch_size      = 10
  enabled         = true
}

resource "aws_lambda_event_source_mapping" "payment_trigger" {
  event_source_arn = aws_sqs_queue.payment.arn
  function_name   = aws_lambda_function.order_handler.arn
  batch_size      = 10
  enabled         = true
}

resource "aws_lambda_event_source_mapping" "fulfillment_trigger" {
  event_source_arn = aws_sqs_queue.fulfillment.arn
  function_name   = aws_lambda_function.order_handler.arn
  batch_size      = 10
  enabled         = true
}

# CloudWatch Alarm for queue depth
resource "aws_cloudwatch_metric_alarm" "inventory_queue_depth" {
  alarm_name          = "${local.project_name}-inventory-queue-depth-${var.environment}"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  metric_name        = "ApproximateNumberOfMessages"
  namespace         = "AWS/SQS"
  period            = 300
  statistic         = "Maximum"
  threshold         = 1000
  alarm_description = "Inventory queue depth exceeds threshold"
  alarm_actions     = [aws_sns_topic.alerts.arn]
  
  dimensions = {
    QueueName = aws_sqs_queue.inventory.name
  }
  
  tags = local.tags
}

resource "aws_cloudwatch_metric_alarm" "payment_queue_depth" {
  alarm_name          = "${local.project_name}-payment-queue-depth-${var.environment}"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  metric_name        = "ApproximateNumberOfMessages"
  namespace         = "AWS/SQS"
  period            = 300
  statistic         = "Maximum"
  threshold         = 1000
  alarm_description = "Payment queue depth exceeds threshold"
  alarm_actions     = [aws_sns_topic.alerts.arn]
  
  dimensions = {
    QueueName = aws_sqs_queue.payment.name
  }
  
  tags = local.tags
}

# SNS Topic for alerts
resource "aws_sns_topic" "alerts" {
  name = "${local.project_name}-alerts-${var.environment}"
  tags = local.tags
}

# Outputs
output "lambda_function_name" {
  description = "Lambda function name"
  value       = aws_lambda_function.order_handler.function_name
}

output "sqs_queue_urls" {
  description = "SQS queue URLs"
  value = {
    inventory   = aws_sqs_queue.inventory.url
    payment     = aws_sqs_queue.payment.url
    fulfillment = aws_sqs_queue.fulfillment.url
    dlq         = aws_sqs_queue.dlq.url
  }
}

output "dynamodb_table_names" {
  description = "DynamoDB table names"
  value = {
    orders          = aws_dynamodb_table.orders.name
    idempotency_keys = aws_dynamodb_table.idempotency_keys.name
    token_buckets   = aws_dynamodb_table.token_buckets.name
  }
}

output "s3_bucket_name" {
  description = "S3 bucket for receipts"
  value       = aws_s3_bucket.receipts.id
}