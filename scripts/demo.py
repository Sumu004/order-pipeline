#!/usr/bin/env python3
"""
Visual Demo Script for Order Pipeline
Run with: python3 scripts/demo.py
"""

import os
import sys

os.environ['AWS_ACCESS_KEY_ID'] = 'test'
os.environ['AWS_SECRET_ACCESS_KEY'] = 'test'
os.environ['AWS_REGION'] = 'eu-west-2'
os.environ['DYNAMODB_ENDPOINT'] = 'http://localhost:4566'
os.environ['SQS_ENDPOINT'] = 'http://localhost:4566'
os.environ['S3_ENDPOINT'] = 'http://localhost:4566'

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from lambda_handlers.handler import handle_rate_limit_check, handle_place_order
import boto3
import json

def main():
    print("=" * 55)
    print("  PROJECT 1 - ADAPTIVE RATE-LIMITED ORDER PIPELINE")
    print("=" * 55)

    # 1. Rate Limiter
    print("\n[1] RATE LIMITER (token bucket)")
    print("-" * 40)
    result = handle_rate_limit_check({'tenant_id': 'demo-tenant'})
    data = json.loads(result['body'])
    print(f"  ✓ Allowed: {data.get('allowed')}")
    print(f"  ✓ Tokens: {data.get('tokens')}")

    # 2. Place Order
    print("\n[2] PLACE_order")
    print("-" * 40)
    result = handle_place_order({
        'customer_id': 'cust-001',
        'items': [
            {'product_id': 'PROD-001', 'quantity': 2, 'price': 25.00},
            {'product_id': 'PROD-002', 'quantity': 1, 'price': 49.99}
        ],
        'tenant_id': 'demo-tenant',
        'idempotency_key': 'idem-demo-123'
    })
    data = json.loads(result['body'])
    order_id = data.get('order_id', 'N/A')[:18] if data.get('order_id') else 'N/A'
    print(f"  ✓ Order ID: {order_id}...")
    print(f"  ✓ Status: {data.get('status')}")
    total = data.get('total', '0')
    print(f"  ✓ Total: £{float(total):.2f}" if total else "  ✓ Total: £0.00")

    # 3. Exactly-Once (duplicate)
    print("\n[3] EXACTLY-ONCE SEMANTICS")
    print("-" * 40)
    result = handle_place_order({
        'customer_id': 'cust-001',
        'items': [{'product_id': 'PROD-001', 'quantity': 2, 'price': 25.00}],
        'tenant_id': 'demo-tenant',
        'idempotency_key': 'idem-demo-123'  # Same!
    })
    if result['statusCode'] == 409:
        print("  ✓ Duplicate blocked (409 Conflict)")
        print("  → Same idempotency key = no double charge")
    else:
        print(f"  Status: {result['statusCode']}")

    # 4. DynamoDB data
    print("\n[4] DYNAMODB (idempotency-keys)")
    print("-" * 40)
    try:
        ddb = boto3.client('dynamodb', region_name='eu-west-2', endpoint_url='http://localhost:4566')
        resp = ddb.scan(TableName='idempotency-keys')
        for item in resp.get('Items', []):
            key = item.get('idempotency_key', {}).get('S', 'N/A')
            status = item.get('status', {}).get('S', 'N/A')
            order = item.get('order_id', {}).get('S', 'N/A')[:8] + '...'
            print(f"  {key[:16]:16} → {status:10} (order: {order})")
    except Exception as e:
        print(f"  (DynamoDB demo mode - {e})")

    # 5. PostgreSQL
    print("\n[5] POSTGRESQL (orders)")
    print("-" * 40)
    try:
        import psycopg2
        try:
            # Try localhost first
            try:
                conn = psycopg2.connect(host='localhost', port=5432, database='order_pipeline', user='postgres')
            except:
                # Try via container IP
                conn = psycopg2.connect(host='127.0.0.1', port=5432, database='order_pipeline', user='postgres')
        except:
            conn = psycopg2.connect(host='localhost', port=5432, database='order_pipeline', user='postgres', password='')
        
        cur = conn.cursor()
        cur.execute("SELECT status, COUNT(*), SUM(total_amount) FROM orders GROUP BY status")
        for row in cur.fetchall():
            print(f"  {row[0]:12} | {row[1]:3} orders | £{row[2]:.2f}")
    except Exception as e:
        print(f"  (PostgreSQL not connected - {e})")

    print("\n" + "=" * 55)
    print("  ✅ ORDER PIPELINE DEMO COMPLETE")
    print("=" * 55)
    print("""
KEY FEATURES:
• Token bucket rate limiter (DynamoDB atomic)
• Exactly-once semantics (idempotency keys)
• Event-driven (SQS queues)
• Data persistence (PostgreSQL + DynamoDB)
""")

if __name__ == '__main__':
    main()