"""
PostgreSQL Partitioning Benchmark Script
Compares query performance before and after partitioning
"""

import os
import sys
import time
import random
import json
from datetime import datetime, timedelta, timezone

try:
    import psycopg2
    from psycopg2 import sql
except ImportError:
    print("psycopg2 not installed. Run: pip install psycopg2-binary")
    sys.exit(1)


DB_CONFIG = {
    'host': os.environ.get('DB_HOST', 'localhost'),
    'port': int(os.environ.get('DB_PORT', 5432)),
    'database': os.environ.get('DB_NAME', 'order_pipeline'),
    'user': os.environ.get('DB_USER', 'postgres'),
    'password': os.environ.get('DB_PASSWORD', 'postgres')
}


def get_connection():
    """Create database connection."""
    return psycopg2.connect(**DB_CONFIG)


def create_unpartitioned_table(cursor):
    """Create unpartitioned orders table for comparison."""
    cursor.execute("DROP TABLE IF EXISTS orders_unpartitioned CASCADE")
    cursor.execute("""
        CREATE TABLE orders_unpartitioned (
            order_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            customer_id UUID NOT NULL,
            status VARCHAR(50) NOT NULL DEFAULT 'pending',
            total_amount DECIMAL(12, 2) NOT NULL,
            idempotency_key VARCHAR(255) UNIQUE,
            charge_id VARCHAR(255),
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        )
    """)
    cursor.execute("CREATE INDEX idx_orders_unp_created ON orders_unpartitioned(created_at)")
    cursor.execute("CREATE INDEX idx_orders_unp_customer ON orders_unpartitioned(customer_id)")
    print("Created unpartitioned table")


def create_partitioned_table(cursor):
    """Create partitioned orders table."""
    cursor.execute("DROP TABLE IF EXISTS orders_partitioned CASCADE")
    cursor.execute("""
        CREATE TABLE orders_partitioned (
            order_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            customer_id UUID NOT NULL,
            status VARCHAR(50) NOT NULL DEFAULT 'pending',
            total_amount DECIMAL(12, 2) NOT NULL,
            idempotency_key VARCHAR(255) UNIQUE,
            charge_id VARCHAR(255),
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        ) PARTITION BY RANGE (created_at)
    """)
    
    for month_offset in range(-6, 7):
        target_date = datetime.now(timezone.utc) + timedelta(days=month_offset * 30)
        partition_name = f"orders_p_{target_date.strftime('%Y_%m')}"
        start_date = target_date.replace(day=1)
        if month_offset < 6:
            end_date = (target_date.replace(day=28) + timedelta(days=4)).replace(day=1)
        else:
            end_date = (datetime.now(timezone.utc) + timedelta(days=30)).replace(day=1)
        
        cursor.execute(sql.SQL("""
            CREATE TABLE {} PARTITION OF orders_partitioned
            FOR VALUES FROM ('{}') TO ('{}')
        """).format(
            sql.Identifier(partition_name),
            sql.Literal(start_date.isoformat()),
            sql.Literal(end_date.isoformat())
        ))
    
    cursor.execute("""
        CREATE INDEX idx_orders_p_created ON orders_partitioned(created_at)
    """)
    cursor.execute("""
        CREATE INDEX idx_orders_p_customer ON orders_partitioned(customer_id)
    """)
    print("Created partitioned table with monthly partitions")


def seed_data(cursor, table_name, num_orders):
    """Seed test data."""
    print(f"Seeding {num_orders} orders to {table_name}...")
    
    customer_ids = [f"a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a{i:02d}" for i in range(1, 6)]
    statuses = ['pending', 'processing', 'completed', 'failed']
    
    cursor.execute(f"TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE")
    
    batch_size = 1000
    for batch in range(0, num_orders, batch_size):
        values = []
        for i in range(batch, min(batch + batch_size, num_orders)):
            order_id = f"a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a{str(i).zfill(4)}"
            customer_id = customer_ids[i % 5]
            status = statuses[i % 4]
            total = 10 + i * 0.5
            created_at = datetime.now(timezone.utc) - timedelta(days=random.randint(0, 365))
            
            values.append(f"('{order_id}', '{customer_id}', '{status}', {total}, 'idem-{i}', 'ch_{i}', '{created_at.isoformat()}', '{created_at.isoformat()}')")
        
        cursor.execute(f"""
            INSERT INTO {table_name} (order_id, customer_id, status, total_amount, idempotency_key, charge_id, created_at, updated_at)
            VALUES {', '.join(values)}
        """)
        
        if (batch + batch_size) % 10000 == 0:
            print(f"  Seeded {batch + batch_size} orders...")
    
    print(f"  Seeding complete: {num_orders} orders")


def run_explain_analyze(cursor, query):
    """Run EXPLAIN ANALYZE and return results."""
    cursor.execute(f"EXPLAIN ANALYZE {query}")
    result = cursor.fetchall()
    execution_time = None
    for row in result:
        if row[0].startswith('Execution Time'):
            execution_time = float(row[0].split(':')[1].strip().replace(' ms', ''))
    return result, execution_time


def run_benchmark(cursor, table_name, row_count):
    """Run benchmark queries on table."""
    results = {}
    
    query_30_days = f"""
        SELECT * FROM {table_name}
        WHERE created_at >= NOW() - INTERVAL '30 days'
    """
    print(f"\n  Query: Last 30 days ({table_name})")
    _, exec_time = run_explain_analyze(cursor, query_30_days)
    results['30_days'] = exec_time
    print(f"    Execution time: {exec_time:.2f} ms")
    
    query_7_days = f"""
        SELECT * FROM {table_name}
        WHERE created_at >= NOW() - INTERVAL '7 days'
    """
    print(f"\n  Query: Last 7 days ({table_name})")
    _, exec_time = run_explain_analyze(cursor, query_7_days)
    results['7_days'] = exec_time
    print(f"    Execution time: {exec_time:.2f} ms")
    
    query_aggregate = f"""
        SELECT status, COUNT(*), SUM(total_amount)
        FROM {table_name}
        WHERE created_at >= NOW() - INTERVAL '90 days'
        GROUP BY status
    """
    print(f"\n  Query: Aggregate 90 days ({table_name})")
    _, exec_time = run_explain_analyze(cursor, query_aggregate)
    results['90_days_aggregate'] = exec_time
    print(f"    Execution time: {exec_time:.2f} ms")
    
    query_by_customer = f"""
        SELECT customer_id, COUNT(*), SUM(total_amount)
        FROM {table_name}
        WHERE created_at >= NOW() - INTERVAL '180 days'
        GROUP BY customer_id
    """
    print(f"\n  Query: By customer 180 days ({table_name})")
    _, exec_time = run_explain_analyze(cursor, query_by_customer)
    results['180_days_customer'] = exec_time
    print(f"    Execution time: {exec_time:.2f} ms")
    
    return results


def main():
    """Run partitioning benchmark."""
    print("=" * 60)
    print("PostgreSQL Partitioning Benchmark")
    print("=" * 60)
    
    row_counts = [10000, 50000, 100000, 500000, 1000000]
    
    conn = get_connection()
    conn.autocommit = True
    cursor = conn.cursor()
    
    benchmarks = {}
    
    for row_count in row_counts:
        print(f"\n{'=' * 40}")
        print(f"Benchmark with {row_count:,} rows")
        print(f"{'=' * 40}")
        
        print("\n--- UNPARTITIONED TABLE ---")
        create_unpartitioned_table(cursor)
        time.sleep(1)
        seed_data(cursor, 'orders_unpartitioned', row_count)
        unpartitioned_results = run_benchmark(cursor, 'orders_unpartitioned', row_count)
        
        print("\n--- PARTITIONED TABLE ---")
        create_partitioned_table(cursor)
        time.sleep(1)
        seed_data(cursor, 'orders_partitioned', row_count)
        partitioned_results = run_benchmark(cursor, 'orders_partitioned', row_count)
        
        benchmarks[row_count] = {
            'unpartitioned': unpartitioned_results,
            'partitioned': partitioned_results
        }
        
        improvement = {}
        for query in unpartitioned_results:
            if unpartitioned_results[query]:
                imp = ((unpartitioned_results[query] - partitioned_results[query]) / 
                      unpartitioned_results[query] * 100)
                improvement[query] = imp
        
        print(f"\n  IMPROVEMENT SUMMARY ({row_count:,} rows):")
        for query, imp in improvement.items():
            print(f"    {query}: {imp:.1f}%")
    
    print("\n" + "=" * 60)
    print("FINAL SUMMARY")
    print("=" * 60)
    
    for row_count, data in benchmarks.items():
        print(f"\n{row_count:,} rows:")
        for query, imp in zip(['30_days', '7_days', '90_days_aggregate', '180_days_customer'],
                           range(4)):
            if query in data['unpartitioned'] and data['unpartitioned'][query]:
                imp = ((data['unpartitioned'][query] - data['partitioned'][query]) / 
                      data['unpartitioned'][query] * 100)
                print(f"  {query}: {imp:.1f}% improvement")
    
    print("\nBenchmark complete!")
    
    with open('partitioning_benchmark_results.json', 'w') as f:
        json.dump(benchmarks, f, indent=2)
    print("Results saved to partitioning_benchmark_results.json")
    
    cursor.close()
    conn.close()


if __name__ == '__main__':
    main()