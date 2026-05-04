"""
Locust Load Test for Order Pipeline
Tests the system under various load levels to find the failure cliff
"""

import json
import os
import random
import uuid
from locust import HttpUser, task, between, events, constant
from locust.runners import Runner


class OrderUser(HttpUser):
    """Simulates a user placing orders."""
    
    wait_time = between(0.1, 0.5)
    host = "http://localhost:9000" if not os.environ.get("TARGET_HOST") else os.environ["TARGET_HOST"]
    
    def on_start(self):
        """Initialize user with unique idempotency key."""
        self.tenant_id = "tenant-1"
        self.customer_id = str(uuid.uuid4())
    
    @task(3)
    def place_order(self):
        """Place a new order."""
        idempotency_key = f"idem-{uuid.uuid4().hex[:12]}"
        items = [
            {
                "product_id": f"PROD-{random.randint(1, 5):03d}",
                "quantity": random.randint(1, 3),
                "price": round(random.uniform(5, 100), 2)
            }
        ]
        
        payload = {
            "action": "place_order",
            "customer_id": self.customer_id,
            "items": items,
            "tenant_id": self.tenant_id,
            "idempotency_key": idempotency_key
        }
        
        with self.client.post("/orders", json=payload, catch_response=True) as response:
            if response.status_code == 429:
                response.failure("Rate limited")
            elif response.status_code == 409:
                response.failure("Duplicate order")
            elif response.status_code >= 400:
                response.failure(f"Error: {response.status_code}")
            else:
                response.success()
    
    @task(1)
    def check_rate_limit(self):
        """Check rate limit status."""
        payload = {
            "action": "check_rate_limit",
            "tenant_id": self.tenant_id
        }
        
        with self.client.post("/orders", json=payload, catch_response=True) as response:
            if response.status_code == 429:
                response.failure("Rate limited")
            else:
                response.success()
    
    @task(1)
    def check_idempotency(self):
        """Check idempotency key."""
        idempotency_key = f"idem-check-{random.randint(1, 1000)}"
        payload = {
            "action": "check_idempotency",
            "idempotency_key": idempotency_key
        }
        
        self.client.post("/orders", json=payload, catch_response=True)


class BurstUser(HttpUser):
    """Simulates burst traffic for stress testing."""
    
    wait_time = constant(0)
    host = "http://localhost:9000" if not os.environ.get("TARGET_HOST") else os.environ["TARGET_HOST"]
    
    @task
    def place_order_burst(self):
        """Place orders in rapid succession."""
        idempotency_key = f"burst-{uuid.uuid4().hex[:12]}"
        
        payload = {
            "action": "place_order",
            "customer_id": str(uuid.uuid4()),
            "items": [{"product_id": "PROD-001", "quantity": 1, "price": 10.0}],
            "tenant_id": "tenant-burst",
            "idempotency_key": idempotency_key
        }
        
        with self.client.post("/orders", json=payload, catch_response=True) as response:
            if response.status_code == 429:
                response.failure("Rate limited at burst")
            elif response.status_code >= 500:
                response.failure(f"Server error: {response.status_code}")
            else:
                response.success()


def on_quitting(
    environment: "Environment",
    reverse: bool,
    forced: bool,
    **kwargs
):
    """Print summary when test finishes."""
    if not forced:
        print("\n=== Load Test Summary ===")
        print(f"Total requests: {environment.stats.total.num_requests}")
        print(f"Total failures: {environment.stats.total.num_failures}")
        print(f"Fail percentage: {environment.stats.total.fail_ratio * 100:.2f}%")
        print(f"Average response time: {environment.stats.total.avg_response_time:.2f}ms")
        print(f"p50: {environment.stats.total.get_response_time_percentile(0.5):.2f}ms")
        print(f"p99: {environment.stats.total.get_response_time_percentile(0.99):.2f}ms")


events.quitting.add_listener(on_quitting)