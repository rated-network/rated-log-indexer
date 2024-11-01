from prometheus_client import start_http_server, Counter, Histogram
import random
import time

# Test counter with organization identifier (user_id) label
test_requests = Counter(
    "test_requests_total", "Test Requests Total", ["endpoint", "user_id", "region"]
)

# Test histogram with organization identifier
test_latency = Histogram(
    "test_latency_seconds",
    "Test Latency",
    ["endpoint", "user_id"],
    buckets=[0.005, 0.01, 0.025, 0.05, 0.075, 0.1],
)


def generate_test_data():
    endpoints = ["/api/test1", "/api/test2"]
    user_ids = ["user123", "user456", "user789"]
    regions = ["us-east1", "us-west1"]

    # Initialize counters for each combination
    for endpoint in endpoints:
        for user_id in user_ids:
            for region in regions:
                test_requests.labels(
                    endpoint=endpoint, user_id=user_id, region=region
                ).inc(random.randint(1, 10))

    while True:
        endpoint = random.choice(endpoints)
        user_id = random.choice(user_ids)
        region = random.choice(regions)

        # Increment request counter
        test_requests.labels(endpoint=endpoint, user_id=user_id, region=region).inc()

        # Add latency metrics with user_id label
        with test_latency.labels(
            endpoint=endpoint, user_id=user_id  # Consistently include user_id
        ).time():
            time.sleep(random.random() * 0.1)

        time.sleep(1)


if __name__ == "__main__":
    start_http_server(8080)
    print("Started metrics server on port 8080")
    generate_test_data()
