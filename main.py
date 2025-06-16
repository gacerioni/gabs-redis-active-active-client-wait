import os
import time
import redis
from typing import List, Dict
from dotenv import load_dotenv
load_dotenv()

# Redis configuration from environment
redis_crdt_1_pwd = os.getenv("REDIS_CRDT_1_PASSWORD", "segredo123")
redis_crdt_2_pwd = os.getenv("REDIS_CRDT_2_PASSWORD", "segredo123")

def build_redis_client(host: str, port: int, username: str, password: str) -> redis.Redis:
    pool = redis.ConnectionPool(
        host=host,
        port=port,
        username=username,
        password=password,
        decode_responses=True,
        socket_timeout=5
    )
    return redis.Redis(connection_pool=pool)


def connect_to_endpoints(configs: List[Dict]) -> Dict[str, redis.Redis]:
    clients = {}
    for cfg in configs:
        clients[cfg["name"]] = build_redis_client(
            host=cfg["host"],
            port=cfg["port"],
            username=cfg["username"],
            password=cfg["password"]
        )
    return clients


def write_key(client: redis.Redis, key: str, value: str):
    print(f"[writer] Setting key '{key}' = '{value}'")
    client.set(key, value)
    client.wait(1, 2000)  # ensure at least 1 replica gets it (local HA)


def wait_for_replication(clients: Dict[str, redis.Redis], key: str, expected_value: str, timeout_sec: int = 10):
    print("\n⏳ Waiting for CRDT propagation...")
    start_time = time.time()
    remaining_clients = clients.copy()
    results = {}

    while time.time() - start_time < timeout_sec and remaining_clients:
        for name in list(remaining_clients.keys()):
            try:
                val = remaining_clients[name].get(key)
                if val == expected_value:
                    results[name] = round(time.time() - start_time, 3)
                    del remaining_clients[name]
            except Exception as e:
                print(f"[{name}] Read error: {e}")
        time.sleep(0.2)  # poll every 200ms

    return results, list(remaining_clients.keys())


def check_final_results(results: Dict[str, float], failed: List[str], key: str, expected_value: str):
    print(f"\n🔍 Final replication status for key '{key}' = '{expected_value}':")
    for name, latency in results.items():
        print(f"[{name}] ✅ OK (replicated in {latency}s)")
    for name in failed:
        print(f"[{name}] ❌ FAILED (not replicated within timeout)")


def run_crdt_demo():
    endpoints = [
        {"name": "us-east-1", "host": "redis-17139.mc1292-0.us-east-1-mz.ec2.cloud.rlrcp.com", "port": 17139, "username": "default", "password": redis_crdt_1_pwd},
        {"name": "us-east-2", "host": "redis-17139.mc1292-1.us-east-2-mz.ec2.cloud.rlrcp.com", "port": 17139, "username": "default", "password": redis_crdt_2_pwd},
    ]

    clients = connect_to_endpoints(endpoints)

    key = "crdt_demo_key"
    value = "demo_crdt_value"
    first_endpoint_name = endpoints[0]["name"]

    write_key(clients[first_endpoint_name], key, value)
    results, failed = wait_for_replication(clients, key, value)
    check_final_results(results, failed, key, value)


if __name__ == "__main__":
    run_crdt_demo()