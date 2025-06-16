import os
import redis
from typing import List, Dict, Any
from dotenv import load_dotenv
import json

load_dotenv()

ENABLE_LOCAL_WAIT = True  # Set to False to disable local HA replica wait

# Redis credentials from .env
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


def connect_to_endpoints(configs: List[Dict[str, Any]]) -> Dict[str, redis.Redis]:
    clients = {}
    for cfg in configs:
        clients[cfg["name"]] = build_redis_client(
            host=cfg["host"],
            port=cfg["port"],
            username=cfg["username"],
            password=cfg["password"]
        )
    return clients


def fanout_write(clients: Dict[str, redis.Redis], *command_parts):
    print(f"\n🔁 Executing: {' '.join(map(str, command_parts))} on all CRDT participants...")
    for name, client in clients.items():
        try:
            client.execute_command(*command_parts)
            if ENABLE_LOCAL_WAIT:
                client.wait(1, 2000)  # Wait for 1 local replica, max 2s
            print(f"[{name}] ✅ Command succeeded{' (with WAIT)' if ENABLE_LOCAL_WAIT else ''}")
        except Exception as e:
            print(f"[{name}] ❌ Command failed: {e}")
            raise RuntimeError(f"Write failed on {name}")


def run_dual_write_demo():
    endpoints = [
        {"name": "us-east-1", "host": "redis-17139.mc1292-0.us-east-1-mz.ec2.cloud.rlrcp.com", "port": 17139, "username": "default", "password": redis_crdt_1_pwd},
        {"name": "us-east-2", "host": "redis-17139.mc1292-1.us-east-2-mz.ec2.cloud.rlrcp.com", "port": 17139, "username": "default", "password": redis_crdt_2_pwd},
    ]

    clients = connect_to_endpoints(endpoints)

    print("\n🔧 Starting dual-write demo...")

    print("Writing a string key to both CRDTs...")
    fanout_write(clients, "SET", "crdt_dual_key", "this was dual-written")

    print("Writing a hash to both CRDTs...")
    fanout_write(clients, "HSET", "user:1", "name", "gabs", "role", "nerdola")

    print("Writing a JSON to both CRDTs...")
    json_str = json.dumps({"name": "gabs", "role": "thenerdola", "age": 42, "active": True})
    fanout_write(clients, "JSON.SET", "user:1:json", "$", json_str)


if __name__ == "__main__":
    run_dual_write_demo()