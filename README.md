# Replicated Counter Service

Distributed Systems assignment implementing a gRPC counter service with retries, idempotency, Lamport clocks, three replicas, majority quorum writes, failure injection, and performance testing.

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install grpcio grpcio-tools pytest
python3 -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. counter.proto
```

## Start one replica

```bash
python3 server.py --port 50051
```

## Start three replicas

Run in three terminals:

```bash
python3 server.py --port 50051
python3 server.py --port 50052
python3 server.py --port 50053
```

## Increment using quorum

```bash
python3 client.py incr likes:post-42 --by 1
```

A write is committed when at least 2 of 3 replicas acknowledge it.

## Get a value

```bash
python3 client.py --port 50051 get likes:post-42
```

## Failure injection

```bash
python3 server.py --port 50051 --fault drop-after-recv
python3 server.py --port 50051 --delay-ms 3000
```

## Run tests

```bash
python3 -m pytest -q
```

## Run benchmark

```bash
python3 perf_benchmark.py
```

The benchmark measures single-replica and three-replica quorum Increment latency with 1 and 16 concurrent clients, using 2000 requests per configuration.

## Lamport trace

Task B2 trace is stored in:

```text
logs/b2_trace.log
```
