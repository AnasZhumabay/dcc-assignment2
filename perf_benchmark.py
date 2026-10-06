import math
import statistics
import time
import uuid
from concurrent import futures

import grpc

import counter_pb2
import counter_pb2_grpc
from server import CounterServicer


REQUESTS = 2000


class QuietCounterServicer(CounterServicer):
    def _log(self, message):
        pass


def start_server(name):
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=8)
    )

    counter_pb2_grpc.add_CounterServicer_to_server(
        QuietCounterServicer(name=name),
        server
    )

    port = server.add_insecure_port("localhost:0")
    server.start()

    return server, port


def percentile_95(values):
    ordered = sorted(values)

    index = math.ceil(
        0.95 * len(ordered)
    ) - 1

    return ordered[index]


def summarize(latencies):
    return (
        statistics.median(latencies),
        percentile_95(latencies)
    )


def run_requests(operation, clients):
    latencies = []

    def measured_call(index):
        start = time.perf_counter()

        operation(index)

        elapsed_ms = (
            time.perf_counter() - start
        ) * 1000

        return elapsed_ms

    if clients == 1:
        for i in range(REQUESTS):
            latencies.append(
                measured_call(i)
            )

    else:
        with futures.ThreadPoolExecutor(
            max_workers=clients
        ) as executor:
            pending = [
                executor.submit(
                    measured_call,
                    i
                )
                for i in range(REQUESTS)
            ]

            for future in futures.as_completed(
                pending
            ):
                latencies.append(
                    future.result()
                )

    return summarize(latencies)


def main():
    servers = []
    channels = []

    try:
        ports = []

        for name in (
            "bench-A",
            "bench-B",
            "bench-C"
        ):
            server, port = start_server(name)

            servers.append(server)
            ports.append(port)

        stubs = []

        for port in ports:
            channel = grpc.insecure_channel(
                f"localhost:{port}"
            )

            channels.append(channel)

            stubs.append(
                counter_pb2_grpc.CounterStub(
                    channel
                )
            )

        single_stub = stubs[0]

        def single_increment(index):
            request = counter_pb2.IncrementRequest(
                counter_id="bench-single",
                delta=1,
                idempotency_key=(
                    f"single-{uuid.uuid4()}"
                ),
                lamport_time=1
            )

            single_stub.Increment(
                request,
                timeout=2.0
            )

        rpc_pool = futures.ThreadPoolExecutor(
            max_workers=48
        )

        def quorum_increment(index):
            key = f"quorum-{uuid.uuid4()}"

            request = counter_pb2.IncrementRequest(
                counter_id="bench-quorum",
                delta=1,
                idempotency_key=key,
                lamport_time=1
            )

            calls = [
                rpc_pool.submit(
                    stub.Increment,
                    request,
                    timeout=2.0
                )
                for stub in stubs
            ]

            acknowledgements = 0

            for call in calls:
                try:
                    call.result()
                    acknowledgements += 1
                except grpc.RpcError:
                    pass

            if acknowledgements < 2:
                raise RuntimeError(
                    "Quorum not reached"
                )

        configurations = [
            (
                "Single replica, 1 client",
                single_increment,
                1
            ),
            (
                "Single replica, 16 clients",
                single_increment,
                16
            ),
            (
                "Quorum (3 replicas), 1 client",
                quorum_increment,
                1
            ),
            (
                "Quorum (3 replicas), 16 clients",
                quorum_increment,
                16
            )
        ]

        results = []

        for label, operation, clients in configurations:
            print(f"Running: {label}...")

            median, p95 = run_requests(
                operation,
                clients
            )

            results.append(
                (
                    label,
                    median,
                    p95,
                    REQUESTS
                )
            )

        rpc_pool.shutdown()

        print()
        print(
            "| Configuration | Median latency (ms) "
            "| p95 latency (ms) | Requests |"
        )
        print(
            "|---|---:|---:|---:|"
        )

        for label, median, p95, requests in results:
            print(
                f"| {label} | "
                f"{median:.3f} | "
                f"{p95:.3f} | "
                f"{requests} |"
            )

    finally:
        for channel in channels:
            channel.close()

        for server in servers:
            server.stop(0).wait()


if __name__ == "__main__":
    main()
