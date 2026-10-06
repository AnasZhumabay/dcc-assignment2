from concurrent import futures
import time

import grpc

import counter_pb2
import counter_pb2_grpc
from client import increment_counter, quorum_increment
from server import CounterServicer


def start_server(servicer):
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=8)
    )

    counter_pb2_grpc.add_CounterServicer_to_server(
        servicer,
        server
    )

    port = server.add_insecure_port(
        "localhost:0"
    )

    server.start()

    return server, port


def get_value(port, counter_id):
    channel = grpc.insecure_channel(
        f"localhost:{port}"
    )

    try:
        stub = counter_pb2_grpc.CounterStub(
            channel
        )

        return stub.Get(
            counter_pb2.GetRequest(
                counter_id=counter_id,
                lamport_time=1
            ),
            timeout=2.0
        )

    finally:
        channel.close()


def test_replica_crash_mid_request():
    faulty, faulty_port = start_server(
        CounterServicer(
            name="faulty-replica",
            fault="drop-after-recv"
        )
    )

    replica_b, port_b = start_server(
        CounterServicer(name="replica-B")
    )

    replica_c, port_c = start_server(
        CounterServicer(name="replica-C")
    )

    servers = [
        faulty,
        replica_b,
        replica_c
    ]

    try:
        result = quorum_increment(
            "crash-counter",
            1,
            key="crash-key-1",
            ports=(
                faulty_port,
                port_b,
                port_c
            ),
            name="failure-client"
        )

        assert result["committed"] is True
        assert result["ack_count"] == 2
        assert result["required"] == 2

        assert faulty_port in result["errors"]

        assert (
            result["errors"][faulty_port]
            == grpc.StatusCode.UNAVAILABLE
        )

        assert get_value(
            port_b,
            "crash-counter"
        ).value == 1

        assert get_value(
            port_c,
            "crash-counter"
        ).value == 1

    finally:
        for server in servers:
            server.stop(0).wait()


def test_request_duplication():
    server, port = start_server(
        CounterServicer(
            name="duplicate-replica"
        )
    )

    channel = grpc.insecure_channel(
        f"localhost:{port}"
    )

    stub = counter_pb2_grpc.CounterStub(
        channel
    )

    try:
        request = counter_pb2.IncrementRequest(
            counter_id="duplicate-counter",
            delta=1,
            idempotency_key="same-key",
            lamport_time=1
        )

        first = stub.Increment(
            request,
            timeout=2.0
        )

        second = stub.Increment(
            request,
            timeout=2.0
        )

        final = get_value(
            port,
            "duplicate-counter"
        )

        assert first.new_value == 1
        assert first.was_duplicate is False

        assert second.new_value == 1
        assert second.was_duplicate is True

        assert final.value == 1

    finally:
        channel.close()
        server.stop(0).wait()


def test_induced_timeout_with_retry():
    server, port = start_server(
        CounterServicer(
            name="slow-replica",
            delay_ms=2500
        )
    )

    channel = grpc.insecure_channel(
        f"localhost:{port}"
    )

    stub = counter_pb2_grpc.CounterStub(
        channel
    )

    try:
        response = increment_counter(
            stub,
            "timeout-counter",
            1,
            key="timeout-key-1",
            name="timeout-client"
        )

        time.sleep(0.4)

        final = get_value(
            port,
            "timeout-counter"
        )

        assert response.new_value == 1
        assert response.was_duplicate is True

        assert final.found is True
        assert final.value == 1

    finally:
        channel.close()
        server.stop(0).wait()
