from concurrent import futures
import threading
import time

import grpc
import pytest

import counter_pb2
import counter_pb2_grpc
from server import CounterServicer
from client import increment_counter, quorum_increment


@pytest.fixture
def running_server():
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=8)
    )

    counter_pb2_grpc.add_CounterServicer_to_server(
        CounterServicer(name="test-replica"),
        server
    )

    port = server.add_insecure_port("localhost:0")
    server.start()

    channel = grpc.insecure_channel(
        f"localhost:{port}"
    )
    stub = counter_pb2_grpc.CounterStub(channel)

    try:
        yield stub
    finally:
        channel.close()
        server.stop(0).wait()


def test_increment_applies_delta(running_server):
    request = counter_pb2.IncrementRequest(
        counter_id="x",
        delta=5,
        idempotency_key="delta-key-1",
        lamport_time=1
    )

    response = running_server.Increment(
        request,
        timeout=2.0
    )

    get_response = running_server.Get(
        counter_pb2.GetRequest(
            counter_id="x",
            lamport_time=2
        ),
        timeout=2.0
    )

    assert response.new_value == 5
    assert response.was_duplicate is False
    assert get_response.found is True
    assert get_response.value == 5


def test_duplicate_key_not_reapplied(running_server):
    request = counter_pb2.IncrementRequest(
        counter_id="x",
        delta=5,
        idempotency_key="duplicate-key-1",
        lamport_time=1
    )

    first = running_server.Increment(
        request,
        timeout=2.0
    )

    second = running_server.Increment(
        request,
        timeout=2.0
    )

    get_response = running_server.Get(
        counter_pb2.GetRequest(
            counter_id="x",
            lamport_time=2
        ),
        timeout=2.0
    )

    assert first.new_value == 5
    assert first.was_duplicate is False
    assert second.new_value == 5
    assert second.was_duplicate is True
    assert get_response.value == 5


def test_get_missing_counter(running_server):
    response = running_server.Get(
        counter_pb2.GetRequest(
            counter_id="missing-counter",
            lamport_time=1
        ),
        timeout=2.0
    )

    assert response.found is False
    assert response.value == 0


def test_concurrent_increments_exact(running_server):
    def worker(worker_id):
        for i in range(1000):
            response = running_server.Increment(
                counter_pb2.IncrementRequest(
                    counter_id="concurrent-counter",
                    delta=1,
                    idempotency_key=f"worker-{worker_id}-{i}",
                    lamport_time=i + 1
                ),
                timeout=2.0
            )

            assert response.was_duplicate is False

    with futures.ThreadPoolExecutor(max_workers=2) as executor:
        first = executor.submit(worker, 1)
        second = executor.submit(worker, 2)

        first.result()
        second.result()

    final = running_server.Get(
        counter_pb2.GetRequest(
            counter_id="concurrent-counter",
            lamport_time=1
        ),
        timeout=2.0
    )

    assert final.found is True
    assert final.value == 2000


class DelayedFirstIncrementServicer(CounterServicer):
    def __init__(self):
        super().__init__(name="delayed-replica")
        self._delay_lock = threading.Lock()
        self._first_call = True

    def Increment(self, request, context):
        with self._delay_lock:
            should_delay = self._first_call
            self._first_call = False

        if should_delay:
            time.sleep(2.5)

        return super().Increment(request, context)


def _start_test_server(servicer):
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=8)
    )

    counter_pb2_grpc.add_CounterServicer_to_server(
        servicer,
        server
    )

    port = server.add_insecure_port("localhost:0")
    server.start()

    return server, port


def _get_from_port(port, counter_id):
    channel = grpc.insecure_channel(
        f"localhost:{port}"
    )

    try:
        stub = counter_pb2_grpc.CounterStub(channel)

        return stub.Get(
            counter_pb2.GetRequest(
                counter_id=counter_id,
                lamport_time=1
            ),
            timeout=2.0
        )

    finally:
        channel.close()


@pytest.fixture
def running_replicas():
    servers = []
    ports = []

    for index in range(3):
        server, port = _start_test_server(
            CounterServicer(
                name=f"replica-{index + 1}"
            )
        )

        servers.append(server)
        ports.append(port)

    group = {
        "servers": servers,
        "ports": ports
    }

    try:
        yield group

    finally:
        for server in group["servers"]:
            if server is not None:
                server.stop(0).wait()


def test_retry_after_timeout_is_safe():
    server, port = _start_test_server(
        DelayedFirstIncrementServicer()
    )

    channel = grpc.insecure_channel(
        f"localhost:{port}"
    )
    stub = counter_pb2_grpc.CounterStub(channel)

    try:
        response = increment_counter(
            stub,
            "retry-counter",
            1,
            key="retry-key-1",
            name="retry-client"
        )

        time.sleep(0.5)

        final = stub.Get(
            counter_pb2.GetRequest(
                counter_id="retry-counter",
                lamport_time=1
            ),
            timeout=2.0
        )

        assert response.new_value == 1
        assert final.found is True
        assert final.value == 1

    finally:
        channel.close()
        server.stop(0).wait()


def test_majority_commit_two_acks(running_replicas):
    ports = running_replicas["ports"]

    running_replicas["servers"][0].stop(0).wait()
    running_replicas["servers"][0] = None

    result = quorum_increment(
        "majority-counter",
        1,
        key="majority-key-1",
        ports=ports,
        name="quorum-test"
    )

    assert result["committed"] is True
    assert result["ack_count"] == 2
    assert result["required"] == 2


def test_no_commit_below_majority(running_replicas):
    ports = running_replicas["ports"]

    running_replicas["servers"][0].stop(0).wait()
    running_replicas["servers"][0] = None

    running_replicas["servers"][2].stop(0).wait()
    running_replicas["servers"][2] = None

    result = quorum_increment(
        "below-majority-counter",
        1,
        key="below-majority-key-1",
        ports=ports,
        name="quorum-test"
    )

    live_value = _get_from_port(
        ports[1],
        "below-majority-counter"
    )

    assert result["committed"] is False
    assert result["ack_count"] == 1
    assert result["required"] == 2
    assert live_value.value == 1


def test_replicas_converge(running_replicas):
    ports = running_replicas["ports"]

    for index in range(5):
        result = quorum_increment(
            "converge-counter",
            1,
            key=f"converge-key-{index}",
            ports=ports,
            name="quorum-test"
        )

        assert result["committed"] is True

    values = [
        _get_from_port(
            port,
            "converge-counter"
        ).value
        for port in ports
    ]

    assert values == [5, 5, 5]
