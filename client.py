import argparse
import time
import uuid
from concurrent import futures

import grpc

import counter_pb2
import counter_pb2_grpc
from clocks import LamportClock


DEFAULT_REPLICA_PORTS = (50051, 50052, 50053)


def increment_counter(
    stub,
    counter_id,
    delta,
    key=None,
    clock=None,
    name="client-1"
):
    if key is None:
        key = str(uuid.uuid4())

    if clock is None:
        clock = LamportClock()

    backoff_times = [0.2, 0.4, 0.8]

    for attempt in range(4):
        send_time = clock.tick()

        request = counter_pb2.IncrementRequest(
            counter_id=counter_id,
            delta=delta,
            idempotency_key=key,
            lamport_time=send_time
        )

        print(
            f"[{name}] SEND "
            f"Increment(counter={counter_id}, delta={delta}) "
            f"L={send_time}"
        )

        try:
            response = stub.Increment(
                request,
                timeout=2.0
            )

            recv_time = clock.update(
                response.lamport_time
            )

            print(
                f"[{name}] RECV "
                f"IncrementReply(new_value={response.new_value}) "
                f"L={recv_time} "
                f"(received L={response.lamport_time})"
            )

            return response

        except grpc.RpcError as error:
            retryable = error.code() in (
                grpc.StatusCode.DEADLINE_EXCEEDED,
                grpc.StatusCode.UNAVAILABLE
            )

            if not retryable or attempt == 3:
                raise

            time.sleep(backoff_times[attempt])


def get_counter(
    stub,
    counter_id,
    clock=None,
    name="client-1"
):
    if clock is None:
        clock = LamportClock()

    backoff_times = [0.2, 0.4, 0.8]

    for attempt in range(4):
        send_time = clock.tick()

        request = counter_pb2.GetRequest(
            counter_id=counter_id,
            lamport_time=send_time
        )

        print(
            f"[{name}] SEND "
            f"Get(counter={counter_id}) "
            f"L={send_time}"
        )

        try:
            response = stub.Get(
                request,
                timeout=2.0
            )

            recv_time = clock.update(
                response.lamport_time
            )

            print(
                f"[{name}] RECV "
                f"GetReply(value={response.value}, found={response.found}) "
                f"L={recv_time} "
                f"(received L={response.lamport_time})"
            )

            return response

        except grpc.RpcError as error:
            retryable = error.code() in (
                grpc.StatusCode.DEADLINE_EXCEEDED,
                grpc.StatusCode.UNAVAILABLE
            )

            if not retryable or attempt == 3:
                raise

            time.sleep(backoff_times[attempt])


def _increment_replica(
    port,
    counter_id,
    delta,
    key,
    clock,
    name
):
    channel = grpc.insecure_channel(
        f"localhost:{port}"
    )

    try:
        stub = counter_pb2_grpc.CounterStub(channel)

        response = increment_counter(
            stub,
            counter_id,
            delta,
            key=key,
            clock=clock,
            name=name
        )

        return port, response

    finally:
        channel.close()


def quorum_increment(
    counter_id,
    delta,
    key=None,
    ports=DEFAULT_REPLICA_PORTS,
    clock=None,
    name="client-1"
):
    if key is None:
        key = str(uuid.uuid4())

    if clock is None:
        clock = LamportClock()

    ports = tuple(ports)
    required = len(ports) // 2 + 1

    acknowledgements = {}
    errors = {}

    with futures.ThreadPoolExecutor(
        max_workers=len(ports)
    ) as executor:
        future_to_port = {
            executor.submit(
                _increment_replica,
                port,
                counter_id,
                delta,
                key,
                clock,
                name
            ): port
            for port in ports
        }

        for future in futures.as_completed(future_to_port):
            port = future_to_port[future]

            try:
                _, response = future.result()
                acknowledgements[port] = response

            except grpc.RpcError as error:
                errors[port] = error.code()

    committed = len(acknowledgements) >= required

    return {
        "committed": committed,
        "ack_count": len(acknowledgements),
        "required": required,
        "responses": acknowledgements,
        "errors": errors,
        "key": key
    }


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--port",
        type=int,
        default=50051
    )

    subparsers = parser.add_subparsers(
        dest="command",
        required=True
    )

    incr_parser = subparsers.add_parser("incr")
    incr_parser.add_argument("counter_id")
    incr_parser.add_argument(
        "--by",
        type=int,
        required=True
    )
    incr_parser.add_argument(
        "--key",
        default=None
    )

    get_parser = subparsers.add_parser("get")
    get_parser.add_argument("counter_id")

    args = parser.parse_args()
    clock = LamportClock()

    channel = grpc.insecure_channel(
        f"localhost:{args.port}"
    )

    stub = counter_pb2_grpc.CounterStub(channel)

    if args.command == "incr":
        response = increment_counter(
            stub,
            args.counter_id,
            args.by,
            args.key,
            clock
        )

        print(
            f"OK value={response.new_value} "
            f"duplicate={response.was_duplicate}"
        )

    elif args.command == "get":
        response = get_counter(
            stub,
            args.counter_id,
            clock
        )

        if response.found:
            print(f"value={response.value}")
        else:
            print("counter not found")


if __name__ == "__main__":
    main()
