import argparse
import time
import uuid

import grpc

import counter_pb2
import counter_pb2_grpc


def increment_counter(stub, counter_id, delta, key=None):
    if key is None:
        key = str(uuid.uuid4())

    request = counter_pb2.IncrementRequest(
        counter_id=counter_id,
        delta=delta,
        idempotency_key=key
    )

    backoff_times = [0.2, 0.4, 0.8]

    for attempt in range(4):
        try:
            return stub.Increment(
                request,
                timeout=2.0
            )

        except grpc.RpcError as error:
            retryable = error.code() in (
                grpc.StatusCode.DEADLINE_EXCEEDED,
                grpc.StatusCode.UNAVAILABLE
            )

            if not retryable or attempt == 3:
                raise

            time.sleep(backoff_times[attempt])


def get_counter(stub, counter_id):
    request = counter_pb2.GetRequest(
        counter_id=counter_id
    )

    backoff_times = [0.2, 0.4, 0.8]

    for attempt in range(4):
        try:
            return stub.Get(
                request,
                timeout=2.0
            )

        except grpc.RpcError as error:
            retryable = error.code() in (
                grpc.StatusCode.DEADLINE_EXCEEDED,
                grpc.StatusCode.UNAVAILABLE
            )

            if not retryable or attempt == 3:
                raise

            time.sleep(backoff_times[attempt])


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

    channel = grpc.insecure_channel(
        f"localhost:{args.port}"
    )

    stub = counter_pb2_grpc.CounterStub(channel)

    if args.command == "incr":
        response = increment_counter(
            stub,
            args.counter_id,
            args.by,
            args.key
        )

        print(
            f"OK value={response.new_value} "
            f"duplicate={response.was_duplicate}"
        )

    elif args.command == "get":
        response = get_counter(
            stub,
            args.counter_id
        )

        if response.found:
            print(f"value={response.value}")
        else:
            print("counter not found")


if __name__ == "__main__":
    main()
