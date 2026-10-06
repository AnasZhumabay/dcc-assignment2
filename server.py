import argparse
import threading
from concurrent import futures

import grpc

import counter_pb2
import counter_pb2_grpc


class CounterServicer(counter_pb2_grpc.CounterServicer):
    def __init__(self):
        self._lock = threading.Lock()
        self._values = {}
        self._seen = {}

    def Increment(self, request, context):
        with self._lock:
            if request.idempotency_key in self._seen:
                _, stored_value = self._seen[request.idempotency_key]

                return counter_pb2.IncrementReply(
                    new_value=stored_value,
                    was_duplicate=True
                )

            current_value = self._values.get(request.counter_id, 0)
            new_value = current_value + request.delta

            self._values[request.counter_id] = new_value
            self._seen[request.idempotency_key] = (
                request.counter_id,
                new_value
            )

            return counter_pb2.IncrementReply(
                new_value=new_value,
                was_duplicate=False
            )

    def Get(self, request, context):
        with self._lock:
            if request.counter_id in self._values:
                return counter_pb2.GetReply(
                    value=self._values[request.counter_id],
                    found=True
                )

            return counter_pb2.GetReply(
                value=0,
                found=False
            )


def serve(port):
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=8)
    )

    counter_pb2_grpc.add_CounterServicer_to_server(
        CounterServicer(),
        server
    )

    server.add_insecure_port(f"[::]:{port}")
    server.start()

    print(f"Counter server running on port {port}")

    server.wait_for_termination()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--port",
        type=int,
        default=50051
    )

    args = parser.parse_args()
    serve(args.port)
