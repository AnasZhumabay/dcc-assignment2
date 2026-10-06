import argparse
import threading
from concurrent import futures

import grpc

import counter_pb2
import counter_pb2_grpc
from clocks import LamportClock


class CounterServicer(counter_pb2_grpc.CounterServicer):
    def __init__(self, name="replica-A"):
        self._lock = threading.Lock()
        self._values = {}
        self._seen = {}
        self._clock = LamportClock()
        self._name = name

    def Increment(self, request, context):
        recv_time = self._clock.update(request.lamport_time)

        print(
            f"[{self._name}] RECV "
            f"Increment(counter={request.counter_id}, delta={request.delta}) "
            f"L={recv_time} (received L={request.lamport_time})"
        )

        with self._lock:
            if request.idempotency_key in self._seen:
                _, stored_value = self._seen[request.idempotency_key]

                send_time = self._clock.tick()

                print(
                    f"[{self._name}] SEND "
                    f"IncrementReply(new_value={stored_value}) "
                    f"L={send_time}"
                )

                return counter_pb2.IncrementReply(
                    new_value=stored_value,
                    was_duplicate=True,
                    lamport_time=send_time
                )

            current_value = self._values.get(request.counter_id, 0)
            new_value = current_value + request.delta

            self._values[request.counter_id] = new_value
            self._seen[request.idempotency_key] = (
                request.counter_id,
                new_value
            )

            apply_time = self._clock.tick()

            print(
                f"[{self._name}] APPLY "
                f"counter={request.counter_id} -> {new_value} "
                f"L={apply_time}"
            )

            send_time = self._clock.tick()

            print(
                f"[{self._name}] SEND "
                f"IncrementReply(new_value={new_value}) "
                f"L={send_time}"
            )

            return counter_pb2.IncrementReply(
                new_value=new_value,
                was_duplicate=False,
                lamport_time=send_time
            )

    def Get(self, request, context):
        recv_time = self._clock.update(request.lamport_time)

        print(
            f"[{self._name}] RECV "
            f"Get(counter={request.counter_id}) "
            f"L={recv_time} (received L={request.lamport_time})"
        )

        with self._lock:
            if request.counter_id in self._values:
                value = self._values[request.counter_id]
                found = True
            else:
                value = 0
                found = False

        send_time = self._clock.tick()

        print(
            f"[{self._name}] SEND "
            f"GetReply(value={value}, found={found}) "
            f"L={send_time}"
        )

        return counter_pb2.GetReply(
            value=value,
            found=found,
            lamport_time=send_time
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
