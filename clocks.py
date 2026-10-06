import threading


class LamportClock:
    def __init__(self):
        self._time = 0
        self._lock = threading.Lock()

    def tick(self):
        with self._lock:
            self._time += 1
            return self._time

    def update(self, received_time):
        with self._lock:
            self._time = max(self._time, received_time) + 1
            return self._time

    def value(self):
        with self._lock:
            return self._time
