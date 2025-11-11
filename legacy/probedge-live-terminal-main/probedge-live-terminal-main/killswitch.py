import threading

class KillSwitch:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._tripped = False

    def trip(self) -> None:
        with self._lock:
            self._tripped = True

    def reset(self) -> None:
        with self._lock:
            self._tripped = False

    def is_tripped(self) -> bool:
        with self._lock:
            return self._tripped
