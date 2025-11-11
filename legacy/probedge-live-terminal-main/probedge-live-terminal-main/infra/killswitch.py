import threading

class KillSwitch:
    def __init__(self):
        self._lock = threading.Lock()
        self._on = False
    def on(self):
        with self._lock:
            self._on = True
    def off(self):
        with self._lock:
            self._on = False
    def is_on(self):
        with self._lock:
            return self._on

KILL = KillSwitch()
