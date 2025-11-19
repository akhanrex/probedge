# probedge/ops/supervisor.py
#
# Simple process supervisor for Probedge:
# - starts API, batch_agent, agg5
# - monitors them
# - if any exits, marks system DOWN and stops all

from __future__ import annotations

import subprocess
import sys
import time
from typing import List, Tuple

from probedge.infra.health import set_system_status


def _start_process(cmd: list[str], name: str) -> subprocess.Popen:
    p = subprocess.Popen(cmd)
    print(f"[supervisor] started {name} pid={p.pid} :: {' '.join(cmd)}")
    return p


def main():
    procs: List[Tuple[str, subprocess.Popen]] = []

    try:
        # Assume you already activated the venv before running supervisor.
        # API (FastAPI + Uvicorn) - no reload in live mode.
        api = _start_process(
            ["uvicorn", "apps.api.main:app", "--host", "127.0.0.1", "--port", "9002"],
            "api",
        )
        procs.append(("api", api))

        # Batch agent (plans + live_state)
        batch = _start_process(
            [sys.executable, "-m", "probedge.ops.batch_agent"],
            "batch_agent",
        )
        procs.append(("batch_agent", batch))

        # 5-min aggregator from ticks
        agg = _start_process(
            [sys.executable, "-m", "probedge.realtime.agg5"],
            "agg5",
        )
        procs.append(("agg5", agg))

        set_system_status("WARN", "supervisor started; waiting for heartbeats")

        # Monitor loop
        while True:
            time.sleep(2.0)
            for name, p in list(procs):
                ret = p.poll()
                if ret is not None:
                    print(f"[supervisor] {name} exited with code {ret}")
                    set_system_status(
                        "DOWN",
                        f"{name} exited with code {ret}; supervisor stopping all components",
                    )

                    # terminate others
                    for n2, p2 in procs:
                        if p2 is not p and p2.poll() is None:
                            print(f"[supervisor] terminating {n2} pid={p2.pid}")
                            p2.terminate()

                    time.sleep(2.0)

                    for n2, p2 in procs:
                        if p2.poll() is None:
                            print(f"[supervisor] killing {n2} pid={p2.pid}")
                            p2.kill()

                    return

    except KeyboardInterrupt:
        print("[supervisor] KeyboardInterrupt, shutting down...")
        set_system_status("DOWN", "supervisor interrupted by user")

        for name, p in procs:
            if p.poll() is None:
                print(f"[supervisor] terminating {name} pid={p.pid}")
                p.terminate()

        time.sleep(2.0)

        for name, p in procs:
            if p.poll() is None:
                print(f"[supervisor] killing {name} pid={p.pid}")
                p.kill()


if __name__ == "__main__":
    main()
