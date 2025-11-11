# Basic smoke tests assuming API at http://127.0.0.1:9002
import requests, sys
base = "http://127.0.0.1:9002"
def check(path):
    u = base + path
    r = requests.get(u, timeout=5)
    r.raise_for_status()
    print(path, "OK")
    return r.json()
check("/api/health")
cfg = check("/api/config")
sym = cfg["symbols"][0]
check(f"/api/tm5?symbol={sym}&limit=5")
check(f"/api/matches?symbol={sym}&ot=BULL&ol=OAR&pdc=TR")
print("All good.")
