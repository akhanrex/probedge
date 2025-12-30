import os, requests

BASE = os.getenv("BASE", "http://127.0.0.1:9002")

def test_arm_and_stop():
    r = requests.post(f"{BASE}/api/arm", json={"symbol":"TMPV","side":"BUY","qty":1}, timeout=5)
    assert r.status_code == 200
    r = requests.post(f"{BASE}/api/stop", timeout=5)
    assert r.status_code == 200

def test_settings_present():
    r = requests.get(f"{BASE}/api/settings", timeout=5)
    assert r.status_code == 200
    j = r.json()
    assert "paths" in j and "symbols" in j
