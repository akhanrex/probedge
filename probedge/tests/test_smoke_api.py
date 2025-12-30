import requests, os
BASE = os.getenv("BASE", "http://127.0.0.1:9002")

def test_health():
    r = requests.get(f"{BASE}/api/health", timeout=5)
    assert r.status_code == 200 and r.json().get("ok") is True

def test_settings():
    r = requests.get(f"{BASE}/api/settings", timeout=5)
    j = r.json()
    assert r.status_code == 200 and "paths" in j and "symbols" in j

def test_tm5():
    r = requests.get(f"{BASE}/api/tm5", params={"symbol":"TMPV","limit":5}, timeout=5)
    j = r.json()
    assert r.status_code == 200 and j["symbol"] == "TMPV" and j["rows"] <= 5
