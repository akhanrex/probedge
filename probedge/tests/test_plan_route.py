import requests, os
BASE = os.getenv("BASE", "http://127.0.0.1:9002")

def test_plan_tmpv():
    r = requests.get(f"{BASE}/api/plan", params={"symbol":"TMPV"}, timeout=5)
    assert r.status_code == 200
    j = r.json()
    for k in ["symbol","date","tags","entry","orb_high","orb_low"]:
        assert k in j
