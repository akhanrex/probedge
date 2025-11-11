import os, sys, urllib.parse, http.server, webbrowser
from kiteconnect import KiteConnect
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("KITE_API_KEY") or ""
API_SECRET = os.getenv("KITE_API_SECRET") or ""
REDIRECT_URL = os.getenv("KITE_REDIRECT_URL", "http://127.0.0.1:8765/callback")

if not API_KEY or not API_SECRET:
    print("ERROR: Set KITE_API_KEY and KITE_API_SECRET in .env first.")
    sys.exit(1)

kc = KiteConnect(api_key=API_KEY)
login_url = kc.login_url()

print("\n1) Ensure your Kite app's Redirect URL is set to:", REDIRECT_URL)
print("2) Opening login URL in your browser...")
print(login_url, "\n")

# One-shot local catcher
request_token_holder = {"value": None}
class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path.startswith("/callback"):
            qs = urllib.parse.urlparse(self.path).query
            q = urllib.parse.parse_qs(qs)
            rt = (q.get("request_token") or [None])[0]
            request_token_holder["value"] = rt
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"You can close this tab. Return to terminal.")
        else:
            self.send_response(404); self.end_headers()
    def log_message(self, *args, **kwargs):
        return  # keep terminal clean

try:
    srv = http.server.HTTPServer(("127.0.0.1", 8765), Handler)
except OSError:
    srv = None
    print("Note: Port 8765 in use. If auto-callback fails, use manual paste flow.")
webbrowser.open(login_url)

if srv:
    print("Waiting for browser redirect on", REDIRECT_URL, " ...")
    srv.handle_request()  # one request
    request_token = request_token_holder["value"]
else:
    request_token = None

if not request_token:
    print("\nManual method:")
    print(" - After login, copy the FULL redirected URL and paste below.")
    full = input("\nPaste redirected URL here: ").strip()
    try:
        qs = urllib.parse.urlparse(full).query
        request_token = urllib.parse.parse_qs(qs).get("request_token", [None])[0]
    except Exception:
        request_token = None

if not request_token:
    print("ERROR: Could not obtain request_token.")
    sys.exit(2)

print("\nrequest_token =", request_token)
sess = kc.generate_session(request_token=request_token, api_secret=API_SECRET)
access_token = sess["access_token"]
print("\nSUCCESS. access_token =", access_token)

# Write into .env
path = ".env"
try:
    with open(path, "r", encoding="utf-8") as f:
        lines = f.read().splitlines()
except FileNotFoundError:
    lines = []
def set_kv(lines, k, v):
    kept, replaced = [], False
    for ln in lines:
        if ln.strip().startswith(k + "="):
            kept.append(f"{k}={v}"); replaced = True
        else:
            kept.append(ln)
    if not replaced:
        kept.append(f"{k}={v}")
    return kept
lines = set_kv(lines, "KITE_ACCESS_TOKEN", access_token)
with open(path, "w", encoding="utf-8") as f:
    f.write("\n".join(lines) + "\n")
print(f"\nWrote KITE_ACCESS_TOKEN to {path}")
