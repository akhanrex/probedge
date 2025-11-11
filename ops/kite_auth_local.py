# Minimal local redirect server to capture Kite request_token and exchange for access_token.
import http.server, threading, webbrowser, os
from urllib.parse import urlparse, parse_qs
from kiteconnect import KiteConnect

PORT = 8765
REDIRECT = f"http://127.0.0.1:{PORT}/callback"

api_key = os.environ.get("KITE_API_KEY","").strip()
api_secret = os.environ.get("KITE_API_SECRET","").strip()
kite = KiteConnect(api_key=api_key)

class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path.startswith("/callback"):
            q = parse_qs(urlparse(self.path).query)
            rq = q.get("request_token", [""])[0]
            try:
                data = kite.generate_session(rq, api_secret=api_secret)
                access_token = data["access_token"]
                env_path = ".env"
                new_lines = []
                if os.path.exists(env_path):
                    with open(env_path) as f: new_lines = f.read().splitlines()
                replaced = False
                for i, line in enumerate(new_lines):
                    if line.startswith("KITE_ACCESS_TOKEN="):
                        new_lines[i] = f"KITE_ACCESS_TOKEN={access_token}"
                        replaced = True
                        break
                if not replaced:
                    new_lines.append(f"KITE_ACCESS_TOKEN={access_token}")
                with open(env_path, "w") as f:
                    f.write("\n".join(new_lines) + "\n")
                self.send_response(200); self.end_headers()
                self.wfile.write(b"ACCESS_TOKEN saved to .env. You can close this tab.")
                print("✅ KITE_ACCESS_TOKEN saved to .env")
            except Exception as e:
                self.send_response(500); self.end_headers()
                self.wfile.write(str(e).encode("utf-8"))
                print("ERROR:", e)
        else:
            self.send_response(404); self.end_headers()

def run_server():
    http.server.HTTPServer(("127.0.0.1", PORT), Handler).serve_forever()

if __name__ == "__main__":
    t = threading.Thread(target=run_server, daemon=True); t.start()
    url = kite.login_url()
    print("\nOpen in browser (if not auto-opened):\n", url, "\n")
    webbrowser.open(url)
    print(f"Listening on {REDIRECT} ...")
    t.join()
