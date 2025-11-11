import os, webbrowser, threading, urllib.parse, http.server
from kiteconnect import KiteConnect

API_KEY = os.getenv("KITE_API_KEY")
API_SECRET = os.getenv("KITE_API_SECRET")
if not API_KEY or not API_SECRET:
    raise SystemExit("Set KITE_API_KEY and KITE_API_SECRET in .env, then `set -a; source .env; set +a`")

PORT = 8765
REDIRECT = f"http://127.0.0.1:{PORT}/callback"
kite = KiteConnect(api_key=API_KEY)

class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path.startswith("/callback"):
            qs = urllib.parse.urlparse(self.path).query
            q = dict(urllib.parse.parse_qsl(qs))
            token = q.get("request_token")
            if not token:
                self.send_response(400); self.end_headers()
                self.wfile.write(b"Missing request_token")
                return
            try:
                sess = kite.generate_session(token, api_secret=API_SECRET)
                access = sess["access_token"]
                # write to .env
                with open(".env","r",encoding="utf-8") as f: txt = f.read()
                if "KITE_ACCESS_TOKEN=" in txt:
                    import re
                    txt = re.sub(r"^KITE_ACCESS_TOKEN=.*", f"KITE_ACCESS_TOKEN={access}", txt, flags=re.M)
                else:
                    txt += f"\nKITE_ACCESS_TOKEN={access}\n"
                with open(".env","w",encoding="utf-8") as f: f.write(txt)
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
    # Start local server
    t = threading.Thread(target=run_server, daemon=True)
    t.start()
    # Open login URL in browser
    url = kite.login_url()  # must match your app's redirect setting
    print("\nOpen in browser (if not auto-opened):\n", url, "\n")
    webbrowser.open(url)
    print(f"Listening on {REDIRECT} ...")
    t.join()
