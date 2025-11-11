import os, re, pathlib
from kiteconnect import KiteConnect

api_key = os.environ.get("KITE_API_KEY")
api_secret = os.environ.get("KITE_API_SECRET")
if not api_key or not api_secret:
    raise SystemExit("Set KITE_API_KEY / KITE_API_SECRET in .env and source it first.")

kite = KiteConnect(api_key=api_key)
print("\n=== KITE LOGIN URL ===\n" + kite.login_url() + "\n")
print("1) Open the URL, login, and allow.")
print("2) You'll be redirected to your app's Redirect URL (configured in Kite developer console).")
print("3) Copy ONLY the value of 'request_token' from the browser address bar, even if the page shows an error.\n")

request_token = input("Paste request_token here: ").strip()
if not request_token:
    raise SystemExit("Empty request_token; re-run and paste only the token string.")

session = kite.generate_session(request_token, api_secret=api_secret)
access_token = session["access_token"]

p = pathlib.Path(".env")
txt = p.read_text()
if "KITE_ACCESS_TOKEN=" in txt:
    txt = re.sub(r'^KITE_ACCESS_TOKEN=.*', f'KITE_ACCESS_TOKEN={access_token}', txt, flags=re.M)
else:
    txt += f'\nKITE_ACCESS_TOKEN={access_token}\n'
p.write_text(txt)
print("\n✅ Saved KITE_ACCESS_TOKEN to .env")
