# ops_get_kite_token.py
#
# One-off helper to fetch today's KITE_ACCESS_TOKEN and print it.
# Run:
#   python ops_get_kite_token.py
#
# Steps:
#  1) It reads KITE_API_KEY / KITE_API_SECRET from .env
#  2) Prints the login URL -> open in browser, complete login
#  3) Copy the "request_token" from redirect URL and paste back
#  4) It prints ACCESS_TOKEN = ...  -> put that in .env as KITE_ACCESS_TOKEN

import os
from dotenv import load_dotenv
from kiteconnect import KiteConnect

load_dotenv()

api_key = os.getenv("KITE_API_KEY", "").strip()
api_secret = os.getenv("KITE_API_SECRET", "").strip()

if not api_key or not api_secret:
    raise SystemExit("Set KITE_API_KEY and KITE_API_SECRET in .env first.")

kite = KiteConnect(api_key=api_key)
login_url = kite.login_url()
print("1) Open this URL in your browser and login:")
print(login_url)
print()
request_token = input("2) Paste the 'request_token' from the redirect URL here: ").strip()

data = kite.generate_session(request_token, api_secret=api_secret)
access_token = data["access_token"]
print()
print("3) ACCESS_TOKEN:", access_token)
print("   Put this into .env as:")
print("   KITE_ACCESS_TOKEN=" + access_token)

