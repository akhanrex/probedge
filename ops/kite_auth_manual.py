import os
from kiteconnect import KiteConnect

api_key    = os.environ["KITE_API_KEY"]
api_secret = os.environ["KITE_API_SECRET"]

kite = KiteConnect(api_key=api_key)
print("\nLogin URL:\n", kite.login_url(), "\n")

# IMPORTANT: paste ONLY the request_token here (a single use short string)
req = input("Paste request_token here: ").strip()

sess = kite.generate_session(req, api_secret=api_secret)
print("\nACCESS_TOKEN=", sess["access_token"])
