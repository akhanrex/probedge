import os
from pathlib import Path
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, PlainTextResponse
from kiteconnect import KiteConnect

ROOT = Path(__file__).resolve().parents[1]
OUT  = ROOT / "data" / "diagnostics"
OUT.mkdir(parents=True, exist_ok=True)

app = FastAPI()

# Expect these in env:
# KITE_API_KEY, KITE_API_SECRET
# In your Kite app settings, set Redirect URL to:
# http://127.0.0.1:9100/callback
API_KEY    = os.environ["KITE_API_KEY"]
API_SECRET = os.environ["KITE_API_SECRET"]

kite = KiteConnect(api_key=API_KEY)

@app.get("/")
def index():
    url = kite.login_url()
    html = f"""
    <h2>Kite Auth</h2>
    <p><a href="{url}" target="_blank">Click to Login with Zerodha Kite</a></p>
    <p>After login, you will be redirected to <code>/callback</code>.</p>
    """
    return HTMLResponse(html)

@app.get("/callback")
def callback(request_token: str = ""):
    if not request_token:
        return PlainTextResponse("Missing request_token", status_code=400)
    try:
        sess = kite.generate_session(request_token, api_secret=API_SECRET)
        access = sess["access_token"]
        (OUT / "kite_access_token.txt").write_text(access)
        # Show + copy-paste exports for your shell
        html = f"""
        <h3>Success</h3>
        <p>ACCESS_TOKEN captured. Save these in your shell for today:</p>
        <pre>export KITE_API_KEY={API_KEY}
export KITE_API_SECRET={API_SECRET}
export KITE_ACCESS_TOKEN={access}</pre>
        <p>Token also saved to <code>data/diagnostics/kite_access_token.txt</code>.</p>
        """
        return HTMLResponse(html)
    except Exception as e:
        return PlainTextResponse(f"Auth error: {e}", status_code=500)
