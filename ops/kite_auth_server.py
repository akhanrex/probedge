import os, re
from pathlib import Path
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, PlainTextResponse
from kiteconnect import KiteConnect

ROOT = Path(__file__).resolve().parents[1]
ENV  = ROOT / ".env"
OUT  = ROOT / "data" / "diagnostics"
OUT.mkdir(parents=True, exist_ok=True)

API_KEY    = os.environ.get("KITE_API_KEY", "")
API_SECRET = os.environ.get("KITE_API_SECRET", "")
REDIRECT   = os.environ.get("KITE_REDIRECT_URL", "http://127.0.0.1:8765/callback")

if not API_KEY or not API_SECRET:
    raise SystemExit("Set KITE_API_KEY and KITE_API_SECRET in your environment first.")

app = FastAPI()
kite = KiteConnect(api_key=API_KEY)

def _upsert_env_line(text: str, key: str, value: str) -> str:
    lines = [] if not text else text.splitlines()
    pat = re.compile(rf"^{re.escape(key)}=")
    found = False
    new_lines = []
    for ln in lines:
        if pat.match(ln):
            new_lines.append(f"{key}={value}")
            found = True
        else:
            new_lines.append(ln)
    if not found:
        new_lines.append(f"{key}={value}")
    return "\n".join(new_lines) + "\n"

@app.get("/")
def index():
    url = kite.login_url()
    html = f"""
    <h2>Kite Auth</h2>
    <p>Redirect must be <code>{REDIRECT}</code> in your Kite app settings.</p>
    <p><a href="{url}" target="_blank">Login with Zerodha Kite</a></p>
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

        env_txt = ENV.read_text() if ENV.exists() else ""
        for k,v in [
            ("KITE_API_KEY", API_KEY),
            ("KITE_API_SECRET", API_SECRET),
            ("KITE_REDIRECT_URL", REDIRECT),
            ("KITE_ACCESS_TOKEN", access),
        ]:
            env_txt = _upsert_env_line(env_txt, k, v)
        ENV.write_text(env_txt)

        html = f"""
        <h3>Success</h3>
        <p>ACCESS_TOKEN saved to <code>.env</code> and <code>data/diagnostics/kite_access_token.txt</code>.</p>
        <p>For this terminal session, run:</p>
        <pre>export KITE_API_KEY={API_KEY}
export KITE_API_SECRET={API_SECRET}
export KITE_ACCESS_TOKEN={access}</pre>
        """
        return HTMLResponse(html)
    except Exception as e:
        return PlainTextResponse(f"Auth error: {e}", status_code=500)
