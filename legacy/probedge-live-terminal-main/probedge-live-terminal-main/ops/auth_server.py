#!/usr/bin/env python3
import os, asyncio, webbrowser, json
from aiohttp import web
from dotenv import dotenv_values, set_key
from kiteconnect import KiteConnect

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ENV_PATH = os.path.join(ROOT, ".env")

def load_env():
    # prefer .env over .env.example
    if not os.path.exists(ENV_PATH):
        # first use: copy from example if exists
        ex = os.path.join(ROOT, ".env.example")
        if os.path.exists(ex):
            with open(ex, "r") as fsrc, open(ENV_PATH, "w") as fdst:
                fdst.write(fsrc.read())
    return dotenv_values(ENV_PATH)

async def handle_start(request):
    # Build login URL and open browser
    env = load_env()
    api_key = env.get("KITE_API_KEY")
    api_secret = env.get("KITE_API_SECRET")
    if not api_key or not api_secret:
        return web.json_response({"ok": False, "error": "Missing KITE_API_KEY/SECRET in .env"}, status=400)

    kc = KiteConnect(api_key=api_key)
    login_url = kc.login_url()  # uses app redirect URL set in console
    # Open default browser
    webbrowser.open(login_url)
    return web.json_response({"ok": True, "login_url": login_url})

async def handle_callback(request):
    """
    Kite redirects here with ?request_token=...&status=success
    We will exchange it for access_token and save into .env.
    """
    env = load_env()
    api_key = env.get("KITE_API_KEY")
    api_secret = env.get("KITE_API_SECRET")
    req_token = request.query.get("request_token")
    status = request.query.get("status")

    if not req_token or status != "success":
        return web.Response(text="Auth failed or cancelled. No request_token.", status=400)

    kc = KiteConnect(api_key=api_key)
    try:
        sess = kc.generate_session(req_token, api_secret=api_secret)
        access_token = sess["access_token"]
        # Persist into .env
        set_key(ENV_PATH, "KITE_ACCESS_TOKEN", access_token)
        # Confirm to user
        html = f"""
        <html><body style="font-family:system-ui;background:#0b0e11;color:#e6edf3">
        <h2>✅ Kite access token saved</h2>
        <p><code>KITE_ACCESS_TOKEN</code> updated in <b>.env</b>.</p>
        <p>You can now close this tab and run: <code>./ops/run_live.sh</code></p>
        </body></html>
        """
        return web.Response(text=html, content_type="text/html")
    except Exception as e:
        return web.Response(text=f"Error exchanging request_token: {e}", status=500)

def make_app():
    app = web.Application()
    app.add_routes([
        web.get("/api/auth/start", handle_start),
        web.get("/api/auth/callback", handle_callback),
    ])
    return app

if __name__ == "__main__":
    web.run_app(make_app(), host="127.0.0.1", port=8999)
