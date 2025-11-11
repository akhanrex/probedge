#!/usr/bin/env python3
import os, asyncio, webbrowser
from aiohttp import web
from dotenv import dotenv_values, set_key
from kiteconnect import KiteConnect

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ENV_PATH = os.path.join(ROOT, ".env")
print(f"[auth] Using env file: {ENV_PATH}")

def load_env():
    if not os.path.exists(ENV_PATH):
        raise RuntimeError("Missing .env in repo root. Create it and put KITE_API_KEY/SECRET.")
    return dotenv_values(ENV_PATH)

async def handle_start(request):
    env = load_env()
    api_key = env.get("KITE_API_KEY")
    api_secret = env.get("KITE_API_SECRET")
    if not api_key or not api_secret:
        return web.json_response({"ok": False, "error": "Missing KITE_API_KEY/KITE_API_SECRET in .env"}, status=400)

    kc = KiteConnect(api_key=api_key)
    login_url = kc.login_url()
    print("[auth] Login URL:", login_url)
    try:
        webbrowser.open(login_url)
    except Exception:
        pass
    return web.json_response({"ok": True, "login_url": login_url})

async def handle_callback(request):
    env = load_env()
    api_key = env.get("KITE_API_KEY")
    api_secret = env.get("KITE_API_SECRET")
    req_token = request.query.get("request_token")
    status = request.query.get("status")

    if (not req_token) or (status != "success"):
        return web.Response(text="Auth failed or cancelled. No request_token.", status=400)

    kc = KiteConnect(api_key=api_key)
    try:
        sess = kc.generate_session(request_token=req_token, api_secret=api_secret)
        access_token = sess["access_token"]
        
        # Write to .env
        set_key(ENV_PATH, "KITE_ACCESS_TOKEN", access_token)
        
        # Verify write
        from dotenv import dotenv_values
        post = dotenv_values(ENV_PATH)
        print("[auth] Saved token length:", len((post.get("KITE_ACCESS_TOKEN") or "")))
        
        html = """
        <html><body style="font-family:system-ui;background:#0b0e11;color:#e6edf3">
        <h2>✅ Kite access token saved</h2>
        <p><code>KITE_ACCESS_TOKEN</code> updated in <b>.env</b>.</p>
        <p>You can close this tab.</p>
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
    print("[auth] Starting on http://127.0.0.1:8999 …")
    web.run_app(make_app(), host="127.0.0.1", port=8999)
