# apps/runtime/kite_auth_cli.py

from probedge.broker.kite_session import (
    get_login_url,
    handle_callback,
    kite_status,
)

import webbrowser


def main():
    status = kite_status()
    if status.get("authenticated"):
        print("Already authenticated:")
        print(status)
        return

    # 1) Get login URL
    url = get_login_url()
    print("\nSTEP 1: Open this URL in your browser and login to Kite:")
    print(url)
    print()

    try:
        webbrowser.open(url)
    except Exception:
        # If it fails, you can still copy-paste the URL manually.
        pass

    # 2) After login, Zerodha will redirect you to the redirect URL
    #    e.g. http://127.0.0.1:9002/api/kite/callback?request_token=XXXX&status=success
    #    You just need the value after 'request_token='.
    request_token = input(
        "STEP 2: After login, copy the 'request_token' from the browser URL "
        "and paste here: "
    ).strip()

    session = handle_callback(request_token)
    print("\nSTEP 3: Session saved. Basic info:")
    print({k: session[k] for k in ("user_id", "login_time")})


if __name__ == "__main__":
    main()
