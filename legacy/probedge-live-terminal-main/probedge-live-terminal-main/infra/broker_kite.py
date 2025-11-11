#!/usr/bin/env python3
"""
Lightweight Kite client bootstrap.

Reads credentials from env:
  KITE_API_KEY
  KITE_ACCESS_TOKEN    (already generated)  OR
  KITE_REQUEST_TOKEN   (one-time; will exchange to access_token if provided)
  KITE_API_SECRET      (needed only when exchanging request_token)

Usage:
  from infra.broker_kite import get_kite, ensure_access_token
  kite = get_kite()
"""
import os
from typing import Optional

_KITE = None   # singleton

def get_kite():
    global _KITE
    if _KITE is not None:
        return _KITE

    api_key = os.getenv("KITE_API_KEY")
    if not api_key:
        raise RuntimeError("KITE_API_KEY env missing")

    try:
        from kiteconnect import KiteConnect
    except Exception as e:
        raise RuntimeError("kiteconnect not installed. pip install kiteconnect") from e

    kite = KiteConnect(api_key=api_key)

    access_token = os.getenv("KITE_ACCESS_TOKEN", "").strip()
    request_token = os.getenv("KITE_REQUEST_TOKEN", "").strip()
    api_secret    = os.getenv("KITE_API_SECRET", "").strip()

    # If caller passed a request_token (fresh login flow), try to exchange
    if request_token and api_secret and not access_token:
        data = kite.generate_session(request_token, api_secret=api_secret)
        access_token = data["access_token"]

    if not access_token:
        raise RuntimeError(
            "KITE_ACCESS_TOKEN missing. Either set it directly or provide "
            "KITE_REQUEST_TOKEN + KITE_API_SECRET once to generate it."
        )

    kite.set_access_token(access_token)
    _KITE = kite
    return kite


def ensure_access_token() -> Optional[str]:
    """
    Returns a non-empty access token if available.
    """
    at = os.getenv("KITE_ACCESS_TOKEN", "").strip()
    if at:
        return at
    rt = os.getenv("KITE_REQUEST_TOKEN", "").strip()
    sec = os.getenv("KITE_API_SECRET", "").strip()
    return at if at else (rt and sec)
