# probedge/cache.py — unified caching + invalidation
import os, time, hashlib
import streamlit as st

SESSION_KEY = "__PE_SESSION_VERSION__"

def _bump_version():
    st.session_state[SESSION_KEY] = str(time.time())

def invalidate_all():
    try:
        st.cache_data.clear()
    except Exception:
        pass
    try:
        st.cache_resource.clear()
    except Exception:
        pass
    _bump_version()

# Decorators that weave the session version into cache keys

def memo_data(**kwargs):
    def _wrap(fn):
        def _inner(*a, **k):
            k = {**k, "__v": st.session_state.get(SESSION_KEY, "0")}
            return st.cache_data(**kwargs)(fn)(*a, **k)
        return _inner
    return _wrap

def memo_resource(**kwargs):
    def _wrap(fn):
        def _inner(*a, **k):
            k = {**k, "__v": st.session_state.get(SESSION_KEY, "0")}
            return st.cache_resource(**kwargs)(fn)(*a, **k)
        return _inner
    return _wrap
