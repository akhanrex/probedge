# app/ui.py — shared UI helpers (no behavior change)

import os, base64, mimetypes
from typing import Optional
import numpy as np
import plotly.graph_objects as go
import streamlit as st
from datetime import time as dtime
# pull donut sizing from the same theme helper your app already uses
from probedge.ui_adapters.components.theme import donut_size as ui_donut_size

# tag styling helpers
from probedge.core.rules import _canon_tag_value, _tag_color_map, _pretty_value


# -------------------------
# Secrets/env convenience
# -------------------------
def _get_secret_first(*names: str, default: Optional[str] = None) -> Optional[str]:
    try:
        for n in names:
            v = st.secrets.get(n)
            if v:
                return str(v)
    except Exception:
        pass
    for n in names:
        v = os.getenv(n) or os.getenv(n.upper())
        if v:
            return str(v)
    return default


LOGO_URL = _get_secret_first("PROBEDGE_LOGO_URL", default=None)

# -------------------------
# Theme + simple colors
# -------------------------
THEMES = {
    "light": {
        "bg": "#ffffff",
        "card": "#ffffff",
        "border": "#e5e7eb",
        "text": "#0f172a",
        "muted": "#64748b",
        "grid": "#e5e7eb",
        "primary": "#7C3AED",
        "primary2": "#EC4899",
        "green": "#10b981",
        "red": "#ef4444",
    },
    "dark": {
        "bg": "#0b1220",
        "card": "#111827",
        "border": "#1f2937",
        "text": "#e5e7eb",
        "muted": "#9ca3af",
        "grid": "#1f2937",
        "primary": "#8B5CF6",
        "primary2": "#EC4899",
        "green": "#34d399",
        "red": "#f87171",
    },
}


def _current_theme():
    name = st.session_state.get("_pe_theme_name", "light")
    return THEMES["dark" if name == "dark" else "light"]


def traffic_color(v: float, *, green_at: float, amber_at: float) -> str:
    if v >= green_at:
        return "#16a34a"
    if v >= amber_at:
        return "#f59e0b"
    return "#dc2626"


# -------------------------
# Small utils
# -------------------------
def _rgba(hex_color: str, alpha: float) -> str:
    h = (hex_color or "").lstrip("#")
    if len(h) == 6:
        r = int(h[0:2], 16)
        g = int(h[2:4], 16)
        b = int(h[4:6], 16)
        return f"rgba({r}, {g}, {b}, {alpha})"
    return hex_color or "rgba(148,163,184,0.2)"


def _img_src_maybe_data_uri(path_or_url: str) -> str:
    s = str(path_or_url or "").strip()
    if s.startswith(("http://", "https://", "data:")):
        return s
    try:
        mime = mimetypes.guess_type(s)[0] or "image/png"
        with open(s, "rb") as f:
            b64 = base64.b64encode(f.read()).decode("ascii")
        return f"data:{mime};base64,{b64}"
    except Exception:
        return ""


def _find_logo_path() -> Optional[str]:
    candidates = [
        "probedge_logo.png",
        "probedge_main.png",
        "probedge_main.PNG",
        os.path.join("assets", "probedge_main.png"),
        os.path.join("assets", "probedge_main.PNG"),
        os.path.join("/mnt/data", "probedge_logo.png"),
        os.path.join("/mnt/data", "probedge_main.png"),
    ]
    for p in candidates:
        try:
            if os.path.exists(p) and os.path.isfile(p):
                return p
        except Exception:
            pass
    return None


def show_logo(centered: bool = False):
    src = LOGO_URL or _find_logo_path()
    logo_html = (
        f"<img src='{_img_src_maybe_data_uri(src)}' style='max-width:200px;height:auto;'/>"
        if src
        else "<div style='font-weight:700;font-size:18px;'>ProbEdge</div>"
    )
    if centered:
        st.markdown(
            f"<div style='width:100%;text-align:center;padding:8px 0 12px 0'>{logo_html}</div>",
            unsafe_allow_html=True,
        )
    else:
        st.markdown(logo_html, unsafe_allow_html=True)


# -------------------------
# Quote snapshot (Kite)
# -------------------------
def _safe_get(d: dict, *keys, default=None):
    cur = d
    try:
        for k in keys:
            cur = cur[k]
        return cur
    except Exception:
        return default


def _safe_quote_snapshot(symbol: str) -> dict:
    """
    Looks for a global 'kite' object in the host module (run.py) and uses it if present.
    Falls back to a 'Not connected' stub when absent.
    """
    try:
        import sys as _sys

        k = getattr(_sys.modules.get("run"), "kite", None) or st.session_state.get(
            "kite"
        )
        if k is None:
            return {"ltp": None, "prev_close": None, "err": "Not connected"}
        q = k.quote(symbol)
        ltp = float(_safe_get(q, symbol, "last_price"))
        prev_close = float(_safe_get(q, symbol, "ohlc", "close"))
        return {"ltp": ltp, "prev_close": prev_close, "err": None}
    except Exception as e:
        return {"ltp": None, "prev_close": None, "err": str(e)}

def _is_market_open_ist() -> bool:
    try:
        from app.config import now_ist
        now = now_ist()
    except Exception:
        # Fallback: treat as closed if we can't tell time
        return False
    # Mon–Fri only
    if now.weekday() >= 5:
        return False
    t = now.time()
    return dtime(9, 15) <= t <= dtime(15, 30)

def _quote_triplet_markup(ltp, chg, pct, *, justify: str, color_pos: str, color_neg: str, muted: str, text: str):
    """Builds aligned grid markup with tabular digits."""
    num_css = (
        "font-size:clamp(11px,2.2vw,14px);"
        "font-weight:700;"
        "text-align:right;"
        "font-variant-numeric: tabular-nums;"
        "font-feature-settings: 'tnum' 1, 'lnum' 1;"
        "font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace;"
        "line-height:1.1;"
        "white-space:nowrap;"
    )
    lab_css = f"font-size:clamp(10px,1.8vw,12px);color:{muted};white-space:nowrap;line-height:1.1;"
    def cell(label, value, color):
        return (
            f"<span style='{lab_css}'>{label}</span>"
            f"<span style='{num_css}color:{color}'>{value}</span>"
        )
    return f"""
    <div style="display:flex;justify-content:{justify}">
      <div style="
        display:grid;
        grid-auto-flow:column;
        grid-template-columns: auto 10ch auto 7ch auto 7ch;
        align-items:center;
        column-gap:8px;
        max-width:100%;
        overflow:hidden;">
        {cell('Last',  text,    'var(--pe-text, currentColor)')}
        {cell('Chg',   chg,     color_pos if chg.startswith('+') else color_neg if chg.startswith('-') else muted)}
        {cell('Chg%',  pct,     color_pos if pct.startswith('+') else color_neg if pct.startswith('-') else muted)}
      </div>
    </div>
    """

@st.fragment(run_every=2)  # only this little fragment refreshes during market hours
def _render_quote_triplet_live(symbol: str, align: str = "left"):
    TZ = _current_theme()
    justify = "flex-end" if str(align).lower() == "right" else "flex-start"
    snap = _safe_quote_snapshot(symbol)
    ltp, prev = snap.get("ltp"), snap.get("prev_close")
    if isinstance(ltp, (int, float)) and isinstance(prev, (int, float)) and prev not in (0, None):
        chg_val = float(ltp) - float(prev)
        pct_val = (chg_val / float(prev)) * 100.0
        markup = _quote_triplet_markup(
            ltp, f"{chg_val:+.2f}", f"{pct_val:+.2f}%",
            justify=justify, color_pos=TZ["green"], color_neg=TZ["red"], muted=TZ["muted"],
            text=f"₹{ltp:,.2f}"
        )
    else:
        markup = _quote_triplet_markup(None, "—", "—",
            justify=justify, color_pos=TZ["green"], color_neg=TZ["red"], muted=TZ["muted"],
            text="—"
        )
    st.markdown(markup, unsafe_allow_html=True)

@st.fragment  # static — no periodic refresh outside market hours
def _render_quote_triplet_static(symbol: str, align: str = "left"):
    TZ = _current_theme()
    justify = "flex-end" if str(align).lower() == "right" else "flex-start"
    snap = _safe_quote_snapshot(symbol)
    ltp, prev = snap.get("ltp"), snap.get("prev_close")
    if isinstance(ltp, (int, float)) and isinstance(prev, (int, float)) and prev not in (0, None):
        chg_val = float(ltp) - float(prev)
        pct_val = (chg_val / float(prev)) * 100.0
        markup = _quote_triplet_markup(
            ltp, f"{chg_val:+.2f}", f"{pct_val:+.2f}%",
            justify=justify, color_pos=TZ["green"], color_neg=TZ["red"], muted=TZ["muted"],
            text=f"₹{ltp:,.2f}"
        )
    else:
        markup = _quote_triplet_markup(None, "—", "—",
            justify=justify, color_pos=TZ["green"], color_neg=TZ["red"], muted=TZ["muted"],
            text="—"
        )
    st.markdown(markup, unsafe_allow_html=True)

def render_quote_triplet(symbol: str = "NSE:TATAMOTORS", key: str = "tm_triplet", align: str = "left"):
    """During market hours render the live fragment, else render a static snapshot.
    Only this fragment refreshes; the rest of the app does not rerun.
    """
    if _is_market_open_ist():
        _render_quote_triplet_live(symbol, align=align)
    else:
        _render_quote_triplet_static(symbol, align=align)


# -------------------------
# Donut + chips + header
# -------------------------
def donut(
    value, center_top, center_bottom, color, max_value=100, size=None, show_percent=True
):
    if size is None:
        size = ui_donut_size()
    try:
        v = float(value)
        if not np.isfinite(v):
            v = 0.0
    except Exception:
        v = 0.0
    fill = min(max(v, 0.0), float(max_value))
    arc = int(round(100.0 * fill / float(max_value)))
    arc = int(np.clip(arc, 0, 100))
    vals = [arc, 100 - arc]
    fig = go.Figure(
        go.Pie(
            values=vals,
            hole=0.72,
            sort=False,
            direction="clockwise",
            marker_colors=[color, "#eef2f9"],
            textinfo="none",
        )
    )
    fig.update_layout(
        width=size,
        height=size,
        showlegend=False,
        margin=dict(t=8, b=8, l=8, r=8),
        annotations=[
            dict(
                x=0.5,
                y=0.56,
                text=f"<b style='font-size:20px'>{center_top}</b>",
                showarrow=False,
            ),
            dict(
                x=0.5,
                y=0.38,
                text=f"<span style='font-size:11px;color:#475569'>{center_bottom}</span>",
                showarrow=False,
            ),
        ],
    )
    return fig


def _render_signature_chips_inline(tags: dict) -> str:
    fixed_order = [
        "PrevDayContext",
        "OpenLocation",
        "FirstCandleType",
        "OpeningTrend",
        "RangeStatus",
    ]
    chips = []
    for label in fixed_order:
        raw = _canon_tag_value(label, tags.get(label))
        col = _tag_color_map(label, raw)
        bg = _rgba(col, 0.12)
        brd = _rgba(col, 0.33)
        disp = _pretty_value(label, raw)
        chips.append(
            f"<div style='padding:6px 10px;border-radius:12px;border:1px solid {brd};"
            f"background:{bg};font-weight:700;font-size:12px;color:{col};'>{disp}</div>"
        )
    return (
        "<div style='display:flex;flex-wrap:wrap;gap:8px'>" + "".join(chips) + "</div>"
    )


def ui_title_with_metrics(
    h: str,
    sub: str,
    *,
    symbol: Optional[str] = None,
    label: Optional[str] = None,
    icon_url: Optional[str] = None,
):
    TZ = _current_theme()
    with st.container(border=True):
        c_left, c_right = st.columns([3, 2])

        with c_left:
            st.markdown(
                f"""
                <div style="display:flex;align-items:flex-start;gap:12px;">
                  <div style="font-size:28px;font-weight:800;
                    background: linear-gradient(135deg,{TZ['primary']},{TZ['primary2']});
                    -webkit-background-clip: text; background-clip: text; color: transparent;">
                    {h}
                  </div>
                </div>
                <div style="color:{TZ['muted']};font-size:13px">{sub}</div>
                """,
                unsafe_allow_html=True,
            )

        with c_right:
            # 3 columns: logo | label/symbol | auto-updating triplet
            w1, w2, w3 = st.columns([0.8, 2.4, 5.2], vertical_alignment="center")

            with w1:
                if icon_url:
                    src = _img_src_maybe_data_uri(icon_url)
                    st.markdown(
                        f'<img src="{src}" style="width:22px;height:22px;border-radius:4px;object-fit:contain;border:1px solid {_current_theme()["border"]}; background:white;"/>',
                        unsafe_allow_html=True,
                    )
                else:
                    abbr = (label or "").strip()[:2].upper() or "—"
                    st.markdown(
                        f'<div style="width:22px;height:22px;border-radius:4px;display:flex;align-items:center;justify-content:center;background:#7C3AED;color:white;font-size:10px;font-weight:700;line-height:1;border:1px solid #e5e7eb">{abbr}</div>',
                        unsafe_allow_html=True,
                    )

            with w2:
                st.markdown(
                    f'<div style="font-weight:700;font-size:12px;color:{_current_theme()["text"]};line-height:1.1;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">{(label or "").upper()}</div>'
                    f'<div style="font-size:11px;color:{_current_theme()["muted"]};line-height:1.1;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">{symbol or ""}</div>',
                    unsafe_allow_html=True,
                )

            with w3:
                # Only this fragment refreshes (and only during market hours)
                render_quote_triplet(symbol or "NSE:TATAMOTORS", key=f"{(label or 'tm').lower()}_triplet_header", align="right")
