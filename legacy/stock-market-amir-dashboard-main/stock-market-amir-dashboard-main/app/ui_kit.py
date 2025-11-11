# ui_kit.py — one-file responsive helper for Streamlit + Plotly
import streamlit as st

# (Optional) viewport probe; falls back gracefully if package missing
try:
    from streamlit_js_eval import get_page_details
except Exception:
    get_page_details = None


def _viewport_width() -> int:
    # cache last seen width to avoid blocking
    w = int(st.session_state.get("_vw", 1200))
    if get_page_details is not None:
        try:
            info = get_page_details()
            if isinstance(info, dict) and "width" in info:
                w = int(info["width"])
                st.session_state["_vw"] = w
        except Exception:
            pass
    return w


def breakpoint() -> str:
    """'mobile' (<=640px) | 'tablet' (<=1024px) | 'desktop' (>1024px)"""
    w = _viewport_width()
    if w <= 640:
        return "mobile"
    if w <= 1024:
        return "tablet"
    return "desktop"


# ---- Sizes you can reuse anywhere ----
def donut_size() -> int:
    bp = breakpoint()
    return 130 if bp == "mobile" else (150 if bp == "tablet" else 160)


def thumb_height() -> int:
    bp = breakpoint()
    return 180 if bp == "mobile" else (200 if bp == "tablet" else 220)


def big_height() -> int:
    bp = breakpoint()
    return 360 if bp == "mobile" else (460 if bp == "tablet" else 520)


def cols(n_desktop=3, n_tablet=2, n_mobile=1):
    bp = breakpoint()
    n = n_mobile if bp == "mobile" else (n_tablet if bp == "tablet" else n_desktop)
    return st.columns(n)


# ---- One-time CSS + minor global tweaks ----
def init_responsive_css() -> None:
    st.markdown(
        """
    <style>
      /* Full-width buttons/inputs on phones */
      @media (max-width: 640px){
        button, .stButton > button { width: 100% !important; }
        .stDownloadButton > button { width: 100% !important; }
      }
      /* Tighter gutters on phones */
      @media (max-width: 640px){
        div[data-testid="column"] { padding-left: 6px !important; padding-right: 6px !important; }
        section[data-testid="stSidebar"] { width: 80vw !important; min-width: 280px !important; }
      }
      /* Slightly smaller headings/chips on phones */
      @media (max-width: 640px){
        h1, h2, h3 { font-size: 1.05rem !important; }
        .pe-chip { font-size: 11px !important; padding: 4px 8px !important; }
      }
      /* Reduce plot margins on phones */
      @media (max-width: 640px){
        .stPlotlyChart, .stPlot { margin-top: 4px !important; margin-bottom: 4px !important; }
      }
    </style>
    """,
        unsafe_allow_html=True,
    )
