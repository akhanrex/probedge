import streamlit as st


# --- Theme ---
def get_theme():
    return {
        "bg": "#0b1220",
        "fg": "#e5e7eb",
    }


def init_responsive_css():
    css = """
    <style>
      /* simple responsive grid helpers */
      .block-container { padding-top: 1.2rem; padding-bottom: 2rem; }
      @media (max-width: 900px) {
        .block-container { padding-left: 0.8rem; padding-right: 0.8rem; }
      }
    </style>
    """
    st.markdown(css, unsafe_allow_html=True)


# --- Layout helpers used by run.py ---
def cols(desktop: int = 3, tablet: int = 2, mobile: int = 1):
    """
    Return a list of Streamlit columns tuned for desktop.
    (We don't dynamically switch counts in Streamlit, but we keep the signature.)
    """
    n = max(1, int(desktop))
    weights = [1] * n
    return st.columns(weights)


# --- Size helpers used by run.py ---
def donut_size() -> int:
    return 180  # px


def thumb_height() -> int:
    return 220  # px for small chart tiles


def big_height() -> int:
    return 480  # px for expanded chart
