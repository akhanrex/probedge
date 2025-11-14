from datetime import time as dtime

# Session timeline (IST)
T0 = dtime(9, 40)   # entry window start
T1 = dtime(15, 5)   # session end
SESSION_START = dtime(9, 15)
ORB_END = dtime(9, 35)

# Numeric thresholds (from Colab)
T_POST = 0.60
NEAR_ZERO_BAND = 0.20

GAP_THRESH_PP = 10.0
MIN3, MIN2, MIN1, MIN0 = 8, 6, 4, 3
TIF_WEAK = 0.70
ABSTAIN_BELOW = 0

CONF_WEIGHTS = {"freq": 0.50, "strength": 0.10, "reach": 0.30, "persist": 0.10}
CONF_TEMP = 1.6
CONF_QUALITY_FLOOR = 0.60

USE_OPENING_TREND = True
USE_PREV_DAY_CONTEXT = False

CLOSE_PCT = 0.0025
CLOSE_FR_ORB = 0.20

LOOKBACK_YEARS = 6
EDGE_PP = 8.0
CONF_FLOOR = 55
REQUIRE_OT_ALIGN = True
