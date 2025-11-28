# probedge/http/app.py (example)

from fastapi import FastAPI
from probedge.http import api_state   # import the router we just created

app = FastAPI()

app.include_router(api_state.router)
# (later) app.include_router(api_plan.router)
# (later) app.include_router(api_journal.router)
# etc.
