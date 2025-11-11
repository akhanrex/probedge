# Probedge Alignment Patch (Nov 11, 2025)

This bundle fills the gaps to align API + Live stack with the single-UI plan.

## What’s included

- `apps/api/routes/tm5.py`: JSON-safe `/api/tm5` with `limit`.
- `apps/api/routes/matches.py`: robust L3→L0 matcher on OT/OL/PDC.
- `probedge/realtime/kite_live.py`: real KiteTicker adapter (NSE tokens).
- `probedge/orders/idempotency.py`: safe client order IDs.
- `probedge/orders/broker_kite.py`: LIMIT-only OMS primitives.
- `ops/kite_auth_local.py`: local helper to write `KITE_ACCESS_TOKEN` into `.env`.
- `config/frequency.yaml`: symbols + path conventions (authoritative).
- `.env.example`: environment template.
- `tests/smoke_tests.py`: quick endpoint verification.

## Apply

From repo root (where `apps/` and `probedge/` live):

```bash
unzip -o probedge_alignment_patch_2025-11-11.zip -d .
# Ensure routers are mounted in apps/api/main.py:
#   from apps.api.routes import tm5 as tm5_route
#   from apps.api.routes import matches as matches_route
#   app.include_router(tm5_route.router)
#   app.include_router(matches_route.router)
```
