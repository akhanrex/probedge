# ProbEdge Development Handbook — v1.0.0 (FROZEN)

**Status:** ✅ Frozen for migration-only release (no new features).  
**Scope:** Separate engine from UI, introduce stable API, private access. Preserve outputs parity with the Streamlit app.

---

## 0) Philosophy & Non-Negotiables

- **Engine is the plan; UI is the skin.** Logic must be framework-free and testable.
- **Single source of truth:** `probedge/core` hosts tags → result → probabilities → decision.
- **Risk discipline is code, not willpower:** ₹10k daily loss guard, formula sizing, no averaging.
- **Parsimony:** Add layers only to decouple concerns (Core ⟂ API ⟂ UI ⟂ OMS).
- **Auditability:** Every transformation is deterministic and reproducible.

---

## 1) System Architecture (Layered)

### 1.1 Layers

- **Core (Pure Python):**
  - `probedge/core/engine.py` — canonicalize tags, result (two-leg), probabilities (binary + weighted), A+ gate, decision.
  - `probedge/core/classifiers.py` — PrevDayContext, GapType, OpenLocation, FirstCandleType, OpeningTrend, RangeStatus.
  - `probedge/core/result.py` — first-leg (open→mid), second-leg (mid→15:05), TR band (±20% PD range).
  - `probedge/core/sizing.py` — `qty = floor(10000 / |entry−stop|)`
  - `probedge/core/stats.py` — quality, sample depth, completeness, Wilson-like bounds (if used).
  - `probedge/core/rules.py` — gates, penalties, bonuses.

- **Data & Journal:**
  - `probedge/data/io.py` — CSV/Excel/Parquet I/O, atomic saves, caching.
  - `probedge/data/config.py` — YAML/.env → typed config objects.
  - `probedge/data/fees.py` — brokerage & taxes from `config/journal_fees.yaml`.
  - `probedge/journal/ingest.py` — Zerodha P&L/tradebook normalize.
  - `probedge/journal/merge.py` — join with Master; compute `day_R_net`, KPIs.

- **Updater:**
  - `probedge/updater/weekly.py` — `update_master_if_needed`, `compute_live_weekly_tags`.

- **API (Thin, Typed):**
  - `probedge/api/main.py` (FastAPI + uvicorn):
    - `/health`, `/auth/login`
    - `/engine/score`, `/masters/{instrument}`, `/journal/summary`, `/updater/run`

- **UI (Swappable):**
  - `frontend/` (Next.js + shadcn + ECharts/Plotly) — Terminal, Live, Journal, Settings.
  - `ui_adapters/streamlit.py` (temporary) — parity harness to old UI.

- **OMS (Order Management Service) — Deferred to v2.0.0 (NOT in v1.0.0):**
  - `probedge/oms/` (separate process) for auto-execution with guards and kill-switches.

### 1.2 Directory Map

```
probedge/
  core/
    __init__.py
    engine.py
    classifiers.py
    result.py
    sizing.py
    stats.py
    rules.py
  data/
    __init__.py
    io.py
    config.py
    fees.py
  journal/
    __init__.py
    ingest.py
    merge.py
  updater/
    __init__.py
    weekly.py
  api/
    __init__.py
    main.py
  cli/
    __main__.py
  ui_adapters/
    streamlit.py
frontend/           # Next.js app
config/
  journal_config.yaml
  journal_fees.yaml
data/
  masters/
  latest/
  journal/
tests/
  core/
  journal/
  api/
docker/
  docker-compose.yml
  caddy/Caddyfile
  .env.example
```

### 1.3 Data Stores

- **DuckDB + Parquet** for analytics (Masters/Journal) — fast local queries.
- **CSV** only for exports/interchange.
- **Redis** (optional) for caching/live pubsub.
- **PostgreSQL** (optional) for transactional logs (not required for v1.0.0).

---

## 2) API Contract (v1.0.0)

- `GET /health` → `{status, version}`
- `POST /auth/login` → sets HTTP-only session cookie (Argon2 hashed password, optional TOTP later)
- `POST /engine/score` → body: `{date, instrument}` → `{tags, result, probs, quality, depth, aplus, decision, diagnostics}`
- `GET /masters/{instrument}?from=YYYY-MM-DD&to=YYYY-MM-DD` → rows for charts
- `POST /journal/summary` → body: `{from?, to?, instrument?}` → `{kpis, calendar, table}`
- `POST /updater/run` → `{since:"auto"|YYYY-MM-DD, instrument}` → `{added, last_date}`

**Notes**  
- All responses are Pydantic-typed; dates are ISO-8601; numbers are floats/ints; strings uppercase for tags.

---

## 3) UI Pages (Parity with Streamlit)

- **Login:** password gate; on success → session cookie; redirect to Terminal.
- **Terminal:** 5 donuts (direction %, sample depth %, quality %, A+ gate, final ENTER/ABSTAIN). Tag chips; instrument switcher.
- **Live:** LTP snapshot/mini-history; same donuts with live tags snapshot.
- **Journal:** KPIs (Net R/₹, Win rate, PF, Sharpe, Sortino, MaxDD, streaks, %Green(20)), calendar heatmap, tables, CSV exports.
- **Settings:** data paths, passwords, theme (defer cosmetic edits to ≥v1.1.x).

---

## 4) Security & Access

- **Private access (pick one):**
  - **Tailscale (free)** — mesh VPN to your mini-PC; app not publicly exposed.
  - **Cloudflare Tunnel + Access (free)** — mTLS tunnel + OTP wall on a private subdomain.
- **App Auth:** Argon2 hashed password; session cookies (HttpOnly, SameSite=Strict).  
- **Secrets:** `.env` (local), or 1Password/Vault later.

---

## 5) Operational Playbooks

### 5.1 Local Dev
```bash
# backend
uvicorn probedge.api.main:app --host 127.0.0.1 --port 8787 --reload
# frontend
cd frontend && npm run dev  # open http://localhost:3000
```

### 5.2 Docker Compose (single-node)
```bash
docker compose up -d  # frontend, api, redis, caddy
```

### 5.3 Daily Start
- Start API/UI (Docker or dev servers).  
- Verify `/health` returns `ok`.  
- Open Terminal page; validate donuts vs yesterday (sanity).

### 5.4 Backups
- On every write to `data/masters/*.csv`, create copy under `data/backups/YYYYMMDD/`.  
- Keep last 30 backups. Rotate weekly.

### 5.5 Logging
- Structured JSON logs to `logs/probedge.log` (rotating, 10MB × 10 files).  
- Error notifications (optional) via Telegram webhook.

---

## 6) Migration SOP (v1.0.0)

### Phase 0 — Lock Rules (Day 0)
- Select 10–20 **golden dates** from `TataMotors_Master.csv` with known outcomes.
- Create `tests/core/test_engine_golden.py` asserting:
  - Canonical tags, `Result`, probabilities, A+ gate, final decision match **old Streamlit**.
- Freeze the CSV slice used for tests under `tests/data/master_sample.csv`.

**Deliverable:** Golden tests passing on old code.

### Phase 1 — Extract Core (Days 1–2)
- Move logic into `probedge/core`, `probedge/journal`, `probedge/data`, `probedge/updater`.
- Remove Streamlit imports from these modules.  
- Keep interfaces pure (function inputs/outputs typed).

**Exit criteria:** Golden tests pass using new core modules.

### Phase 2 — API Layer (Days 3–4)
- Implement endpoints listed in §2 using FastAPI.  
- Add `/auth/login` (password from env or config).  
- Add simple **rate limit** on login attempts.

**Exit criteria:** Terminal & Journal data fetchable via API.

### Phase 3 — UI Wrapper (Days 5–7)
- Build Next.js pages that call the API.  
- Replicate 5 donuts + Journal KPIs/tables.  
- Add login screen; store session in HttpOnly cookie.

**Exit criteria:** New UI matches the Streamlit visuals/values 1:1.

### Phase 4 — Deployment & Access (Days 8–9)
- Add `docker/docker-compose.yml` for `frontend`, `api`, `redis`, `caddy`.  
- Configure **Tailscale** or **Cloudflare Tunnel** (choose one).  
- Smoke test from external device on mobile network.

**Exit criteria:** App reachable privately, password-gated.

### Phase 5 — Freeze v1.0.0 (Day 10)
- Run full test suite; verify donuts & Journal parity.  
- Tag `v1.0.0`. Update README.  
- Create `CHANGELOG.md` (Added/Changed/Removed).

**Out of scope:** OMS/auto-trading (planned for v2.0.0).

---

## 7) Flowcharts (ASCII-safe)

### 7.1 High-Level Flow
```
[Browser UI] --> [API (FastAPI)] --> [Core Engine]
     |               |                   |
     |               |--> [Journal/Updater/Data]
     |               |--> [DuckDB/Parquet (read)]
     |               '--> [CSV exports]
     '----------------------------------------> [Rendered charts/KPIs]
```

### 7.2 Terminal Request Path
```
UI: /terminal page
  -> API: GET /masters/TATAMOTORS [optional window]
  -> API: POST /engine/score {date, instrument}
      -> Core: canonicalize tags -> result (two-leg)
               -> probabilities -> quality/depth -> A+ gate -> decision
      <- API: {probs, aplus, decision, diagnostics}
  -> UI: render 5 donuts + chips + decision
```

### 7.3 Journal Flow
```
UI: /journal page
  -> API: POST /journal/summary {from?, to?}
      -> Journal: ingest P&L/tradebook -> session filter (09:15–15:05)
                  -> merge Master -> compute day_R_net, KPIs
      <- API: {kpis, calendar, table}
  -> UI: render KPIs, heatmap, tables (+ download CSV)
```

---

## 8) Testing Strategy

- **Golden tests** (parity) — ensure new engine == old Streamlit outputs.
- **Unit tests** — classifiers, result two-leg, probability combiner, A+ gate.
- **API contract** — response models, error codes, auth flow.
- **Smoke tests** — terminal & journal endpoints return within <300ms on local machine.

**Commands**
```bash
pytest -q
ruff check . && ruff format --check .
mypy probedge
```

---

## 9) Versioning & Change Control

- **v1.0.0** — migration complete, parity achieved (FROZEN).
- **v1.1.x** — UI/UX only; no core logic changes.
- **v2.0.0** — OMS/auto-execution, risk guards in code, broker wiring.

**Rule:** No feature branches merged to `main` without passing golden tests.

---

## 10) Kanban Checklist (print this)

**Phase 0**
- [ ] Pick 10–20 golden dates
- [ ] Freeze sample CSV in `tests/data/`
- [ ] Implement golden tests

**Phase 1**
- [ ] Create `probedge/core/*`
- [ ] Move classifiers/result/engine
- [ ] Move journal/data/updater
- [ ] All imports UI-free
- [ ] Golden tests pass

**Phase 2**
- [ ] Implement FastAPI endpoints
- [ ] Add `/auth/login` and session
- [ ] Contract tests pass

**Phase 3**
- [ ] Next.js pages: Login, Terminal, Journal
- [ ] Render donuts & KPIs
- [ ] UI parity verified

**Phase 4**
- [ ] Docker Compose up
- [ ] Tailscale/Cloudflare configured
- [ ] External smoke test

**Phase 5**
- [ ] Tag v1.0.0
- [ ] Update README & CHANGELOG
- [ ] Archive Streamlit as `ui_adapters/streamlit.py`

---

## 11) Freeze Statement

> **ProbEdge v1.0.0 is hereby frozen for migration only.**  
> No new features (OMS, order placement, indicators) will be added until v1.0.0 is released. All changes must preserve engine outputs parity verified by golden tests.

---

## 12) Contacts & Ownership (Single User)

- **Owner:** Aamir (single-user system)
- **Repository:** Private GitHub (main branch protected; tests required)
- **Secrets:** Local `.env` (not committed)

---

*End of Handbook — v1.0.0*
