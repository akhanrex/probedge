# Probedge Monorepo (Scaffold)
- FastAPI backend in `apps/api`
- Core library in `probedge/`
- Config in `config/`
- Data area at `data/`
- Legacy repos in `legacy/`
## Setup
```
cd probedge
python -m venv .venv && source .venv/bin/activate
pip install -e .
cp .env.example .env
uvicorn apps.api.main:app --reload
```
