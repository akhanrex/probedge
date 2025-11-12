from probedge.infra.settings import SETTINGS
from probedge.storage.resolver import intraday_path, master_path, journal_path, state_path, locate_for_read

print("MODE:", SETTINGS.mode)
print("RISK_BUDGET_RS:", SETTINGS.risk_budget_rs)
print("SYMBOLS:", SETTINGS.symbols)
print("CANONICAL PATHS:")
print("  journal:", journal_path())
print("  state  :", state_path())
for s in SETTINGS.symbols:
    print(f"  {s} tm5:", intraday_path(s))
    print(f"  {s} master:", master_path(s))

print("\nCHECK EXISTING READABLE PATHS (falling back to legacy if needed):")
for s in SETTINGS.symbols:
    print(f"  {s} tm5 (read):", locate_for_read("intraday", s))
    print(f"  {s} master(read):", locate_for_read("masters", s))
