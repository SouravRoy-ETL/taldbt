"""
Quick test: verify ALL cloud LLM providers individually.
Run: python -m taldbt.tests.test_llm_provider
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from taldbt.llm.llm_provider import get_chain, get_active_provider, _call_single, check_provider_status

TEST_PROMPT = """Convert this Talend Java expression to DuckDB SQL. Return ONLY the SQL expression, nothing else. No markdown, no explanation, no thinking.

Java: TalendDate.formatDate("yyyy-MM-dd", row1.StartDate)

SQL:"""

TEST_HARD = """Convert this Talend Java expression to DuckDB SQL. Return ONLY the SQL expression, nothing else. No markdown, no thinking.

Java: row1.Status != null ? row1.Status.toUpperCase() : "UNKNOWN"

SQL:"""

# SQL quality checks
SQL_KEYWORDS = ('SELECT', 'CASE', 'WHEN', 'UPPER', 'LOWER', 'STRFTIME', 'STRPTIME',
                'CAST', 'COALESCE', 'NULL', 'TRIM', 'SUBSTRING', 'IS NOT NULL', 'IS NULL')

def validate_sql(response: str, test_name: str) -> tuple[bool, str]:
    """Check if response is valid SQL, not thinking blocks or garbage."""
    if response.startswith("-- ERROR:"):
        return False, response[10:60]
    if not response or len(response.strip()) < 3:
        return False, "empty response"
    if "<think>" in response:
        return False, f"raw <think> tags leaked: {response[:40]}..."
    if response.startswith("{") or response.startswith("["):
        return False, f"got JSON instead of SQL: {response[:40]}..."
    # Check for at least one SQL-like token
    upper = response.upper()
    has_sql = any(kw in upper for kw in SQL_KEYWORDS) or "(" in response
    if not has_sql:
        return False, f"no SQL keywords found: {response[:50]}..."
    return True, response.split("\n")[0].strip()[:70]


print("=" * 60)
print("taldbt LLM Provider Test — CLOUD ONLY")
print("=" * 60)

chain = get_chain()
cloud_chain = [(n, c) for n, c in chain if n != "ollama"]

if not cloud_chain:
    print("\n❌ NO CLOUD PROVIDERS — set keys in .streamlit/secrets.toml")
    sys.exit(1)

print(f"\n🔗 {len(chain)} total, testing {len(cloud_chain)} cloud providers\n")

results = {}

for i, (name, cfg) in enumerate(cloud_chain):
    print(f"{'─' * 55}")
    print(f"[{i+1}/{len(cloud_chain)}] {cfg.name}")
    print(f"  Model:    {cfg.default_model}")
    print(f"  Endpoint: {cfg.base_url}")
    key_display = f"✅ {cfg.api_key[:8]}..." if cfg.api_key else "❌ missing"
    print(f"  Key:      {key_display}")

    passed = 0

    # Test 1
    print(f"  Test 1 (date format):", end=" ", flush=True)
    r1 = _call_single(TEST_PROMPT, "", 0.1, 200, cfg)
    ok1, msg1 = validate_sql(r1, "date_format")
    print(f"{'✅' if ok1 else '❌'} {msg1}")
    if ok1: passed += 1

    # Test 2
    print(f"  Test 2 (ternary):   ", end=" ", flush=True)
    r2 = _call_single(TEST_HARD, "", 0.1, 200, cfg)
    ok2, msg2 = validate_sql(r2, "ternary")
    print(f"{'✅' if ok2 else '❌'} {msg2}")
    if ok2: passed += 1

    results[name] = "PASS" if passed == 2 else "PARTIAL" if passed == 1 else "FAIL"

print(f"\n{'─' * 55}")
print(f"\n📊 Results:\n")
for name, status in results.items():
    icon = "✅" if status == "PASS" else "⚠️" if status == "PARTIAL" else "❌"
    cfg = next(c for n, c in cloud_chain if n == name)
    print(f"  {icon} {cfg.name:25s} {status}")

print(f"\n{'=' * 60}")
active = get_active_provider()
print(f"⚡ Active: {active.name} — {active.default_model}")
working = sum(1 for s in results.values() if s in ("PASS", "PARTIAL"))
total = len(cloud_chain)
if working == total:
    print(f"✅ All {total} cloud providers working — fallback chain ready")
else:
    print(f"⚠️  {working}/{total} cloud providers working")
print(f"{'=' * 60}")
