"""
LLM Provider: unified interface with auto-detection and intelligent fallback.

Priority chain (auto-detected):
  1. Local Ollama (if running) — zero latency, any model
  2. Cerebras (if key set) — Qwen3 235B, 1400 tok/s, 1M free/day
  3. Groq (if key set) — Qwen3 32B, 535 tok/s, 1K req/day free
  4. OpenRouter (if key set) — any model, community-funded free tier
  5. Together.ai (if key set) — Qwen3 Coder, signup credit only

Each provider is health-checked at startup. If the active provider fails
mid-request (429 rate limit, 503 outage), it automatically retries on
the next healthy provider in the chain.

Configuration:
  Automatic: Just set API keys — the system figures out the rest.
    - CEREBRAS_API_KEY, GROQ_API_KEY, etc. in env or st.secrets
  Explicit: Force a specific provider via st.secrets:
    [llm]
    provider = "cerebras"
    api_key = "csk-..."
    model = "qwen-3-235b-a22b-instruct-2507"  # optional
"""
from __future__ import annotations
import os
import time
import requests
from typing import Optional
from dataclasses import dataclass, field


# ═══════════════════════════════════════════════════════════
# Provider Registry
# ═══════════════════════════════════════════════════════════

@dataclass
class ProviderConfig:
    name: str
    base_url: str
    default_model: str
    api_key: str = ""
    max_tokens: int = 8192
    is_coder: bool = False      # True if the default model is code-specialized


# All known providers. Order = fallback priority.
PROVIDERS: dict[str, ProviderConfig] = {
    "ollama": ProviderConfig(
        name="Ollama (local)",
        base_url="http://localhost:11434/v1",
        default_model="qwen3-coder:30b",
        api_key="ollama",
        is_coder=True,
    ),
    "cerebras": ProviderConfig(
        name="Cerebras",
        base_url="https://api.cerebras.ai/v1",
        default_model="qwen-3-235b-a22b-instruct-2507",
        max_tokens=8192,
    ),
    "groq": ProviderConfig(
        name="Groq",
        base_url="https://api.groq.com/openai/v1",
        default_model="qwen/qwen3-32b",
        max_tokens=4096,
    ),
    "openrouter": ProviderConfig(
        name="OpenRouter",
        base_url="https://openrouter.ai/api/v1",
        default_model="qwen/qwen3-32b:free",
    ),
    "together": ProviderConfig(
        name="Together.ai",
        base_url="https://api.together.xyz/v1",
        default_model="Qwen/Qwen3-Coder-30B-Instruct",
        is_coder=True,
    ),
}

# Cerebras model options — pick the best available
CEREBRAS_MODELS = {
    "coder": "qwen3-coder-480b",                    # paid only
    "frontier": "qwen-3-235b-a22b-instruct-2507",   # free, best quality
    "fast": "qwen-3-32b",                            # free, fastest
}


# ═══════════════════════════════════════════════════════════
# Secrets / Config reader
# ═══════════════════════════════════════════════════════════

def _get_secret(key: str, default: str = "") -> str:
    """Read from Streamlit secrets → env vars → default."""
    try:
        import streamlit as st
        val = st.secrets.get("llm", {}).get(key, "")
        if val: return str(val)
    except Exception:
        pass
    val = os.environ.get(f"LLM_{key.upper()}", "")
    if val: return val
    # Also check provider-specific env keys (CEREBRAS_API_KEY, GROQ_API_KEY)
    if key == "api_key":
        for prefix in ["CEREBRAS", "GROQ", "OPENROUTER", "TOGETHER"]:
            val = os.environ.get(f"{prefix}_API_KEY", "")
            if val: return val
    return default


def _get_provider_key(provider_name: str) -> str:
    """Get API key for a specific provider from env or secrets."""
    # Check provider-specific env var first
    env_key = f"{provider_name.upper()}_API_KEY"
    val = os.environ.get(env_key, "")
    if val: return val
    # Check st.secrets
    try:
        import streamlit as st
        s = st.secrets.get("llm", {})
        if s.get("provider", "").lower() == provider_name:
            return str(s.get("api_key", ""))
        # Also check provider-specific sections: [cerebras] api_key = ...
        ps = st.secrets.get(provider_name, {})
        if ps.get("api_key"): return str(ps["api_key"])
    except Exception:
        pass
    return ""


# ═══════════════════════════════════════════════════════════
# Health checking
# ═══════════════════════════════════════════════════════════

_health_cache: dict[str, tuple[bool, float]] = {}  # provider -> (healthy, timestamp)
_HEALTH_TTL = 300  # re-check every 5 minutes


def _check_health(provider_name: str, cfg: ProviderConfig) -> bool:
    """Probe a provider to see if it's reachable. Cached for 5 min."""
    now = time.time()
    cached = _health_cache.get(provider_name)
    if cached and (now - cached[1]) < _HEALTH_TTL:
        return cached[0]

    healthy = False

    if provider_name == "ollama":
        try:
            r = requests.get("http://localhost:11434/api/tags", timeout=2)
            healthy = r.status_code == 200 and len(r.json().get("models", [])) > 0
        except Exception:
            healthy = False
    else:
        if not cfg.api_key:
            healthy = False
        else:
            try:
                headers = {"Authorization": f"Bearer {cfg.api_key}"}
                r = requests.get(f"{cfg.base_url.rstrip('/')}/models", headers=headers, timeout=5)
                healthy = r.status_code == 200
            except Exception:
                healthy = False

    _health_cache[provider_name] = (healthy, now)
    return healthy


def invalidate_health(provider_name: str):
    """Force re-check on next call (after a failure)."""
    _health_cache.pop(provider_name, None)


# ═══════════════════════════════════════════════════════════
# Auto-detect: build ordered chain of available providers
# ═══════════════════════════════════════════════════════════

_active_chain: list[tuple[str, ProviderConfig]] | None = None


def _build_chain() -> list[tuple[str, ProviderConfig]]:
    """Build an ordered list of (name, config) for all providers that have keys.
    Checks explicit config first, then auto-discovers from env/secrets."""
    global _active_chain

    chain = []

    # 1. Check explicit provider config
    explicit = _get_secret("provider", "").lower()
    explicit_key = _get_secret("api_key", "")
    explicit_model = _get_secret("model", "")

    # 2. Always try Ollama first (local, no key needed)
    ollama_cfg = ProviderConfig(**{k: v for k, v in PROVIDERS["ollama"].__dict__.items()})
    try:
        r = requests.get("http://localhost:11434/api/tags", timeout=2)
        if r.status_code == 200:
            models = r.json().get("models", [])
            if models:
                # Prefer coder model if available
                coder = next((m["name"] for m in models if "coder" in m["name"].lower()), None)
                ollama_cfg.default_model = coder or models[0]["name"]
                ollama_cfg.is_coder = coder is not None
                chain.append(("ollama", ollama_cfg))
    except Exception:
        pass

    # 3. Add explicitly configured provider
    if explicit and explicit != "local" and explicit != "ollama":
        if explicit == "custom":
            base_url = _get_secret("base_url", "")
            if base_url and explicit_key:
                chain.append(("custom", ProviderConfig(
                    name="Custom", base_url=base_url,
                    default_model=explicit_model or "gpt-4o-mini",
                    api_key=explicit_key,
                )))
        elif explicit in PROVIDERS:
            cfg = ProviderConfig(**{k: v for k, v in PROVIDERS[explicit].__dict__.items()})
            cfg.api_key = explicit_key or cfg.api_key
            if explicit_model: cfg.default_model = explicit_model
            chain.append((explicit, cfg))

    # 4. Auto-discover other providers with keys set
    for pname in ["cerebras", "groq", "openrouter", "together"]:
        if any(n == pname for n, _ in chain):
            continue  # already in chain
        key = _get_provider_key(pname)
        if key:
            cfg = ProviderConfig(**{k: v for k, v in PROVIDERS[pname].__dict__.items()})
            cfg.api_key = key
            chain.append((pname, cfg))

    _active_chain = chain
    return chain


def get_chain() -> list[tuple[str, ProviderConfig]]:
    """Get the provider chain, building it on first call."""
    global _active_chain
    if _active_chain is None:
        _build_chain()
    return _active_chain or []


def get_active_provider() -> ProviderConfig:
    """Get the first healthy provider from the chain."""
    chain = get_chain()

    for name, cfg in chain:
        if _check_health(name, cfg):
            return cfg

    # Nothing healthy — return first configured or Ollama default
    if chain:
        return chain[0][1]
    return PROVIDERS["ollama"]


# ═══════════════════════════════════════════════════════════
# Unified LLM call — with automatic fallback
# ═══════════════════════════════════════════════════════════

def llm_complete(
    prompt: str,
    system: str = "",
    temperature: float = 0.1,
    max_tokens: int = 4096,
    provider: Optional[ProviderConfig] = None,
) -> str:
    """Send a prompt to the LLM. Auto-falls back on failure.

    If the primary provider returns 429/503/connection error,
    automatically tries the next provider in the chain.
    """
    chain = get_chain()
    if provider:
        # Explicit provider — try it, then fall back
        providers_to_try = [(provider.name, provider)] + [(n, c) for n, c in chain if c != provider]
    else:
        providers_to_try = chain if chain else [("ollama", PROVIDERS["ollama"])]

    last_error = ""

    for pname, pcfg in providers_to_try:
        result = _call_single(prompt, system, temperature, max_tokens, pcfg)

        if not result.startswith("-- ERROR:"):
            return result

        # Retryable errors → try next provider
        last_error = result
        invalidate_health(pname)

        if "Rate limited" in result or "Cannot reach" in result or "503" in result:
            continue  # try next
        if "Invalid API key" in result:
            continue  # skip broken provider
        # Non-retryable error → still try next but log it
        continue

    return last_error  # all providers failed


def _clean_response(raw: str) -> str:
    """Strip <think> blocks, markdown fences, and whitespace from LLM output."""
    import re
    # Remove <think>...</think> reasoning blocks (Qwen3, DeepSeek R1)
    cleaned = re.sub(r'<think>.*?</think>', '', raw, flags=re.DOTALL)
    # Remove markdown code fences
    cleaned = re.sub(r'```(?:sql)?\s*', '', cleaned)
    cleaned = re.sub(r'```\s*', '', cleaned)
    cleaned = cleaned.strip()
    return cleaned if cleaned else raw.strip()


def _call_single(
    prompt: str, system: str, temperature: float,
    max_tokens: int, provider: ProviderConfig,
) -> str:
    """Single call to one provider."""
    messages = []
    if system:
        messages.append({"role": "system", "content": system})
    messages.append({"role": "user", "content": prompt})

    headers = {"Content-Type": "application/json"}
    if provider.api_key and provider.api_key != "ollama":
        headers["Authorization"] = f"Bearer {provider.api_key}"

    body = {
        "model": provider.default_model,
        "messages": messages,
        "temperature": temperature,
        "max_tokens": min(max_tokens, provider.max_tokens),
    }

    url = f"{provider.base_url.rstrip('/')}/chat/completions"

    try:
        resp = requests.post(url, json=body, headers=headers, timeout=300)
        resp.raise_for_status()
        data = resp.json()

        choices = data.get("choices", [])
        if choices:
            raw = choices[0].get("message", {}).get("content", "").strip()
            return _clean_response(raw)
        raw = data.get("content", data.get("response", "")).strip()
        return _clean_response(raw)

    except requests.exceptions.ConnectionError:
        return f"-- ERROR: Cannot reach {provider.name} at {provider.base_url}"
    except requests.exceptions.HTTPError as e:
        status = e.response.status_code if e.response else 0
        body_text = e.response.text[:200] if e.response else ""
        if status == 429:
            return f"-- ERROR: Rate limited by {provider.name}. Falling back."
        if status == 401:
            return f"-- ERROR: Invalid API key for {provider.name}"
        return f"-- ERROR: {provider.name} returned {status}: {body_text}"
    except Exception as e:
        return f"-- ERROR: {str(e)[:200]}"


# ═══════════════════════════════════════════════════════════
# Status — for UI display
# ═══════════════════════════════════════════════════════════

def check_provider_status() -> dict:
    """Report active provider + full chain health for UI."""
    chain = get_chain()
    active = get_active_provider()
    is_local = "localhost" in active.base_url or "127.0.0.1" in active.base_url

    # Build chain status for display
    chain_info = []
    for name, cfg in chain:
        healthy = _check_health(name, cfg)
        chain_info.append({"name": name, "display": cfg.name, "model": cfg.default_model,
                           "healthy": healthy, "is_coder": cfg.is_coder})

    if is_local:
        try:
            r = requests.get("http://localhost:11434/api/tags", timeout=3)
            if r.status_code == 200:
                models = [m["name"] for m in r.json().get("models", [])]
                return {
                    "running": True, "models": models,
                    "has_target_model": bool(models),
                    "target_model": active.default_model,
                    "provider_name": active.name, "is_cloud": False,
                    "chain": chain_info,
                }
        except Exception:
            pass
        # Ollama not running but cloud providers in chain
        if len(chain) > 1:
            cloud = next((c for n, c in chain if n != "ollama"), None)
            if cloud:
                return {
                    "running": True, "models": [cloud.default_model],
                    "has_target_model": True,
                    "target_model": cloud.default_model,
                    "provider_name": cloud.name, "is_cloud": True,
                    "chain": chain_info,
                }
        return {
            "running": False, "models": [], "has_target_model": False,
            "target_model": active.default_model,
            "provider_name": active.name, "is_cloud": False,
            "chain": chain_info,
        }
    else:
        return {
            "running": bool(active.api_key),
            "models": [active.default_model],
            "has_target_model": bool(active.api_key),
            "target_model": active.default_model,
            "provider_name": active.name, "is_cloud": True,
            "chain": chain_info,
        }


def rebuild_chain():
    """Force rebuild of provider chain (after config change)."""
    global _active_chain
    _active_chain = None
    _health_cache.clear()
