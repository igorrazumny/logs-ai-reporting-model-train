# Project: logs-ai-reporting-model-train — File: src/llm/adapter.py
import os
import requests

"""
Strict LLM adapter with exactly two supported backends — no silent defaults.

Required environment variables:

# Select backend explicitly
LLM_PROVIDER                # 'gemini' | 'ollama' (no default; must be set)

# If LLM_PROVIDER=gemini (Gemini 1.5 Pro via Gemini API / Studio)
LLM_MODEL                   # e.g. 'gemini-1.5-pro' (must be set; no fallback)
LLM_API_KEY                 # Gemini API key

# If LLM_PROVIDER=ollama (local dev/fallback)
LLM_HOST                    # e.g. 'http://ollama:11434'
LLM_MODEL                   # e.g. 'llama3:8b'

Notes:
- temperature is fixed at 0 for SQL stability
- timeout is enforced by the caller (default 300s)
- no defaults or fallbacks are applied; missing/invalid envs raise immediately
"""

def _req(name: str) -> str:
    """Fetch a required env var; raise with a clear message if missing/empty."""
    val = os.getenv(name)
    if not val:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return val

def call_llm(system_prompt: str, user_query: str, timeout_s: int = 300) -> str:
    provider = _req("LLM_PROVIDER").lower()

    # ---------------- Gemini 1.5 Pro (Gemini API / Studio) ----------------
    if provider == "gemini":
        model = _req("LLM_MODEL")          # e.g. 'gemini-1.5-pro'
        api_key = _req("LLM_API_KEY")

        try:
            import google.generativeai as genai
        except Exception as e:
            raise RuntimeError(
                "google-generativeai is not installed. "
                "Add to requirements.txt: google-generativeai>=0.7.0"
            ) from e

        genai.configure(api_key=api_key)
        model_obj = genai.GenerativeModel(model_name=model, system_instruction=system_prompt)
        resp = model_obj.generate_content(
            user_query,
            generation_config={"temperature": 0, "max_output_tokens": 1024},
            safety_settings=None,
        )
        return (resp.text or "").strip()

    # ---------------- Local Ollama (dev/fallback) ----------------
    if provider == "ollama":
        host  = _req("LLM_HOST")           # e.g. 'http://ollama:11434'
        model = _req("LLM_MODEL")          # e.g. 'llama3:8b'

        body = {
            "model": model,
            "system": system_prompt,
            "prompt": user_query,
            "stream": False,
            "options": {}
        }
        r = requests.post(f"{host}/api/generate", json=body, timeout=timeout_s)
        r.raise_for_status()
        return (r.json().get("response", "") or "").strip()

    # ---------------- Unsupported provider ----------------
    raise RuntimeError(
        f"Unsupported LLM_PROVIDER='{provider}'. Allowed: 'gemini' or 'ollama'. "
        "Set LLM_PROVIDER and required env vars explicitly."
    )