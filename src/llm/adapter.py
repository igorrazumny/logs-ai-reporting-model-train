# Project: logs-ai-reporting-model-train — File: src/llm/adapter.py
import os
import requests

from config.keys import (
    GEMINI_1_5_PRO,
    GEMINI_1_5_FLASH,
    ENV_GEMINI_STUDIO_API_KEY,
)

"""
Strict adapter with exactly two explicit providers. No silent defaults.

Configuration (must be set explicitly):

# Select provider
LLM_PROVIDER                # 'gemini' | 'ollama'  (required)

# If LLM_PROVIDER=gemini (Gemini 1.5 via Google AI Studio API)
GEMINI_STUDIO_API_KEY       # Studio API key (required)
LLM_MODEL                   # 'gemini-1.5-pro' | 'gemini-1.5-flash' (required; no fallback)

# If LLM_PROVIDER=ollama (local dev/fallback)
OLLAMA_HOST                 # e.g. 'http://ollama:11434' (required)
LLM_MODEL                   # e.g. 'llama3:8b', 'mistral:7b' (required)

Notes:
- Temperature fixed at 0 for stable SQL generation.
- The caller enforces a 300s timeout (override via timeout_s if needed).
- Any missing/invalid configuration raises immediately with a clear message.
"""

def _req(name: str) -> str:
    """Fetch a required env var; raise with a clear message if missing/empty."""
    val = os.getenv(name)
    if not val:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return val

def _is_allowed_gemini_id(model_id: str) -> bool:
    mid = (model_id or "").strip().lower()
    return mid in {GEMINI_1_5_PRO, GEMINI_1_5_FLASH}

def call_llm(system_prompt: str, user_query: str, timeout_s: int = 300) -> str:
    provider = _req("LLM_PROVIDER").strip().lower()

    # ---------------- Gemini 1.5 (Google AI Studio API) ----------------
    if provider == "gemini":
        api_key = _req(ENV_GEMINI_STUDIO_API_KEY)
        model_id = _req("LLM_MODEL").strip()
        if not _is_allowed_gemini_id(model_id):
            raise RuntimeError(
                f"Unsupported LLM_MODEL='{model_id}' for Gemini. "
                f"Allowed: '{GEMINI_1_5_PRO}', '{GEMINI_1_5_FLASH}'."
            )

        try:
            import google.generativeai as genai
        except Exception as e:
            raise RuntimeError(
                "google-generativeai is not installed. "
                "Add to requirements.txt: google-generativeai>=0.7.0"
            ) from e

        genai.configure(api_key=api_key)
        model = genai.GenerativeModel(model_name=model_id, system_instruction=system_prompt)
        resp = model.generate_content(
            user_query,
            generation_config={"temperature": 0, "max_output_tokens": 1024},
            safety_settings=None,  # use provider defaults
        )
        return (resp.text or "").strip()

    # ---------------- Ollama (local dev / fallback) ----------------
    if provider == "ollama":
        host  = _req("OLLAMA_HOST")          # e.g. 'http://ollama:11434'
        model = _req("LLM_MODEL")            # e.g. 'llama3:8b', 'mistral:7b'

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