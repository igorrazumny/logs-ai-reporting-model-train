# File: src/logs_train/llm_client.py
import os
import json
import time
import requests
from typing import Optional, Tuple

from logs_train.prompts import PKM_SINGLE_JSON_PROMPT as _SYS_PROMPT


def _get_env_or_fail(key: str) -> str:
    val = os.getenv(key)
    if not val:
        raise RuntimeError(f"[env] missing required environment variable: {key}")
    return val


# Strict: no silent fallbacks
OLLAMA_HOST   = _get_env_or_fail("LOG_LLM_HOST")
LLM_MODEL     = _get_env_or_fail("LOG_LLM_MODEL")
LLM_TIMEOUT_S = int(os.getenv("LLM_TIMEOUT_S", "60"))  # this one can be optional


def _truncate(s: str, n: int = 240) -> str:
    return s if len(s) <= n else s[:n] + "…"


def _clean_llm_output(txt: str) -> str:
    # Light cleaner in case a model ignores format=json (rare).
    clean = txt.strip()
    if clean.startswith("```"):
        parts = clean.split("```")
        candidates = [p for p in parts if "{" in p]
        clean = max(candidates, key=len) if candidates else clean
    clean = clean.lstrip().lstrip("json").lstrip()
    return clean


def call_llm_single(record: str, timeout_s: int = LLM_TIMEOUT_S) -> Tuple[Optional[dict], str]:
    prompt = f"{_SYS_PROMPT}\nLOG RECORD:\n{record}"
    print(f"[llm] PROMPT: {_truncate(prompt)}")
    print(f"[llm] → sending at {time.strftime('%H:%M:%S')}")
    try:
        r = requests.post(
            f"{OLLAMA_HOST}/api/generate",
            json={
                "model": LLM_MODEL,
                "prompt": prompt,
                "stream": False,
                "format": "json",       # ask Ollama to constrain to JSON
                "options": {
                    "temperature": 0.0,
                    "num_predict": 256
                },
            },
            timeout=timeout_s,
        )
        r.raise_for_status()
        payload = r.json() or {}
        txt = (payload.get("response", "") or "").strip()
        print(f"[llm] ← received at {time.strftime('%H:%M:%S')} (len={len(txt)})")
        print(f"[llm] RAW: {_truncate(txt)}")

        # Some builds return parsed JSON under payload["format"]["json"]
        if isinstance(payload.get("format"), dict) and "json" in payload.get("format", {}):
            return payload["format"]["json"], txt

        # Otherwise parse the string (should be strict JSON due to format=json)
        try:
            return json.loads(txt), txt
        except Exception:
            clean = _clean_llm_output(txt)
            s, e = clean.find("{"), clean.rfind("}")
            if s != -1 and e != -1:
                try:
                    return json.loads(clean[s:e+1]), txt
                except Exception:
                    pass
            return None, txt

    except Exception as e:
        print(f"[llm] request error: {e}")
        return None, ""