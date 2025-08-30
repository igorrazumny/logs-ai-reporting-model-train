# File: src/logs_train/llm_client.py
import os
import json
import time
import requests
from typing import Optional, Tuple
from logs_train.prompts import PKM_SINGLE_JSON_PROMPT as _SYS_PROMPT

OLLAMA_HOST   = os.getenv("LLM_HOST", "http://ollama:11434")
LLM_MODEL     = os.getenv("LLM_MODEL", "llama3.2:1b-instruct-q4_K_M")
LLM_TIMEOUT_S = int(os.getenv("LLM_TIMEOUT_S", "60"))

def _truncate(s: str, n: int = 240) -> str:
    return s if len(s) <= n else s[:n] + "…"

def _clean_llm_output(txt: str) -> str:
    # still keep a light cleaner in case the model ignores format=json
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
                # 👇 this is the key change: ask Ollama to return strict JSON
                "format": "json",
                "options": {
                    "temperature": 0.0,
                    # optional: reduce rambling; adjust if you see truncation
                    "num_predict": 256
                },
            },
            timeout=timeout_s,
        )
        r.raise_for_status()
        # with format=json, content-type is JSON and 'response' should be a JSON object string
        payload = r.json() or {}
        txt = (payload.get("response", "") or "").strip()
        print(f"[llm] ← received at {time.strftime('%H:%M:%S')} (len={len(txt)})")
        print(f"[llm] RAW: {_truncate(txt)}")

        # if the server already returned a parsed JSON object, some builds put it under 'format'/'json'
        if isinstance(payload.get("format"), dict) and "json" in payload.get("format", {}):
            return payload["format"]["json"], txt

        # otherwise parse the string we received (should be strict JSON due to format=json)
        try:
            return json.loads(txt), txt
        except Exception:
            # extremely rare: fall back to light cleaner and retry
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