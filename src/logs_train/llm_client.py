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
            json={"model": LLM_MODEL, "prompt": prompt, "stream": False, "options": {"temperature": 0.0}},
            timeout=timeout_s,
        )
        r.raise_for_status()
        txt = (r.json() or {}).get("response", "") if r.headers.get("content-type","").startswith("application/json") else r.text
        txt = (txt or "").strip()
        print(f"[llm] ← received at {time.strftime('%H:%M:%S')} (len={len(txt)})")
        print(f"[llm] RAW: {_truncate(txt)}")
        clean = _clean_llm_output(txt)
        s, e = clean.find("{"), clean.rfind("}")
        if s != -1 and e != -1:
            try:
                return json.loads(clean[s:e+1]), txt
            except Exception as e:
                print(f"[llm] JSON decode error: {e}")
                return None, txt
        return None, txt
    except Exception as e:
        print(f"[llm] request error: {e}")
        return None, ""