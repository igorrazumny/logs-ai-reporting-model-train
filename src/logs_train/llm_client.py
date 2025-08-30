# File: src/logs_train/llm_client.py
import json, time, requests
from typing import Optional, Tuple
from logs_train.prompts import PKM_SINGLE_JSON_PROMPT as _SYS_PROMPT
from logs_train.config import OLLAMA_HOST, LLM_MODEL, LLM_TIMEOUT_S, RETRIES, BACKOFF_S, truncate, clean_llm_output

def call_llm_single(record: str, timeout_s: int = LLM_TIMEOUT_S) -> Tuple[Optional[dict], str]:
    """Send one record to the local model. Return (parsed_json_or_None, raw_text)."""
    prompt = f"{_SYS_PROMPT}\nLOG RECORD:\n{record}"
    print(f"[llm] PROMPT: {truncate(prompt)}")
    print(f"[llm] → sending at {time.strftime('%H:%M:%S')}")
    last_txt = ""
    for attempt in range(RETRIES + 1):
        try:
            r = requests.post(
                f"{OLLAMA_HOST}/api/generate",
                json={"model": LLM_MODEL, "prompt": prompt, "stream": False, "options": {"temperature": 0.0}},
                timeout=timeout_s,
            )
            r.raise_for_status()
            txt = (r.json() or {}).get("response", "") if r.headers.get("content-type","").startswith("application/json") else r.text
            txt = (txt or "").strip()
            last_txt = txt
            print(f"[llm] ← received at {time.strftime('%H:%M:%S')} (len={len(txt)})")
            print(f"[llm] RAW: {truncate(txt)}")
            clean = clean_llm_output(txt)
            start, end = clean.find("{"), clean.rfind("}")
            if start != -1 and end != -1:
                js = clean[start:end+1]
                try:
                    return json.loads(js), txt
                except Exception as e:
                    print(f"[llm] JSON decode error: {e}")
                    try:
                        return json.loads(js.strip()), txt
                    except Exception as e2:
                        print(f"[llm] still bad JSON: {e2}")
                        return None, txt
            return None, txt
        except Exception as e:
            wait = BACKOFF_S * (2 ** attempt)
            print(f"[llm] request error (attempt {attempt+1}/{RETRIES+1}): {e}, waiting {wait}s")
            time.sleep(wait)
    return None, last_txt