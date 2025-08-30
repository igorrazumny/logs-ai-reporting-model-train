# File: src/logs_train/config.py
import os

# ======== Basic config (shared) ========
OLLAMA_HOST   = os.getenv("LLM_HOST", "http://ollama:11434")
LLM_MODEL     = os.getenv("LLM_MODEL", "llama3.2:1b-instruct-q4_K_M")
LLM_TIMEOUT_S = int(os.getenv("LLM_TIMEOUT_S", "60"))
RETRIES       = int(os.getenv("LLM_RETRIES", "2"))
BACKOFF_S     = float(os.getenv("LLM_BACKOFF_S", "1.0"))

def truncate(s: str, n: int = 240) -> str:
    return s if len(s) <= n else s[:n] + "…"

def clean_llm_output(txt: str) -> str:
    """Strip common fences/backticks and leading 'json'."""
    clean = txt.strip()
    if clean.startswith("```"):
        parts = clean.split("```")
        candidates = [p for p in parts if "{" in p]
        clean = max(candidates, key=len) if candidates else clean
    clean = clean.lstrip().lstrip("json").lstrip()
    return clean