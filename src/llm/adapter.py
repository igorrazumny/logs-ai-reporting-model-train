# Project: logs-ai-reporting-model-train — File: src/llm/adapter.py
import os
import requests

# Single entrypoint the app calls. Chooses provider by env.
#   LLM_PROVIDER = openai | together | groq | ollama  (default: ollama)
#   LLM_API_KEY  = <api key for hosted providers>
#   LLM_MODEL    = model name/tag per provider
#   LLM_HOST     = http://ollama:11434  (only for ollama)
#
# NOTE: temperature=0 for stable SQL; 300s timeout is enforced by the caller.

def call_llm(system_prompt: str, user_query: str, timeout_s: int = 300) -> str:
    provider = (os.getenv("LLM_PROVIDER") or "ollama").lower()

    if provider == "openai":
        api_key = os.getenv("LLM_API_KEY")
        if not api_key:
            raise RuntimeError("Missing LLM_API_KEY for OpenAI.")
        url = "https://api.openai.com/v1/chat/completions"
        model = os.getenv("LLM_MODEL") or "gpt-4o-mini"
        headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
        payload = {
            "model": model,
            "messages": [{"role": "system", "content": system_prompt},
                         {"role": "user", "content": user_query}],
            "temperature": 0,
            "max_tokens": 1024,
        }
        r = requests.post(url, headers=headers, json=payload, timeout=timeout_s)
        r.raise_for_status()
        data = r.json()
        return data["choices"][0]["message"]["content"]

    if provider == "together":
        api_key = os.getenv("LLM_API_KEY")
        if not api_key:
            raise RuntimeError("Missing LLM_API_KEY for Together.")
        url = "https://api.together.xyz/v1/chat/completions"
        model = os.getenv("LLM_MODEL") or "meta-llama/Meta-Llama-3-8B-Instruct-Turbo"
        headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
        payload = {
            "model": model,
            "messages": [{"role": "system", "content": system_prompt},
                         {"role": "user", "content": user_query}],
            "temperature": 0,
            "max_tokens": 1024,
        }
        r = requests.post(url, headers=headers, json=payload, timeout=timeout_s)
        r.raise_for_status()
        data = r.json()
        return data["choices"][0]["message"]["content"]

    if provider == "groq":
        api_key = os.getenv("LLM_API_KEY")
        if not api_key:
            raise RuntimeError("Missing LLM_API_KEY for Groq.")
        url = "https://api.groq.com/openai/v1/chat/completions"
        model = os.getenv("LLM_MODEL") or "llama3-8b-8192"
        headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
        payload = {
            "model": model,
            "messages": [{"role": "system", "content": system_prompt},
                         {"role": "user", "content": user_query}],
            "temperature": 0,
            "max_tokens": 1024,
        }
        r = requests.post(url, headers=headers, json=payload, timeout=timeout_s)
        r.raise_for_status()
        data = r.json()
        return data["choices"][0]["message"]["content"]

    # default: local Ollama
    host = os.getenv("LLM_HOST")
    model = os.getenv("LLM_MODEL")
    if not host or not model:
        raise RuntimeError("Missing LLM_HOST or LLM_MODEL for ollama.")
    body = {"model": model, "system": system_prompt, "prompt": user_query,
            "stream": False, "options": {}}
    r = requests.post(f"{host}/api/generate", json=body, timeout=timeout_s)
    r.raise_for_status()
    data = r.json()
    return data.get("response", "")