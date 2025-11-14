#!/usr/bin/env python3
# File: .claude/costs-tracker.py
# Purpose: Track Claude Code token usage and costs per project
# Usage: python .claude/costs-tracker.py <total_tokens_used> <output_tokens_estimate>

import json
import sys
from datetime import datetime
from pathlib import Path

# Pricing (USD per million tokens) - Claude Sonnet 4.5
PRICING = {
    "input": 3.00,   # $3 per 1M input tokens
    "output": 15.00  # $15 per 1M output tokens
}

def update_costs(total_tokens: int, output_tokens: int = 0):
    """Update costs.json with new token usage

    Args:
        total_tokens: Total tokens used in session (from Claude's output)
        output_tokens: Estimated output tokens (default 0, will estimate as 20% of total)
    """
    costs_file = Path(__file__).parent / "costs.json"

    # Estimate output tokens if not provided (rough: 20% output, 80% input)
    if output_tokens == 0:
        output_tokens = int(total_tokens * 0.2)

    input_tokens = total_tokens - output_tokens

    # Calculate costs
    input_cost = (input_tokens / 1_000_000) * PRICING["input"]
    output_cost = (output_tokens / 1_000_000) * PRICING["output"]
    total_cost = input_cost + output_cost

    # Load existing data
    if costs_file.exists():
        with open(costs_file) as f:
            data = json.load(f)
    else:
        data = {
            "pricing": {
                "input_per_million": PRICING["input"],
                "output_per_million": PRICING["output"],
                "model": "claude-sonnet-4.5"
            },
            "hourly": {},
            "daily": {},
            "monthly": {},
            "yearly": {},
            "total": {
                "input_tokens": 0,
                "output_tokens": 0,
                "total_tokens": 0,
                "input_cost": 0.0,
                "output_cost": 0.0,
                "total_cost": 0.0,
                "sessions": 0
            }
        }

    now = datetime.now()
    hour_key = now.strftime("%Y-%m-%d %H:00")
    day_key = now.strftime("%Y-%m-%d")
    month_key = now.strftime("%Y-%m")
    year_key = now.strftime("%Y")

    # Update each aggregation level
    for key, bucket in [
        (hour_key, data["hourly"]),
        (day_key, data["daily"]),
        (month_key, data["monthly"]),
        (year_key, data["yearly"])
    ]:
        if key not in bucket:
            bucket[key] = {
                "input_tokens": 0,
                "output_tokens": 0,
                "total_tokens": 0,
                "input_cost": 0.0,
                "output_cost": 0.0,
                "total_cost": 0.0,
                "sessions": 0
            }

        bucket[key]["input_tokens"] += input_tokens
        bucket[key]["output_tokens"] += output_tokens
        bucket[key]["total_tokens"] += total_tokens
        bucket[key]["input_cost"] += input_cost
        bucket[key]["output_cost"] += output_cost
        bucket[key]["total_cost"] += total_cost
        bucket[key]["sessions"] += 1

    # Update totals
    data["total"]["input_tokens"] += input_tokens
    data["total"]["output_tokens"] += output_tokens
    data["total"]["total_tokens"] += total_tokens
    data["total"]["input_cost"] += input_cost
    data["total"]["output_cost"] += output_cost
    data["total"]["total_cost"] += total_cost
    data["total"]["sessions"] += 1
    data["last_updated"] = now.isoformat()
    data["last_session"] = {
        "timestamp": now.isoformat(),
        "tokens": total_tokens,
        "cost": total_cost
    }

    # Save
    with open(costs_file, 'w') as f:
        json.dump(data, f, indent=2)

    return data

def print_summary(data):
    """Print cost summary"""
    now = datetime.now()
    hour_key = now.strftime("%Y-%m-%d %H:00")
    day_key = now.strftime("%Y-%m-%d")
    month_key = now.strftime("%Y-%m")

    print(f"\n💰 Claude Code Costs - {data['pricing']['model']}")
    print("=" * 60)

    if hour_key in data["hourly"]:
        h = data["hourly"][hour_key]
        print(f"\nThis Hour ({hour_key}):")
        print(f"  Sessions: {h['sessions']}")
        print(f"  Tokens: {h['total_tokens']:,} (in: {h['input_tokens']:,} / out: {h['output_tokens']:,})")
        print(f"  Cost: ${h['total_cost']:.4f} (in: ${h['input_cost']:.4f} / out: ${h['output_cost']:.4f})")

    if day_key in data["daily"]:
        d = data["daily"][day_key]
        print(f"\nToday ({day_key}):")
        print(f"  Sessions: {d['sessions']}")
        print(f"  Tokens: {d['total_tokens']:,} (in: {d['input_tokens']:,} / out: {d['output_tokens']:,})")
        print(f"  Cost: ${d['total_cost']:.4f} (in: ${d['input_cost']:.4f} / out: ${d['output_cost']:.4f})")

    if month_key in data["monthly"]:
        m = data["monthly"][month_key]
        print(f"\nThis Month ({month_key}):")
        print(f"  Sessions: {m['sessions']}")
        print(f"  Tokens: {m['total_tokens']:,} (in: {m['input_tokens']:,} / out: {m['output_tokens']:,})")
        print(f"  Cost: ${m['total_cost']:.4f} (in: ${m['input_cost']:.4f} / out: ${m['output_cost']:.4f})")

    t = data["total"]
    print(f"\nAll Time:")
    print(f"  Sessions: {t['sessions']}")
    print(f"  Tokens: {t['total_tokens']:,} (in: {t['input_tokens']:,} / out: {t['output_tokens']:,})")
    print(f"  Cost: ${t['total_cost']:.4f} (in: ${t['input_cost']:.4f} / out: ${t['output_cost']:.4f})")
    print("=" * 60)

    # Cost breakdown
    avg_session_cost = t['total_cost'] / t['sessions'] if t['sessions'] > 0 else 0
    print(f"\nAverage per session: ${avg_session_cost:.4f}")
    print(f"Pricing: ${data['pricing']['input_per_million']}/M input, ${data['pricing']['output_per_million']}/M output")
    print(f"\nCosts file: {Path(__file__).parent / 'costs.json'}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python .claude/costs-tracker.py <total_tokens> [output_tokens]")
        print("\nExample:")
        print("  python .claude/costs-tracker.py 105234        # Auto-estimate output as 20%")
        print("  python .claude/costs-tracker.py 105234 21000  # Specify exact output tokens")
        print("\nGet tokens from Claude's output: 'Token usage: 105,234/200,000'")
        sys.exit(1)

    total_tokens = int(sys.argv[1].replace(",", ""))
    output_tokens = int(sys.argv[2].replace(",", "")) if len(sys.argv) > 2 else 0

    data = update_costs(total_tokens, output_tokens)
    print_summary(data)
