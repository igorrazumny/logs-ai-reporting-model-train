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

    # Calculate costs rounded to cents
    input_cost = round((input_tokens / 1_000_000) * PRICING["input"], 2)
    output_cost = round((output_tokens / 1_000_000) * PRICING["output"], 2)
    total_cost = round(input_cost + output_cost, 2)

    # Load existing data
    if costs_file.exists():
        with open(costs_file) as f:
            data = json.load(f)
    else:
        data = {
            "pricing": {
                "model": "claude-sonnet-4.5",
                "input_per_million": "$3.00",
                "output_per_million": "$15.00"
            },
            "current_session": {
                "total_cost": "$0.00",
                "input_cost": "$0.00",
                "output_cost": "$0.00",
                "last_update": "",
                "session_start": "",
                "total_tokens": 0,
                "input_tokens": 0,
                "output_tokens": 0,
                "turns": 0
            },
            "today": {
                "total_cost": "$0.00",
                "date": "",
                "total_tokens": 0,
                "sessions": 0
            },
            "this_week": {
                "total_cost": "$0.00",
                "week": "",
                "total_tokens": 0,
                "sessions": 0
            },
            "this_month": {
                "total_cost": "$0.00",
                "month": "",
                "total_tokens": 0,
                "sessions": 0
            },
            "this_year": {
                "total_cost": "$0.00",
                "year": "",
                "total_tokens": 0,
                "sessions": 0
            },
            "all_time": {
                "total_cost": "$0.00",
                "last_session": "",
                "first_session": "",
                "total_tokens": 0,
                "total_sessions": 0
            }
        }

    now = datetime.now()
    now_str = now.strftime("%Y-%m-%d %H:%M:%S")
    day_key = now.strftime("%Y-%m-%d")
    week_key = now.strftime("%Y-W%U")
    month_key = now.strftime("%Y-%m")
    year_key = now.strftime("%Y")

    # Update current session (cumulative in this session)
    if data["current_session"]["session_start"] == "":
        data["current_session"]["session_start"] = now_str

    data["current_session"]["total_tokens"] += total_tokens
    data["current_session"]["input_tokens"] += input_tokens
    data["current_session"]["output_tokens"] += output_tokens
    data["current_session"]["turns"] += 1
    data["current_session"]["last_update"] = now_str

    session_input_cost = round((data["current_session"]["input_tokens"] / 1_000_000) * PRICING["input"], 2)
    session_output_cost = round((data["current_session"]["output_tokens"] / 1_000_000) * PRICING["output"], 2)
    session_total_cost = round(session_input_cost + session_output_cost, 2)

    data["current_session"]["input_cost"] = f"${session_input_cost:.2f}"
    data["current_session"]["output_cost"] = f"${session_output_cost:.2f}"
    data["current_session"]["total_cost"] = f"${session_total_cost:.2f}"

    # Update today
    if data["today"]["date"] != day_key:
        # New day, reset
        data["today"] = {
            "total_cost": "$0.00",
            "date": day_key,
            "total_tokens": 0,
            "sessions": 0
        }

    data["today"]["total_tokens"] += total_tokens
    data["today"]["sessions"] = 1  # Current session
    today_cost = round((data["today"]["total_tokens"] / 1_000_000) * ((PRICING["input"] * 0.8) + (PRICING["output"] * 0.2)), 2)
    data["today"]["total_cost"] = f"${today_cost:.2f}"

    # Update this week
    if data["this_week"]["week"] != week_key:
        # New week, reset
        data["this_week"] = {
            "total_cost": "$0.00",
            "week": week_key,
            "total_tokens": 0,
            "sessions": 0
        }

    data["this_week"]["total_tokens"] += total_tokens
    data["this_week"]["sessions"] = 1
    week_cost = round((data["this_week"]["total_tokens"] / 1_000_000) * ((PRICING["input"] * 0.8) + (PRICING["output"] * 0.2)), 2)
    data["this_week"]["total_cost"] = f"${week_cost:.2f}"

    # Update this month
    if data["this_month"]["month"] != month_key:
        # New month, reset
        data["this_month"] = {
            "total_cost": "$0.00",
            "month": month_key,
            "total_tokens": 0,
            "sessions": 0
        }

    data["this_month"]["total_tokens"] += total_tokens
    data["this_month"]["sessions"] = 1
    month_cost = round((data["this_month"]["total_tokens"] / 1_000_000) * ((PRICING["input"] * 0.8) + (PRICING["output"] * 0.2)), 2)
    data["this_month"]["total_cost"] = f"${month_cost:.2f}"

    # Update this year
    if data["this_year"]["year"] != year_key:
        # New year, reset
        data["this_year"] = {
            "total_cost": "$0.00",
            "year": year_key,
            "total_tokens": 0,
            "sessions": 0
        }

    data["this_year"]["total_tokens"] += total_tokens
    data["this_year"]["sessions"] = 1
    year_cost = round((data["this_year"]["total_tokens"] / 1_000_000) * ((PRICING["input"] * 0.8) + (PRICING["output"] * 0.2)), 2)
    data["this_year"]["total_cost"] = f"${year_cost:.2f}"

    # Update all time
    if data["all_time"]["first_session"] == "":
        data["all_time"]["first_session"] = now_str

    data["all_time"]["last_session"] = now_str
    data["all_time"]["total_tokens"] += total_tokens
    data["all_time"]["total_sessions"] = 1
    all_time_cost = round((data["all_time"]["total_tokens"] / 1_000_000) * ((PRICING["input"] * 0.8) + (PRICING["output"] * 0.2)), 2)
    data["all_time"]["total_cost"] = f"${all_time_cost:.2f}"

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
