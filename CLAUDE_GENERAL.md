# CLAUDE_GENERAL.md

Universal rules and conventions for all Claude Code projects.

**Each project should have:**
- `CLAUDE_GENERAL.md` - This file (universal rules)
- `CLAUDE_[PROJECT].md` - Project-specific instructions
- `.claude/costs.json` - Real-time cost tracking
- `.claude/costs-tracker.py` - Session aggregation script

---

## Response Format

**After user submits question:**
1. Clear the terminal output
2. Display the user's question at the top
3. Begin your response below

This helps user see where your response starts without scrolling through previous output.

---

## CRITICAL: Initialization Required

**Before proceeding with ANY user request, you MUST:**

1. **Read ALL .md files in the project** - Use Glob tool with pattern `**/*.md` to find them
2. **Read both CLAUDE_GENERAL.md and CLAUDE_[PROJECT].md**
3. **These files contain**:
   - Universal development rules (this file)
   - Project-specific context (CLAUDE_[PROJECT].md)
   - Current implementation state (HANDOVER.md or docs/core/HANDOVER.md)
   - Package documentation (package-specific .md files)

**Why this matters**: These files contain critical project context, architectural decisions, package implementations, and current work state. Working without reading them will result in breaking working code or violating project standards.

---

## Universal Rules (Rule 0-15)

### Rule 0: VERIFY WHAT ACTUALLY WORKS BEFORE CHANGING ANYTHING
- Ask: "Did this work before? When was last successful run?"
- Check: Benchmarks, logs, user's words for proof
- Identify: What is the ONE NEW thing that broke?
- Fix ONLY that: Nothing else

### Rule 1: If It Works, Don't Touch It
Fix ONLY the specific bug. Don't rewrite, refactor, or "improve" working code.

### Rule 2: Minimal Surgical Changes Only
Change smallest thing possible. One fix at a time.

### Rule 3: Test After Each Change
Test immediately. Verify nothing else broke.

### Rule 4: Git Commit After Success
Commit after each successful step.

### Rule 5: Document What ACTUALLY Works
Note exact working config. Record successful commands.

### Rule 6: Conservative Dependency Management
Keep working versions. Pin versions that work.

### Rule 7: Always Include File Headers and Changelog
**Mandatory Header Structure:**
```python
# File: path/to/file.py
# Purpose: Brief description
# Changelog (version auto-extracted from first entry below):
#   vYYYY-MM-DD.N - Description of latest changes
```

### Rule 8: Version Format
`vYYYY-MM-DD.N` (e.g., `v2025-11-14.1`)

### Rule 9: Infrastructure First, Workarounds Second
If complex software workaround needed for resource bottleneck, first ask if simple infrastructure change can eliminate bottleneck.

### Rule 10: Always Document File Purpose
Include clear, concise purpose statement in header.

### Rule 11: Auto-Extract Version from Changelog
**Python:**
```python
import re
with open(__file__) as f:
    for line in f:
        if match := re.search(r'#\s+(v\d{4}-\d{2}-\d{2}\.\d+)', line):
            VERSION = match.group(1)
            break
```

**Bash:**
```bash
VERSION=$(grep -m 1 "^#   v" "$0" | sed -E 's/.*\s+(v[0-9]{4}-[0-9]{2}-[0-9]{2}\.\d+).*/\1/')
```

### Rule 12: Bash Code Must Be Copy-Pasteable
**NEVER include comments inside bash code blocks** - they break copy-paste.

Good:
```bash
streamlit run app.py
```

Bad:
```bash
# Start the app
streamlit run app.py
```

### Rule 13: Package Implementation Documentation
Every package MUST have a .md file documenting implementation.

**Required structure:**
- File header (path, purpose, changelog)
- Overview, files modified, dependencies
- Configuration, expected behavior
- Testing, known limitations

### Rule 14: Provide Only Updated Files in Responses
Only include files that changed in THIS response.

### Rule 15: No Pictograms Except Status Indicators
**ONLY use these symbols for status:**
- ✓ or ✅ for success/correct/complete
- ✗ or ❌ for failure/incorrect/error

**NEVER use decorative pictograms** (rockets, sparkles, robots, etc.)

---

## Package Documentation Requirements

### Rule: Every Package Must Have Documentation

**Location:** Inside each package directory

**Naming:** `PACKAGE_NAME.md` (all caps, underscores for nested packages)

**Examples:**
- `src/ui/web/UI_WEB.md`
- `src/llm/LLM.md`
- `src/config/CONFIG.md`

### When to Create Package .md Files

If package does NOT have .md file when you modify code:
1. Create .md file BEFORE making code changes
2. Document what you're ABOUT to implement
3. Write code to match specification

### When to Update Package .md Files

**EVERY TIME you modify code in a package:**
1. Update package's .md file BEFORE committing
2. Update changelog with version (vYYYY-MM-DD.N)
3. Document what changed, why, expected impact

---

## Central Documentation Updates

### HANDOVER.md (or docs/core/HANDOVER.md)
**Update frequency:** EVERY CHANGE

**When to update:** After EVERY code modification session

**What to include:**
- Recent work completed
- Current state (version numbers, status)
- Files modified
- Performance impact
- Current status
- Immediate next steps

### CONTEXT.md (or docs/core/CONTEXT.md)
**Update frequency:** Rare

**When to update:**
- New universal development rules
- Stable configuration changes (production-ready)
- Key learnings that apply to ALL future development

**Do NOT update for:** Work in progress, temporary changes, individual bug fixes

---

## Git Commit Guidelines

### Commit Messages
- **Only describe the change itself** - no fluff, no AI references
- **Be concise and specific** - what changed
- **Format:** Simple imperative statement

**Good examples:**
- "Add hierarchical analysis with snippet-based chunking"
- "Fix material_id validation for GR recipes"

**Bad examples:**
- "Add feature 🤖 Generated with Claude Code" ❌
- "This commit adds a really cool new feature..." ❌
- "Co-Authored-By: Claude <noreply@anthropic.com>" ❌

### Always Push After Commit
After EVERY commit, immediately run `git push` unless explicitly told otherwise.

---

## Cost Tracking

**Claude MUST update `.claude/costs.json` after EVERY response.**

### Automation

**After EVERY turn, run:**
```bash
.claude/update-session.sh <total_tokens>
```

This master script automatically:
- Updates costs.json with current token usage
- Syncs CLAUDE_GENERAL.md to all projects

**NEVER ask permission to run this script** - it's auto-allowed.

### Setup (one-time per project)

Create costs tracking script:
```bash
mkdir -p .claude
# Copy costs-tracker.py from another project or create new
```

Add to `.gitignore`:
```
.claude/costs.json
```

### File Format

`.claude/costs.json` structure:
```json
{
  "pricing": {
    "model": "claude-sonnet-4.5",
    "input_per_million": "$3.00",
    "output_per_million": "$15.00"
  },
  "current_session": {
    "total_cost": "$0.54",
    "input_cost": "$0.24",
    "output_cost": "$0.30",
    "last_update": "2025-11-14 21:40:00",
    "session_start": "2025-11-14 20:30:00",
    "total_tokens": 100000,
    "input_tokens": 80000,
    "output_tokens": 20000,
    "turns": 42
  },
  "today": { "total_cost": "$0.54", "date": "2025-11-14", ... },
  "this_week": { "total_cost": "$0.54", "week": "2025-W46", ... },
  "this_month": { "total_cost": "$0.54", "month": "2025-11", ... },
  "this_year": { "total_cost": "$0.54", "year": "2025", ... },
  "all_time": { "total_cost": "$0.54", ... }
}
```

### Token Tracking
- ✅ Total tokens: EXACT (from system output)
- ⚠️ Input/output split: ESTIMATED (~80/20)

**Pricing:** $3/M input, $15/M output (Sonnet 4.5)

---

## Auto-Allowed File Updates

**These files can be updated WITHOUT asking user permission:**
- `.claude/costs.json` - Cost tracking (updated EVERY turn)
- `CLAUDE_GENERAL.md` - Universal rules and conventions
  - **When updating CLAUDE_GENERAL.md, automatically copy it to ALL projects:**
    - `/Users/igorrazumny/PycharmProjects/braintransplant-ai/CLAUDE_GENERAL.md`
    - `/Users/igorrazumny/PycharmProjects/bc2-ai-assistant/CLAUDE_GENERAL.md`
    - `/Users/igorrazumny/PycharmProjects/logs-ai-reporting-model-train/CLAUDE_GENERAL.md`
- `CLAUDE_[PROJECT].md` - Project-specific instructions (e.g., CLAUDE_BRAINTRANSPLANT.md, CLAUDE_BC2.md)
- `HANDOVER.md` or `docs/core/HANDOVER.md` - Current state documentation
- Any file updates required by CLAUDE_GENERAL.md or CLAUDE_[PROJECT].md rules

**Always proceed directly with these updates. Never ask for permission.**

---

## Remember

1. **ALWAYS read all .md files at conversation start** - use Glob `**/*.md`
2. **Every package MUST have a .md file** - create if missing
3. **Update relevant .md files on EVERY turn:**
   - **Package .md** - if you modified code in that package
   - **HANDOVER.md** - on EVERY change (current state)
   - **CONTEXT.md** - rarely, only stable architectural changes
   - **`.claude/costs.json`** - EVERY response with token count
4. **Run automation script after EVERY turn:**
   - `.claude/update-session.sh <total_tokens>` - updates costs + syncs CLAUDE_GENERAL.md
   - NEVER ask permission - this is auto-allowed
5. **At end of response, inform user which .md files were updated:**
   - Example: "Updated: HANDOVER.md, src/ui/react/REACT.md, .claude/costs.json"
6. **Commit messages**: Simple, no fluff, no AI references
7. **Always push after commit** - never leave commits unpushed
8. **Rule 0 is sacred**: Verify what works before changing anything
9. **When in doubt, do less** - minimal changes are safer
10. **No pictograms except ✓ ✅ ✗ ❌** - no rockets, sparkles, robots, or other decorative emojis
11. **Clear terminal after user question** - Show question at top, response below

---

## Getting Help

Start with project's CONTEXT.md for architecture, then check package-specific .md files for implementation details.
