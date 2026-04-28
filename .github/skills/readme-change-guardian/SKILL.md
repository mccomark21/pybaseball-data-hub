---
name: readme-change-guardian
description: "Use when code, data schema, commands, or workflows changed and README must be updated. Triggers: significant change, doc sync, documentation drift, update readme, release notes."
argument-hint: "Describe what changed and which user-facing behavior was impacted."
user-invocable: true
---

# README Change Guardian

Ensure project documentation stays accurate after meaningful repository changes.

## Use When

- A significant code or pipeline change was made.
- Outputs, schemas, commands, configuration, or workflows changed.
- You want to prevent README drift before merging.

## Inputs

- Summary of what changed.
- Files touched by the change.
- Intended user impact.

## Workflow

1. Classify whether the change is significant.
Decision logic:
- Significant if it changes user-visible behavior, CLI usage, output files, data schema, configuration contract, dependencies, automation workflow, or agent workflow.
- Not significant if it is purely internal refactor, formatting, comments, or tests with no behavior/config/output impact.

2. If not significant, record a one-line rationale and stop.

3. If significant, map change areas to README sections.
Typical mapping:
- Command or script behavior -> Running Locally / Project Structure
- New or changed artifacts -> Output Files / schema sections
- Config changes -> Configuration
- Agent or automation changes -> Custom Copilot Agents or workflow docs

4. Update README with the minimum accurate edits.
- Add or revise only impacted sections.
- Keep terminology consistent with code and filenames.
- Prefer concise updates over large rewrites.

5. Run completion checks before finishing.
- All changed user-facing behavior is documented.
- Referenced commands and paths exist.
- Output/schema descriptions match current code behavior.
- No stale instructions remain from prior behavior.

6. Provide a short documentation-change summary.
- What was updated in README.
- Why it was required.
- Any remaining assumptions to verify.

## Rule

Whenever significant changes are made, update `README.md` to ensure documentation remains accurate.