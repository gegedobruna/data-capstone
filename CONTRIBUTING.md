# Contributing Guidelines

## Branch Naming
feature/<role>-<short-description>
Example: feature/r3-bronze-ingestion

## Commit Format
[RX] Short description
Example: [R3] Add bronze ingestion notebook with schema enforcement

## Pull Requests
- Every PR must reference a task ID (e.g. "Closes P2.2")
- Assign at least 1 reviewer who is NOT the author
- PR title must follow commit format above

## Merge Authority
Only R1 merges to main. Do not merge your own PRs.

## Never Commit
- CSV or any data files
- .env files, SAS tokens, API keys, personal access tokens
- Any file containing credentials

## Every Notebook Must Have
# Purpose:
# Author: [name + role]
# Last updated: [date]
# Dependencies: [what runs before this]