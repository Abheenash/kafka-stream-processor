## What this changes

<!-- One or two sentences. What is different after this merges? -->

## Why

<!-- The reason, not the restatement. If it fixes something, what was the symptom? -->

## How it was verified

<!-- Say what you actually ran, not what you intended to. "Validated, not applied"
     and "tested locally, not against AWS" are both fine answers — an unverified
     claim is the thing to avoid. -->

## CI gates on this repo

These run automatically; they are listed so a reviewer can tell at a glance what is
and is not covered by the green tick:

- [ ] `gitleaks`
- [ ] `pytest (fake broker)`
- [ ] `checkov + terraform`
- [ ] `terraform test`
- [ ] `tflint`
- [ ] `ruff`
- [ ] `CodeQL`
- [ ] `dependency-review`
- [ ] `pytest (real broker)`

## Anything a reviewer should push back on

<!-- Shortcuts taken, things left out, decisions you are not sure about. Leave
     "nothing" only if that is true. -->
