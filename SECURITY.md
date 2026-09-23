# Security policy

## Reporting a vulnerability

Please **do not open a public issue** for a security problem.

Use GitHub's [private vulnerability reporting](https://docs.github.com/en/code-security/security-advisories/guidance-on-reporting-and-writing/privately-reporting-a-security-vulnerability)
on this repository (Security → Report a vulnerability). If that is unavailable,
email the address on the maintainer's GitHub profile.

I'll acknowledge within 7 days. This is a personal portfolio project maintained
in my own time — I can't offer a commercial SLA, and I'd rather say that than
imply one.

## Scope

These repositories are **demonstration and portfolio projects**. Infrastructure
is stood up to be measured and then destroyed; most of it is not continuously
running. Findings in the *code and configuration* are in scope and welcome.

Out of scope: anything requiring credentials I have not published, and the
deliberately-unsafe drill switches (`FORCE_500`, `FORCE_SLOW`, `FAIL_READY`),
which exist so failure can be induced on purpose and are documented as such.

## What this repo already does

Every push runs, as blocking gates:

- **gitleaks** — secrets in the diff and history
- **checkov** — IaC misconfiguration, against a reviewed baseline where each
  accepted finding carries a written reason
- **tflint** — provider pinning, unused declarations, invalid values
- **trivy** — HIGH/CRITICAL CVEs and secrets in image layers, where a container
  is built
- **unit tests**

Dependencies are updated by Renovate, with security advisories un-scheduled so
they raise a PR immediately rather than waiting for the weekly window.

## A note on pinning

Third-party GitHub Actions are pinned to release tags, never to `@master` or
`@main`. This is not pedantry: `aquasecurity/trivy-action` was compromised twice
in March 2026 ([CVE-2026-33634](https://github.com/advisories/GHSA-69fq-xp46-6x23)),
roughly 75 tags were overwritten with a credential stealer, and a moving ref was
the delivery path. The trivy binary is likewise installed at a pinned version
rather than from an install script on a moving branch.
