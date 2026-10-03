---
name: update-tdd
description: Write or rewrite TDD.md (the Technical Design Document) whenever code changes or a new design is introduced in RustCache. Use after any change to protocol, store, server, web-api, configuration, limits, error handling, security or tests, and when asked to document, redesign or update the TDD.
---

# Update the Technical Design Document

`TDD.md` at the repository root must always match the code. Run this skill at the end of every task that changes code or design, before finishing.

## When to run
- Any change to `cache-proto`, `cache-server`, `web-api`, workspace layout, dependencies, config/env vars, limits, error mapping, TLS/security, or tests.
- A new feature, redesign, or decision (including rejected alternatives worth recording).
- If TDD.md is missing, create it from scratch.

## Steps
1. Review what changed: `git --no-pager diff` and `git --no-pager status`, and read the touched source files. Never document from memory; verify against code.
2. Open `TDD.md` and update every affected section. Keep this structure:
   1. Overview (goals, non-goals)
   2. Architecture (diagram, crate responsibilities)
   3. Wire protocol
   4. Store
   5. TCP server
   6. Web API (routes, client pool, error mapping, TLS)
   7. Security considerations
   8. Testing strategy
   9. Open items / future work
   Add new sections for new components; remove content that no longer applies. Rewrite whole sections if incremental edits would leave them inconsistent.
3. Refresh the `_Last updated_` date line.
4. Keep details consistent with `README.md` (command table, routes, env vars). Link to the README instead of duplicating long tables; update the README too if user-facing behaviour changed.
5. Record design rationale and trade-offs (why, not just what), known limitations, and move completed items out of "Open items".
6. Verify: every limit, default, route, env var and error code stated in TDD.md exists in code (use grep). Fix any mismatch.

## Style
- Concise, factual, present tense; tables and short bullets over prose.
- Use relative links for repo files. No secrets, certificates or private paths.
- Do not describe unimplemented behaviour as implemented; place it under future work.
