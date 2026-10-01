# Design

## Context

See `proposal.md` — Why. The constraints that shape the approach:

- **The system is finished, not in progress.** `ROADMAP.md` holds 358 completed tasks and
  zero pending ones. There is no in-flight work to carry forward, so this change migrates
  *claims about the system*, not *unfinished tasks*.
- **The documentation is the real source of truth.** Eleven documents under `docs/`, nine
  ADRs, a generated event-flow diagram and a generated saga diagram. The last ten commits
  corrected drift between that prose and the code, so the prose is unusually trustworthy
  right now — which is exactly why the backfill can be grounded in it.
- **Some of it is already machine-guarded.** Generated diagrams, the AsyncAPI/OpenAPI
  documents, the event catalogue and the saga registry have tests that fail on drift
  (`tests/unit/docs`, `tests/unit/contracts`). Prose does not.
- **The repository is a pet project.** One maintainer, no authentication, no users, no
  continuous integration (and `AGENTS.md` forbids adding any). Anything that smells of
  governance ceremony is out.
- **`ROADMAP.md` is written in Russian**; `README.md`, `CONTRIBUTING.md`, `AGENTS.md` and
  every file under `docs/` are English. `openspec/config.yaml` is still the untouched
  template.
- **Untracked, not ignored.** `git status` shows `?? .agents/` and `?? openspec/`. Both are
  meant to be committed. `.docs/` *is* gitignored, so its references to `CHANGELOG.md` are
  private working notes and out of scope.

## Goals / Non-Goals

**Goals:**

- Remove the two root documents without losing a single durable claim: the roadmap's
  working rules survive in `CONTRIBUTING.md`, and its Definition of Done survives as
  baseline capability specs.
- Give OpenSpec a baseline on day one, so the next change has something to write a delta
  against instead of an empty `openspec/specs/`.
- Make the commit skill describe *this* repository, verifiably.
- Leave no dangling reference to either removed file anywhere in tracked content.
- Present the repository honestly: no badge, claim or link that the repository does not
  back up.

**Non-Goals:**

- Not preserving the roadmap's per-task, per-phase narrative inside the repository. Git
  already keeps it, unchanged and addressable.
- Not reconstructing the eleven phases as archived OpenSpec changes. That would invent
  provenance for work that predates OpenSpec.
- Not changing any runtime behaviour. The two source-file edits are docstrings.
- Not adding a changelog replacement file, a release-notes generator, or a version file.
- Not adding continuous integration, workflows, container orchestration or governance
  documents.
- Not touching `.docs/` (gitignored) or `docs/coverage.html` (a build product).

## Decisions

### D1 — The roadmap's content is split by kind, and only two kinds survive

Delete `ROADMAP.md` and route its content three ways:

| Kind of content | Where it goes |
|---|---|
| Durable working rules (atomic task = one commit, tests and docs ride with the code, done means done) | `CONTRIBUTING.md`, and the commit skill |
| The Definition of Done — what the platform claims to do | The twelve baseline capability specs |
| The 358 completed task records, phase estimates, milestone table | Nowhere. Git history |

*Alternatives considered:* moving the file to `docs/ROADMAP.md` (keeps 100 KB of ticked
boxes that nobody will maintain); converting each phase into an archived change (eleven
synthetic changes with invented provenance); keeping a trimmed "phases" page (duplicates
what the specs now say, and drifts).

The roadmap's rules are not all worth keeping. "Update this file as you go", "mark
cancelled tasks `[-]`" and "progress is visible in GitHub Projects" are bookkeeping for a
file that is being deleted, and OpenSpec supersedes them. What survives is the part that
is about *how work is done*: one atomic task per commit, Definition of Done before
closing, tests and documentation delivered with the code rather than after it.

### D2 — The baseline is written as delta specs in this change, never as hand-written main specs

Every capability is expressed as `## ADDED Requirements` in
`openspec/changes/modernize-repo-presentation/specs/<capability>/spec.md`.
`openspec/specs/` is left alone; `openspec archive` merges these deltas into it.

*Why:* the CLI owns the main specs, and archive is the mechanism that puts deltas there.
Hand-writing files under `openspec/specs/` would bypass that flow and leave the main specs
with no provenance — no change to point at, no delta to modify later.

*Consequence to be explicit about:* `openspec/specs/` stays empty until this change is
archived. "Traceable in OpenSpec" therefore means traceable through this change's
artifacts, which is also the only way OpenSpec offers.

### D3 — Twelve capabilities, at the granularity of the documented surface

One capability per area that a document already describes, so each spec has an
authoritative source to be checked against: `plant-collection`, `care-scheduling`,
`care-journal`, `species-catalog`, `iot-telemetry`, `notifications`, `read-models`,
`process-managers`, `event-transport`, `integration-contracts`, `developer-workflow`,
`repository-presentation`.

*Alternatives considered:* one spec per bounded context (too coarse — telemetry would have
to hold both the ingress and the simulator); one per aggregate (eleven overlapping specs
for `Plant`, `Household`, `CareSchedule`, `Sensor`, `Species`, `Notification`, `User`,
`JournalEntry`); one per document (misses the cross-cutting transport and contract
concerns, which no single document owns).

### D4 — Specs describe observable behaviour, not code

Requirements name events, endpoints, tables, keys, thresholds and invariants — the things
a consumer or an operator can observe — and avoid class and module names.

*Why:* it keeps the specs valid when the implementation is refactored, and it is what a
spec is for. The code-level detail stays in `docs/`, which this change does not duplicate.

*Trade-off:* some precision is lost. A requirement like "moisture below the low threshold
raises an event" does not name `MOISTURE_LOW_THRESHOLD`; the document does. The specs were
written by reading those documents, and every one of them is grounded in prose that was
just drift-corrected.

### D5 — The commit skill keeps its method and loses every fact

`.agents/skills/commit/SKILL.md` is structurally good: survey the worktree, group by
concern, order foundational-to-dependent, write the message, stage explicitly, run a safety
gate, verify, and ask when a grouping is ambiguous. All of that is project-agnostic and
stays. What is wrong is that it is `adapted-for: numenews` — the workspace layout, packages
(`numerology`, `vector`, `agents`, `mcp`, `cli`), secret sources (GDELT, NewsAPI, GNews,
Mediastack, Currents), ignored paths (`.qdrant/`, `.venv-eval/`, hishel caches), coverage
floors (95/80/70 for different packages) and gates (`make test-eval`, `make coverage-check`)
belong to another repository and would actively mislead an agent here.

Three corrections are grounded in this repository's actual history rather than assumed:

1. **Bodies are required, not forbidden.** The skill says "No body. The subject is the
   whole message: this repo's commits do not carry explanatory prose." That is false here:
   all sixty most recent commits carry a body, typically 30–130 words, wrapped near 80
   columns and often using bullet points. The rewrite requires a body explaining why.
2. **Subject length is a target, not a gate.** The skill mandates at most 72 characters;
   the longest subject in this history is 83. The rewrite asks for a single short line and
   does not pretend a hard limit is enforced.
3. **No trailers — verified true here.** No `Co-Authored-By` or tool-attribution trailer
   appears in the last sixty commits, so that rule carries over.

One anti-drift measure: the scope list lives in `CONTRIBUTING.md` and the skill points at
it rather than restating it. The two lists cannot diverge if only one exists.

*Alternative considered:* deleting the skill. Rejected — `git check-ignore` confirms
`.agents/` is untracked but **not** ignored, so it is meant to be committed, and the
method it encodes is worth keeping.

### D6 — The README is extended, not rewritten

The existing README is accurate and was just corrected for drift. It already covers the
stack, layout, quick start, contracts, documentation index, ADRs, coverage and the
no-authentication note. The work is to add what is missing (title line, badges, key
features, an OpenSpec section, contributing, license, release notes) and to remove the two
dead links — not to replace good prose with new prose.

**The hand-drawn Mermaid sketch stays.** It was corrected in the most recent commit and is
explicitly labelled as hand-drawn and not drift-guarded, with the two drift-tested
generated diagrams linked beside it. It is the fastest orientation a visitor can get.

**Badges are limited to claims the repository can back:** license (MIT), Python 3.14+, the
`uv` package manager, and the latest GitHub release. There is deliberately **no build or
CI badge**, because there is no CI, and a badge that is permanently "unknown" or, worse,
green by accident is a lie in the one place a visitor looks first. This is enforced by a
scenario in the `repository-presentation` spec rather than left as good intentions.

### D7 — Release history lives in GitHub Releases, and v0.1.0 is published first

`CHANGELOG.md` is deleted and the README links to the repository's Releases page
(`github.com/DisMosGit/plantkeeper`, the `origin` remote). `CONTRIBUTING.md`'s
pull-request checklist loses "Update `CHANGELOG.md` under `[Unreleased]`" and gains
nothing in its place, because recording history is now a release-time act, not a
per-change one.

The hard constraint that drives the ordering: v0.1.0's notes exist **only** in
`CHANGELOG.md`, and there is an `[Unreleased]` section holding the recent documentation
corrections. Both must be published as a GitHub Release **before** the file is deleted, or
they are lost from the project's front page (git history still has them, but nobody
browsing releases would). This is the first task in the change, and it is the rollback
anchor.

*Alternative considered:* generating release notes from conventional commits on tag.
Compatible with the spec, but it needs tooling the project does not have, so it is left as
an optional practice rather than a requirement.

### D8 — `.github/` gets templates only

One pull-request template and one issue template, both minimal, both referencing the
OpenSpec change. No workflows: continuous integration is out of scope by `AGENTS.md`, and
adding a workflow directory that contains only templates keeps that clearly true.

*Alternative considered:* GitHub issue forms with structured fields. Rejected as ceremony
for a single-maintainer project.

### D9 — `CODE_OF_CONDUCT.md` and `SECURITY.md` are skipped

A code of conduct governs a community this project does not have, and a security policy
implies a security surface this project does not have: no authentication, no users, no
network exposure beyond localhost, and `AGENTS.md` states there is no auth to add. Nothing
links to either file, so their absence breaks nothing. The trade-off is that GitHub's
community-profile checklist will show them as missing, which is acceptable and honest.

This is recorded as a decision so it is not silently re-litigated later.

### D10 — References are repointed when a real target exists, and deleted when none does

Ten tracked files reference the two documents. Dispositions:

| File | Reference | Disposition |
|---|---|---|
| `README.md` | documentation index links | Delete the roadmap line; replace the changelog line with the Releases link |
| `CONTRIBUTING.md` | PR checklist step 3 | Delete; the OpenSpec workflow replaces it |
| `docs/architecture.md` | two references | Delete both; the phase narrative they point at is gone |
| `docs/domain.md` | references link | Delete the line |
| `docs/event-sourcing.md` | "the phase's close-out is in ROADMAP.md" | Repoint to the `care-journal` spec |
| `docs/grpc.md` | "`ROADMAP.md` §7" | Repoint to the `integration-contracts` spec |
| `docs/adr/0005-…md` | "Phase 4 adds four process managers (`ROADMAP.md` §4.1–4.6)" and a references link | Repoint to the `process-managers` spec |
| `tools/iot-simulator/README.md` | "`ROADMAP.md` Phase 5 describes the behaviour" | Repoint to the `iot-telemetry` spec |
| `tools/…/scenarios.py` | docstring, "the ones `ROADMAP.md` names" | Reword — the scenarios are named right there in the module |
| `tests/integration/test_sagas.py` | docstring, "(ROADMAP 4.1)" | Reword to name the saga infrastructure instead of a phase |

The two source-file edits are docstrings only: no behaviour, no signature, no test change.

The acceptance test is mechanical — a search of all tracked content for either name must
return nothing — which is stronger than reading the diff, and it is a task in its own right.

### D11 — Three documentation errors found during the backfill, handled deliberately

Grounding twelve specs in the documentation surfaced three inconsistencies. They are fixed
or recorded rather than carried silently:

1. **`docs/cqrs.md` under-reports the catalogue projection.** Its projection table lists
   only `SpeciesUpdated` for `SpeciesProjection`, while the code handles `SpeciesAdded` too
   (`apps/admin/src/plantkeeper/admin/projections/catalog.py` registers both) and the same
   document's prose, two paragraphs later, says the synchronisation publishes both. This is
   a one-line factual error in a file this change already touches — **fixed here**.
2. **`docs/architecture.md` and `docs/events.md` call `SensorOffline` the one event with no
   producer, while `docs/domain.md` lists it as a telemetry event** with a `mark_offline`
   operation. The code has the operation and no caller, so the "no producer" claim is
   right about the running system and the domain list is right about the vocabulary.
   **Recorded here**, and expressed in the `iot-telemetry` spec as the honest requirement
   ("a silent sensor is not announced"), so that implementing the silence timer later
   becomes a normal delta against a known baseline rather than a surprise.
3. **`CONTRIBUTING.md` names a test directory that does not exist** — `tests/contract/` in
   its test-level table; the real path is `tests/unit/contracts/`. **Fixed here**, since
   `CONTRIBUTING.md` is being rewritten anyway and the table would otherwise be rewritten
   with the same error in it.

### D12 — English for anything that lands in OpenSpec

The roadmap is Russian and the rest of the project's documentation is English. The migrated
rules go into English files and the specs are English, so the artifacts match the
surrounding convention. No translation of the historical task records happens, because none
of them are migrated.

## Risks / Trade-offs

- **A baseline that asserts something false is worse than no baseline**, and twelve specs
  written quickly is exactly how that happens. → Every requirement is traceable to a
  document that was drift-corrected days earlier; the digests used to write them were
  verbatim extractions, and the specs deliberately avoid code-level claims that a refactor
  would invalidate. The maintainer reviewing this change can check each spec against the
  document it names, and `openspec validate --strict` gates the structure.
- **v0.1.0's release notes are lost from the front page** if the file is deleted before the
  release is published. → Publishing the release is the first task, with the captured
  `[Unreleased]` entries, and the deletion is a later task that depends on it.
- **A dangling reference survives** somewhere a search misses. → The search is over tracked
  content for both names, with an empty result as the acceptance criterion, and it runs
  after all edits rather than per-file.
- **The commit skill drifts back out of date.** → Its scope list is not restated in the
  skill; it points at `CONTRIBUTING.md`. A task verifies every path and `make` target the
  skill names actually exists.
- **The baseline specs go stale as the code moves.** → That is the intended lifecycle: a
  future change modifies them through a delta, which is the whole point of having them.
  Nothing in them is date-stamped or phase-numbered, so age alone does not invalidate them.
- **Removing two familiar files annoys a returning contributor.** → The README keeps a
  documentation index and adds an OpenSpec section explaining where planned work and
  release history went, so the change is explained where someone would look.
- **The three documentation fixes in D11 expand the change slightly** beyond reference
  repair. → They are one-line corrections in files this change already rewrites, and they
  are called out in the proposal's Impact section rather than buried.

## Migration Plan

Order matters, and each step leaves the repository coherent:

1. **Publish the GitHub Release for v0.1.0** (or confirm it exists), carrying the current
   `CHANGELOG.md` content including `[Unreleased]`. Nothing is deleted until this is done.
2. **Land the twelve baseline specs** in this change and pass strict validation.
3. **Repair references** in the seven files that keep existing content, plus the two
   docstrings, then **delete `ROADMAP.md` and `CHANGELOG.md` in the same commit as the last
   reference repair**, so no commit ever contains a link to a file that does not exist.
4. **Rewrite the front door**: `README.md`, `CONTRIBUTING.md`, the commit skill, and add
   `.github/` templates.
5. **Verify**: the tracked-content search returns nothing; `make lint` and `make test` still
   pass; every command the commit skill names exists in the `Makefile`; every relative link
   in the touched files resolves; the README renders.

**Rollback:** revert the commits. Both deleted files return byte-identical from git history
(`git show 8665cb5:ROADMAP.md`, `git show 8665cb5:CHANGELOG.md`). There is no runtime state,
no schema migration, no data migration and no dependency change — this change touches
documentation, templates and one commit skill, so reverting it cannot corrupt anything.

## Open Questions

- **Should release notes be generated from conventional commits on tag?** Deferrable: the
  spec only requires that history is recorded on a GitHub Release, and doing it by hand
  satisfies that. Revisitable without changing any requirement or task.
- **Should `` `make admin` `` be split into a projections process and an admin process?**
  Out of scope; the current coupling is documented in the README and the CHANGELOG as a
  deliberate choice. Noted so it is not lost, not decided here.
- **Should telemetry ever get a read model?** Out of scope; the specs record the current
  absence as a decision rather than an oversight, which is what makes it cheap to revisit.
