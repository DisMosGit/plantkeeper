---
name: commit
description: "Review all uncommitted changes and split them into separate logically grouped conventional commits (type(scope): summary), ordered from foundational to dependent, staging files or hunks explicitly. Use when the user asks to commit, to split uncommitted work into commits, or to write commit messages."
allowed-tools: Bash(git:*)
license: MIT
metadata:
  author: DisMosGit
  version: "2.0"
  adapted-for: plantkeeper
---

Turn the current uncommitted work into a sequence of self-contained conventional commits, ordered so every commit builds on the one before it.

**Input**: Optionally a subset of paths, a requested grouping, or a scope. Without one, the scope is everything uncommitted — staged, unstaged, and untracked.

**Steps**

1. **Survey the worktree**

   Run and read all of:
   - `git status --short` — staged, unstaged, untracked
   - `git diff --stat` and `git diff` — unstaged content
   - `git diff --cached` — already-staged content
   - `git log --format='%s%n%n%b' -10` — the message style this repo actually uses. Nearly every commit here carries a body, so read a few rather than only the subjects.
   - `git check-ignore -v <path>` — separate intentional content from ignored output. `openspec/` and `.agents/` are untracked but **not** ignored, so they are meant to be committed; never infer "ignored" from a filename alone.

   Read enough of every changed file to know *what it does*, not just which directory it sits in. Never group by filename alone: a single file often carries two concerns, and two files often carry one.

2. **Group into concerns**

   Each group is one concern that stands alone and reads coherently on its own. Name every group by the message you intend to write for it before touching the index.

   - A module's tests ride with the code they test (`tests/unit/<layer>/…` with `packages/<layer>/src/…`), and an integration test rides with the persistence or messaging change it covers. A separate `test(<scope>)` commit is for standalone test infrastructure only (`tests/conftest.py`, shared fixtures).
   - Config settings ride with the feature that consumes them; an event or DTO that crosses a boundary rides with the context that publishes it.
   - Docs describing changed behaviour ride with the change; substantial prose becomes its own `docs(<scope>)` commit.
   - OpenSpec artifacts: the change plan first, then the spec delta with the code, and the task tick-off (`- [ ]` → `- [x]` in `tasks.md`) in the same commit as the work that task describes. Never tick a task ahead of its work.
   - A regenerated artifact rides with the change that made it stale: the committed diagrams under `docs/diagrams/` are drift-tested, so regenerate and commit them with the code, never on their own.
   - Formatting-only churn in files unrelated to a change is never mixed into it — leave it out, or ask (step 8). A repo-wide `make format` pass produces exactly this churn.

3. **Order foundational to dependent**

   Dependencies point forward: no commit may need content that only lands in a later one. This repo's ladder:

   openspec plan → `domain` (aggregates, value objects, events; no I/O) → `application` (commands, queries, sagas, ports) → `infrastructure` (persistence, messaging, external ACLs, cache, DI) → `apps/*` (`api`, `workers`, `admin`) → contracts and diagrams → docs → task tick-offs.

   `tools/iot-simulator` depends on none of those layers and stays off the ladder; the import rules pin that. A commit that would make the domain import infrastructure is on the wrong rung. Not every change uses every rung; preserve the relative order of the rungs it does use.

4. **Write the messages**

   `type(scope): summary`, e.g. `feat(care): add AdaptiveWateringSaga`.

   - **Types**: `feat`, `fix`, `docs`, `test`, `chore`, `build` are in use here; `refactor` and `perf` are allowed when none of those fit. There is no `ci` type — this project has no continuous integration, and adding one is out of scope.
   - **Scopes**: take them from the scope list in `CONTRIBUTING.md` (Commits → Scopes). That section is the only copy of the list; do not restate it here, and do not invent a scope outside it without saying why. Pick the narrowest scope that is true.
   - Imperative mood, lowercase, no trailing period. Keep the subject short — the longest subject in this history is 83 characters, so treat a short line as the target rather than a limit to fill.
   - **A body is required.** Nearly every commit in this history carries one; the earliest scaffolding commits are the only exceptions. It says what changed and, more usefully, why it had to change that way, wrapped near 80 columns — typically fifteen to three hundred words, with a median near a hundred, often as bullet points. Write a subject-only commit only when the user explicitly asks for one.
   - No `Co-Authored-By` or "Generated with" trailers — this repo's history has none, and `CONTRIBUTING.md` records the rule.

5. **Stage explicitly**

   Stage one group at a time with explicit paths: `git add -- <path>…`.

   When one file's hunks belong to different groups, split them: `git add -p` when the session is interactive, otherwise build a filtered patch and `git apply --cached <patch>`. Confirm what is actually staged with `git diff --cached --stat` and `git status --short` before committing.

   Never `git add -A`, `git add .`, `git add -f`, or `git commit -a`. Committing is not editing: never modify tracked content to force a clean split. If a file genuinely mixes unrelated concerns, that is an ambiguity — ask (step 8).

6. **Run the safety gate before every commit**

   Read the full staged diff — not just the stat — then check:

   - **Secrets**: the real credentials — the Trefle token among them — live in a locally created `.env`, which is gitignored (`.gitignore` covers `.env`, `.env.*` and `*.env`) and so is normally absent from a fresh checkout. Only `.env.example` is committed, and it holds placeholders — `TREFLE_TOKEN=` is empty there. Never stage `.env`, never stage a credential to "fix it later", and watch for `*.key` and `*.pem`.
   - **Ignored and build output**: never force-add what `.gitignore` covers — `.venv/`, `.docs/` (private working notes), `docs/coverage.html`, the generated gRPC stubs under `apps/api/src/plantkeeper/api/grpc/generated/`, and the generated contract documents `docs/openapi.json`, `docs/asyncapi-read.json` and `docs/asyncapi-write.json` (along with `docs/diagrams/*.png` and `*.svg`). The deliberate exception: `openspec/` and `.agents/` are untracked but **not** ignored, so they belong in the history.
   - **Generated files**: `docs/diagrams/event-flow.md` and `docs/diagrams/sagas.md` are rendered from the event and process-manager registries and **are** committed, because GitHub renders them; `tests/unit/docs` and `tests/unit/contracts` fail when they drift. Regenerate with `make diagrams` (or `make contracts`) and commit the result with the code that changed it — never hand-edit a diagram. `uv run python tools/contracts.py --check` reports a stale committed artifact.
   - **Build**: the gate is `make lint` (ruff check, ruff format --check, `mypy --strict` and import-linter) together with `make test`; `make coverage` reports each layer against its documented floor, and the floors live in `README.md` and in the `coverage` target rather than here. For a change's spec delta also run `openspec validate "<name>" --strict`. `make format` rewrites files in place — re-check `git status` afterwards and put each resulting edit in the commit it belongs to, or ask. Never commit a tree the commit itself breaks.
   - **Hooks**: pre-commit runs `ruff check --fix`, `ruff format` and `mypy`. Never `--no-verify`. If a hook fails, fix the cause and re-stage.

7. **Commit and confirm the sequence**

   Commit each group with its message: `git commit -m "<subject>" -m "<body>"`. Then re-run `git status --short` before starting the next group. Never `--amend` unless the user asks for it, and never amend a commit that may be pushed — check `git branch -r --contains <sha>` first.

8. **Stop and ask when a grouping is ambiguous**

   Ask — with the concrete candidates, the exact paths or hunks each would take, and a recommended option — when:

   - a file's hunks mix unrelated concerns and cannot be split without editing it;
   - a change plausibly belongs to two groups, or its tests/docs could accompany either of two commits;
   - formatting-only churn lands in files unrelated to the change;
   - a file's intent is genuinely unclear, or work is already partly committed so an amend would be needed;
   - credential-looking content appears in the diff — never rewrite history to remove an already-committed secret, stop and report it;
   - OpenSpec task tick-offs disagree with the code actually present;
   - the user's requested grouping conflicts with a dependency order.

   Otherwise proceed: the instruction is to ask on ambiguity, not to gate every commit.

9. **Verify and report**

   After the last commit, check the result:

   - `git log --oneline <original-head>..HEAD` — the commits, in order
   - `git status --short` — only intentionally-left changes remain

   Report each commit with its hash, message, and files/hunks; what was intentionally left uncommitted and why; anything the safety gate skipped; and any commit that did not come out self-contained.

**Guardrails**

- Never push, create or delete branches, rebase, reset, or drop stashes.
- Never rewrite history — not even to remove a secret; stop and report instead.
- Never commit unless committing is what was asked for.
- In plan mode, present the grouping plan for approval instead of committing.
- Grouping is a judgment call about the user's intent: when the diff does not settle it, ask rather than guess.
