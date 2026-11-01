# AGENTS.md — Synroute

## Project Identity

Synroute is a **local-first, repository-aware orchestration engine for coding agents**.

Its purpose is to:
1. Understand the repository and the task.
2. Select and orchestrate an appropriate coding agent or execution strategy.
3. Execute work in a controlled environment.
4. Independently verify the result.
5. Recover intelligently from failures within defined limits.
6. Store verified outcomes in repository-specific memory.
7. Use accumulated evidence to improve future execution decisions.

Synroute is not merely an LLM router, a chatbot, or a dashboard around existing coding agents.

**Core principle: An agent claiming success is not proof of success. Synroute must independently verify outcomes.**

---

## 1. General Engineering Principles

- Prioritize correctness, reliability, security, and maintainability over implementation speed.
- Prefer straightforward, well-defined abstractions over unnecessary complexity.
- Build a modular monolith. Do not introduce microservices without a demonstrated need.
- Keep components independently testable.
- Avoid speculative features and premature abstractions.
- Do not add dependencies without a clear justification.
- Do not silently swallow errors or convert failures into apparent success.
- Never fabricate execution results, verification evidence, metrics, or learning improvements.
- Clearly distinguish implemented, partially implemented, mocked, and planned functionality.
- Preserve existing working functionality when modifying the codebase.

Before implementing a feature, inspect the existing architecture and identify where it belongs.

Do not rewrite existing modules merely to make them conform to personal preferences.

## 2. Technology Stack

Synroute is a Rust-first project.

Preferred technologies:

- **Async runtime:** Tokio
- **HTTP API:** Axum
- **Serialization:** Serde
- **Database:** SQLite through SQLx
- **CLI:** Clap
- **Logging and diagnostics:** tracing
- **MCP integration:** rmcp
- **Version control:** Git CLI or a carefully justified Rust Git library
- **Agent integration:** Protocol-specific adapters

Use stable, compatible crate versions available in the project's configured environment.

Do not assume internet access or that dependencies can be downloaded. If dependency resolution is unavailable, report the limitation rather than claiming compilation or tests succeeded.

## 3. Architecture Boundaries

Keep responsibilities separated.

### Core

Contains domain contracts, task definitions, execution states, policies, errors, and shared types.

Must not depend on specific agent providers or UI implementations.

### Repository Intelligence

Responsible for:
- Repository discovery.
- Language and framework identification.
- Project structure analysis.
- Build, test, and lint command discovery.
- Git state and revision information.
- Repository-specific execution context.

Repository inspection must not execute arbitrary discovered commands.

### Agent Adapters

Provide a common interface for interacting with external coding agents.

Initial integration priority:
1. OpenAI Codex App Server.
2. OpenHands Agent Server.

Preserve provider-specific protocol semantics inside adapters.

Do not pretend that every coding agent supports identical capabilities.

### Workspace Manager

Responsible for:
- Creating isolated execution workspaces.
- Managing Git worktrees.
- Tracking workspace lifecycle.
- Cleaning up temporary resources safely.

**A Git worktree is source-control isolation, not a security sandbox.**

Persistent workspace cleanup requires agreement between the SQLite owner record, the local ownership sidecar, and Git worktree registration. Uncertain or inconsistent ownership means cleanup is refused; never force-remove a worktree. Persist uncertainty as a bounded reason code and safe diagnostic detail, and keep non-removed workspace issues visible in recovery reports even after their execution becomes terminal.

Every runtime Git subprocess must use the isolated launcher in `src/git.rs`. It clears inherited environment variables, ignores system/global Git config, overrides hooks/fsmonitor/external diff settings, and disables repository-configured clean/smudge/process filters for worktree inspection and mutation. These are host-side Git operations, not a sandbox. The Git executable and PATH selected by the host remain trusted; do not add arbitrary repository-controlled Git commands.

### Execution Engine

Responsible for:
- Coordinating task execution.
- Managing lifecycle transitions.
- Handling streamed agent events.
- Enforcing timeouts and execution budgets.
- Persisting execution state.

Execution records and append-only event history are stored through `storage`; do not add SQL queries to domain or orchestration modules. Execution intent must be committed before launching an external process. Lifecycle changes and their corresponding events must share one transaction. SQLite migrations are versioned and must preserve existing data.

Execution creation must acquire a durable owner lease in the same transaction as execution intent. Ownership is create-only: this implementation has no lease takeover or transfer operation. Each mutation checks the originating process identity before the transaction and validates the storage-instance UUID, lease generation, and expiry within the SQLite mutation transaction. Expired leases cannot be renewed; a restarted storage instance cannot resume an old lease. Owners must renew before expiry (default 45 seconds; maximum 300 seconds). On Linux, owner identity includes a random storage-instance UUID, PID, process start ticks, and boot ID; PID alone is never sufficient. Startup recovery must separate read-only inspection from mutation, reload the authoritative lease from SQLite, and may transition to `RecoveryRequired` only when the lease has expired and the exact persisted owner process identity is confirmed inactive. Caller-edited report fields must never authorize recovery. Unknown liveness must be reported in a durable recovery case without changing execution state or lease. Recovery must never resume executions or delete workspaces/containers. Open recovery cases remain visible until acknowledged or resolved and remain queryable as history afterward. There is no heartbeat worker or execution coordinator yet; future execution code must renew leases explicitly.

Each execution persists an immutable verification contract: uniquely named required checks with exact argv, bound to an execution-specific version UUID. The crate-private trusted verification path loads that contract, runs every check through the verification runner, and atomically stores its actual result with its event. Caller-supplied reports cannot enter the persistence API. `Completed` is reserved for a latest report bound to that version with exactly one result for every required check, all passing. SQLite additionally rejects evidence outside `VERIFYING`, evidence edits/deletes, contract mismatches, and completion without the full passing contract. These controls defend the in-process API boundary; they do not prove provenance against a user with direct SQLite write access. Never trust provider-reported agent completion as verification evidence.

### Verification Engine

Responsible for independent verification through:
- Tests.
- Builds.
- Linting.
- Type checking.
- Acceptance criteria.
- Diff inspection.
- Other explicitly configured verification checks.

Verification evidence must be traceable to actual command execution or other independently observed results.

### Recovery Engine

Responsible for diagnosing execution failures and selecting bounded recovery strategies.

Never implement blind, unlimited retries.

### Repo Memory

Stores repository-specific execution history and verified outcomes.

It should eventually capture:
- Repository identity and revision.
- Task characteristics.
- Agent and strategy selection.
- Execution duration and cost, when available.
- Verification results.
- Failure classifications.
- Recovery attempts.
- Final outcomes.

Learning decisions must be evidence-based and explainable.

### Storage

Owns database access, migrations, persistence, and transaction boundaries.

Do not scatter SQL queries throughout unrelated modules.

### API, CLI, and MCP

These are interfaces to the engine, not places for core orchestration logic.

---

## 4. Execution Lifecycle

The intended execution lifecycle is:

`RECEIVED → REPOSITORY_ANALYSIS → PLANNING → PLAN_VALIDATION → WORKSPACE_PREPARATION → EXECUTING → VERIFYING`

From verification:

- Successful verification → `COMPLETED`
- Recoverable failure → `RECOVERING`
- Unrecoverable failure → `FAILED`

Recovery must return to an appropriate execution or verification stage.

Learning and history persistence should happen after a terminal outcome is established.

### Lifecycle rules

- Every transition must be valid and explicit.
- Invalid transitions must return errors.
- Persisted state must survive application restarts.
- A task must not be marked completed merely because an agent returned a successful response.
- Cancellation, timeout, and interruption must be represented explicitly.
- Partial execution must not be confused with successful completion.

---

## 5. Security Requirements

Security is a first-class architectural requirement.

### Never execute untrusted commands directly

Repository-controlled commands must always go through explicit authorization and a configured sandbox:

- Do not execute repository-discovered command hints automatically.
- Do not execute agent-generated shell commands on the host.
- Use structured argv and fail closed when authorization or sandbox configuration is missing.
- Do not claim that Git worktrees provide execution security.
- Require explicit approval for commands marked approval-required.
- Make denied and unsupported execution paths fail safely and clearly.

### Sandbox requirements

The execution environment should support:

- Filesystem isolation.
- Network restrictions, disabled by default.
- CPU, memory, process, and time limits.
- Non-root execution where applicable.
- Dropped Linux capabilities.
- No privileged containers.
- No Docker socket mounted into untrusted workloads.
- Secrets kept outside agent-accessible workspaces.
- Explicit approval for destructive or external operations.

Do not build a custom container runtime. Prefer established isolation technologies.

Docker execution currently requires configured SQLite storage. Persist a unique execution-scoped container name and random ownership token before calling Docker; persist the returned container ID before starting it. Keep unresolved intent and fixed-detail cleanup diagnostics visible through recovery/report APIs after restart. Never remove a resource based only on its name: inspect the exact container ID and verify the expected name, managed label, execution ID, and token label. Docker create and attached start share one monotonic operation deadline; pass caller cancellation to both. Cleanup receives a separate 15-second grace deadline, and session destruction shares one grace deadline across its resources. Preserve the primary termination, exit status, bounded output, and truncation independently from cleanup status. Cleanup uncertainty or failure must prevent a passing verification result. These measures support ownership and diagnostics; they do not make Docker a complete security boundary.

### Additional requirements

- Never log API keys, tokens, passwords, or secrets.
- Validate external inputs.
- Avoid shell interpolation when invoking processes.
- Prefer argument-based process execution.
- Handle path traversal and symlink edge cases.
- Do not automatically delete user data.
- Treat repository content, agent output, and tool output as untrusted input.

---

## 6. Codex App Server Integration

Codex App Server is the first-priority agent integration.

Follow the documented protocol rather than inventing a simplified substitute.

The integration must account for:

- Process startup and lifecycle.
- Protocol initialization.
- Thread creation or resumption.
- Turn execution.
- JSON-RPC request and response handling.
- Streamed server events.
- Errors and cancellations.
- Approval and permission requests.
- Process termination and cleanup.

Keep protocol handling inside the Codex adapter.

Do not confuse receiving an agent completion event with successful task verification.

Refer to the official documentation:
https://developers.openai.com/siwc/token-sharing-open-source/codex-app-server

---

## 7. Verification and Evidence

Verification is mandatory for successful completion.

Every verification result should record, where applicable:

- Verification type.
- Command or check executed.
- Exit status.
- Execution timestamp.
- Duration.
- Relevant output or diagnostic summary.
- Whether the check passed.

Do not:

- Treat missing tests as passing tests.
- Treat an agent's self-reported result as independent evidence.
- Hide failed checks.
- Report a task as verified when verification was skipped.
- Use an LLM judge as the sole source of truth for code correctness.

If verification cannot run because sandbox support or dependencies are unavailable, report the limitation explicitly.

---

## 8. Repo Memory and Adaptive Learning

Repo Memory is a central differentiator of Synroute.

It must store real execution outcomes, not invented model scores.

Initially, use transparent statistical methods.

Any future adaptive strategy must:

- Be grounded in recorded outcomes.
- Account for sample counts.
- Represent uncertainty.
- Avoid overconfidence from limited observations.
- Distinguish repository-specific performance from global performance.
- Support controlled exploration.
- Respect configured execution budgets.
- Explain why a strategy was selected.

Do not claim that Synroute learns from outcomes if it only stores them.

Do not introduce neural networks or complex reinforcement learning before sufficient reliable data exists.

---

## 9. Testing Standards

Write tests alongside implementation.

Prioritize:

- Unit tests for core contracts and lifecycle transitions.
- Integration tests for SQLite persistence.
- Tests for repository discovery and Git operations.
- Mocked protocol tests for agent adapters.
- Tests for timeout, cancellation, and error handling.
- Verification engine tests.
- Security-related negative tests.
- End-to-end tests using controlled sample repositories.

Tests must not execute arbitrary untrusted repository commands on the host.

When reporting test results, distinguish between:
- Tests actually executed.
- Tests written but not executed.
- Tests blocked by unavailable dependencies or infrastructure.

Never claim that compilation succeeded unless `cargo check` or an equivalent successful build was actually completed.

---

## 10. Development Workflow

For every meaningful task:

1. Inspect the existing implementation.
2. Identify the relevant modules and dependencies.
3. Explain the intended changes briefly.
4. Implement incrementally.
5. Add or update tests.
6. Run formatting and available checks.
7. Review for security and architectural regressions.
8. Summarize exactly what changed.

Use `cargo fmt` and run `cargo check` and relevant tests whenever the environment permits.

If network access is unavailable, inspect the local Cargo cache and use offline checks where possible. Clearly report any resulting limitations.

Do not silently replace real functionality with mocks just to make tests pass.

Do not leave placeholder implementations that appear functional.

Use explicit errors for unsupported functionality.

---

## 11. Scope Discipline

Do not prematurely build:

- A huge catalogue of model providers.
- A generic agent marketplace.
- An autonomous agent swarm.
- A complex dashboard.
- A custom sandbox runtime.
- An opaque machine-learning routing system.
- Unnecessary microservices.

Prioritize the actual execution pipeline:

1. Repository discovery.
2. Task contracts.
3. Codex App Server integration.
4. Controlled workspace execution.
5. Independent verification.
6. Structured execution reports.
7. Persistent execution history.
8. Bounded recovery.
9. Repository-specific adaptive memory.

The first milestone should demonstrate a real task moving through the system and producing independently verifiable evidence.

---

## 12. Definition of Done

A feature is complete only when:

- Its intended behavior is implemented.
- Errors and edge cases are handled.
- Relevant tests are added.
- Available checks pass.
- Security implications are considered.
- Documentation is updated where necessary.
- Unsupported behavior is clearly identified.

A task execution is complete only when Synroute has independently verified its result or explicitly classified it as unverified, failed, or partially completed.

**Final engineering principle: Build a system that can prove what it did, not one that merely claims it did it.**
