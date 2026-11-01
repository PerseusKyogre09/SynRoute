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

### Execution Engine

Responsible for:
- Coordinating task execution.
- Managing lifecycle transitions.
- Handling streamed agent events.
- Enforcing timeouts and execution budgets.
- Persisting execution state.

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

Until a real sandbox implementation exists:

- Do not execute arbitrary commands discovered from a repository.
- Do not execute agent-generated shell commands on the host.
- Do not claim that Git worktrees provide execution security.
- Restrict verification to explicitly approved, trusted operations.
- Make unsupported execution paths fail safely and clearly.

### Future sandbox requirements

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