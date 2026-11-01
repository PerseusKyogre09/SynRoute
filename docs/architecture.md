# Initial architecture

Synroute starts as a modular monolith. `core` owns serializable task and lifecycle contracts; `repository` discovers Git identity and project hints; `workspace` owns detached worktree lifecycle; `agent` is the provider boundary; `verification` runs deterministic checks and records raw output; `execution` will coordinate state transitions; `memory` will compute profiles only from persisted evidence. API and CLI are presentation surfaces over the same application services.

## First vertical slice

The initial slice is repository inspection, pinned-revision worktree setup, Codex App Server execution, independent verification, and SQLite event/evidence persistence. A Git worktree provides isolation from the developer checkout but is not a security boundary. Untrusted repository commands must not run until a container or equivalent sandbox implementation exists with resource and network controls.

## Dependencies

Tokio/Axum for async runtime and future API; Serde for contracts; SQLx SQLite for local persistence; Clap for CLI; tracing for structured logs. Provider protocols stay behind `CodingAgent`. No model ranking or learning is enabled until real evidence exists.

## Decisions

- Initial DB: local SQLite, with migrations committed.
- Initial repository pin: full Git commit hash.
- Verification: explicit argv arrays, never shell strings; success requires every configured check to exit zero.
- Worktrees: detached at the pinned commit, with cleanup explicit.
- Security: worktree alone is not sandboxing; execution remains gated until sandbox support is implemented.
