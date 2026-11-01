# Architecture

Synroute is a modular monolith. `core` owns task contracts and lifecycle transitions; `repository` discovers a canonical Git root and project hints; `workspace` creates detached worktrees and tracks current-process ownership; `sandbox` defines provider-independent execution types, authorization policy, and a Docker CLI provider; `verification` authorizes and runs explicit argv checks only through a configured sandbox. `agent` remains an adapter boundary. `execution` currently re-exports lifecycle contracts; persistence, orchestration, Repo Memory, Codex integration, and API execution endpoints are not implemented.

## Phase 2 execution boundary

Verification fails closed when an allowed command has no sandbox provider/configuration. Denied, unsupported, and approval-required commands are not executed. Empty contracts are incomplete. Docker uses non-root execution, no network, dropped capabilities, no-new-privileges, read-only root, explicit workspace and temporary mounts, CPU/memory/PID limits, bounded stdout/stderr, timeouts, cooperative cancellation, and label-checked cleanup. See [security.md](security.md) for assumptions and operational limits.

The Docker provider is not a complete host security guarantee. It trusts the local CLI/daemon and host kernel. Git worktrees are not sandboxes. The unsandboxed verification helper has been removed from the public path and cannot execute repository commands directly.

## Persistence and ownership

SQLite schema exists, but storage integration does not. Workspace ownership remains process-local: after restart cleanup is refused. Durable ownership metadata is deferred rather than inferred from a Git worktree alone.
