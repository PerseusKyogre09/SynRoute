# Initial architecture

Synroute is a modular monolith. `core` owns serializable task contracts and validates lifecycle edges; `repository` discovers a canonical Git root, pinned revision, manifest hints, and the caller's working subdirectory; `workspace` creates Git worktrees and tracks ownership for the current process; `agent` is the provider boundary; `verification` represents deterministic check evidence; `execution` currently re-exports lifecycle contracts; `memory` is not implemented.

## Current phase boundary

Repository discovery and worktree lifecycle helpers are implemented. Discovery treats manifest-derived commands as informational and never runs them. Worktree removal requires an ID and path registered by the current `WorkspaceManager`, confirms that Git still registers it, refuses modified or untracked content, and uses non-forced Git removal. The ownership registry is in-memory: after process restart, the manager refuses cleanup because it cannot establish ownership. Git worktrees provide source-control isolation only, not execution security.

Execution orchestration, Codex App Server, independent sandboxed verification, SQLite persistence, recovery, and repository-specific learning are not implemented. The verification helper remains an unsandboxed process runner and must not be used for untrusted repository commands.

## Decisions

- Initial DB schema is SQLite, but there is no storage implementation yet.
- Repository identity is pinned to the current full Git commit ID; a nested input path is retained separately.
- Project manifests are checked at repository root. Command hints are not executed.
- Lifecycle transitions are centrally validated. `Completed` means independent verification succeeded; agent-reported completion is a separate observation. Partial, unverified, failure, cancellation, interruption, and timeout are explicit terminal classifications.
- Worktrees are detached and tracked by a process-local manager. Cleanup never forces removal and refuses unknown ownership or dirty state.
