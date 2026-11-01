# Sandbox security boundary

## Implemented boundary

Repository verification is routed through `SandboxProvider`; there is no host-process fallback. The policy matches exact executable names and returns allowed, denied, approval-required, or unsupported. Only allowed structured argv commands are submitted. Empty verification contracts return `incomplete`, never success. The provider API accepts explicit environment variables, per-command timeouts, resource limits, bounded output, and a cancellation signal.

The Docker provider uses the installed Docker CLI as a structured host control interface. For every command it creates a uniquely named container with a random UUID and Synroute ownership labels, runs the requested executable and arguments directly (no shell construction by Synroute), then checks ownership labels before removal. Cancellation or timeout kills the attached Docker CLI and force-removes only the specifically labelled container created for that execution. Both paths remove its anonymous Docker volumes. Normal completion uses non-forced removal. If the CLI times out before returning a container ID, cleanup uses only the generated execution-scoped name after label verification. If ownership cannot be verified, cleanup refuses and reports the error.

Container settings include a non-root numeric UID/GID, `--network=none`, all Linux capabilities dropped, `no-new-privileges`, read-only container root, an explicitly writable workspace bind mount, a bounded noexec `/tmp` tmpfs, CPU/memory/PID limits, and Docker's default seccomp profile. Only explicitly configured environment variables are passed. No Docker socket or unrelated host paths are mounted.

## Authorization and usage

The policy is a command-level gate, not a proof that an allowed program's arguments are safe. A permitted build tool can execute repository build scripts inside the container. Policies should be configured for the task and trust level; approval-required commands remain skipped until approval is explicitly supplied. Repository-discovered hints are not executed automatically. Agent-generated shell strings are not accepted by this interface.

To opt into the real Docker integration test, set `SYNROUTE_DOCKER_IMAGE` to an already available image that contains `sh` and run:

```sh
SYNROUTE_DOCKER_IMAGE=your-image cargo test docker_integration -- --ignored --nocapture
```

The integration test is ignored during the ordinary suite so tests do not unexpectedly contact a daemon or pull an image.

## Assumptions and limitations

The Docker CLI and daemon are trusted, local host components. Access to a rootful Docker daemon is effectively host-privileged; a compromised daemon, kernel, or container runtime can defeat this boundary. Docker containers share the host kernel and are not a complete defense against kernel vulnerabilities or all side channels. Prefer a rootless Docker daemon where operationally suitable. The selected image must be trusted and available locally; Synroute does not pull images. Linux container support is assumed.

The command timeout applies to the container execution and cleanup. Docker CLI daemon operations have bounded but separate control timeouts. Cancellation is cooperative through the provided cancellation receiver; aborting the Rust task itself is not a supported cleanup protocol and may leave an owned container for manual inspection. The Docker provider does not yet persist container ownership across Synroute process restarts.

Workspace ownership remains process-local as in Phase 1. Durable metadata could be stored in an application-owned registry outside worktrees, bound to execution ID, canonical repository/workspace paths, and Git registration, with strict validation on load. That registry is deferred until storage/restart-recovery design exists; uncertain ownership continues to fail closed.

## Docker references

The implementation uses Docker's documented container-create options for resource constraints, read-only root filesystems, dropped capabilities, no-new-privileges, and network selection: [container create reference](https://docs.docker.com/reference/cli/docker/container/create/) and [container run reference](https://docs.docker.com/reference/cli/docker/container/run/). Docker documents that access to a rootful daemon is highly privileged; see [Docker Engine security](https://docs.docker.com/engine/security/) and [rootless mode](https://docs.docker.com/engine/security/rootless/).
