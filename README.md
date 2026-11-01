# SynRoute

SynRoute is a repository-aware model selector and coding-agent orchestration engine. It profiles repository structure, discovers configured OpenAI-compatible models, uses persisted benchmark evidence when available, and keeps coding-agent execution behind the existing isolated execution and independent verification boundaries.

## Install and build

Requirements: a supported Rust toolchain (Cargo) and SQLite support supplied by the bundled SQLx configuration. Docker is needed only for the existing sandbox verification demo. From this checkout:

```sh
cargo build --release
```

Run the built CLI as `./target/release/synroute`. The database defaults to `sqlite://synroute.db`; override it with `--database` or `SYNROUTE_DATABASE`.

## Model configuration

SynRoute reads `synroute-models.json` in the current directory, or the path in `SYNROUTE_MODELS_CONFIG`. An example with local LM Studio and llama.cpp plus Cerebras is available at [`examples/synroute-models.example.json`](examples/synroute-models.example.json). Copy it to `synroute-models.json` and adjust model IDs and endpoint URLs. API key values belong only in environment variables; configuration stores the environment variable *name*.

LM Studio: load a model in LM Studio, start its local server, and use the default OpenAI-compatible URL `http://127.0.0.1:1234/v1`.

llama.cpp: start `llama-server` with its OpenAI-compatible API enabled (commonly port 8080); the default URL is `http://127.0.0.1:8080/v1`.

Cerebras: configure its model ID in the example, set `CEREBRAS_API_KEY` in your shell or secret manager, and explicitly opt in for remote requests with `SYNROUTE_MODELS_ALLOW_REMOTE=true`. SynRoute does not read keys from config files or print them. Remote providers remain blocked without this consent flag. Other generic OpenAI-compatible remote providers can be configured in the same way.

Registry URL and timeout overrides include `SYNROUTE_MODELS_LM_STUDIO_URL`, `SYNROUTE_MODELS_LLAMA_CPP_URL`, `SYNROUTE_MODELS_CEREBRAS_URL`, and `SYNROUTE_MODELS_REQUEST_TIMEOUT_SECONDS`. Local providers default to a 60-second request timeout; set `request_timeout_seconds` per provider or use the environment override to change it (1–300 seconds).

## Commands

```sh
synroute profile ./repo
synroute models --refresh
synroute benchmark ./repo --rounds 3
synroute select ./repo --task "Fix CPU arithmetic flags"
synroute ask ./repo --task "Explain the CPU module"
synroute demo ./repo --task "Explain the CPU module"
synroute demo ./examples/demo-project
```

Use `--json` with `models`, `benchmark`, `select`, `ask`, and task `demo` for machine-readable output. The task demo reports repository analysis, discovered available/unavailable providers, task category, stored evidence or metadata fallback, the selection explanation, an actual model response when inference succeeds, and coordinator/verification status. Profile metadata and path names may be sent to an explicitly consented remote model; repository source file contents are not included in inference requests.

The no-task `demo` command remains the fixed, explicitly authorized pytest contract running through Docker. It does not invoke a coding agent.

## Example task demo output

This is an output shape example only; bracketed values are placeholders, not reported measurements or a live run:

```text
SynRoute task demo
1. Repository analysis
example-repo — Rust at <revision> (fingerprint <profile fingerprint>)
2. Model discovery and task classification
Task category: Debugging
Selection mode: MetadataFallback
Available: lm-studio / <discovered model id>
Recommendation: lm-studio / <discovered model id> (<display name>)
Benchmark evidence: none; recommendation uses labelled metadata fallback.
3. Inference
Actual response from lm-studio / <discovered model id>:
<actual response text returned by that model>
4. Coding-agent execution
Status: not_started_or_unsupported
Execution detail: agent codex-app-server does not provide a proven controlled execution boundary
5. Independent verification
Status: not_run
No persisted verification evidence is available.
```

SynRoute currently fails closed when a persistent, safely confined Codex runtime and scoped credential broker are unavailable. Therefore the task demo may complete repository analysis, selection, and inference while coding-agent execution and verification remain unavailable. It never treats an inference answer as a code change or verification result. Heuristic benchmark scoring is evidence for ranking and cannot prove technical correctness. No model scores, results, or execution success are fabricated.

More detail: [model registry](docs/model-registry.md), [benchmarking](docs/benchmarking.md), [model selection](docs/model-selection.md), [inference](docs/inference.md), and [security](docs/security.md).
