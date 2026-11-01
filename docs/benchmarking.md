# Repository model benchmarks

Run `synroute benchmark <repository>` to evaluate the available configured
models against a compact prompt generated from the repository profile. Options:

- `--rounds N`: request one to three rounds (default 3).
- `--model ID`: benchmark one model ID.
- `--timeout SECONDS`: per-request timeout (default 60, maximum 300).
- `--json`: emit the persisted run, individual rounds, and aggregate scores.

Only project summary, language counts, manifest and test paths, and directory
roles are sent. Repository file contents are not included in the prompt. Remote
providers require `SYNROUTE_MODELS_ALLOW_REMOTE=true`, matching model registry
consent. API keys are read only from each provider's configured environment
variable name.

Responses are capped at 50 generated tokens and 16 KiB stored bytes. Provider
failures are recorded independently; responses, hashes, token counts reported
by the provider, latency, four heuristic score components, and failure details
are persisted in SQLite migrations. Aggregate scores are means over successful
rounds, while latency and token usage remain separate measurements.

Scoring is deterministic and versioned, but it is heuristic. In particular,
heuristic scoring cannot prove technical correctness. Scores must not be used
as verified quality claims or as model-selection decisions.
