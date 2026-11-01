# Inference and `ask`

`synroute ask <repository> --task "Explain the CPU module"` discovers models,
uses the repository-aware selector, and sends a chat-completions request to the
selected available model. Add `--json` for the selection and inference response
records. The command is an inference interface; it does not run a coding agent.

LM Studio, llama.cpp, generic OpenAI-compatible endpoints, and Cerebras use the
same provider request path. Cerebras is configured by default at
`https://api.cerebras.ai/v1` with the environment variable name
`CEREBRAS_API_KEY`; the key value is never stored in the provider configuration.
Other providers use their configured `api_key_env` variable name. Remote model
discovery and inference require `SYNROUTE_MODELS_ALLOW_REMOTE=true`.

The request includes repository profile metadata and relevant file paths only.
It never reads or transmits source file contents. The response reports the
actual provider text, latency, and token counts when supplied. If selection or
inference fails, the CLI reports that state without creating a response.

Local provider calls default to a 60-second timeout, matching the benchmark
client's default. Configure `request_timeout_seconds` per provider or set
`SYNROUTE_MODELS_REQUEST_TIMEOUT_SECONDS` (1–300 seconds). Inference responses
remain bounded to 1 MiB, and `ask` caps generated output at 256 tokens to avoid
unnecessarily long local generations. Benchmarks remain limited to 50 tokens
and 16 KiB.
