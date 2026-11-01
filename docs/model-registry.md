# Model registry

SynRoute reads `synroute-models.json` from the current directory. Set
`SYNROUTE_MODELS_CONFIG` to use another path. If the file is absent, the
registry checks the standard local LM Studio (`http://127.0.0.1:1234/v1`)
and llama.cpp server (`http://127.0.0.1:8080/v1`) endpoints.

Example configuration (no API keys are stored in this file):

```json
{
  "providers": [
    {
      "id": "lm-studio",
      "name": "LM Studio",
      "kind": "lm_studio",
      "base_url": "http://127.0.0.1:1234/v1",
      "local": true,
      "api_key_env": null,
      "model_ids": [],
      "metadata": {},
      "request_timeout_seconds": 15,
      "enabled": true
    },
    {
      "id": "team-inference",
      "name": "Team inference endpoint",
      "kind": "open_ai_compatible",
      "base_url": "https://inference.example.invalid/v1",
      "local": false,
      "api_key_env": "TEAM_INFERENCE_KEY",
      "model_ids": [],
      "metadata": {
        "team-model": {
          "display_name": "Team model",
          "context_window": 32768,
          "capabilities": ["chat"]
        }
      },
      "request_timeout_seconds": 20,
      "enabled": true
    }
  ]
}
```

Provider kinds are `lm_studio`, `llama_cpp`, and `open_ai_compatible`. Each
provider is queried through `{base_url}/models`. Remote providers are never
queried unless `SYNROUTE_MODELS_ALLOW_REMOTE=true` is set for that invocation.
The secret value itself is read only from the named environment variable and
is never included in registry output or error text.

Environment overrides: `SYNROUTE_MODELS_LM_STUDIO_URL`,
`SYNROUTE_MODELS_LLAMA_CPP_URL`, and `SYNROUTE_MODELS_REQUEST_TIMEOUT_SECONDS`.
The config path is selected by `SYNROUTE_MODELS_CONFIG`.

Run `synroute models`, `synroute models --refresh`, or add `--json` for the
structured provider and model records. `--refresh` explicitly triggers a live
`/models` request; an ordinary `models` invocation also checks endpoints to
report current availability. SQLite is not used. Provider metadata is retained
separately from user overrides. Capability and context values remain unknown
unless explicitly supplied by the provider or config.
