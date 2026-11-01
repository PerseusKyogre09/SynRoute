# Evidence-based model selection

Run `synroute select <repository> --task "Fix CPU arithmetic flags"` to classify
the task, discover currently available configured models, and rank them against
successful benchmark rounds for the repository's current fingerprint. Add
`--json` for the complete selection report. Remote discovery uses the model
registry's `SYNROUTE_MODELS_ALLOW_REMOTE=true` consent requirement.

Task categories are coding, debugging, testing, documentation, and reasoning.
Benchmark scores are combined with task-specific fixed weights, renormalized
over dimensions actually stored for a model. Mean latency breaks score ties;
provider and model identifiers break remaining ties. The report includes the
stored component means and sample count used for the choice.

If no currently available model has successful benchmark evidence for this
repository and the active benchmark/scoring versions, the selector labels the
result as a metadata fallback. That order uses only known capability matches,
known context windows, and stable identifiers. It does not generate benchmark
scores or make performance claims. Unavailable providers and models are never
selected. The selector makes a single recommendation and does not perform
multi-user or adaptive learning.
