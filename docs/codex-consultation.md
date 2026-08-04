# Codex implementation consultation

Weft can exchange deterministic, read-only implementation-consultation packets with an external governed Codex process. The bridge is optional and offline. It does not call a model, spawn a process, contact a network service, alter compiler output, or write canonical state.

## Boundary

- Protocol: `codex-consultation-v1`
- Schema version: `1`
- Request content is bounded to project identity, repository-relative source and definition paths, matching SHA-256 digests, implementation phase, prompt-contract version, requested check enums, and immutable safety constraints.
- Source and definition paths must be normalized repository-relative paths. Absolute paths, drive-qualified paths, backslashes, empty segments, `.` segments, and `..` traversal are rejected.
- Source text, prompt text, credentials, environment variables, and arbitrary operator prose are not request fields.
- Responses are advisory. Weft only validates and receipts them; `executed` is always `false`.
- Response structs reject unknown fields.
- Model advice must include provider, model, role, execution ID, policy digest, and prompt digest.
- `advice` requires findings. `blocked` requires at least one explanatory finding. `abstain` forbids findings.
- A response bound to any other request digest is rejected.

## Emit a request

```console
weft-codex-consult emit \
  --project-id 018f5f2e-2f08-7f6b-9d47-9c2b19b6a123 \
  --source-sha256 aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \
  --definition-sha256 bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb \
  --source-path crates/weft-compiler/src/codex_consultation.rs \
  --definition-path docs/codex-consultation.md \
  --phase validate \
  --prompt-contract-version codex-consultation-v1 \
  --check security \
  --check policy > request.json
```

The command writes only JSON to stdout. Redirecting stdout is an operator-controlled filesystem action outside the bridge.

## External governed handoff

An authorized external Codex runner may consume `request.json`. That runner is responsible for provider routing, timeout enforcement, credential handling, prompt-contract validation, execution attestation, and producing a response matching this closed schema:

```json
{
  "schema_version": 1,
  "protocol": "codex-consultation-v1",
  "request_sha256": "<digest copied from request packet>",
  "status": "advice",
  "findings": [
    {
      "code": "stable-code",
      "severity": "warning",
      "message": "Advisory text only.",
      "evidence_ids": ["codex:domain:evidence-id"]
    }
  ],
  "provenance": {
    "producer_kind": "model",
    "provider": "policy-routed",
    "model": "model-id",
    "role": "implementation_consultant",
    "policy_sha256": "<64 lowercase hex characters>",
    "prompt_sha256": "<64 lowercase hex characters>",
    "execution_id": "consultation-run-id"
  }
}
```

Allowed statuses are `advice`, `abstain`, and `blocked`. Allowed severities are `info`, `warning`, and `error`.

## Verify a response

```console
weft-codex-consult verify --request request.json --response response.json
```

On success, stdout contains a compact receipt with:

- `outcome: "verified_advisory"`
- the response status
- request and response SHA-256 digests
- `executed: false`

Malformed JSON, unknown fields, weakened constraints, unsupported protocol versions, invalid provenance, and digest mismatch fail closed with a non-zero exit.

## Deterministic fallback

Library integrations must use `evaluate_consultation` and select one explicit input:

- `Disabled`
- `Unavailable(NotConfigured)`
- `Unavailable(AdapterUnavailable)`
- `Unavailable(Timeout)`
- `Response(json)`

Disabled and unavailable cases return an explicit `NotConsulted` disposition. They do not alter deterministic compilation and are never silently upgraded to a model call. Only `Response(json)` enters response validation.
