use sha2::{Digest, Sha256};
use uuid::Uuid;
use weft_compiler::codex_consultation::{
    ConsultationCheck, ConsultationContext, ConsultationDisposition, ConsultationInput,
    ConsultationPacket, ConsultationStatus, ImplementationPhase, NotConsultedReason,
    UnavailableReason, evaluate_consultation, validate_response_json,
};

const SOURCE_SHA256: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const DEFINITION_SHA256: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

fn context() -> ConsultationContext {
    ConsultationContext {
        project_id: Uuid::parse_str("018f5f2e-2f08-7f6b-9d47-9c2b19b6a123").unwrap(),
        source_sha256: SOURCE_SHA256.to_string(),
        definition_sha256: DEFINITION_SHA256.to_string(),
        source_path: "crates/weft-compiler/src/codex_consultation.rs".to_string(),
        definition_path: "docs/codex-consultation.md".to_string(),
        implementation_phase: ImplementationPhase::Validate,
        prompt_contract_version: "codex-consultation-v1".to_string(),
        requested_checks: vec![
            ConsultationCheck::Security,
            ConsultationCheck::Policy,
            ConsultationCheck::Evidence,
        ],
    }
}

#[test]
fn deterministic_packet_contains_only_bounded_context_and_digests() {
    let first = ConsultationPacket::new(context()).unwrap();
    let second = ConsultationPacket::new(context()).unwrap();

    assert_eq!(first, second);
    assert_eq!(first.schema_version, 1);
    assert_eq!(first.protocol, "codex-consultation-v1");
    assert_eq!(first.request.schema_version, 1);
    assert!(first.request.constraints.read_only);
    assert!(first.request.constraints.no_canonical_writes);
    assert!(first.request.constraints.no_external_side_effects);
    assert_eq!(
        first.request.source_path,
        "crates/weft-compiler/src/codex_consultation.rs"
    );
    assert_eq!(first.request.definition_path, "docs/codex-consultation.md");
    assert_eq!(
        first.request.requested_checks,
        vec![
            ConsultationCheck::Evidence,
            ConsultationCheck::Policy,
            ConsultationCheck::Security,
        ]
    );

    let json = first.to_canonical_json().unwrap();
    assert_eq!(json, second.to_canonical_json().unwrap());
    assert!(!json.contains("api_key"));
    assert!(!json.contains("source_body"));
    assert!(!json.contains("prompt_body"));
    assert_eq!(first.request_sha256.len(), 64);
}

#[test]
fn packet_rejects_absolute_and_traversing_source_locators() {
    let mut traversing = context();
    traversing.source_path = "../secrets.env".to_string();
    assert!(
        ConsultationPacket::new(traversing)
            .unwrap_err()
            .to_string()
            .contains("source_path")
    );

    let mut absolute = context();
    absolute.definition_path = "C:/outside/policy.md".to_string();
    assert!(
        ConsultationPacket::new(absolute)
            .unwrap_err()
            .to_string()
            .contains("definition_path")
    );
}

#[test]
fn response_is_advisory_model_attributed_and_bound_to_request() {
    let packet = ConsultationPacket::new(context()).unwrap();
    let response = serde_json::json!({
        "schema_version": 1,
        "protocol": "codex-consultation-v1",
        "request_sha256": packet.request_sha256,
        "status": "advice",
        "findings": [{
            "code": "policy-boundary",
            "severity": "warning",
            "message": "Keep consultation outside the deterministic compile decision.",
            "evidence_ids": ["codex:agents:compiler-boundary"]
        }],
        "provenance": {
            "producer_kind": "model",
            "provider": "policy-routed",
            "model": "model-id",
            "role": "implementation_consultant",
            "policy_sha256": "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
            "prompt_sha256": "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
            "execution_id": "consult-001"
        }
    });

    let validated = validate_response_json(&packet, &response.to_string()).unwrap();

    assert_eq!(validated.response.status, ConsultationStatus::Advice);
    assert_eq!(validated.response.findings.len(), 1);
    assert_eq!(validated.response_sha256.len(), 64);
}

#[test]
fn packet_parser_rejects_weakened_read_only_constraints() {
    let packet = ConsultationPacket::new(context()).unwrap();
    let mut value: serde_json::Value =
        serde_json::from_str(&packet.to_canonical_json().unwrap()).unwrap();
    value["request"]["constraints"]["read_only"] = serde_json::Value::Bool(false);

    let error = ConsultationPacket::from_json(&value.to_string()).unwrap_err();

    assert!(error.to_string().contains("constraints"));
}

#[test]
fn abstain_cannot_smuggle_findings() {
    let packet = ConsultationPacket::new(context()).unwrap();
    let response = serde_json::json!({
        "schema_version": 1,
        "protocol": "codex-consultation-v1",
        "request_sha256": packet.request_sha256,
        "status": "abstain",
        "findings": [{
            "code": "hidden-advice",
            "severity": "warning",
            "message": "This must not be accepted under abstain.",
            "evidence_ids": []
        }],
        "provenance": {
            "producer_kind": "model",
            "provider": "policy-routed",
            "model": "model-id",
            "role": "implementation_consultant",
            "policy_sha256": "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
            "prompt_sha256": "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
            "execution_id": "consult-002"
        }
    });

    let error = validate_response_json(&packet, &response.to_string()).unwrap_err();

    assert!(error.to_string().contains("abstain"));
}

#[test]
fn disabled_and_unavailable_modes_have_explicit_deterministic_fallbacks() {
    let packet = ConsultationPacket::new(context()).unwrap();

    let disabled = evaluate_consultation(&packet, ConsultationInput::Disabled).unwrap();
    let timeout = evaluate_consultation(
        &packet,
        ConsultationInput::Unavailable(UnavailableReason::Timeout),
    )
    .unwrap();
    let missing = evaluate_consultation(
        &packet,
        ConsultationInput::Unavailable(UnavailableReason::NotConfigured),
    )
    .unwrap();

    assert_eq!(
        disabled,
        ConsultationDisposition::NotConsulted(NotConsultedReason::Disabled)
    );
    assert_eq!(
        timeout,
        ConsultationDisposition::NotConsulted(NotConsultedReason::Timeout)
    );
    assert_eq!(
        missing,
        ConsultationDisposition::NotConsulted(NotConsultedReason::NotConfigured)
    );
}

#[test]
fn malformed_unknown_and_digest_mismatched_responses_fail_closed() {
    let packet = ConsultationPacket::new(context()).unwrap();
    assert!(validate_response_json(&packet, "{not-json").is_err());

    let base = serde_json::json!({
        "schema_version": 1,
        "protocol": "codex-consultation-v1",
        "request_sha256": packet.request_sha256,
        "status": "abstain",
        "findings": [],
        "provenance": {
            "producer_kind": "model",
            "provider": "policy-routed",
            "model": "model-id",
            "role": "implementation_consultant",
            "policy_sha256": "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
            "prompt_sha256": "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
            "execution_id": "consult-003"
        }
    });

    let mut unknown = base.clone();
    unknown["canonical_write"] = serde_json::Value::Bool(true);
    assert!(validate_response_json(&packet, &unknown.to_string()).is_err());

    let mut mismatched = base;
    mismatched["request_sha256"] = serde_json::Value::String(
        "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee".to_string(),
    );
    let error = validate_response_json(&packet, &mismatched.to_string()).unwrap_err();
    assert!(error.to_string().contains("different request"));
}

#[test]
fn response_validation_rejects_digest_consistent_weakened_packet() {
    let mut packet = ConsultationPacket::new(context()).unwrap();
    packet.request.constraints.read_only = false;
    packet.request_sha256 = format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(&packet.request).unwrap())
    );
    let response = serde_json::json!({
        "schema_version": 1,
        "protocol": "codex-consultation-v1",
        "request_sha256": packet.request_sha256,
        "status": "abstain",
        "findings": [],
        "provenance": {
            "producer_kind": "model",
            "provider": "policy-routed",
            "model": "model-id",
            "role": "implementation_consultant",
            "policy_sha256": "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
            "prompt_sha256": "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
            "execution_id": "consult-004"
        }
    });

    let error = validate_response_json(&packet, &response.to_string()).unwrap_err();

    assert!(error.to_string().contains("constraints"));
}

#[test]
fn blocked_response_requires_explanatory_finding() {
    let packet = ConsultationPacket::new(context()).unwrap();
    let response = serde_json::json!({
        "schema_version": 1,
        "protocol": "codex-consultation-v1",
        "request_sha256": packet.request_sha256,
        "status": "blocked",
        "findings": [],
        "provenance": {
            "producer_kind": "model",
            "provider": "policy-routed",
            "model": "model-id",
            "role": "implementation_consultant",
            "policy_sha256": "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
            "prompt_sha256": "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
            "execution_id": "consult-005"
        }
    });

    let error = validate_response_json(&packet, &response.to_string()).unwrap_err();

    assert!(error.to_string().contains("blocked"));
}
