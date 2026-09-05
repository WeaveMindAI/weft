use std::io::Write;
use std::process::Command;

use tempfile::NamedTempFile;
use uuid::Uuid;
use weft_compiler::codex_consultation::{
    ConsultationCheck, ConsultationContext, ConsultationPacket, ImplementationPhase,
};

#[test]
fn emit_prints_offline_consultation_packet_without_runtime() {
    let output = Command::new(env!("CARGO_BIN_EXE_weft-codex-consult"))
        .args([
            "emit",
            "--project-id",
            "018f5f2e-2f08-7f6b-9d47-9c2b19b6a123",
            "--source-sha256",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "--definition-sha256",
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            "--source-path",
            "crates/weft-compiler/src/codex_consultation.rs",
            "--definition-path",
            "docs/codex-consultation.md",
            "--phase",
            "validate",
            "--prompt-contract-version",
            "codex-consultation-v1",
            "--check",
            "security",
            "--check",
            "policy",
        ])
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["protocol"], "codex-consultation-v1");
    assert_eq!(value["request"]["constraints"]["read_only"], true);
    assert_eq!(
        value["request"]["requested_checks"],
        serde_json::json!(["policy", "security"])
    );
}

#[test]
fn verify_prints_receipt_without_executing_advice() {
    let packet = ConsultationPacket::new(ConsultationContext {
        project_id: Uuid::parse_str("018f5f2e-2f08-7f6b-9d47-9c2b19b6a123").unwrap(),
        source_sha256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            .to_string(),
        definition_sha256: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
            .to_string(),
        source_path: "crates/weft-compiler/src/codex_consultation.rs".to_string(),
        definition_path: "docs/codex-consultation.md".to_string(),
        implementation_phase: ImplementationPhase::Validate,
        prompt_contract_version: "codex-consultation-v1".to_string(),
        requested_checks: vec![ConsultationCheck::Policy],
    })
    .unwrap();
    let response = serde_json::json!({
        "schema_version": 1,
        "protocol": "codex-consultation-v1",
        "request_sha256": packet.request_sha256,
        "status": "advice",
        "findings": [{
            "code": "keep-read-only",
            "severity": "warning",
            "message": "Do not execute this advice automatically.",
            "evidence_ids": ["codex:agents:read-only"]
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
    let mut request_file = NamedTempFile::new().unwrap();
    request_file
        .write_all(packet.to_canonical_json().unwrap().as_bytes())
        .unwrap();
    let mut response_file = NamedTempFile::new().unwrap();
    response_file
        .write_all(response.to_string().as_bytes())
        .unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_weft-codex-consult"))
        .args([
            "verify",
            "--request",
            request_file.path().to_str().unwrap(),
            "--response",
            response_file.path().to_str().unwrap(),
        ])
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let receipt: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(receipt["outcome"], "verified_advisory");
    assert_eq!(receipt["status"], "advice");
    assert_eq!(receipt["request_sha256"], packet.request_sha256);
    assert_eq!(receipt["response_sha256"].as_str().unwrap().len(), 64);
    assert_eq!(receipt["executed"], false);
}
