//! Deterministic, offline packets for implementation-time Codex consultation.
//!
//! This module deliberately performs no model, process, filesystem, or network
//! calls. It binds bounded compiler context into a read-only request that an
//! external governed Codex adapter may consume.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use uuid::Uuid;

pub const CONSULTATION_SCHEMA_VERSION: u32 = 1;
pub const CONSULTATION_PROTOCOL: &str = "codex-consultation-v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConsultationCheck {
    Correctness,
    Evidence,
    Policy,
    Security,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ImplementationPhase {
    Design,
    Generate,
    Validate,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsultationContext {
    pub project_id: Uuid,
    pub source_sha256: String,
    pub definition_sha256: String,
    pub source_path: String,
    pub definition_path: String,
    pub implementation_phase: ImplementationPhase,
    pub prompt_contract_version: String,
    pub requested_checks: Vec<ConsultationCheck>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConsultationConstraints {
    pub read_only: bool,
    pub no_canonical_writes: bool,
    pub no_external_side_effects: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConsultationRequest {
    pub schema_version: u32,
    pub project_id: Uuid,
    pub source_sha256: String,
    pub definition_sha256: String,
    pub source_path: String,
    pub definition_path: String,
    pub implementation_phase: ImplementationPhase,
    pub prompt_contract_version: String,
    pub requested_checks: Vec<ConsultationCheck>,
    pub constraints: ConsultationConstraints,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConsultationPacket {
    pub schema_version: u32,
    pub protocol: String,
    pub request_sha256: String,
    pub request: ConsultationRequest,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConsultationStatus {
    Advice,
    Abstain,
    Blocked,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FindingSeverity {
    Info,
    Warning,
    Error,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConsultationFinding {
    pub code: String,
    pub severity: FindingSeverity,
    pub message: String,
    pub evidence_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConsultationProvenance {
    pub producer_kind: String,
    pub provider: String,
    pub model: String,
    pub role: String,
    pub policy_sha256: String,
    pub prompt_sha256: String,
    pub execution_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConsultationResponse {
    pub schema_version: u32,
    pub protocol: String,
    pub request_sha256: String,
    pub status: ConsultationStatus,
    pub findings: Vec<ConsultationFinding>,
    pub provenance: ConsultationProvenance,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidatedConsultation {
    pub response: ConsultationResponse,
    pub response_sha256: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnavailableReason {
    NotConfigured,
    AdapterUnavailable,
    Timeout,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NotConsultedReason {
    Disabled,
    NotConfigured,
    AdapterUnavailable,
    Timeout,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConsultationInput<'a> {
    Disabled,
    Unavailable(UnavailableReason),
    Response(&'a str),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConsultationDisposition {
    NotConsulted(NotConsultedReason),
    Validated(Box<ValidatedConsultation>),
}

#[derive(Debug, Error)]
pub enum ConsultationError {
    #[error("{field} must be a lowercase 64-character SHA-256 digest")]
    InvalidDigest { field: &'static str },
    #[error(
        "prompt contract version must use 1-64 lowercase ASCII letters, digits, '.', '_', or '-'"
    )]
    InvalidPromptContractVersion,
    #[error("{field} must be a normalized repository-relative path")]
    InvalidRelativePath { field: &'static str },
    #[error("at least one consultation check is required")]
    EmptyChecks,
    #[error("unsupported consultation schema version or protocol")]
    UnsupportedProtocol,
    #[error("consultation packet request digest does not verify")]
    InvalidPacketDigest,
    #[error(
        "consultation packet constraints must remain read-only with no canonical writes or external side effects"
    )]
    WeakenedConstraints,
    #[error("consultation response is bound to a different request")]
    RequestDigestMismatch,
    #[error("consultation response provenance is invalid: {0}")]
    InvalidProvenance(&'static str),
    #[error("advice response must contain at least one finding")]
    EmptyAdvice,
    #[error("blocked response must contain at least one finding")]
    EmptyBlocked,
    #[error("abstain response must not contain findings")]
    AbstainWithFindings,
    #[error("consultation finding is invalid: {0}")]
    InvalidFinding(&'static str),
    #[error("consultation JSON serialization failed: {0}")]
    Serialization(#[from] serde_json::Error),
}

impl ConsultationPacket {
    pub fn new(context: ConsultationContext) -> Result<Self, ConsultationError> {
        validate_sha256("source_sha256", &context.source_sha256)?;
        validate_sha256("definition_sha256", &context.definition_sha256)?;
        validate_relative_path("source_path", &context.source_path)?;
        validate_relative_path("definition_path", &context.definition_path)?;
        validate_contract_version(&context.prompt_contract_version)?;
        if context.requested_checks.is_empty() {
            return Err(ConsultationError::EmptyChecks);
        }

        let mut requested_checks = context.requested_checks;
        requested_checks.sort_unstable();
        requested_checks.dedup();
        let request = ConsultationRequest {
            schema_version: CONSULTATION_SCHEMA_VERSION,
            project_id: context.project_id,
            source_sha256: context.source_sha256,
            definition_sha256: context.definition_sha256,
            source_path: context.source_path,
            definition_path: context.definition_path,
            implementation_phase: context.implementation_phase,
            prompt_contract_version: context.prompt_contract_version,
            requested_checks,
            constraints: ConsultationConstraints {
                read_only: true,
                no_canonical_writes: true,
                no_external_side_effects: true,
            },
        };
        let request_bytes = serde_json::to_vec(&request)?;
        let request_sha256 = sha256_hex(&request_bytes);
        Ok(Self {
            schema_version: CONSULTATION_SCHEMA_VERSION,
            protocol: CONSULTATION_PROTOCOL.to_string(),
            request_sha256,
            request,
        })
    }

    pub fn to_canonical_json(&self) -> Result<String, ConsultationError> {
        Ok(serde_json::to_string(self)?)
    }

    pub fn from_json(json: &str) -> Result<Self, ConsultationError> {
        let packet: Self = serde_json::from_str(json)?;
        packet.validate_integrity()?;
        Ok(packet)
    }

    fn validate_integrity(&self) -> Result<(), ConsultationError> {
        if self.schema_version != CONSULTATION_SCHEMA_VERSION
            || self.protocol != CONSULTATION_PROTOCOL
            || self.request.schema_version != CONSULTATION_SCHEMA_VERSION
        {
            return Err(ConsultationError::UnsupportedProtocol);
        }
        let constraints = &self.request.constraints;
        if !constraints.read_only
            || !constraints.no_canonical_writes
            || !constraints.no_external_side_effects
        {
            return Err(ConsultationError::WeakenedConstraints);
        }
        validate_sha256("source_sha256", &self.request.source_sha256)?;
        validate_sha256("definition_sha256", &self.request.definition_sha256)?;
        validate_relative_path("source_path", &self.request.source_path)?;
        validate_relative_path("definition_path", &self.request.definition_path)?;
        validate_contract_version(&self.request.prompt_contract_version)?;
        if self.request.requested_checks.is_empty() {
            return Err(ConsultationError::EmptyChecks);
        }
        let expected_request_sha256 = sha256_hex(&serde_json::to_vec(&self.request)?);
        if self.request_sha256 != expected_request_sha256 {
            return Err(ConsultationError::InvalidPacketDigest);
        }
        Ok(())
    }
}

pub fn evaluate_consultation(
    packet: &ConsultationPacket,
    input: ConsultationInput<'_>,
) -> Result<ConsultationDisposition, ConsultationError> {
    packet.validate_integrity()?;
    let disposition = match input {
        ConsultationInput::Disabled => {
            ConsultationDisposition::NotConsulted(NotConsultedReason::Disabled)
        }
        ConsultationInput::Unavailable(reason) => {
            let reason = match reason {
                UnavailableReason::NotConfigured => NotConsultedReason::NotConfigured,
                UnavailableReason::AdapterUnavailable => NotConsultedReason::AdapterUnavailable,
                UnavailableReason::Timeout => NotConsultedReason::Timeout,
            };
            ConsultationDisposition::NotConsulted(reason)
        }
        ConsultationInput::Response(response_json) => ConsultationDisposition::Validated(Box::new(
            validate_response_json(packet, response_json)?,
        )),
    };
    Ok(disposition)
}

pub fn validate_response_json(
    packet: &ConsultationPacket,
    response_json: &str,
) -> Result<ValidatedConsultation, ConsultationError> {
    packet.validate_integrity()?;

    let response: ConsultationResponse = serde_json::from_str(response_json)?;
    if response.schema_version != CONSULTATION_SCHEMA_VERSION
        || response.protocol != CONSULTATION_PROTOCOL
    {
        return Err(ConsultationError::UnsupportedProtocol);
    }
    if response.request_sha256 != packet.request_sha256 {
        return Err(ConsultationError::RequestDigestMismatch);
    }
    validate_response_content(&response)?;

    Ok(ValidatedConsultation {
        response,
        response_sha256: sha256_hex(response_json.as_bytes()),
    })
}

fn validate_response_content(response: &ConsultationResponse) -> Result<(), ConsultationError> {
    let provenance = &response.provenance;
    if provenance.producer_kind != "model" {
        return Err(ConsultationError::InvalidProvenance(
            "producer_kind must be model",
        ));
    }
    for (value, label) in [
        (&provenance.provider, "provider is empty"),
        (&provenance.model, "model is empty"),
        (&provenance.role, "role is empty"),
        (&provenance.execution_id, "execution_id is empty"),
    ] {
        if value.trim().is_empty() {
            return Err(ConsultationError::InvalidProvenance(label));
        }
    }
    validate_sha256("policy_sha256", &provenance.policy_sha256)
        .map_err(|_| ConsultationError::InvalidProvenance("policy_sha256 is invalid"))?;
    validate_sha256("prompt_sha256", &provenance.prompt_sha256)
        .map_err(|_| ConsultationError::InvalidProvenance("prompt_sha256 is invalid"))?;

    if response.status == ConsultationStatus::Advice && response.findings.is_empty() {
        return Err(ConsultationError::EmptyAdvice);
    }
    if response.status == ConsultationStatus::Blocked && response.findings.is_empty() {
        return Err(ConsultationError::EmptyBlocked);
    }
    if response.status == ConsultationStatus::Abstain && !response.findings.is_empty() {
        return Err(ConsultationError::AbstainWithFindings);
    }
    for finding in &response.findings {
        if finding.code.trim().is_empty() {
            return Err(ConsultationError::InvalidFinding("code is empty"));
        }
        if finding.message.trim().is_empty() {
            return Err(ConsultationError::InvalidFinding("message is empty"));
        }
        if finding.evidence_ids.iter().any(|id| id.trim().is_empty()) {
            return Err(ConsultationError::InvalidFinding("evidence id is empty"));
        }
    }
    Ok(())
}

fn validate_sha256(field: &'static str, value: &str) -> Result<(), ConsultationError> {
    let valid = value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b));
    if valid {
        Ok(())
    } else {
        Err(ConsultationError::InvalidDigest { field })
    }
}

fn validate_relative_path(field: &'static str, value: &str) -> Result<(), ConsultationError> {
    let valid = !value.is_empty()
        && value.len() <= 240
        && !value.starts_with('/')
        && !value.ends_with('/')
        && !value.contains('\\')
        && !value.contains(':')
        && !value.chars().any(char::is_control)
        && value
            .split('/')
            .all(|segment| !segment.is_empty() && segment != "." && segment != "..");
    if valid {
        Ok(())
    } else {
        Err(ConsultationError::InvalidRelativePath { field })
    }
}

fn validate_contract_version(value: &str) -> Result<(), ConsultationError> {
    let valid = !value.is_empty()
        && value.len() <= 64
        && value.bytes().all(|b| {
            b.is_ascii_lowercase() || b.is_ascii_digit() || matches!(b, b'.' | b'_' | b'-')
        });
    if valid {
        Ok(())
    } else {
        Err(ConsultationError::InvalidPromptContractVersion)
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    format!("{digest:x}")
}
