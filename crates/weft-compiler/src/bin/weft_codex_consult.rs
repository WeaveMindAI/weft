use anyhow::{Context, bail};
use clap::{Parser, Subcommand, ValueEnum};
use std::path::PathBuf;
use uuid::Uuid;
use weft_compiler::codex_consultation::{
    ConsultationCheck, ConsultationContext, ConsultationPacket, ImplementationPhase,
    validate_response_json,
};

#[derive(Debug, Parser)]
#[command(
    name = "weft-codex-consult",
    about = "Offline, read-only bridge for governed Codex implementation consultation"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Emit a deterministic consultation packet to stdout. Performs no external calls.
    Emit {
        #[arg(long)]
        project_id: Uuid,
        #[arg(long)]
        source_sha256: String,
        #[arg(long)]
        definition_sha256: String,
        #[arg(long)]
        source_path: String,
        #[arg(long)]
        definition_path: String,
        #[arg(long)]
        phase: PhaseArg,
        #[arg(long)]
        prompt_contract_version: String,
        #[arg(long = "check", required = true)]
        checks: Vec<CheckArg>,
    },
    /// Verify a Codex response and emit a non-executing validation receipt.
    Verify {
        #[arg(long)]
        request: PathBuf,
        #[arg(long)]
        response: PathBuf,
    },
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum PhaseArg {
    Design,
    Generate,
    Validate,
}

impl From<PhaseArg> for ImplementationPhase {
    fn from(value: PhaseArg) -> Self {
        match value {
            PhaseArg::Design => Self::Design,
            PhaseArg::Generate => Self::Generate,
            PhaseArg::Validate => Self::Validate,
        }
    }
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum CheckArg {
    Correctness,
    Evidence,
    Policy,
    Security,
}

impl From<CheckArg> for ConsultationCheck {
    fn from(value: CheckArg) -> Self {
        match value {
            CheckArg::Correctness => Self::Correctness,
            CheckArg::Evidence => Self::Evidence,
            CheckArg::Policy => Self::Policy,
            CheckArg::Security => Self::Security,
        }
    }
}

fn main() -> anyhow::Result<()> {
    match Cli::parse().command {
        Command::Emit {
            project_id,
            source_sha256,
            definition_sha256,
            source_path,
            definition_path,
            phase,
            prompt_contract_version,
            checks,
        } => {
            if checks.is_empty() {
                bail!("at least one --check is required");
            }
            let packet = ConsultationPacket::new(ConsultationContext {
                project_id,
                source_sha256,
                definition_sha256,
                source_path,
                definition_path,
                implementation_phase: phase.into(),
                prompt_contract_version,
                requested_checks: checks.into_iter().map(Into::into).collect(),
            })
            .context("build consultation packet")?;
            println!("{}", packet.to_canonical_json()?);
            Ok(())
        }
        Command::Verify { request, response } => {
            let request_json = std::fs::read_to_string(&request)
                .with_context(|| format!("read consultation request {}", request.display()))?;
            let response_json = std::fs::read_to_string(&response)
                .with_context(|| format!("read consultation response {}", response.display()))?;
            let packet = ConsultationPacket::from_json(&request_json)
                .context("validate consultation request")?;
            let validated = validate_response_json(&packet, &response_json)
                .context("validate consultation response")?;
            let receipt = serde_json::json!({
                "outcome": "verified_advisory",
                "status": validated.response.status,
                "request_sha256": packet.request_sha256,
                "response_sha256": validated.response_sha256,
                "executed": false
            });
            println!("{}", serde_json::to_string(&receipt)?);
            Ok(())
        }
    }
}
