use thiserror::Error;

#[derive(Debug, Error)]
pub enum CompileError {
    #[error("project load error: {0}")]
    Project(String),
    #[error("parse error: {0}")]
    Parse(String),
    #[error("enrichment error: {0}")]
    Enrich(String),
    #[error("validation error: {0}")]
    Validate(String),
    #[error("codegen error: {0}")]
    Codegen(String),
    #[error("build error: {0}")]
    Build(String),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("other: {0}")]
    Other(#[from] anyhow::Error),
}

pub type CompileResult<T> = Result<T, CompileError>;
