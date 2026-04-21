use thiserror::Error;

#[derive(Debug, Error)]
pub enum WeftError {
    #[error("config error: {0}")]
    Config(String),

    #[error("input error: {0}")]
    Input(String),

    #[error("type error: {0}")]
    Type(String),

    #[error("node execution failed: {0}")]
    NodeExecution(String),

    #[error("suspension failed: {0}")]
    Suspension(String),

    #[error("cancelled")]
    Cancelled,

    #[error("runtime error: {0}")]
    Runtime(#[from] anyhow::Error),
}

pub type WeftResult<T> = Result<T, WeftError>;
