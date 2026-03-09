use thiserror::Error;

#[derive(Error, Debug)]
pub enum GalacticaError {
    #[error("internal error: {0}")]
    Internal(String),

    #[error("not found: {0}")]
    NotFound(String),

    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    #[error("service unavailable: {0}")]
    Unavailable(String),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("database error: {0}")]
    Database(String),
}

pub type Result<T> = std::result::Result<T, GalacticaError>;
