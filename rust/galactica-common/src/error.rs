use std::fmt::{Display, Formatter};

use tonic::Code;

pub type Result<T> = std::result::Result<T, GalacticaError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GalacticaError {
    NotFound(String),
    InvalidArgument(String),
    Unavailable(String),
    FailedPrecondition(String),
    Internal(String),
}

impl GalacticaError {
    pub fn internal(message: impl Into<String>) -> Self {
        Self::Internal(message.into())
    }

    pub fn invalid_argument(message: impl Into<String>) -> Self {
        Self::InvalidArgument(message.into())
    }

    pub fn not_found(message: impl Into<String>) -> Self {
        Self::NotFound(message.into())
    }

    pub fn unavailable(message: impl Into<String>) -> Self {
        Self::Unavailable(message.into())
    }

    pub fn failed_precondition(message: impl Into<String>) -> Self {
        Self::FailedPrecondition(message.into())
    }

    pub fn code(&self) -> Code {
        match self {
            Self::NotFound(_) => Code::NotFound,
            Self::InvalidArgument(_) => Code::InvalidArgument,
            Self::Unavailable(_) => Code::Unavailable,
            Self::FailedPrecondition(_) => Code::FailedPrecondition,
            Self::Internal(_) => Code::Internal,
        }
    }

    pub fn message(&self) -> &str {
        match self {
            Self::NotFound(message)
            | Self::InvalidArgument(message)
            | Self::Unavailable(message)
            | Self::FailedPrecondition(message)
            | Self::Internal(message) => message,
        }
    }
}

impl Display for GalacticaError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.message())
    }
}

impl std::error::Error for GalacticaError {}

impl From<GalacticaError> for tonic::Status {
    fn from(value: GalacticaError) -> Self {
        tonic::Status::new(value.code(), value.to_string())
    }
}
