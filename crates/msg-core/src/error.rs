use thiserror::Error;

/// Result type used by the pure domain layer.
pub type DomainResult<T> = Result<T, DomainError>;

/// Errors raised when constructing or mutating domain values would break an invariant.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum DomainError {
    #[error("{field} must not be empty")]
    Empty { field: &'static str },

    #[error("{field} length {actual} exceeds maximum {max}")]
    TooLong {
        field: &'static str,
        max: usize,
        actual: usize,
    },

    #[error("{field} contains invalid characters; allowed: {allowed}")]
    InvalidCharacters {
        field: &'static str,
        allowed: &'static str,
    },

    #[error("{field} must not start or end with '.'")]
    InvalidDotBoundary { field: &'static str },

    #[error("{field} must not contain '..'")]
    ConsecutiveDots { field: &'static str },

    #[error("{field} must be at least {min}; got {actual}")]
    TooSmall {
        field: &'static str,
        min: u64,
        actual: u64,
    },

    #[error("{field} must not be empty")]
    EmptyCollection { field: &'static str },

    #[error("{field} reference mismatch; expected {expected}, got {actual}")]
    InvalidReference {
        field: &'static str,
        expected: String,
        actual: String,
    },
}
