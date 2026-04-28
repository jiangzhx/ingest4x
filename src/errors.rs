use std::error::Error;
use std::fmt;
use std::fmt::Display;

#[derive(Debug)]
pub enum ValidError {
    Empty(String),
    MissingField(String),
    XContext,
    UnknownOS,
    UnknownCurrencyTypeError(String),
    Number,
}

#[derive(Debug, Clone)]
pub struct EmptyError {
    pub message: String,
}

impl Display for EmptyError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "EmptyError: {} is empty", self.message)
    }
}

impl Error for EmptyError {}

#[derive(Debug, Clone)]
pub struct MissingFieldError {
    pub message: String,
}

impl Display for MissingFieldError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "MissingFieldError: missing field `{}`", self.message)
    }
}

impl Error for MissingFieldError {}

#[derive(Debug, Clone)]
pub struct UnknownCurrencyTypeError {
    pub message: String,
}

impl Display for UnknownCurrencyTypeError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "`{}` UnknownCurrencyTypeError", self.message)
    }
}

impl Error for UnknownCurrencyTypeError {}

impl Display for ValidError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ValidError::XContext => write!(f, "XContextError"),
            ValidError::Empty(err) => write!(f, "EmptyError: {err} is empty"),
            ValidError::MissingField(err) => {
                write!(f, "MissingFieldError: missing field `{err}`")
            }
            ValidError::UnknownOS => write!(f, "UnknownOSError"),
            ValidError::UnknownCurrencyTypeError(err) => {
                write!(f, "`{err}` UnknownCurrencyTypeError")
            }
            ValidError::Number => write!(f, "NumberError"),
        }
    }
}
