use std::error::Error;
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RulesValidationError {
    code: RulesValidationCode,
    message: String,
    path: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RulesValidationCode {
    // Rules execution failed because a required field was absent or empty.
    RequiredFieldMissing,
    // A present field did not match the configured primitive type.
    FieldTypeMismatch,
    // A string field was outside the configured enum set.
    EnumValueInvalid,
    // Numeric validation needed a number but the value could not be read as one.
    NumberParseFailed,
    // A numeric value parsed successfully but violated gt/gte/lt/lte.
    NumberConstraintFailed,
    // A conditional requirement matched and its required field set was absent.
    ConditionalRequiredMissing,
}

impl RulesValidationError {
    pub(crate) fn new(
        code: RulesValidationCode,
        message: impl Into<String>,
        path: Option<impl Into<String>>,
    ) -> Self {
        Self {
            code,
            message: message.into(),
            path: path.map(Into::into),
        }
    }

    pub fn code(&self) -> &'static str {
        self.code.as_str()
    }

    pub fn message(&self) -> &str {
        self.message.as_str()
    }

    pub fn path(&self) -> Option<&str> {
        self.path.as_deref()
    }
}

impl RulesValidationCode {
    const fn as_str(self) -> &'static str {
        match self {
            Self::RequiredFieldMissing => "rules_required_field_missing",
            Self::FieldTypeMismatch => "rules_field_type_mismatch",
            Self::EnumValueInvalid => "rules_enum_value_invalid",
            Self::NumberParseFailed => "rules_number_parse_failed",
            Self::NumberConstraintFailed => "rules_number_constraint_failed",
            Self::ConditionalRequiredMissing => "rules_conditional_required_missing",
        }
    }
}

impl Display for RulesValidationError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl Error for RulesValidationError {}
