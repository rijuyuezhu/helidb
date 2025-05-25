//! Database value types and operations.
//!
//! Contains the fundamental Value and ValueNotNull types that represent
//! all possible data values in the database system.

use crate::error::{DBResult, DBSingleError};
use bincode::{Decode, Encode};
use std::borrow::Cow;

/// A non-null database value.
#[derive(Debug, Clone, PartialEq, Eq, Decode, Encode, Hash)]
pub enum ValueNotNull {
    /// 32-bit integer value
    Int(i32),
    /// Variable-length string value
    Varchar(String),
}

impl std::fmt::Display for ValueNotNull {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ValueNotNull::Int(i) => write!(f, "{}", i),
            ValueNotNull::Varchar(s) => write!(f, "{}", s),
        }
    }
}

/// A nullable database value.
///
/// Wraps `ValueNotNull` in an Option to represent SQL NULL values.
#[derive(Debug, Clone, PartialEq, Eq, Default, Decode, Encode, Hash)]
pub struct Value(pub Option<ValueNotNull>);

impl Value {
    /// Converts the value to a string representation.
    ///
    /// # Returns
    /// - For Int: string representation of the number
    /// - For Varchar: the string itself
    /// - For NULL: empty string
    pub fn to_string(&self) -> Cow<'_, str> {
        match &self.0 {
            Some(ValueNotNull::Int(x)) => x.to_string().into(),
            Some(ValueNotNull::Varchar(s)) => s.into(),
            None => "".into(),
        }
    }
    /// Creates a new Varchar value.
    ///
    /// # Arguments
    /// * `s` - String value
    pub fn from_varchar(s: String) -> Self {
        Value(Some(ValueNotNull::Varchar(s)))
    }
    /// Creates a new Int value.
    ///
    /// # Arguments
    /// * `i` - Integer value
    pub fn from_int(i: i32) -> Self {
        Value(Some(ValueNotNull::Int(i)))
    }
    /// Creates a new NULL value.
    pub fn from_null() -> Self {
        Value(None)
    }
    /// Checks if the value is NULL.
    pub fn is_null(&self) -> bool {
        self.0.is_none()
    }
    /// Creates a boolean value (stored as Int 0/1).
    ///
    /// # Arguments
    /// * `b` - Boolean value
    pub fn from_bool(b: bool) -> Self {
        Self::from_int(b as i32)
    }
    /// Attempts to convert the value to a boolean.
    ///
    /// # Returns
    /// - Some(true/false) for valid boolean representations
    /// - None for NULL
    /// - Error for invalid conversions
    ///
    /// # Examples
    /// ```
    /// # use simple_db::core::data_structure::value::Value;
    /// #
    /// let v = Value::from_varchar("true".to_string());
    /// assert_eq!(v.try_to_bool().unwrap(), Some(true));
    /// let v = Value::from_varchar("f".to_string());
    /// assert_eq!(v.try_to_bool().unwrap(), Some(false));
    /// let v = Value::from_null();
    /// assert_eq!(v.try_to_bool().unwrap(), None);
    /// ```
    pub fn try_to_bool(&self) -> DBResult<Option<bool>> {
        Ok(match &self.0 {
            Some(ValueNotNull::Int(x)) => Some(*x != 0),
            Some(ValueNotNull::Varchar(s)) => match s.as_ref() {
                "true" | "t" | "yes" | "y" | "on" | "1" => Some(true),
                "false" | "f" | "no" | "n" | "off" | "0" => Some(false),
                _ => Err(DBSingleError::OtherError(format!(
                    "Cannot convert {} to bool",
                    s
                )))?,
            },
            None => None,
        })
    }
}

impl From<Option<ValueNotNull>> for Value {
    fn from(value: Option<ValueNotNull>) -> Self {
        Value(value)
    }
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match (&self.0, &other.0) {
            (Some(ValueNotNull::Int(x)), Some(ValueNotNull::Int(y))) => x.partial_cmp(y),
            (Some(ValueNotNull::Varchar(x)), Some(ValueNotNull::Varchar(y))) => x.partial_cmp(y),
            _ => None,
        }
    }
}
