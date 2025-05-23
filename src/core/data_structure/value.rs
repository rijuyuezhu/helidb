use crate::error::{DBResult, DBSingleError};
use std::borrow::Cow;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValueNotNull {
    Int(i32),
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

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Value(pub Option<ValueNotNull>);

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match (&self.0, &other.0) {
            (Some(ValueNotNull::Int(x)), Some(ValueNotNull::Int(y))) => x.partial_cmp(y),
            (Some(ValueNotNull::Varchar(x)), Some(ValueNotNull::Varchar(y))) => x.partial_cmp(y),
            _ => None,
        }
    }
}

impl Value {
    pub fn to_string(&self) -> Cow<'_, str> {
        match &self.0 {
            Some(ValueNotNull::Int(x)) => x.to_string().into(),
            Some(ValueNotNull::Varchar(s)) => s.into(),
            None => "".into(),
        }
    }
    pub fn from_varchar(s: String) -> Self {
        Value(Some(ValueNotNull::Varchar(s)))
    }
    pub fn from_int(i: i32) -> Self {
        Value(Some(ValueNotNull::Int(i)))
    }
    pub fn from_null() -> Self {
        Value(None)
    }
    pub fn is_null(&self) -> bool {
        self.0.is_none()
    }
    pub fn from_bool(b: bool) -> Self {
        Self::from_int(b as i32)
    }
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
