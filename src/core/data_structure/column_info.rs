use crate::error::{DBResult, DBSingleError};
use sqlparser::ast;

#[derive(Debug, Clone, Copy)]
pub enum ColumnTypeSpecific {
    Int { display_width: Option<u64> },
    Varchar { max_length: u64 },
}

fn varchar_length_convert(length: Option<ast::CharacterLength>) -> DBResult<u64> {
    match length {
        Some(ast::CharacterLength::IntegerLength { length, .. }) => Ok(length),
        Some(ast::CharacterLength::Max) => Ok(u64::MAX),
        None => Ok(u64::MAX),
    }
}

impl ColumnTypeSpecific {
    pub fn from_column_def(def: &ast::ColumnDef) -> DBResult<Self> {
        Ok(match def.data_type {
            ast::DataType::Int(width) => ColumnTypeSpecific::Int {
                display_width: width,
            },
            ast::DataType::Varchar(length) => ColumnTypeSpecific::Varchar {
                max_length: varchar_length_convert(length)?,
            },
            _ => Err(DBSingleError::UnsupportedOPError(format!(
                "unsupported type {}",
                def.data_type
            )))?,
        })
    }
}

#[derive(Debug, Clone)]
pub struct ColumnInfo {
    pub name: String,
    pub nullable: bool,
    pub unique: bool,
    pub type_specific: ColumnTypeSpecific,
}
