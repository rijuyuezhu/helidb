use super::SQLExecutor;

impl SQLExecutor<'_, '_> {
    pub(super) fn get_content_from_span(&self, span: sqlparser::tokenizer::Span) -> Option<String> {
        let start = span.start;
        let end = span.end;
        if start.line != end.line || start.column > end.column || start.line == 0 || end.line == 0 {
            return None;
        }
        let line = start.line as usize;
        let sql_line = self.sql_statements.lines().nth(line - 1)?;
        let start_column = start.column as usize - 1;
        let end_column = end.column as usize - 1;
        if sql_line.len() < end_column {
            return None;
        }
        Some(sql_line[start_column..end_column].to_string())
    }
}

