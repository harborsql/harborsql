use crate::error::{HarborError, Result};

use super::{ResolvedTableRef, extract_table_refs, validate_select_only};

pub(super) fn resolve_query_table_refs(
    sql: &str,
    default_catalog: &str,
    default_schema: &str,
) -> Result<Vec<ResolvedTableRef>> {
    validate_select_only(sql)?;
    let refs = extract_table_refs(sql, default_catalog, default_schema)?;
    if refs.is_empty() {
        return Err(HarborError::UnsupportedSql(
            "no FROM/JOIN table references were found".into(),
        ));
    }
    Ok(refs)
}
