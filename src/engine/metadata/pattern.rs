use regex::{Regex, RegexBuilder};

use crate::error::{HarborError, Result};

pub(super) struct ShowPattern {
    regex: Option<Regex>,
}

impl ShowPattern {
    pub(super) fn new(pattern: Option<&str>) -> Result<Self> {
        let Some(pattern) = pattern else {
            return Ok(Self { regex: None });
        };
        let pattern = pattern.trim();
        if pattern.is_empty() {
            return Err(HarborError::UnsupportedSql(
                "SHOW pattern cannot be empty".into(),
            ));
        }
        let regex = databricks_show_regex(pattern);
        let regex = RegexBuilder::new(&regex)
            .case_insensitive(true)
            .build()
            .map_err(|err| {
                HarborError::UnsupportedSql(format!("invalid SHOW pattern `{pattern}`: {err}"))
            })?;
        Ok(Self { regex: Some(regex) })
    }

    pub(super) fn matches(&self, value: &str) -> bool {
        self.regex
            .as_ref()
            .is_none_or(|regex| regex.is_match(value))
    }
}

fn databricks_show_regex(pattern: &str) -> String {
    let mut regex = String::from("^(?:");
    for ch in pattern.chars() {
        if ch == '*' {
            regex.push_str(".*");
        } else {
            regex.push(ch);
        }
    }
    regex.push_str(")$");
    regex
}
