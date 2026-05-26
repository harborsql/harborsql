use sqlparser::{
    dialect::GenericDialect,
    tokenizer::{Token, Tokenizer, Whitespace, Word},
};

use crate::error::{HarborError, Result};

use super::{MetadataStatement, ObjectName};

pub(super) fn parse_show_statement(sql: &str) -> Result<Option<MetadataStatement>> {
    if !starts_with_show(sql) {
        return Ok(None);
    }

    let dialect = GenericDialect {};
    let tokens = Tokenizer::new(&dialect, sql)
        .tokenize()
        .map_err(|err| HarborError::UnsupportedSql(format!("invalid SHOW statement: {err}")))?
        .into_iter()
        .filter(|token| !matches!(token, Token::Whitespace(_)))
        .collect::<Vec<_>>();

    let mut parser = ShowParser::new(tokens);
    parser.parse().map(Some)
}

fn starts_with_show(sql: &str) -> bool {
    let trimmed = sql.trim_start();
    let bytes = trimmed.as_bytes();
    if bytes.len() < 4 || !bytes[..4].eq_ignore_ascii_case(b"show") {
        return false;
    }
    trimmed[4..]
        .chars()
        .next()
        .is_none_or(|ch| !is_identifier_char(ch))
}

fn is_identifier_char(ch: char) -> bool {
    ch == '_' || ch == '$' || ch.is_ascii_alphanumeric()
}

struct ShowParser {
    tokens: Vec<Token>,
    index: usize,
}

impl ShowParser {
    fn new(mut tokens: Vec<Token>) -> Self {
        while matches!(tokens.last(), Some(Token::SemiColon)) {
            tokens.pop();
        }
        Self { tokens, index: 0 }
    }

    fn parse(&mut self) -> Result<MetadataStatement> {
        self.expect_keyword("SHOW")?;

        if self.consume_keyword("CATALOGS") {
            if self.peek_keyword("FROM") || self.peek_keyword("IN") {
                return Err(HarborError::UnsupportedSql(
                    "SHOW CATALOGS does not accept a namespace".into(),
                ));
            }
            let pattern = self.parse_optional_pattern()?;
            self.expect_end()?;
            return Ok(MetadataStatement::Catalogs { pattern });
        }

        if self.consume_keyword("SCHEMAS") || self.consume_keyword("DATABASES") {
            if self.peek_keyword("HISTORY") {
                return Err(HarborError::UnsupportedSql(
                    "SHOW SCHEMAS HISTORY is not supported".into(),
                ));
            }
            let catalog = self.parse_optional_in_name()?;
            let pattern = self.parse_optional_pattern()?;
            self.expect_end()?;
            return Ok(MetadataStatement::Schemas { catalog, pattern });
        }

        if self.consume_keyword("TABLES") {
            if self.peek_keyword("EXTENDED") || self.peek_keyword("HISTORY") {
                return Err(HarborError::UnsupportedSql(
                    "use SHOW TABLE EXTENDED for extended table metadata".into(),
                ));
            }
            let schema = self.parse_optional_in_name()?;
            let pattern = self.parse_optional_pattern()?;
            self.expect_end()?;
            return Ok(MetadataStatement::Tables { schema, pattern });
        }

        if self.consume_keyword("VIEWS") {
            let schema = self.parse_optional_in_name()?;
            let pattern = self.parse_optional_pattern()?;
            self.expect_end()?;
            return Ok(MetadataStatement::Views { schema, pattern });
        }

        if self.consume_keyword("COLUMNS") {
            let table =
                self.parse_required_in_name("SHOW COLUMNS requires IN or FROM table_name")?;
            let schema = self.parse_optional_in_name()?;
            self.expect_end()?;
            return Ok(MetadataStatement::Columns { table, schema });
        }

        if self.consume_keyword("TABLE") {
            self.expect_keyword("EXTENDED")?;
            let schema = self.parse_optional_in_name()?;
            self.expect_keyword("LIKE")?;
            let pattern = self.parse_pattern_until_partition()?;
            let partition = self.parse_optional_partition()?;
            self.expect_end()?;
            return Ok(MetadataStatement::TableExtended {
                schema,
                pattern,
                partition,
            });
        }

        Err(HarborError::UnsupportedSql(
            "unsupported SHOW statement".into(),
        ))
    }

    fn parse_required_in_name(&mut self, message: &str) -> Result<ObjectName> {
        if self.consume_keyword("FROM") || self.consume_keyword("IN") {
            self.parse_object_name()
        } else {
            Err(HarborError::UnsupportedSql(message.into()))
        }
    }

    fn parse_optional_in_name(&mut self) -> Result<Option<ObjectName>> {
        if self.consume_keyword("FROM") || self.consume_keyword("IN") {
            Ok(Some(self.parse_object_name()?))
        } else {
            Ok(None)
        }
    }

    fn parse_object_name(&mut self) -> Result<ObjectName> {
        let mut parts = vec![self.parse_identifier()?];
        while self.consume_token(&Token::Period) {
            parts.push(self.parse_identifier()?);
        }
        Ok(ObjectName(parts))
    }

    fn parse_identifier(&mut self) -> Result<String> {
        match self.next() {
            Some(Token::Word(word)) => Ok(word.value),
            Some(Token::DoubleQuotedString(value)) => Ok(value),
            Some(token) => Err(HarborError::UnsupportedSql(format!(
                "expected identifier in SHOW statement, found `{token}`"
            ))),
            None => Err(HarborError::UnsupportedSql(
                "expected identifier in SHOW statement".into(),
            )),
        }
    }

    fn parse_optional_pattern(&mut self) -> Result<Option<String>> {
        if self.is_end() {
            return Ok(None);
        }
        if self.consume_keyword("LIKE") {
            return self.parse_pattern_until_end().map(Some);
        }
        self.parse_pattern_until_end().map(Some)
    }

    fn parse_pattern_until_end(&mut self) -> Result<String> {
        self.parse_pattern_until(|_| false)
    }

    fn parse_pattern_until_partition(&mut self) -> Result<String> {
        self.parse_pattern_until(|token| token_is_keyword(token, "PARTITION"))
    }

    fn parse_pattern_until(&mut self, stop: impl Fn(&Token) -> bool) -> Result<String> {
        let mut parts = Vec::new();
        while !self.is_end() && !stop(self.peek().expect("checked end")) {
            let token = self.next().expect("checked end");
            parts.push(pattern_token(token)?);
        }
        if parts.is_empty() {
            return Err(HarborError::UnsupportedSql(
                "SHOW pattern cannot be empty".into(),
            ));
        }
        Ok(parts.join(""))
    }

    fn parse_optional_partition(&mut self) -> Result<Option<String>> {
        if !self.consume_keyword("PARTITION") {
            return Ok(None);
        }
        if !self.consume_token(&Token::LParen) {
            return Err(HarborError::UnsupportedSql(
                "SHOW TABLE EXTENDED PARTITION requires a parenthesized clause".into(),
            ));
        }

        let mut depth = 1_u32;
        let mut parts = vec!["(".to_string()];
        while let Some(token) = self.next() {
            match token {
                Token::LParen => {
                    depth += 1;
                    parts.push("(".to_string());
                }
                Token::RParen => {
                    depth -= 1;
                    parts.push(")".to_string());
                    if depth == 0 {
                        return Ok(Some(parts.join("")));
                    }
                }
                token => parts.push(pattern_token(token)?),
            }
        }

        Err(HarborError::UnsupportedSql(
            "SHOW TABLE EXTENDED PARTITION clause is not closed".into(),
        ))
    }

    fn expect_keyword(&mut self, expected: &str) -> Result<()> {
        if self.consume_keyword(expected) {
            Ok(())
        } else {
            Err(HarborError::UnsupportedSql(format!(
                "expected `{expected}` in SHOW statement"
            )))
        }
    }

    fn consume_keyword(&mut self, expected: &str) -> bool {
        if self.peek_keyword(expected) {
            self.index += 1;
            true
        } else {
            false
        }
    }

    fn peek_keyword(&self, expected: &str) -> bool {
        self.peek()
            .is_some_and(|token| token_is_keyword(token, expected))
    }

    fn consume_token(&mut self, expected: &Token) -> bool {
        if self.peek() == Some(expected) {
            self.index += 1;
            true
        } else {
            false
        }
    }

    fn expect_end(&self) -> Result<()> {
        if self.is_end() {
            Ok(())
        } else {
            Err(HarborError::UnsupportedSql(format!(
                "unexpected token `{}` in SHOW statement",
                self.tokens[self.index]
            )))
        }
    }

    fn is_end(&self) -> bool {
        self.index >= self.tokens.len()
    }

    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.index)
    }

    fn next(&mut self) -> Option<Token> {
        let token = self.peek()?.clone();
        self.index += 1;
        Some(token)
    }
}

fn token_is_keyword(token: &Token, expected: &str) -> bool {
    matches!(token, Token::Word(word) if word.value.eq_ignore_ascii_case(expected))
}

fn pattern_token(token: Token) -> Result<String> {
    match token {
        Token::Word(Word { value, .. })
        | Token::SingleQuotedString(value)
        | Token::DoubleQuotedString(value)
        | Token::EscapedStringLiteral(value)
        | Token::NationalStringLiteral(value) => Ok(value),
        Token::Number(value, _) => Ok(value),
        Token::Mul => Ok("*".to_string()),
        Token::Pipe => Ok("|".to_string()),
        Token::Period => Ok(".".to_string()),
        Token::Eq => Ok("=".to_string()),
        Token::Comma => Ok(",".to_string()),
        Token::LParen => Ok("(".to_string()),
        Token::RParen => Ok(")".to_string()),
        Token::Whitespace(Whitespace::Space) => Ok(" ".to_string()),
        token => Err(HarborError::UnsupportedSql(format!(
            "unsupported token `{token}` in SHOW pattern"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ignores_non_show_sql() {
        assert_eq!(parse_show_statement("SELECT 1").unwrap(), None);
        assert_eq!(parse_show_statement("SHOWCASE").unwrap(), None);
    }

    #[test]
    fn parses_show_catalogs_variants() {
        assert_eq!(
            parse_show_statement("SHOW CATALOGS").unwrap(),
            Some(MetadataStatement::Catalogs { pattern: None })
        );
        assert_eq!(
            parse_show_statement("SHOW CATALOGS LIKE 'main*'").unwrap(),
            Some(MetadataStatement::Catalogs {
                pattern: Some("main*".to_string())
            })
        );
        assert_eq!(
            parse_show_statement("SHOW CATALOGS 'main*'").unwrap(),
            Some(MetadataStatement::Catalogs {
                pattern: Some("main*".to_string())
            })
        );
    }

    #[test]
    fn parses_show_schemas_variants() {
        assert_eq!(
            parse_show_statement("show schemas in `main-catalog` like 'sales*';").unwrap(),
            Some(MetadataStatement::Schemas {
                catalog: Some(ObjectName(vec!["main-catalog".to_string()])),
                pattern: Some("sales*".to_string())
            })
        );
        assert_eq!(
            parse_show_statement("SHOW DATABASES FROM main").unwrap(),
            Some(MetadataStatement::Schemas {
                catalog: Some(ObjectName(vec!["main".to_string()])),
                pattern: None
            })
        );
    }

    #[test]
    fn parses_show_tables_and_views_variants() {
        assert_eq!(
            parse_show_statement("SHOW TABLES IN main.sales LIKE 'fact*'").unwrap(),
            Some(MetadataStatement::Tables {
                schema: Some(ObjectName(vec!["main".to_string(), "sales".to_string()])),
                pattern: Some("fact*".to_string())
            })
        );
        assert_eq!(
            parse_show_statement("SHOW VIEWS FROM sales 'dim*'").unwrap(),
            Some(MetadataStatement::Views {
                schema: Some(ObjectName(vec!["sales".to_string()])),
                pattern: Some("dim*".to_string())
            })
        );
    }

    #[test]
    fn parses_show_columns_variants() {
        assert_eq!(
            parse_show_statement("SHOW COLUMNS IN customer").unwrap(),
            Some(MetadataStatement::Columns {
                table: ObjectName(vec!["customer".to_string()]),
                schema: None,
            })
        );
        assert_eq!(
            parse_show_statement("SHOW COLUMNS FROM salessc.customer").unwrap(),
            Some(MetadataStatement::Columns {
                table: ObjectName(vec!["salessc".to_string(), "customer".to_string()]),
                schema: None,
            })
        );
        assert_eq!(
            parse_show_statement("SHOW COLUMNS IN customer IN salessc").unwrap(),
            Some(MetadataStatement::Columns {
                table: ObjectName(vec!["customer".to_string()]),
                schema: Some(ObjectName(vec!["salessc".to_string()])),
            })
        );
    }

    #[test]
    fn parses_show_table_extended() {
        assert_eq!(
            parse_show_statement(
                "SHOW TABLE EXTENDED FROM main.sales LIKE 'fact*' PARTITION (dt='2026-05-25')"
            )
            .unwrap(),
            Some(MetadataStatement::TableExtended {
                schema: Some(ObjectName(vec!["main".to_string(), "sales".to_string()])),
                pattern: "fact*".to_string(),
                partition: Some("(dt=2026-05-25)".to_string())
            })
        );
    }

    #[test]
    fn rejects_unsupported_show_shapes() {
        assert!(parse_show_statement("SHOW SCHEMAS HISTORY").is_err());
        assert!(parse_show_statement("SHOW TABLES EXTENDED").is_err());
        assert!(parse_show_statement("SHOW COLUMNS").is_err());
        assert!(parse_show_statement("SHOW COLUMNS IN customer LIKE 'cust*'").is_err());
        assert!(parse_show_statement("SHOW TABLE EXTENDED IN sales").is_err());
        assert!(
            parse_show_statement("SHOW TABLE EXTENDED IN sales LIKE 'x' PARTITION dt='1'").is_err()
        );
    }
}
