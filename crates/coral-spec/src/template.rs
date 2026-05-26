//! Shared template parsing for source-spec string interpolation.

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::{ManifestError, Result};

/// One parsed template string from the source-spec DSL.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ParsedTemplate {
    raw: String,
    parts: Vec<TemplatePart>,
}

impl ParsedTemplate {
    /// Parse one authored template string into literal and token parts.
    ///
    /// # Errors
    ///
    /// Returns a [`ManifestError`] when the template contains an unclosed token.
    pub fn parse(raw: impl Into<String>) -> Result<Self> {
        let raw = raw.into();
        let mut parts = Vec::new();
        let mut rest = raw.as_str();

        while let Some((literal, after_start)) = rest.split_once("{{") {
            if !literal.is_empty() {
                parts.push(TemplatePart::Literal(literal.to_string()));
            }

            let Some((raw_token, after_token)) = after_start.split_once("}}") else {
                return Err(ManifestError::validation(format!(
                    "unclosed template token in '{raw}'"
                )));
            };
            let token = raw_token.trim();
            let (raw_key, default_value) = match split_default(token) {
                Some((key, default)) if !is_expression_token(key) => {
                    (key.trim(), Some(default.to_string()))
                }
                _ => (token, None),
            };
            let is_expression = is_expression_token(raw_key);
            let (namespace, key) = match raw_key.split_once('.') {
                _ if is_expression => {
                    (TemplateNamespace::Other(raw_key.to_string()), String::new())
                }
                Some((namespace, key)) => (TemplateNamespace::parse(namespace), key.to_string()),
                None => (TemplateNamespace::Other(raw_key.to_string()), String::new()),
            };
            parts.push(TemplatePart::Token(TemplateToken {
                raw: token.to_string(),
                raw_key: raw_key.to_string(),
                namespace,
                key,
                default_value,
            }));
            rest = after_token;
        }

        if !rest.is_empty() {
            parts.push(TemplatePart::Literal(rest.to_string()));
        }

        Ok(Self { raw, parts })
    }

    #[must_use]
    /// Returns the original authored template string.
    pub fn raw(&self) -> &str {
        &self.raw
    }

    #[must_use]
    /// Returns whether the authored template string is empty.
    pub fn is_empty(&self) -> bool {
        self.raw.is_empty()
    }

    #[must_use]
    /// Returns the parsed literal and token parts in source order.
    pub fn parts(&self) -> &[TemplatePart] {
        &self.parts
    }

    /// Iterates over parsed template tokens in source order.
    pub fn tokens(&self) -> impl Iterator<Item = &TemplateToken> {
        self.parts.iter().filter_map(|part| match part {
            TemplatePart::Literal(_) => None,
            TemplatePart::Token(token) => Some(token),
        })
    }
}

impl Serialize for ParsedTemplate {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.raw)
    }
}

impl<'de> Deserialize<'de> for ParsedTemplate {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = String::deserialize(deserializer)?;
        Self::parse(raw).map_err(serde::de::Error::custom)
    }
}

impl PartialEq<&str> for ParsedTemplate {
    fn eq(&self, other: &&str) -> bool {
        self.raw == *other
    }
}

/// One part of a parsed template string.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TemplatePart {
    /// A literal string segment copied directly into rendered output.
    Literal(String),
    /// One parsed interpolation token.
    Token(TemplateToken),
}

/// One parsed `{{namespace.key|default}}` token.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TemplateToken {
    raw: String,
    raw_key: String,
    namespace: TemplateNamespace,
    key: String,
    default_value: Option<String>,
}

impl TemplateToken {
    #[must_use]
    /// Returns the raw token body inside `{{...}}`, after trimming whitespace.
    pub fn raw(&self) -> &str {
        &self.raw
    }

    #[must_use]
    /// Returns the raw namespace-plus-key portion before any default value.
    pub fn raw_key(&self) -> &str {
        &self.raw_key
    }

    #[must_use]
    /// Returns the parsed namespace for this token.
    pub fn namespace(&self) -> &TemplateNamespace {
        &self.namespace
    }

    #[must_use]
    /// Returns the token key after the namespace separator.
    pub fn key(&self) -> &str {
        &self.key
    }

    #[must_use]
    /// Returns the authored default value, if any.
    pub fn default_value(&self) -> Option<&str> {
        self.default_value.as_deref()
    }

    #[must_use]
    /// Returns whether this token uses expression syntax.
    pub fn is_expression(&self) -> bool {
        is_expression_token(self.raw_key.as_str())
    }

    /// Returns the input keys referenced by this token.
    ///
    /// For simple tokens this is either the single `input.KEY` reference or
    /// empty. Expression tokens support the small auth-template expression
    /// syntax used by the renderer: fallbacks with `||` and `concat(...)`.
    pub fn input_keys(&self) -> Vec<&str> {
        if self.is_expression() {
            return expression_keys(self.raw_key.as_str(), "input.");
        }
        if self.namespace == TemplateNamespace::Input {
            return vec![self.key.as_str()];
        }
        Vec::new()
    }

    /// Returns the filter keys referenced by this token.
    pub fn filter_keys(&self) -> Vec<&str> {
        if self.is_expression() {
            expression_keys(self.raw_key.as_str(), "filter.")
        } else if self.namespace == TemplateNamespace::Filter {
            vec![self.key.as_str()]
        } else {
            Vec::new()
        }
    }

    /// Returns the state keys referenced by this token.
    pub fn state_keys(&self) -> Vec<&str> {
        if self.is_expression() {
            expression_keys(self.raw_key.as_str(), "state.")
        } else if self.namespace == TemplateNamespace::State {
            vec![self.key.as_str()]
        } else {
            Vec::new()
        }
    }

    /// Returns the source function argument keys referenced by this token.
    pub fn arg_keys(&self) -> Vec<&str> {
        if self.is_expression() {
            expression_keys(self.raw_key.as_str(), "arg.")
        } else if self.namespace == TemplateNamespace::Arg {
            vec![self.key.as_str()]
        } else {
            Vec::new()
        }
    }
}

/// The namespace component of one template token.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TemplateNamespace {
    /// A declared source input, looked up by authored key. The declared kind
    /// (variable or secret) in the manifest's top-level `inputs` table
    /// determines which store resolves the value.
    Input,
    /// A SQL filter token.
    Filter,
    /// A source-scoped table function request argument token.
    Arg,
    /// A row-expression sub-expression token.
    Expr,
    /// A runtime pagination or request state token.
    State,
    /// Any other namespace, preserved for higher-level validation.
    Other(String),
}

impl TemplateNamespace {
    fn parse(raw: &str) -> Self {
        match raw {
            "input" => Self::Input,
            "filter" => Self::Filter,
            "arg" => Self::Arg,
            "expr" => Self::Expr,
            "state" => Self::State,
            other => Self::Other(other.to_string()),
        }
    }
}

fn is_expression_token(raw: &str) -> bool {
    let trimmed = raw.trim();
    has_top_level_delimiter(trimmed, "||") || trimmed.starts_with("concat(")
}

fn split_default(raw: &str) -> Option<(&str, &str)> {
    let mut depth = 0usize;
    let mut in_quote: Option<char> = None;
    let mut escaped = false;

    for (index, ch) in raw.char_indices() {
        if let Some(quote) = in_quote {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == quote {
                in_quote = None;
            }
            continue;
        }

        match ch {
            '"' | '\'' => in_quote = Some(ch),
            '(' => depth = depth.saturating_add(1),
            ')' => depth = depth.saturating_sub(1),
            '|' if depth == 0 => {
                let prev_is_pipe = raw
                    .get(..index)
                    .and_then(|prefix| prefix.chars().next_back())
                    == Some('|');
                let next_is_pipe = raw
                    .get(index + ch.len_utf8()..)
                    .and_then(|suffix| suffix.chars().next())
                    == Some('|');
                if !prev_is_pipe && !next_is_pipe {
                    return raw.get(..index).zip(raw.get(index + ch.len_utf8()..));
                }
            }
            _ => {}
        }
    }
    None
}

fn has_top_level_delimiter(raw: &str, delimiter: &str) -> bool {
    let mut depth = 0usize;
    let mut in_quote: Option<char> = None;
    let mut escaped = false;

    for (index, ch) in raw.char_indices() {
        if let Some(quote) = in_quote {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == quote {
                in_quote = None;
            }
            continue;
        }

        match ch {
            '"' | '\'' => in_quote = Some(ch),
            '(' => depth = depth.saturating_add(1),
            ')' => depth = depth.saturating_sub(1),
            _ if depth == 0
                && raw
                    .get(index..)
                    .is_some_and(|rest| rest.starts_with(delimiter)) =>
            {
                return true;
            }
            _ => {}
        }
    }
    false
}

fn expression_keys<'a>(raw: &'a str, prefix: &str) -> Vec<&'a str> {
    let mut out = Vec::new();
    let mut cursor = 0;
    let bytes = raw.as_bytes();
    let mut in_quote: Option<u8> = None;
    while cursor < bytes.len() {
        let Some(&byte) = bytes.get(cursor) else {
            break;
        };
        if let Some(quote) = in_quote {
            if byte == b'\\' {
                cursor = cursor.saturating_add(2);
                continue;
            }
            if byte == quote {
                in_quote = None;
            }
            cursor += 1;
            continue;
        }
        if byte == b'\'' || byte == b'"' {
            in_quote = Some(byte);
            cursor += 1;
            continue;
        }
        let rest = raw.get(cursor..).unwrap_or_default();
        if reference_starts_at(raw, cursor, prefix)
            && let Some(after_prefix) = rest.strip_prefix(prefix)
        {
            let key_len = after_prefix
                .find(|ch: char| !(ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.')))
                .unwrap_or(after_prefix.len());
            if key_len > 0
                && let Some(key) = after_prefix.get(..key_len)
            {
                out.push(key);
            }
            cursor += prefix.len() + key_len;
            continue;
        }
        cursor += 1;
    }
    out
}

fn reference_starts_at(raw: &str, cursor: usize, prefix: &str) -> bool {
    if !raw
        .get(cursor..)
        .is_some_and(|rest| rest.starts_with(prefix))
    {
        return false;
    }
    raw.get(..cursor)
        .and_then(|prefix| prefix.chars().next_back())
        .is_none_or(|ch| !(ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.')))
}

#[cfg(test)]
mod tests {
    use super::{ParsedTemplate, TemplateNamespace, TemplatePart};

    #[test]
    fn parses_literals_and_tokens_in_order() {
        let template =
            ParsedTemplate::parse("Bearer {{input.API_TOKEN}} for {{filter.org|openai}}")
                .expect("template");

        assert_eq!(
            template.raw(),
            "Bearer {{input.API_TOKEN}} for {{filter.org|openai}}"
        );
        assert_eq!(template.parts().len(), 4);
        let parts = template.parts();
        assert!(matches!(
            parts.first(),
            Some(TemplatePart::Literal(part)) if part == "Bearer "
        ));
        assert!(matches!(
            parts.get(1),
            Some(TemplatePart::Token(token))
                if token.namespace() == &TemplateNamespace::Input && token.key() == "API_TOKEN"
        ));
        assert!(matches!(
            parts.get(2),
            Some(TemplatePart::Literal(part)) if part == " for "
        ));
        assert!(matches!(
            parts.get(3),
            Some(TemplatePart::Token(token))
                if token.namespace() == &TemplateNamespace::Filter
                    && token.key() == "org"
                    && token.default_value() == Some("openai")
        ));
    }

    #[test]
    fn parses_unknown_token_namespaces_without_rejecting() {
        let template = ParsedTemplate::parse("{{custom.value}}").expect("template");
        let token = template.tokens().next().expect("token");
        assert_eq!(
            token.namespace(),
            &TemplateNamespace::Other("custom".to_string())
        );
        assert_eq!(token.key(), "value");
    }

    #[test]
    fn parses_expr_namespace_tokens() {
        let template = ParsedTemplate::parse("{{expr.slug|untitled}}").expect("template");
        let token = template.tokens().next().expect("token");
        assert_eq!(token.namespace(), &TemplateNamespace::Expr);
        assert_eq!(token.key(), "slug");
        assert_eq!(token.default_value(), Some("untitled"));
    }

    #[test]
    fn rejects_unclosed_tokens() {
        let error = ParsedTemplate::parse("{{input.API_TOKEN").expect_err("unclosed token");
        assert!(error.to_string().contains("unclosed template token"));
    }

    #[test]
    fn token_reports_expression_input_keys() {
        let template =
            ParsedTemplate::parse(r#"{{input.API_KEY || concat("Bearer ", input.OAUTH_TOKEN)}}"#)
                .expect("template");
        let token = template.tokens().next().expect("token");
        assert!(token.is_expression());
        assert_eq!(token.input_keys(), vec!["API_KEY", "OAUTH_TOKEN"]);
    }

    #[test]
    fn token_reports_expression_filter_and_state_keys() {
        let template = ParsedTemplate::parse(r#"{{concat(filter.team, "-", state.cursor)}}"#)
            .expect("template");
        let token = template.tokens().next().expect("token");
        assert!(token.is_expression());
        assert_eq!(token.filter_keys(), vec!["team"]);
        assert_eq!(token.state_keys(), vec!["cursor"]);
    }

    #[test]
    fn token_reports_expression_arg_keys() {
        let template =
            ParsedTemplate::parse(r#"{{arg.query || concat("Bearer ", input.OAUTH_TOKEN)}}"#)
                .expect("template");
        let token = template.tokens().next().expect("token");
        assert!(token.is_expression());
        assert_eq!(token.arg_keys(), vec!["query"]);
        assert_eq!(token.input_keys(), vec!["OAUTH_TOKEN"]);
    }

    #[test]
    fn default_values_can_contain_fallback_operator() {
        let template = ParsedTemplate::parse("{{input.API_KEY|foo||bar}}").expect("template");
        let token = template.tokens().next().expect("token");

        assert!(!token.is_expression());
        assert_eq!(token.namespace(), &TemplateNamespace::Input);
        assert_eq!(token.key(), "API_KEY");
        assert_eq!(token.default_value(), Some("foo||bar"));
        assert_eq!(token.input_keys(), vec!["API_KEY"]);
    }

    #[test]
    fn expression_references_require_namespace_boundary() {
        let template = ParsedTemplate::parse(r"{{filter.input.API_KEY || input.OAUTH_TOKEN}}")
            .expect("template");
        let token = template.tokens().next().expect("token");

        assert!(token.is_expression());
        assert_eq!(token.filter_keys(), vec!["input.API_KEY"]);
        assert_eq!(token.input_keys(), vec!["OAUTH_TOKEN"]);
    }
}
