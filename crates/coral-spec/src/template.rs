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
            let (raw_key, default_value) = match token.split_once('|') {
                Some((key, default)) => (key.trim(), Some(default.to_string())),
                None => (token, None),
            };
            let (namespace, key) = match raw_key.split_once('.') {
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
}
