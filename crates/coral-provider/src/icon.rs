//! The image an Owner uploads to stand for a Provider in every theme.
//!
//! Coral reads the image's type from its content, never from what the
//! uploader claims, and accepts only PNG, JPEG, WebP and SVG. It does not
//! sanitize SVG: whoever serves the icon must keep its scripts from running,
//! as Reef does with a sandboxing Content Security Policy.
use base64::Engine as _;
use sha2::{Digest as _, Sha256};

use crate::Error;

/// The largest icon Coral stores, in bytes.
pub const MAX_PROVIDER_ICON_BYTES: usize = 100 * 1024;

/// The image formats a Provider icon may take.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderIconMediaType {
    /// `image/png`.
    Png,
    /// `image/jpeg`.
    Jpeg,
    /// `image/webp`.
    Webp,
    /// `image/svg+xml`.
    Svg,
}

impl ProviderIconMediaType {
    /// The media type a browser reads the icon as.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Png => "image/png",
            Self::Jpeg => "image/jpeg",
            Self::Webp => "image/webp",
            Self::Svg => "image/svg+xml",
        }
    }

    fn from_storage_str(value: &str) -> Result<Self, Error> {
        [Self::Png, Self::Jpeg, Self::Webp, Self::Svg]
            .into_iter()
            .find(|media_type| media_type.as_str() == value)
            .ok_or_else(|| Error::CorruptData("Provider icon has invalid media type".to_string()))
    }

    /// The format `content` is in, read from its bytes.
    fn sniff(content: &[u8]) -> Option<Self> {
        if content.starts_with(b"\x89PNG\r\n\x1a\n") {
            Some(Self::Png)
        } else if content.starts_with(b"\xff\xd8\xff") {
            Some(Self::Jpeg)
        } else if content.starts_with(b"RIFF") && content.get(8..12) == Some(b"WEBP".as_slice()) {
            Some(Self::Webp)
        } else if is_svg(content) {
            Some(Self::Svg)
        } else {
            None
        }
    }
}

/// An image that stands for a Provider, with the digest that names this
/// exact content.
#[derive(Clone, PartialEq, Eq)]
pub struct ProviderIcon {
    media_type: ProviderIconMediaType,
    content: Vec<u8>,
    digest: String,
}

impl std::fmt::Debug for ProviderIcon {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProviderIcon")
            .field("media_type", &self.media_type)
            .field("bytes", &self.content.len())
            .field("digest", &self.digest)
            .finish()
    }
}

impl ProviderIcon {
    /// Accepts an uploaded image.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidInput`] for content that is empty, larger than
    /// [`MAX_PROVIDER_ICON_BYTES`], or not a PNG, JPEG, WebP or SVG image.
    pub fn parse(content: Vec<u8>) -> Result<Self, Error> {
        let media_type = (!content.is_empty() && content.len() <= MAX_PROVIDER_ICON_BYTES)
            .then(|| ProviderIconMediaType::sniff(&content))
            .flatten()
            .ok_or_else(|| {
                Error::InvalidInput(
                    "Provider icon must be a PNG, JPEG, WebP or SVG image of at most 100 KiB"
                        .to_string(),
                )
            })?;
        let digest =
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(Sha256::digest(&content));
        Ok(Self {
            media_type,
            content,
            digest,
        })
    }

    pub(crate) fn from_storage(
        media_type: &str,
        content: Vec<u8>,
        digest: String,
    ) -> Result<Self, Error> {
        Ok(Self {
            media_type: ProviderIconMediaType::from_storage_str(media_type)?,
            content,
            digest,
        })
    }

    /// The image's format.
    #[must_use]
    pub const fn media_type(&self) -> ProviderIconMediaType {
        self.media_type
    }

    /// The image itself.
    #[must_use]
    pub fn content(&self) -> &[u8] {
        &self.content
    }

    /// Names this exact content; another image has another digest.
    #[must_use]
    pub fn digest(&self) -> &str {
        &self.digest
    }

    /// Takes the image out.
    #[must_use]
    pub fn into_content(self) -> Vec<u8> {
        self.content
    }
}

/// Whether `content` is UTF-8 text whose root element is `<svg>`, after an
/// optional byte order mark, XML declaration, processing instructions and
/// comments. A document type declaration fails, so no entity expands.
fn is_svg(content: &[u8]) -> bool {
    let Ok(text) = std::str::from_utf8(content) else {
        return false;
    };
    if text.contains("<!DOCTYPE") || text.contains("<!ENTITY") {
        return false;
    }
    let mut rest = text.strip_prefix('\u{feff}').unwrap_or(text);
    loop {
        rest = rest.trim_start();
        let (after, closing) = if let Some(after) = rest.strip_prefix("<?") {
            (after, "?>")
        } else if let Some(after) = rest.strip_prefix("<!--") {
            (after, "-->")
        } else {
            break;
        };
        let Some((_skipped, next)) = after.split_once(closing) else {
            return false;
        };
        rest = next;
    }
    rest.strip_prefix("<svg").is_some_and(|after| {
        after.starts_with(|character: char| {
            character.is_ascii_whitespace() || character == '>' || character == '/'
        })
    })
}

#[cfg(test)]
mod tests {
    use super::{MAX_PROVIDER_ICON_BYTES, ProviderIcon, ProviderIconMediaType};

    const SVG: &str = r#"<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 1 1"/>"#;

    #[test]
    fn reads_each_format_from_its_content() {
        let mut webp = b"RIFF\0\0\0\0WEBPVP8 ".to_vec();
        webp.extend([0; 8]);
        let cases: [(&str, Vec<u8>, ProviderIconMediaType); 6] = [
            (
                "png",
                b"\x89PNG\r\n\x1a\n\0\0".to_vec(),
                ProviderIconMediaType::Png,
            ),
            (
                "jpeg",
                b"\xff\xd8\xff\xe0\0".to_vec(),
                ProviderIconMediaType::Jpeg,
            ),
            ("webp", webp, ProviderIconMediaType::Webp),
            (
                "bare svg",
                SVG.as_bytes().to_vec(),
                ProviderIconMediaType::Svg,
            ),
            (
                "svg after a declaration, a comment and a byte order mark",
                format!("\u{feff}<?xml version=\"1.0\"?>\n<!-- logo -->\n{SVG}").into_bytes(),
                ProviderIconMediaType::Svg,
            ),
            (
                "svg at the size limit",
                {
                    let mut svg = b"<svg>".to_vec();
                    svg.resize(MAX_PROVIDER_ICON_BYTES - 6, b' ');
                    svg.extend(b"</svg>");
                    svg
                },
                ProviderIconMediaType::Svg,
            ),
        ];
        for (case, content, expected) in cases {
            let icon = ProviderIcon::parse(content.clone()).expect(case);
            assert_eq!(icon.media_type(), expected, "{case}");
            assert_eq!(icon.content(), content.as_slice(), "{case}");
            assert_eq!(icon.digest().len(), 43, "{case}");
        }
    }

    #[test]
    fn rejects_anything_but_a_bounded_image() {
        let cases: [(&str, Vec<u8>); 7] = [
            ("empty", Vec::new()),
            ("text", b"hello".to_vec()),
            ("html", b"<html><svg></svg></html>".to_vec()),
            ("svg-like element", b"<svgx/>".to_vec()),
            (
                "document type",
                b"<!DOCTYPE svg [<!ENTITY a \"a\">]><svg>&a;</svg>".to_vec(),
            ),
            ("unclosed comment", b"<!-- <svg/>".to_vec()),
            ("over the size limit", {
                let mut png = b"\x89PNG\r\n\x1a\n".to_vec();
                png.resize(MAX_PROVIDER_ICON_BYTES + 1, 0);
                png
            }),
        ];
        for (case, content) in cases {
            assert!(
                matches!(
                    ProviderIcon::parse(content),
                    Err(crate::Error::InvalidInput(_))
                ),
                "{case}"
            );
        }
    }

    #[test]
    fn digest_names_the_content() {
        let first = ProviderIcon::parse(SVG.as_bytes().to_vec()).expect("svg");
        let same = ProviderIcon::parse(SVG.as_bytes().to_vec()).expect("svg");
        let other = ProviderIcon::parse(b"<svg></svg>".to_vec()).expect("svg");
        assert_eq!(first.digest(), same.digest());
        assert_ne!(first.digest(), other.digest());
    }
}
