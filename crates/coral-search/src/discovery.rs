//! What one discovery call asks for and what it returns: a ranked page for
//! a query, or the exact descriptions of named Call Paths.
use coral_catalog::{SEARCH_PATH_MAX_BYTES, SEARCH_PATHS_MAX};

use crate::{
    DESCRIBE_TYPE_NAME_MAX_BYTES, DESCRIBE_TYPES_MAX, Error, ExactAuthoringDescription, SearchPage,
    SearchRequest,
};

/// One discovery request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DiscoveryRequest {
    /// A ranked page of Search Hits, or a listing without a query.
    Query(SearchRequest),
    /// The exact descriptions of named Call Paths.
    Paths(ExactPaths),
}

/// 1 to [`SEARCH_PATHS_MAX`] Call Paths to describe exactly, as the caller
/// wrote them, and the type names to return for the one path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExactPaths {
    paths: Vec<String>,
    types: Vec<String>,
}

impl ExactPaths {
    /// Bounds a lookup. A path need not parse: one that names nothing the
    /// caller can call is reported missing, never rejected.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidInput`] for no path, more than
    /// [`SEARCH_PATHS_MAX`], a path longer than [`SEARCH_PATH_MAX_BYTES`], or
    /// `types` with more than one path, and [`Error::TypesLimit`] when
    /// `types` passes its limits.
    pub fn new(paths: Vec<String>, types: Vec<String>) -> Result<Self, Error> {
        if paths.is_empty() || paths.len() > SEARCH_PATHS_MAX {
            return Err(Error::InvalidInput(format!(
                "pass 1 to {SEARCH_PATHS_MAX} Call Paths"
            )));
        }
        if paths.iter().any(|path| path.len() > SEARCH_PATH_MAX_BYTES) {
            return Err(Error::InvalidInput(format!(
                "a Call Path is at most {SEARCH_PATH_MAX_BYTES} UTF-8 bytes"
            )));
        }
        if !types.is_empty() && paths.len() != 1 {
            return Err(Error::InvalidInput("pass types with one path".to_owned()));
        }
        Self::checked_types(paths, types)
    }

    /// One already parsed path, for `describe`. Unlike [`Self::new`], it
    /// leaves the path's length unbounded, as `describe` always has.
    pub(crate) fn one(path: &crate::CallPath, types: &[String]) -> Result<Self, Error> {
        Self::checked_types(vec![path.as_str().to_owned()], types.to_vec())
    }

    fn checked_types(paths: Vec<String>, types: Vec<String>) -> Result<Self, Error> {
        if types.len() > DESCRIBE_TYPES_MAX
            || types
                .iter()
                .any(|name| name.len() > DESCRIBE_TYPE_NAME_MAX_BYTES)
        {
            return Err(Error::TypesLimit);
        }
        Ok(Self { paths, types })
    }

    /// The paths, in the order named.
    #[must_use]
    pub fn paths(&self) -> &[String] {
        &self.paths
    }

    /// The type names to return instead of the declaration; empty for the
    /// abridged declaration.
    #[must_use]
    pub fn types(&self) -> &[String] {
        &self.types
    }
}

/// What one discovery request found, all from one read snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Discovery {
    /// The page a query or listing returned.
    Page(SearchPage),
    /// The exact descriptions a lookup returned.
    Exact(ExactLookup),
}

/// The exact descriptions of the paths a lookup named that the caller can
/// discover, and the rest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExactLookup {
    pub(crate) descriptions: Vec<ExactAuthoringDescription>,
    pub(crate) missing: Vec<MissingPath>,
}

impl ExactLookup {
    /// The description of each path the caller can discover, in the order
    /// named.
    #[must_use]
    pub fn descriptions(&self) -> &[ExactAuthoringDescription] {
        &self.descriptions
    }

    /// Each path that names nothing the caller can discover, in the order
    /// named.
    #[must_use]
    pub fn missing(&self) -> &[MissingPath] {
        &self.missing
    }
}

/// A named path that is absent, malformed, or concealed from the caller.
/// The three are indistinguishable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MissingPath {
    call_path: String,
    suggestions: Vec<String>,
}

impl MissingPath {
    pub(crate) const fn new(call_path: String, suggestions: Vec<String>) -> Self {
        Self {
            call_path,
            suggestions,
        }
    }

    /// The path as the caller wrote it.
    #[must_use]
    pub fn call_path(&self) -> &str {
        &self.call_path
    }

    /// Up to [`crate::SUGGESTIONS_MAX`] Discoverable Call Paths of the same
    /// Call Root near this one, nearest first; empty when none is near or
    /// the path names no Call Root the caller can search.
    #[must_use]
    pub fn suggestions(&self) -> &[String] {
        &self.suggestions
    }
}
