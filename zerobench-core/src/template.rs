//! Compiled string templates with `{{...}}` substitution.
//!
//! Task 1 introduces this module as a stub so the [`Plan`] data model can
//! reference `Template` without cyclic dependencies. Task 2 fills in the
//! compiler, parts vocabulary, and zero-alloc expansion path.
//!
//! [`Plan`]: crate::plan::Plan

use bytes::Bytes;
use serde::{Deserialize, Serialize};

/// A compiled template — a sequence of literals and substitution parts.
///
/// Produced by [`Template::compile`]; consumed on the hot path by
/// [`Template::expand_into`]. Cheap to clone (all owned bytes are
/// reference-counted via [`bytes::Bytes`]).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Template {
    /// Sequence of parts to emit in order.
    parts: Vec<Part>,
    /// Pre-computed expansion-size hint. Used to pre-reserve output buffers.
    estimated_size: usize,
}

impl Template {
    /// Returns an empty template (expands to no bytes).
    pub fn empty() -> Self {
        Self::default()
    }

    /// Construct a template consisting of a single literal. Primarily used
    /// by parsers that already know the string is static.
    pub fn literal(bytes: impl Into<Bytes>) -> Self {
        let bytes = bytes.into();
        let size = bytes.len();
        Self {
            parts: vec![Part::Literal(bytes)],
            estimated_size: size,
        }
    }

    /// Size hint in bytes. The actual expansion may exceed this for
    /// dynamic parts like `{{rand_str:LEN}}`, which contribute their
    /// configured upper bound.
    pub fn estimated_size(&self) -> usize {
        self.estimated_size
    }

    /// Number of parts (literals + substitutions) the template compiled to.
    pub fn part_count(&self) -> usize {
        self.parts.len()
    }

    // Internal accessors used by the expansion engine (filled in Task 2).
    #[doc(hidden)]
    #[allow(dead_code)] // used in Task 2.
    pub(crate) fn parts(&self) -> &[Part] {
        &self.parts
    }

    #[doc(hidden)]
    #[allow(dead_code)] // used in Task 2.
    pub(crate) fn from_parts(parts: Vec<Part>, estimated_size: usize) -> Self {
        Self {
            parts,
            estimated_size,
        }
    }
}

/// A single substitution unit. The full vocabulary is introduced in Task 2.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Part {
    /// Raw bytes emitted verbatim.
    Literal(Bytes),
}
