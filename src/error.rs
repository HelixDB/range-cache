//! Public validation errors.

use std::ops::Range;

/// A byte-range or payload validation error.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum RangeError {
    /// A range had its end before its start.
    #[error("reversed byte range {start}..{end}")]
    ReversedRange {
        /// Inclusive start offset.
        start: usize,
        /// Exclusive end offset.
        end: usize,
    },
    /// A payload length did not equal the range length.
    #[error("range {range:?} requires {expected} bytes, received {actual}")]
    PayloadLengthMismatch {
        /// Range associated with the payload.
        range: Range<usize>,
        /// Required byte length.
        expected: usize,
        /// Actual byte length.
        actual: usize,
    },
}
