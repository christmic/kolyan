//! Tool-specific historical goal checks. No execution, grants or filesystem reads.
mod file_write;
mod predicate;
pub use file_write::FileWriteCommittedChecker;
pub use predicate::FileWriteCommittedPredicateV1;

#[cfg(test)]
mod tests;
