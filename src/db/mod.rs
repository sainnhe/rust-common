//! Database utilities.

mod stmt_builder;
pub use stmt_builder::{KV, PLACEHOLDER, StmtBuilder};

/// The type of database.
pub enum Type {
    MySQL,
    PostgreSQL,
    SQLite,
}
