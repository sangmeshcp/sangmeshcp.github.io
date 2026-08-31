use std::fmt;
use std::io;

/// All errors that can be produced by rocksdb-rs.
#[derive(Debug)]
pub enum Error {
    /// An underlying I/O error from the filesystem or OS.
    Io(io::Error),
    /// On-disk data failed a checksum or structural validation.
    Corruption(String),
    /// The requested key does not exist in the database.
    NotFound,
    /// A caller-supplied argument was invalid.
    InvalidArgument(String),
    /// A background operation (flush / compaction) failed.
    Background(String),
    /// The database is already open by another process.
    AlreadyOpen,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(e) => write!(f, "IO error: {e}"),
            Error::Corruption(s) => write!(f, "Data corruption: {s}"),
            Error::NotFound => write!(f, "Key not found"),
            Error::InvalidArgument(s) => write!(f, "Invalid argument: {s}"),
            Error::Background(s) => write!(f, "Background error: {s}"),
            Error::AlreadyOpen => write!(f, "Database is already open"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        if let Error::Io(e) = self {
            Some(e)
        } else {
            None
        }
    }
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Error::Io(e)
    }
}

/// Shorthand result type used throughout the codebase.
pub type Result<T> = std::result::Result<T, Error>;
