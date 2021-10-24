use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::io;

#[derive(Debug)]
pub enum FileCenterError {
    MongoDBError(crate::mongodb::error::Error),
    DocumentError(crate::bson::document::ValueAccessError),
    FileSizeThresholdError,
    VersionError,
    DatabaseTooNewError {
        supported_latest: i32,
        current: i32,
    },
    IOError(io::Error),
    IDTokenError(&'static str),
}

impl Display for FileCenterError {
    #[inline]
    fn fmt(&self, f: &mut Formatter) -> Result<(), fmt::Error> {
        match self {
            FileCenterError::MongoDBError(err) => Display::fmt(err, f),
            FileCenterError::DocumentError(err) => Display::fmt(err, f),
            FileCenterError::FileSizeThresholdError => {
                f.write_str("the file size threshold is incorrect")
            }
            FileCenterError::VersionError => f.write_str("the version is incorrect"),
            FileCenterError::DatabaseTooNewError {
                supported_latest,
                current,
            } => {
                f.write_fmt(format_args!(
                    "the current database version is {}, but this library only supports to {}",
                    current, supported_latest
                ))
            }
            FileCenterError::IOError(err) => Display::fmt(err, f),
            FileCenterError::IDTokenError(err) => f.write_str(err),
        }
    }
}

impl Error for FileCenterError {}

impl From<crate::mongodb::error::Error> for FileCenterError {
    #[inline]
    fn from(err: crate::mongodb::error::Error) -> Self {
        FileCenterError::MongoDBError(err)
    }
}

impl From<crate::bson::document::ValueAccessError> for FileCenterError {
    #[inline]
    fn from(err: crate::bson::document::ValueAccessError) -> Self {
        FileCenterError::DocumentError(err)
    }
}

impl From<io::Error> for FileCenterError {
    #[inline]
    fn from(err: io::Error) -> Self {
        FileCenterError::IOError(err)
    }
}
