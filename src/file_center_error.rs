extern crate r2d2;

use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::io;

use crate::bson;
use crate::mongodb_cwal;

#[derive(Debug)]
pub enum FileCenterError {
    R2D2Error(r2d2::Error),
    MongoDBError(mongodb_cwal::Error),
    DocumentError(bson::ValueAccessError),
    IDTokenError(&'static str),
    FileSizeThresholdError,
    VersionError,
    DatabaseTooNewError {
        supported_latest: i32,
        current: i32,
    },
    IOError(io::Error),
    MimeTypeError,
}

impl Display for FileCenterError {
    #[inline]
    fn fmt(&self, f: &mut Formatter) -> Result<(), fmt::Error> {
        match self {
            FileCenterError::R2D2Error(err) => Display::fmt(err, f),
            FileCenterError::MongoDBError(err) => Display::fmt(err, f),
            FileCenterError::DocumentError(err) => Display::fmt(err, f),
            FileCenterError::IDTokenError(err) => f.write_str(err),
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
            FileCenterError::MimeTypeError => f.write_str("the mime type is incorrect"),
        }
    }
}

impl Error for FileCenterError {}

impl From<r2d2::Error> for FileCenterError {
    #[inline]
    fn from(err: r2d2::Error) -> Self {
        FileCenterError::R2D2Error(err)
    }
}

impl From<mongodb_cwal::Error> for FileCenterError {
    #[inline]
    fn from(err: mongodb_cwal::Error) -> Self {
        FileCenterError::MongoDBError(err)
    }
}

impl From<bson::ValueAccessError> for FileCenterError {
    #[inline]
    fn from(err: bson::ValueAccessError) -> Self {
        FileCenterError::DocumentError(err)
    }
}

impl From<io::Error> for FileCenterError {
    #[inline]
    fn from(err: io::Error) -> Self {
        FileCenterError::IOError(err)
    }
}
