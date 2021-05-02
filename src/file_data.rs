use std::io::{self, Read};

use crate::mongodb_cwal::gridfs::file::File;

/// To represent the file data retrieved from MongoDB.
#[derive(Debug)]
pub enum FileData {
    /// Data from collections are buffered in memory as a Vec<u8> instance.
    Collection(Vec<u8>),
    /// Data from GridFS are represented by a GridFS file.
    GridFS(Box<File>),
}

impl FileData {
    /// Turn into a Vec<u8> instance.
    #[inline]
    pub fn into_vec(self) -> Result<Vec<u8>, io::Error> {
        match self {
            FileData::Collection(v) => Ok(v),
            FileData::GridFS(mut f) => {
                let mut buffer = Vec::new();

                f.read_to_end(&mut buffer)?;

                Ok(buffer)
            }
        }
    }
}
