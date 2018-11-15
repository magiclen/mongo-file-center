use std::io::{self, Read};
use mongodb::gridfs::file::File;

/// To represent the file data retrieved from MongoDB.
#[derive(Debug)]
pub enum FileData {
    /// Data from collections are buffered in memory as a Vec<u8> instance.
    Collection(Vec<u8>),
    /// Data from GridFS are represented by a GridFS file.
    GridFS(File),
}

impl FileData {
    /// Turn into a Vec<u8> instance.
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

    /// Turn into a Vec<u8> instance without wrapping. This method is usually used when you are sure the data is from a collection.
    pub fn into_vec_unchecked(self) -> Vec<u8> {
        match self {
            FileData::Collection(v) => v,
            FileData::GridFS(mut f) => {
                let mut buffer = Vec::new();

                f.read_to_end(&mut buffer).unwrap();

                buffer
            }
        }
    }
}