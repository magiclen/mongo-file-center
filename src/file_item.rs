use std::io::{self, Cursor};

use crate::bson::oid::ObjectId;
use crate::bson::DateTime;

use crate::tokio_stream::Stream;

use crate::mime::Mime;

use crate::FileData;

/// To represent the file retrieved from MongoDB.
#[derive(Educe)]
#[educe(Debug)]
pub struct FileItem<T: Stream<Item = Result<Cursor<Vec<u8>>, io::Error>> + Unpin> {
    pub(crate) file_id: ObjectId,
    pub(crate) create_time: DateTime,
    pub(crate) expire_at: Option<DateTime>,
    pub(crate) mime_type: Mime,
    pub(crate) file_size: u64,
    pub(crate) file_name: String,
    pub(crate) file_data: FileData<T>,
}

impl<T: Stream<Item = Result<Cursor<Vec<u8>>, io::Error>> + Unpin> FileItem<T> {
    pub fn get_file_id(&self) -> ObjectId {
        self.file_id
    }

    pub fn get_create_time(&self) -> DateTime {
        self.create_time
    }

    pub fn get_expiration_time(&self) -> Option<DateTime> {
        self.expire_at
    }

    pub fn get_mime_type(&self) -> &Mime {
        &self.mime_type
    }

    pub fn get_file_size(&self) -> u64 {
        self.file_size
    }

    pub fn get_file_name(&self) -> &str {
        &self.file_name
    }

    pub fn into_file_data(self) -> FileData<T> {
        self.file_data
    }
}
