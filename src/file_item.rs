extern crate chrono;

use chrono::prelude::*;

use crate::mime::Mime;
use crate::mongodb_cwal::oid::ObjectId;

use crate::FileData;

/// To represent the file retrieved from MongoDB.
#[derive(Debug)]
pub struct FileItem {
    pub(crate) id: ObjectId,
    pub(crate) create_time: DateTime<Utc>,
    pub(crate) expire_at: Option<DateTime<Utc>>,
    pub(crate) mime_type: Mime,
    pub(crate) file_size: u64,
    pub(crate) file_name: String,
    pub(crate) file_data: FileData,
}

impl FileItem {
    pub fn get_object_id(&self) -> &ObjectId {
        &self.id
    }

    pub fn get_create_time(&self) -> &DateTime<Utc> {
        &self.create_time
    }

    pub fn get_expiration_time(&self) -> Option<&DateTime<Utc>> {
        self.expire_at.as_ref()
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

    pub fn into_file_data(self) -> FileData {
        self.file_data
    }
}
