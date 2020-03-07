/*!
# File Center on MongoDB

This crate aims to build an easy-to-use and no-redundant file storage based on MongoDB.

For perennial files, each of them is unique in the database, and can be retrieved many times without limitation.

For temporary files, they are allowed to be duplicated, but each instance can be retrieved only one time in a minute after it is created.

The file data can be stored in a collection or GridFS. It depends on the size of data. If the size is bigger than the threshold, it stores in GridFS, or it stores in a collection. The max threshold is **16770KB**. The default threshold is **255KiB**.

Temporary files are suggested to store in a collection, otherwise you have to **clear the garbage** in GridFS.

## Example

```rust,ignore
extern crate mongo_file_center;
extern crate mime;

use mongo_file_center::{FileCenter, FileData};

const HOST: &str = "localhost";
const PORT: u16 = 27017;

let database = "test_my_file_storage";

let file_center = FileCenter::new(HOST, PORT, database).unwrap();

let file = file_center.put_file_by_path("/path/to/file", Some("file_name"), Some(mime::IMAGE_JPEG)).unwrap();

let file_id = file.get_object_id();

let id_token = file_center.encrypt_id(&file_id); // this token is safe in public

let file_id = file_center.decrypt_id_token(&id_token).unwrap();

let r_file = file_center.get_file_item_by_id(file_id).unwrap().unwrap();

match r_file.into_file_data() {
    FileData::GridFS(file) => {
        // do something
    }
    FileData::FileData(data) => {
        // do something
    }
}
```
*/
#[macro_use(bson, doc)]
pub extern crate bson;

#[macro_use]
extern crate lazy_static;

extern crate chrono;
pub extern crate mime;
extern crate mime_guess;
pub extern crate mongodb;
extern crate rand;
extern crate sha3;
extern crate short_crypt;

mod file_data;
mod file_item;

pub use file_data::FileData;
pub use file_item::FileItem;

use mongodb::coll::{
    options::{FindOneAndUpdateOptions, FindOptions, IndexOptions, ReturnDocument, UpdateOptions},
    Collection,
};
use mongodb::db::{Database, ThreadedDatabase};
use mongodb::gridfs::{self, Store, ThreadedStore};
use mongodb::{Client, ThreadedClient};

use bson::oid::ObjectId;
use bson::spec::BinarySubtype;
use bson::{Bson, Document};

use mime::Mime;

use rand::{thread_rng, Rng};

use chrono::prelude::*;

use short_crypt::ShortCrypt;

use sha3::{Digest, Sha3_256};

use std::error::Error;
use std::fmt::{Display, Error as FmtError, Formatter};
use std::fs::File;
use std::io::{self, Read, Write};
use std::mem::transmute;
use std::path::Path;
use std::str::FromStr;

const BUFFER_SIZE: usize = 4096;
const DEFAULT_MIME_TYPE: Mime = mime::APPLICATION_OCTET_STREAM;

pub const COLLECTION_FILES_NAME: &str = "file_center";
pub const COLLECTION_SETTINGS_NAME: &str = "file_center_settings";
const MAX_FILE_SIZE_THRESHOLD: i32 = 16_770_000;
const DEFAULT_FILE_SIZE_THRESHOLD: i32 = 261_120;

pub const SETTING_FILE_SIZE_THRESHOLD: &str = "file_size_threshold";
pub const SETTING_CREATE_TIME: &str = "create_time";
pub const SETTING_VERSION: &str = "version";

const VERSION: i32 = 1; // Used for updating the database.

const TEMPORARY_LIFE_TIME: i64 = 60000;

lazy_static! {
    static ref FILE_ITEM_PROJECTION: Document = {
        doc! {
            "create_time": 1,
            "mime_type": 1,
            "file_size": 1,
            "file_name": 1,
            "file_data": 1,
            "file_id": 1,
            "expire_at": 1,
        }
    };
    static ref FILE_ITEM_DELETE_PROJECTION: Document = {
        doc! {
            "_id": 1,
            "count": 1,
            "file_id": 1,
            "file_size": 1,
        }
    };
    static ref ID_PROJECTION: Document = {
        doc! {
            "_id": 1,
        }
    };
    static ref FILE_ID_PROJECTION: Document = {
        doc! {
            "_id": 0,
            "file_id": 1,
        }
    };
    static ref FILE_ID_EXISTS: Document = {
        doc! {
            "file_id": {
                "$exists": true
            }
        }
    };
    static ref COUNT_LTE_ZERO: Document = {
        doc! {
            "count": {
                "$lte": 0
            }
        }
    };
}

/// To store perennial files and temporary files in MongoDB.
#[derive(Debug)]
pub struct FileCenter {
    mongo_client_db: Database,
    file_size_threshold: i32,
    _create_time: DateTime<Utc>,
    _version: i32,
    short_crypt: ShortCrypt,
}

/// A string of an encrypted file ID which can be used as a URL component.
pub type IDToken = String;

#[derive(Debug)]
pub enum FileCenterError {
    MongoDBError(mongodb::Error),
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
    fn fmt(&self, f: &mut Formatter) -> Result<(), FmtError> {
        match self {
            FileCenterError::MongoDBError(err) => Display::fmt(err, f),
            FileCenterError::DocumentError(err) => Display::fmt(err, f),
            FileCenterError::IDTokenError(err) => f.write_str(err),
            FileCenterError::FileSizeThresholdError => {
                f.write_str("The file size threshold is incorrect.")
            }
            FileCenterError::VersionError => f.write_str("The version is incorrect."),
            FileCenterError::DatabaseTooNewError {
                supported_latest,
                current,
            } => {
                f.write_fmt(format_args!(
                    "The current database version is {}, but this library only supports to {}.",
                    current, supported_latest
                ))
            }
            FileCenterError::IOError(err) => Display::fmt(err, f),
            FileCenterError::MimeTypeError => f.write_str("The mime type is incorrect."),
        }
    }
}

impl Error for FileCenterError {}

impl From<mongodb::Error> for FileCenterError {
    #[inline]
    fn from(err: mongodb::Error) -> Self {
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

impl FileCenter {
    /// Create a new FileCenter instance.
    pub fn new<U: AsRef<str>, D: AsRef<str>>(
        uri: U,
        database: D,
    ) -> Result<FileCenter, FileCenterError> {
        Self::new_with_file_size_threshold_inner(uri, database, DEFAULT_FILE_SIZE_THRESHOLD)
    }

    /// Create a new FileCenter instance with a custom initial file size threshold.
    pub fn new_with_file_size_threshold<U: AsRef<str>, D: AsRef<str>>(
        uri: U,
        database: D,
        initial_file_size_threshold: i32,
    ) -> Result<FileCenter, FileCenterError> {
        if initial_file_size_threshold > MAX_FILE_SIZE_THRESHOLD || initial_file_size_threshold <= 0
        {
            return Err(FileCenterError::FileSizeThresholdError);
        }

        Self::new_with_file_size_threshold_inner(uri, database, initial_file_size_threshold)
    }

    fn new_with_file_size_threshold_inner<U: AsRef<str>, D: AsRef<str>>(
        uri: U,
        database: D,
        initial_file_size_threshold: i32,
    ) -> Result<FileCenter, FileCenterError> {
        let mongodb_client = Client::with_uri(uri.as_ref())?;

        let mongo_client_db = mongodb_client.db(database.as_ref());

        let file_size_threshold;
        let create_time;
        let version;

        {
            let collection_settings: Collection =
                mongo_client_db.collection(COLLECTION_SETTINGS_NAME);

            file_size_threshold = match collection_settings
                .find_one(Some(doc! {"_id": SETTING_FILE_SIZE_THRESHOLD}), None)?
            {
                Some(file_size_threshold) => {
                    let file_size_threshold = file_size_threshold.get_i32("value")?;

                    if file_size_threshold > MAX_FILE_SIZE_THRESHOLD || file_size_threshold <= 0 {
                        return Err(FileCenterError::FileSizeThresholdError);
                    }

                    file_size_threshold
                }
                None => {
                    collection_settings.insert_one(doc! {"_id": SETTING_FILE_SIZE_THRESHOLD, "value": initial_file_size_threshold}, None)?;
                    initial_file_size_threshold
                }
            };

            create_time = match collection_settings
                .find_one(Some(doc! {"_id": SETTING_CREATE_TIME}), None)?
            {
                Some(file_size_threshold) => *file_size_threshold.get_utc_datetime("value")?,
                None => {
                    let now = Utc::now();

                    collection_settings
                        .insert_one(doc! {"_id": SETTING_CREATE_TIME, "value": now}, None)?;

                    now
                }
            };

            version =
                match collection_settings.find_one(Some(doc! {"_id": SETTING_VERSION}), None)? {
                    Some(version) => {
                        let version = version.get_i32("value")?;

                        if version <= 0 {
                            return Err(FileCenterError::VersionError);
                        }

                        if version > VERSION {
                            return Err(FileCenterError::DatabaseTooNewError {
                                supported_latest: VERSION,
                                current: version,
                            });
                        }

                        version
                    }
                    None => {
                        collection_settings
                            .insert_one(doc! {"_id": SETTING_VERSION, "value": VERSION}, None)?;

                        VERSION
                    }
                };
        }

        {
            let collection_files: Collection = mongo_client_db.collection(COLLECTION_FILES_NAME);
            collection_files.create_index(doc! {"create_time": 1}, None).unwrap();

            {
                let mut options = IndexOptions::new();
                options.expire_after_seconds = Some(0);
                collection_files.create_index(doc! {"expire_at": 1}, Some(options)).unwrap();
            }

            collection_files.create_index(doc! {"file_id": 1}, None).unwrap();
            collection_files.create_index(doc! {"count": 1}, None).unwrap();

            {
                let mut options = IndexOptions::new();
                options.unique = Some(true);
                collection_files
                    .create_index(
                        doc! {"hash_1": 1, "hash_2": 1, "hash_3": 1, "hash_4": 1},
                        Some(options),
                    )
                    .unwrap();
            }
        }

        let short_crypt =
            ShortCrypt::new(&format!("FileCenter-{}", create_time.timestamp_millis()));

        Ok(FileCenter {
            mongo_client_db,
            file_size_threshold,
            _create_time: create_time,
            _version: version,
            short_crypt,
        })
    }

    /// Change the file size threshold.
    pub fn set_file_size_threshold(
        &mut self,
        file_size_threshold: i32,
    ) -> Result<(), FileCenterError> {
        let collection_settings: Collection =
            self.mongo_client_db.collection(COLLECTION_SETTINGS_NAME);

        if file_size_threshold > MAX_FILE_SIZE_THRESHOLD || file_size_threshold <= 0 {
            return Err(FileCenterError::FileSizeThresholdError);
        }

        if file_size_threshold != self.file_size_threshold {
            let mut options = UpdateOptions::new();
            options.upsert = Some(true);

            collection_settings.update_one(
                doc! {"_id": SETTING_FILE_SIZE_THRESHOLD},
                doc! {"$set": {"value": file_size_threshold}},
                Some(options),
            )?;

            self.file_size_threshold = file_size_threshold;
        }

        Ok(())
    }

    /// Decrypt a ID token to an Object ID.
    pub fn decrypt_id_token<S: AsRef<str>>(
        &self,
        id_token: S,
    ) -> Result<ObjectId, FileCenterError> {
        let id_raw = self
            .short_crypt
            .decrypt_url_component(id_token)
            .map_err(|err| FileCenterError::IDTokenError(err))?;

        let id_raw: [u8; 12] = {
            if id_raw.len() != 12 {
                return Err(FileCenterError::IDTokenError("ID needs to be 12 bytes."));
            }

            let mut fixed_raw = [0u8; 12];

            fixed_raw.copy_from_slice(&id_raw);

            fixed_raw
        };

        Ok(ObjectId::with_bytes(id_raw))
    }

    /// Encrypt an Object ID to an ID token.
    pub fn encrypt_id(&self, id: &ObjectId) -> IDToken {
        let id_raw = id.bytes();

        self.short_crypt.encrypt_to_url_component(&id_raw)
    }

    /// Encrypt an Object ID to an ID token.
    pub fn encrypt_id_to_buffer(&self, id: &ObjectId, buffer: String) -> String {
        let id_raw = id.bytes();

        self.short_crypt.encrypt_to_url_component_and_push_to_string(&id_raw, buffer)
    }

    /// Drop the database.
    pub fn drop_database(self) -> Result<(), FileCenterError> {
        self.mongo_client_db.drop_database()?;
        Ok(())
    }

    /// Drop the file center.
    pub fn drop_file_center(self) -> Result<(), FileCenterError> {
        self.mongo_client_db.drop_collection(COLLECTION_FILES_NAME)?;
        self.mongo_client_db.drop_collection(COLLECTION_SETTINGS_NAME)?;
        self.mongo_client_db.drop_collection("fs.files")?;
        Ok(())
    }

    fn create_file_item(&self, mut document: Document) -> Result<FileItem, FileCenterError> {
        let id = match document
            .remove("_id")
            .ok_or(FileCenterError::DocumentError(bson::ValueAccessError::NotPresent))?
        {
            Bson::ObjectId(b) => b,
            _ => {
                return Err(FileCenterError::DocumentError(bson::ValueAccessError::UnexpectedType));
            }
        };

        let create_time = match document
            .remove("create_time")
            .ok_or(FileCenterError::DocumentError(bson::ValueAccessError::NotPresent))?
        {
            Bson::UtcDatetime(b) => b,
            _ => {
                return Err(FileCenterError::DocumentError(bson::ValueAccessError::UnexpectedType));
            }
        };

        let expire_at = match document.remove("expire_at") {
            Some(expire_at) => {
                match expire_at {
                    Bson::UtcDatetime(b) => Some(b),
                    _ => {
                        return Err(FileCenterError::DocumentError(
                            bson::ValueAccessError::UnexpectedType,
                        ));
                    }
                }
            }
            None => None,
        };

        let mime_type = match document
            .remove("mime_type")
            .ok_or(FileCenterError::DocumentError(bson::ValueAccessError::NotPresent))?
        {
            Bson::String(b) => {
                Mime::from_str(&b).map_err(|_| {
                    FileCenterError::DocumentError(bson::ValueAccessError::UnexpectedType)
                })?
            }
            _ => {
                return Err(FileCenterError::DocumentError(bson::ValueAccessError::UnexpectedType));
            }
        };

        let file_size = document.get_i64("file_size")? as u64;

        let file_name = match document
            .remove("file_name")
            .ok_or(FileCenterError::DocumentError(bson::ValueAccessError::NotPresent))?
        {
            Bson::String(b) => b,
            _ => {
                return Err(FileCenterError::DocumentError(bson::ValueAccessError::UnexpectedType));
            }
        };

        let file_data = match document.remove("file_data") {
            Some(file_data) => {
                match file_data {
                    Bson::Binary(_, d) => FileData::Collection(d),
                    _ => {
                        return Err(FileCenterError::DocumentError(
                            bson::ValueAccessError::UnexpectedType,
                        ));
                    }
                }
            }
            None => {
                let file_id = match document
                    .remove("file_id")
                    .ok_or(FileCenterError::DocumentError(bson::ValueAccessError::NotPresent))?
                {
                    Bson::ObjectId(b) => b,
                    _ => {
                        return Err(FileCenterError::DocumentError(
                            bson::ValueAccessError::UnexpectedType,
                        ));
                    }
                };

                let store = Store::with_db(self.mongo_client_db.clone());

                FileData::GridFS(Box::new(store.open_id(file_id)?))
            }
        };

        Ok(FileItem {
            id,
            create_time,
            expire_at,
            mime_type,
            file_size,
            file_name,
            file_data,
        })
    }

    /// Get the file item via an Object ID.
    pub fn get_file_item_by_id(&self, id: &ObjectId) -> Result<Option<FileItem>, FileCenterError> {
        let collection_files: Collection = self.mongo_client_db.collection(COLLECTION_FILES_NAME);

        let mut options = FindOptions::new();
        options.projection = Some(FILE_ITEM_PROJECTION.clone());

        let file_item = collection_files.find_one(Some(doc! {"_id": id.clone()}), Some(options))?;

        match file_item {
            Some(file_item) => {
                if file_item.contains_key("expire_at") {
                    collection_files.delete_one(doc! {"_id": id.clone()}, None)?;
                }

                let file_item = self.create_file_item(file_item)?;

                Ok(Some(file_item))
            }
            None => Ok(None),
        }
    }

    /// Remove a file item via an Object ID.
    pub fn delete_file_item_by_id(&self, id: &ObjectId) -> Result<Option<u64>, FileCenterError> {
        let collection_files: Collection = self.mongo_client_db.collection(COLLECTION_FILES_NAME);

        let mut options = FindOneAndUpdateOptions::new();
        options.return_document = Some(ReturnDocument::After);
        options.projection = Some(FILE_ITEM_DELETE_PROJECTION.clone());

        let result = collection_files.find_one_and_update(
            doc! {
               "_id": id.clone(),
            },
            doc! {
                "$inc": {
                    "count": -1
                }
            },
            Some(options),
        )?;

        match result {
            Some(mut result) => {
                let count = result.get_i32("count")?;
                let file_size = result.get_i64("file_size")? as u64;

                if count <= 0 {
                    collection_files.delete_one(doc! {"_id": id.clone()}, None)?;
                    if let Some(file_id) = result.remove("file_id") {
                        match file_id {
                            Bson::ObjectId(file_id) => {
                                let store = Store::with_db(self.mongo_client_db.clone());

                                store.remove_id(file_id)?;
                            }
                            _ => {
                                return Err(FileCenterError::DocumentError(
                                    bson::ValueAccessError::UnexpectedType,
                                ));
                            }
                        }
                    }
                }

                Ok(Some(file_size))
            }
            None => Ok(None),
        }
    }

    /// Input a file to the file center via a file path.
    pub fn put_file_by_path<P: AsRef<Path>, S: Into<String>>(
        &self,
        file_path: P,
        file_name: Option<S>,
        mime_type: Option<Mime>,
    ) -> Result<FileItem, FileCenterError> {
        let file_path = file_path.as_ref();

        let (hash_1, hash_2, hash_3, hash_4) = get_hash_by_path(file_path)?;

        let collection_files: Collection = self.mongo_client_db.collection(COLLECTION_FILES_NAME);

        let mut options = FindOneAndUpdateOptions::new();
        options.return_document = Some(ReturnDocument::After);
        options.projection = Some(FILE_ITEM_PROJECTION.clone());

        let result = collection_files.find_one_and_update(
            doc! {
               "hash_1": hash_1,
               "hash_2": hash_2,
               "hash_3": hash_3,
               "hash_4": hash_4,
            },
            doc! {
                "$inc": {
                    "count": 1
                }
            },
            Some(options),
        )?;

        match result {
            Some(result) => Ok(self.create_file_item(result)?),
            None => {
                let file_name = match file_name {
                    Some(file_name) => file_name.into(),
                    None => file_path.file_name().unwrap().to_str().unwrap().to_string(),
                };

                let mut file = File::open(file_path)?;

                let metadata = file.metadata()?;

                let file_size = metadata.len();

                let mut file_item_raw: Document = doc! {
                    "hash_1": hash_1,
                    "hash_2": hash_2,
                    "hash_3": hash_3,
                    "hash_4": hash_4,
                    "file_size": file_size,
                    "count": 1i32
                };

                if file_size >= self.file_size_threshold as u64 {
                    let store = Store::with_db(self.mongo_client_db.clone());
                    let mut store_file: gridfs::file::File = store.create(file_name.clone())?;
                    let mut file = File::open(file_path)?;
                    let mut buffer = [0u8; BUFFER_SIZE];

                    loop {
                        let c = file.read(&mut buffer)?;

                        if c == 0 {
                            break;
                        }

                        store_file.write_all(&buffer[..c])?;
                    }

                    store_file.flush()?;

                    let id = store_file.doc.id.clone();

                    drop(store_file);

                    file_item_raw.insert("file_name", file_name);
                    file_item_raw.insert("file_id", id);
                    drop(file);
                } else {
                    let mut file_data = Vec::new();
                    file.read_to_end(&mut file_data)?;
                    file_item_raw.insert("file_name", file_name);
                    file_item_raw
                        .insert("file_data", Bson::Binary(BinarySubtype::Generic, file_data));
                    drop(file);
                }

                let mime_type = match mime_type {
                    Some(mime_type) => mime_type,
                    None => {
                        match file_path.extension() {
                            Some(extension) => {
                                mime_guess::from_ext(extension.to_str().unwrap())
                                    .first_or_octet_stream()
                            }
                            None => DEFAULT_MIME_TYPE,
                        }
                    }
                };

                file_item_raw.insert("mime_type", mime_type.as_ref());

                file_item_raw.insert("create_time", Utc::now());

                let result = collection_files.insert_one(file_item_raw.clone(), None)?;

                file_item_raw.insert("_id", result.inserted_id.unwrap());

                Ok(self.create_file_item(file_item_raw)?)
            }
        }
    }

    /// Input a file to the file center via a buffer.
    pub fn put_file_by_buffer<S: Into<String>>(
        &self,
        buffer: Vec<u8>,
        file_name: S,
        mime_type: Option<Mime>,
    ) -> Result<FileItem, FileCenterError> {
        let (hash_1, hash_2, hash_3, hash_4) = get_hash_by_buffer(&buffer)?;

        self.put_file_by_buffer_inner(
            buffer,
            file_name,
            mime_type,
            (hash_1, hash_2, hash_3, hash_4),
        )
    }

    fn put_file_by_buffer_inner<S: Into<String>>(
        &self,
        buffer: Vec<u8>,
        file_name: S,
        mime_type: Option<Mime>,
        (hash_1, hash_2, hash_3, hash_4): (i64, i64, i64, i64),
    ) -> Result<FileItem, FileCenterError> {
        let collection_files: Collection = self.mongo_client_db.collection(COLLECTION_FILES_NAME);

        let file_name = file_name.into();

        let mut options = FindOneAndUpdateOptions::new();
        options.return_document = Some(ReturnDocument::After);
        options.projection = Some(FILE_ITEM_PROJECTION.clone());

        let result = collection_files.find_one_and_update(
            doc! {
               "hash_1": hash_1,
               "hash_2": hash_2,
               "hash_3": hash_3,
               "hash_4": hash_4,
            },
            doc! {
                "$inc": {
                    "count": 1
                }
            },
            Some(options),
        )?;

        match result {
            Some(result) => Ok(self.create_file_item(result)?),
            None => {
                let file_size = buffer.len() as u64;

                let mut file_item_raw: Document = doc! {
                    "hash_1": hash_1,
                    "hash_2": hash_2,
                    "hash_3": hash_3,
                    "hash_4": hash_4,
                    "file_size": file_size,
                    "count": 1i32
                };

                if file_size >= self.file_size_threshold as u64 {
                    let store = Store::with_db(self.mongo_client_db.clone());

                    let mut store_file: gridfs::file::File = store.create(file_name.clone())?;

                    store_file.write_all(&buffer)?;

                    store_file.flush()?;

                    let id = store_file.doc.id.clone();

                    drop(store_file);

                    file_item_raw.insert("file_name", file_name);
                    file_item_raw.insert("file_id", id);
                } else {
                    file_item_raw.insert("file_name", file_name);
                    file_item_raw.insert("file_data", Bson::Binary(BinarySubtype::Generic, buffer));
                }

                let mime_type = match mime_type {
                    Some(mime_type) => mime_type,
                    None => DEFAULT_MIME_TYPE,
                };

                file_item_raw.insert("mime_type", mime_type.as_ref());

                file_item_raw.insert("create_time", Utc::now());

                let result = collection_files.insert_one(file_item_raw.clone(), None)?;

                file_item_raw.insert("_id", result.inserted_id.unwrap());

                Ok(self.create_file_item(file_item_raw)?)
            }
        }
    }

    /// Input a file to the file center via a reader.
    pub fn put_file_by_reader<R: Read, S: Into<String>>(
        &self,
        mut reader: R,
        file_name: S,
        mime_type: Option<Mime>,
    ) -> Result<FileItem, FileCenterError> {
        let mut buffer = [0u8; BUFFER_SIZE];

        let mut sha3_256 = Sha3_256::new();

        let mut temp = Vec::new();

        let mut gridfs = false;

        let mut file_size = buffer.len();

        loop {
            let c = reader.read(&mut buffer)?;

            if c == 0 {
                break;
            }

            let buf = &buffer[..c];

            sha3_256.input(buf);

            temp.extend_from_slice(buf);

            file_size += c;

            if temp.len() as u64 >= self.file_size_threshold as u64 {
                gridfs = true;
                break;
            }
        }

        if gridfs {
            let file_name = file_name.into();

            let store = Store::with_db(self.mongo_client_db.clone());
            let mut store_file: gridfs::file::File = store.create(file_name.clone())?;

            store_file.write_all(&temp)?;

            drop(temp);

            loop {
                let c = reader.read(&mut buffer)?;

                if c == 0 {
                    break;
                }

                let buf = &buffer[..c];

                sha3_256.input(buf);

                store_file.write_all(buf)?;

                file_size += c;
            }

            store_file.flush()?;

            let id = store_file.doc.id.clone();

            drop(store_file);

            let (hash_1, hash_2, hash_3, hash_4) = separate_hash(&sha3_256.result());

            let collection_files: Collection =
                self.mongo_client_db.collection(COLLECTION_FILES_NAME);

            let mut options = FindOneAndUpdateOptions::new();
            options.return_document = Some(ReturnDocument::After);
            options.projection = Some(FILE_ITEM_PROJECTION.clone());

            let result = collection_files.find_one_and_update(
                doc! {
                   "hash_1": hash_1,
                   "hash_2": hash_2,
                   "hash_3": hash_3,
                   "hash_4": hash_4,
                },
                doc! {
                    "$inc": {
                        "count": 1
                    }
                },
                Some(options),
            )?;

            match result {
                Some(result) => {
                    store.remove_id(id)?;
                    Ok(self.create_file_item(result)?)
                }
                None => {
                    let mut file_item_raw: Document = doc! {
                        "hash_1": hash_1,
                        "hash_2": hash_2,
                        "hash_3": hash_3,
                        "hash_4": hash_4,
                        "file_name": file_name,
                        "file_size": file_size as u64,
                        "file_id": id,
                        "count": 1i32
                    };

                    let mime_type = match mime_type {
                        Some(mime_type) => mime_type,
                        None => DEFAULT_MIME_TYPE,
                    };

                    file_item_raw.insert("mime_type", mime_type.as_ref());

                    file_item_raw.insert("create_time", Utc::now());

                    let result = collection_files.insert_one(file_item_raw.clone(), None)?;

                    file_item_raw.insert("_id", result.inserted_id.unwrap());

                    Ok(self.create_file_item(file_item_raw)?)
                }
            }
        } else {
            self.put_file_by_buffer_inner(
                temp,
                file_name,
                mime_type,
                separate_hash(&sha3_256.result()),
            )
        }
    }

    /// Temporarily input a file to the file center via a file path.
    pub fn put_file_by_path_temporarily<P: AsRef<Path>, S: Into<String>>(
        &self,
        file_path: P,
        file_name: Option<S>,
        mime_type: Option<Mime>,
    ) -> Result<FileItem, FileCenterError> {
        let collection_files: Collection = self.mongo_client_db.collection(COLLECTION_FILES_NAME);

        let (hash_1, hash_2, hash_3, hash_4) = get_hash_by_random();

        let file_path = file_path.as_ref();

        let file_name = match file_name {
            Some(file_name) => file_name.into(),
            None => file_path.file_name().unwrap().to_str().unwrap().to_string(),
        };

        let mut file = File::open(file_path)?;

        let metadata = file.metadata()?;

        let file_size = metadata.len();

        let mut file_item_raw: Document = doc! {
            "hash_1": hash_1,
            "hash_2": hash_2,
            "hash_3": hash_3,
            "hash_4": hash_4,
            "file_size": file_size,
            "count": 1i32
        };

        if file_size >= self.file_size_threshold as u64 {
            let store = Store::with_db(self.mongo_client_db.clone());
            let mut store_file: gridfs::file::File = store.create(file_name.clone())?;
            let mut file = File::open(file_path)?;
            let mut buffer = [0u8; BUFFER_SIZE];

            loop {
                let c = file.read(&mut buffer)?;

                if c == 0 {
                    break;
                }

                store_file.write_all(&buffer[..c])?;
            }

            store_file.flush()?;

            let id = store_file.doc.id.clone();

            drop(store_file);

            file_item_raw.insert("file_name", file_name);
            file_item_raw.insert("file_id", id);
            drop(file);
        } else {
            let mut file_data = Vec::new();
            file.read_to_end(&mut file_data)?;
            file_item_raw.insert("file_name", file_name);
            file_item_raw.insert("file_data", Bson::Binary(BinarySubtype::Generic, file_data));
            drop(file);
        }

        let mime_type = match mime_type {
            Some(mime_type) => mime_type,
            None => {
                match file_path.extension() {
                    Some(extension) => {
                        mime_guess::from_ext(extension.to_str().unwrap()).first_or_octet_stream()
                    }
                    None => DEFAULT_MIME_TYPE,
                }
            }
        };

        file_item_raw.insert("mime_type", mime_type.as_ref());

        let now = Utc::now();

        let expire: DateTime<Utc> =
            Utc.timestamp_millis(now.timestamp_millis() + TEMPORARY_LIFE_TIME);

        file_item_raw.insert("create_time", now);

        file_item_raw.insert("expire_at", expire);

        let result = collection_files.insert_one(file_item_raw.clone(), None)?;

        file_item_raw.insert("_id", result.inserted_id.unwrap());

        Ok(self.create_file_item(file_item_raw)?)
    }

    /// Temporarily input a file to the file center via a buffer.
    pub fn put_file_by_buffer_temporarily<S: Into<String>>(
        &self,
        buffer: Vec<u8>,
        file_name: S,
        mime_type: Option<Mime>,
    ) -> Result<FileItem, FileCenterError> {
        let collection_files: Collection = self.mongo_client_db.collection(COLLECTION_FILES_NAME);

        let (hash_1, hash_2, hash_3, hash_4) = get_hash_by_random();

        let file_size = buffer.len() as u64;

        let file_name = file_name.into();

        let mut file_item_raw: Document = doc! {
            "hash_1": hash_1,
            "hash_2": hash_2,
            "hash_3": hash_3,
            "hash_4": hash_4,
            "file_size": file_size,
            "count": 1i32
        };

        if file_size >= self.file_size_threshold as u64 {
            let store = Store::with_db(self.mongo_client_db.clone());

            let mut store_file: gridfs::file::File = store.create(file_name.clone())?;

            store_file.write_all(&buffer)?;

            store_file.flush()?;

            let id = store_file.doc.id.clone();

            drop(store_file);

            file_item_raw.insert("file_name", file_name);
            file_item_raw.insert("file_id", id);
        } else {
            file_item_raw.insert("file_name", file_name);
            file_item_raw.insert("file_data", Bson::Binary(BinarySubtype::Generic, buffer));
        }

        let mime_type = match mime_type {
            Some(mime_type) => mime_type,
            None => DEFAULT_MIME_TYPE,
        };

        file_item_raw.insert("mime_type", mime_type.as_ref());

        let now = Utc::now();

        let expire: DateTime<Utc> =
            Utc.timestamp_millis(now.timestamp_millis() + TEMPORARY_LIFE_TIME);

        file_item_raw.insert("create_time", now);

        file_item_raw.insert("expire_at", expire);

        let result = collection_files.insert_one(file_item_raw.clone(), None)?;

        file_item_raw.insert("_id", result.inserted_id.unwrap());

        Ok(self.create_file_item(file_item_raw)?)
    }

    /// Temporarily input a file to the file center via a reader.
    pub fn put_file_by_reader_temporarily<R: Read, S: Into<String>>(
        &self,
        mut reader: R,
        file_name: S,
        mime_type: Option<Mime>,
    ) -> Result<FileItem, FileCenterError> {
        let mut buffer = [0u8; BUFFER_SIZE];

        let mut temp = Vec::new();

        let mut gridfs = false;

        let mut file_size = 0;

        loop {
            let c = reader.read(&mut buffer)?;

            if c == 0 {
                break;
            }

            temp.extend_from_slice(&buffer[..c]);

            file_size += c;

            if temp.len() as u64 >= self.file_size_threshold as u64 {
                gridfs = true;
                break;
            }
        }

        if gridfs {
            let (hash_1, hash_2, hash_3, hash_4) = get_hash_by_random();

            let file_name = file_name.into();

            let store = Store::with_db(self.mongo_client_db.clone());

            let mut store_file: gridfs::file::File = store.create(file_name.clone())?;

            store_file.write_all(&temp)?;

            drop(temp);

            loop {
                let c = reader.read(&mut buffer)?;

                if c == 0 {
                    break;
                }

                store_file.write_all(&buffer[..c])?;

                file_size += c;
            }

            store_file.flush()?;

            let id = store_file.doc.id.clone();

            drop(store_file);

            let collection_files: Collection =
                self.mongo_client_db.collection(COLLECTION_FILES_NAME);

            let mut file_item_raw: Document = doc! {
                "hash_1": hash_1,
                "hash_2": hash_2,
                "hash_3": hash_3,
                "hash_4": hash_4,
                "file_name": file_name,
                "file_size": file_size as u64,
                "file_id": id,
                "count": 1i32
            };

            let mime_type = match mime_type {
                Some(mime_type) => mime_type,
                None => DEFAULT_MIME_TYPE,
            };

            file_item_raw.insert("mime_type", mime_type.as_ref());

            let now = Utc::now();

            let expire: DateTime<Utc> =
                Utc.timestamp_millis(now.timestamp_millis() + TEMPORARY_LIFE_TIME);

            file_item_raw.insert("create_time", now);

            file_item_raw.insert("expire_at", expire);

            let result = collection_files.insert_one(file_item_raw.clone(), None)?;

            file_item_raw.insert("_id", result.inserted_id.unwrap());

            Ok(self.create_file_item(file_item_raw)?)
        } else {
            self.put_file_by_buffer_temporarily(temp, file_name, mime_type)
        }
    }

    /// Remove all unused files in GridFS and in the `COLLECTION_FILES_NAME` collection.
    pub fn clear_garbage(&self) -> Result<(), FileCenterError> {
        let collection_files: Collection = self.mongo_client_db.collection(COLLECTION_FILES_NAME);
        let fs_files: Collection = self.mongo_client_db.collection("fs.files");

        // unnecessary file items which have file_id but the target file does not exist
        {
            let files: Vec<Bson> = {
                let mut options = FindOptions::new();
                options.projection = Some(ID_PROJECTION.clone());

                let mut result = fs_files.find(None, Some(options))?;

                let mut array = Vec::new();

                while result.has_next()? {
                    let mut doc = result.next().unwrap()?;

                    let id = doc.remove("_id").ok_or(FileCenterError::DocumentError(
                        bson::ValueAccessError::NotPresent,
                    ))?;
                    match id {
                        Bson::ObjectId(id) => array.push(Bson::ObjectId(id)),
                        _ => {
                            return Err(FileCenterError::DocumentError(
                                bson::ValueAccessError::UnexpectedType,
                            ));
                        }
                    }
                }

                array
            };

            let afs: Vec<Bson> = {
                let mut options = FindOptions::new();
                options.projection = Some(ID_PROJECTION.clone());

                let mut result = collection_files.find(
                    Some(doc! {
                        "file_id": {
                            "$nin": Bson::Array(files),
                            "$exists": true
                        }
                    }),
                    Some(options),
                )?;

                let mut array = Vec::new();

                while result.has_next()? {
                    let mut doc = result.next().unwrap()?;

                    let id = doc.remove("_id").ok_or(FileCenterError::DocumentError(
                        bson::ValueAccessError::NotPresent,
                    ))?;
                    match id {
                        Bson::ObjectId(id) => array.push(Bson::ObjectId(id)),
                        _ => {
                            return Err(FileCenterError::DocumentError(
                                bson::ValueAccessError::UnexpectedType,
                            ));
                        }
                    }
                }

                array
            };

            if !afs.is_empty() {
                collection_files.delete_many(
                    doc! {
                        "_id": {
                            "$in": Bson::Array(afs)
                        }
                    },
                    None,
                )?;
            }
        }

        // unnecessary file items whose count are smaller than or equal to 0
        {
            let files: Vec<Bson> = {
                let mut options = FindOptions::new();
                options.projection = Some(ID_PROJECTION.clone());

                let mut result =
                    collection_files.find(Some(COUNT_LTE_ZERO.clone()), Some(options))?;

                let mut array = Vec::new();

                while result.has_next()? {
                    let mut doc = result.next().unwrap()?;

                    let id = doc.remove("_id").ok_or(FileCenterError::DocumentError(
                        bson::ValueAccessError::NotPresent,
                    ))?;
                    match id {
                        Bson::ObjectId(id) => array.push(Bson::ObjectId(id)),
                        _ => {
                            return Err(FileCenterError::DocumentError(
                                bson::ValueAccessError::UnexpectedType,
                            ));
                        }
                    }
                }

                array
            };

            if !files.is_empty() {
                let mut result = collection_files.find(
                    Some(doc! {
                        "_id": {
                            "$in": Bson::Array(files.clone())
                        }
                    }),
                    None,
                )?;

                while result.has_next()? {
                    let mut doc: Document = result.next().unwrap()?;

                    if let Some(Bson::ObjectId(b)) = doc.remove("file_id") {
                        fs_files
                            .delete_one(
                                doc! {
                                    "_id": b
                                },
                                None,
                            )
                            .unwrap();
                    }
                }

                collection_files.delete_many(
                    doc! {
                        "_id": {
                            "$in": Bson::Array(files)
                        }
                    },
                    None,
                )?;
            }
        }

        // unnecessary GridFS files which are not used in file items
        {
            let files: Vec<Bson> = {
                let mut options = FindOptions::new();
                options.projection = Some(FILE_ID_PROJECTION.clone());

                let mut result =
                    collection_files.find(Some(FILE_ID_EXISTS.clone()), Some(options))?;

                let mut array = Vec::new();

                while result.has_next()? {
                    let mut doc = result.next().unwrap()?;

                    let id = doc.remove("file_id").ok_or(FileCenterError::DocumentError(
                        bson::ValueAccessError::NotPresent,
                    ))?;
                    match id {
                        Bson::ObjectId(id) => array.push(Bson::ObjectId(id)),
                        _ => {
                            return Err(FileCenterError::DocumentError(
                                bson::ValueAccessError::UnexpectedType,
                            ));
                        }
                    }
                }

                array
            };

            let fsfs: Vec<ObjectId> = {
                let mut options = FindOptions::new();
                options.projection = Some(ID_PROJECTION.clone());

                let mut result = fs_files.find(
                    Some(doc! {
                       "_id": {
                           "$nin": Bson::Array(files)
                       }
                    }),
                    Some(options),
                )?;

                let mut array = Vec::new();

                while result.has_next()? {
                    let mut doc = result.next().unwrap()?;

                    let id = doc.remove("_id").ok_or(FileCenterError::DocumentError(
                        bson::ValueAccessError::NotPresent,
                    ))?;
                    match id {
                        Bson::ObjectId(id) => array.push(id),
                        _ => {
                            return Err(FileCenterError::DocumentError(
                                bson::ValueAccessError::UnexpectedType,
                            ));
                        }
                    }
                }

                array
            };

            if !fsfs.is_empty() {
                let store = Store::with_db(self.mongo_client_db.clone());

                for id in fsfs {
                    store.remove_id(id)?;
                }
            }
        }

        Ok(())
    }

    /// Get the instance of MongoDB Database.
    pub fn get_mongo_client_db(&self) -> &Database {
        &self.mongo_client_db
    }
}

fn get_hash_by_path<P: AsRef<Path>>(file_path: P) -> Result<(i64, i64, i64, i64), io::Error> {
    let file_path = file_path.as_ref();

    let mut file = File::open(file_path)?;

    let mut sha3_256 = Sha3_256::new();

    let mut buffer = [0u8; BUFFER_SIZE];

    loop {
        let c = file.read(&mut buffer)?;

        if c == 0 {
            break;
        }

        sha3_256.input(&buffer[..c]);
    }

    let result = sha3_256.result();

    Ok(separate_hash(&result))
}

fn get_hash_by_buffer<P: AsRef<[u8]>>(buffer: P) -> Result<(i64, i64, i64, i64), io::Error> {
    let buffer = buffer.as_ref();

    let mut sha3_256 = Sha3_256::new();

    sha3_256.input(buffer);

    let result = sha3_256.result();

    Ok(separate_hash(&result))
}

fn get_hash_by_random() -> (i64, i64, i64, i64) {
    let mut rng = thread_rng();
    (rng.gen(), rng.gen(), rng.gen(), rng.gen())
}

fn separate_hash(hash: &[u8]) -> (i64, i64, i64, i64) {
    let mut hash_1 = [0u8; 8];
    let mut hash_2 = [0u8; 8];
    let mut hash_3 = [0u8; 8];
    let mut hash_4 = [0u8; 8];

    hash_1.copy_from_slice(&hash[0..8]);
    hash_2.copy_from_slice(&hash[8..16]);
    hash_3.copy_from_slice(&hash[16..24]);
    hash_4.copy_from_slice(&hash[24..32]);

    (
        unsafe { transmute(hash_1) },
        unsafe { transmute(hash_2) },
        unsafe { transmute(hash_3) },
        unsafe { transmute(hash_4) },
    )
}
