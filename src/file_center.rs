extern crate short_crypt;

use std::io::{self, Cursor, ErrorKind};
use std::path::Path;
use std::str::FromStr;
use std::time::Duration;

use crate::tokio::fs::File;

use crate::tokio::io::{AsyncRead, AsyncReadExt};
use crate::tokio_stream::{Stream, StreamExt};

use crate::bson::document::{Document, ValueAccessError};
use crate::bson::oid::ObjectId;
use crate::bson::spec::BinarySubtype;
use crate::bson::{Binary, Bson, DateTime};

use crate::mongodb::options::{
    ClientOptions, FindOneAndUpdateOptions, FindOneOptions, FindOptions, IndexOptions,
    ReturnDocument, UpdateOptions,
};
use crate::mongodb::results::DeleteResult;
use crate::mongodb::{Client, Collection, Database, IndexModel};

use crate::mime::Mime;

use crate::functions::*;
use crate::{Digest, FileCenterError, FileData, FileItem, Hasher, IDToken, DEFAULT_MIME_TYPE};

use short_crypt::ShortCrypt;

/// The default database name, if there is no database name in the MongoDB URI.
pub const DEFAULT_DATABASE_NAME: &str = "test";

/// The name of the collection which stores file items.
pub const COLLECTION_FILES_NAME: &str = "file_center";
/// The name of the collection which stores file chunks.
pub const COLLECTION_FILES_CHUNKS_NAME: &str = "file_center_chunks";
/// The name of the collection which stores the settings of the file center.
pub const COLLECTION_SETTINGS_NAME: &str = "file_center_settings";

/// The name of the `file_size_threshold` value. When the file size is bigger than `file_size_threshold`, it should be separate into chunks to store in the `COLLECTION_FILES_CHUNKS_NAME` collection.
///
/// `file_size_threshold` cannot bigger than `16_770_000`.
pub const SETTING_FILE_SIZE_THRESHOLD: &str = "file_size_threshold";
/// The name of the `create_time` value, which is the time instant that the file center is being created. The value is also used as the key of ID tokens.
pub const SETTING_CREATE_TIME: &str = "create_time";
/// The name of the `version` value, the version of this file center.
pub const SETTING_VERSION: &str = "version";

#[doc(hidden)]
pub const MAX_FILE_SIZE_THRESHOLD: u32 = 16_770_000;
#[doc(hidden)]
pub const DEFAULT_FILE_SIZE_THRESHOLD: u32 = 262_144;

const TEMPORARY_LIFE_TIME: i64 = 60000;
const TEMPORARY_CHUNK_LIFE_TIME: i64 = 3600000;

const VERSION: i32 = 2; // Used for updating the database.

#[inline]
fn file_item_projection() -> Document {
    doc! {
        "_id": 1,
        "create_time": 1,
        "mime_type": 1,
        "file_size": 1,
        "file_name": 1,
        "file_data": 1,
        "chunk_id": 1,
        "expire_at": 1,
    }
}

#[inline]
fn file_exist_projection() -> Document {
    doc! {
        "_id": 1,
    }
}

#[inline]
fn file_item_delete_projection() -> Document {
    doc! {
        "_id": 0,
        "count": 1,
        "chunk_id": 1,
        "file_size": 1,
    }
}

#[inline]
fn chunk_document(file_id: ObjectId, n: i64, bytes: Vec<u8>) -> Document {
    doc! {
        "file_id": file_id,
        "n": n,
        "data": bson::Binary{ subtype: bson::spec::BinarySubtype::Generic, bytes }
    }
}

#[derive(Debug)]
struct FileCenterCollections {
    files: Collection<Document>,
    files_chunks: Collection<Document>,
    settings: Collection<Document>,
}

/// To store perennial files and temporary files in MongoDB.
#[derive(Debug)]
pub struct FileCenter {
    db: Database,
    collections: FileCenterCollections,
    file_size_threshold: u32,
    _create_time: DateTime,
    _version: i32,
    short_crypt: ShortCrypt,
}

impl FileCenter {
    async fn create_indexes(&self) -> Result<(), FileCenterError> {
        {
            let create_time_index = {
                let mut index = IndexModel::default();

                index.keys = doc! {
                    "create_time": 1
                };

                index
            };

            let expire_at_index = {
                let mut options = IndexOptions::default();
                options.expire_after = Some(Duration::from_secs(0));

                let mut index = IndexModel::default();

                index.keys = doc! {
                    "expire_at": 1
                };

                index.options = Some(options);

                index
            };

            let count_index = {
                let mut index = IndexModel::default();

                index.keys = doc! {
                    "count": 1
                };

                index
            };

            let hash_index = {
                let mut options = IndexOptions::default();
                options.unique = Some(true);
                options.sparse = Some(true);

                let mut index = IndexModel::default();

                index.keys = doc! {
                    "hash_1": 1,
                    "hash_2": 1,
                    "hash_3": 1,
                    "hash_4": 1
                };

                index.options = Some(options);

                index
            };

            let chunk_id_index = {
                let mut options = IndexOptions::default();
                options.unique = Some(true);
                options.sparse = Some(true);

                let mut index = IndexModel::default();

                index.keys = doc! {
                    "chunk_id": 1,
                };

                index.options = Some(options);

                index
            };

            self.collections
                .files
                .create_indexes(
                    [create_time_index, expire_at_index, count_index, hash_index, chunk_id_index],
                    None,
                )
                .await?;
        }

        {
            let file_id_index = {
                let mut index = IndexModel::default();

                index.keys = doc! {
                    "file_id": 1,
                };

                index
            };

            let expire_at_index = {
                let mut options = IndexOptions::default();
                options.expire_after = Some(Duration::from_secs(0));

                let mut index = IndexModel::default();

                index.keys = doc! {
                    "expire_at": 1
                };

                index.options = Some(options);

                index
            };

            self.collections
                .files_chunks
                .create_indexes([file_id_index, expire_at_index], None)
                .await?;
        }

        Ok(())
    }

    async fn new_with_file_size_threshold_inner<U: AsRef<str>>(
        uri: U,
        initial_file_size_threshold: u32,
    ) -> Result<FileCenter, FileCenterError> {
        let uri = uri.as_ref();

        let client_options = ClientOptions::parse(uri).await?;

        let client = Client::with_options(client_options)?;

        // TODO in the future, the client_options should have the default_database method

        let db_name = {
            if let Some(index) = uri.rfind('/') {
                let start = index + 1;

                let end = uri[start..]
                    .rfind('?')
                    .unwrap_or_else(|| uri[start..].rfind('#').unwrap_or(uri.len()));

                if start == end {
                    DEFAULT_DATABASE_NAME
                } else {
                    &uri[start..end]
                }
            } else {
                DEFAULT_DATABASE_NAME
            }
        };

        let db = client.database(db_name);

        let file_size_threshold;
        let create_time;
        let version;

        let collection_settings = db.collection::<Document>(COLLECTION_SETTINGS_NAME);
        let collection_files = db.collection::<Document>(COLLECTION_FILES_NAME);
        let collection_files_chunks = db.collection::<Document>(COLLECTION_FILES_CHUNKS_NAME);

        {
            file_size_threshold = match collection_settings
                .find_one(
                    Some(doc! {
                        "_id": SETTING_FILE_SIZE_THRESHOLD
                    }),
                    None,
                )
                .await?
            {
                Some(file_size_threshold) => {
                    let file_size_threshold = file_size_threshold.get_i32("value")?;

                    if file_size_threshold <= 0 {
                        return Err(FileCenterError::FileSizeThresholdError);
                    }

                    let file_size_threshold = file_size_threshold as u32;

                    if file_size_threshold > MAX_FILE_SIZE_THRESHOLD {
                        return Err(FileCenterError::FileSizeThresholdError);
                    }

                    file_size_threshold
                }
                None => {
                    collection_settings
                        .insert_one(
                            doc! {
                                "_id": SETTING_FILE_SIZE_THRESHOLD,
                                "value": initial_file_size_threshold
                            },
                            None,
                        )
                        .await?;

                    initial_file_size_threshold
                }
            };

            create_time = match collection_settings
                .find_one(
                    Some(doc! {
                        "_id": SETTING_CREATE_TIME
                    }),
                    None,
                )
                .await?
            {
                Some(create_time) => *create_time.get_datetime("value")?,
                None => {
                    let now = DateTime::now();

                    collection_settings
                        .insert_one(
                            doc! {
                                "_id": SETTING_CREATE_TIME,
                                "value": now
                            },
                            None,
                        )
                        .await?;

                    now
                }
            };

            version = match collection_settings
                .find_one(
                    Some(doc! {
                        "_id": SETTING_VERSION
                    }),
                    None,
                )
                .await?
            {
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
                        .insert_one(
                            doc! {
                                "_id": SETTING_VERSION,
                                "value": VERSION
                            },
                            None,
                        )
                        .await?;

                    VERSION
                }
            };
        }

        let short_crypt =
            ShortCrypt::new(&format!("FileCenter-{}", create_time.timestamp_millis()));

        let file_center = FileCenter {
            db,
            collections: FileCenterCollections {
                files: collection_files,
                files_chunks: collection_files_chunks,
                settings: collection_settings,
            },
            file_size_threshold,
            _create_time: create_time,
            _version: version,
            short_crypt,
        };

        file_center.create_indexes().await?;

        Ok(file_center)
    }

    /// Create a new FileCenter instance.
    #[inline]
    pub async fn new<U: AsRef<str>>(uri: U) -> Result<FileCenter, FileCenterError> {
        Self::new_with_file_size_threshold_inner(uri, DEFAULT_FILE_SIZE_THRESHOLD).await
    }

    /// Create a new FileCenter instance with a custom initial file size threshold.
    #[inline]
    pub async fn new_with_file_size_threshold<U: AsRef<str>>(
        uri: U,
        initial_file_size_threshold: u32,
    ) -> Result<FileCenter, FileCenterError> {
        if initial_file_size_threshold > MAX_FILE_SIZE_THRESHOLD || initial_file_size_threshold == 0
        {
            return Err(FileCenterError::FileSizeThresholdError);
        }

        Self::new_with_file_size_threshold_inner(uri, initial_file_size_threshold).await
    }
}

impl FileCenter {
    /// Change the file size threshold.
    #[inline]
    pub const fn get_file_size_threshold(&self) -> u32 {
        self.file_size_threshold
    }

    /// Change the file size threshold.
    pub async fn set_file_size_threshold(
        &mut self,
        file_size_threshold: u32,
    ) -> Result<(), FileCenterError> {
        let collection_settings = &self.collections.settings;

        if file_size_threshold > MAX_FILE_SIZE_THRESHOLD || file_size_threshold == 0 {
            return Err(FileCenterError::FileSizeThresholdError);
        }

        if file_size_threshold != self.file_size_threshold {
            let mut options = UpdateOptions::default();
            options.upsert = Some(true);

            collection_settings
                .update_one(
                    doc! {
                        "_id": SETTING_FILE_SIZE_THRESHOLD
                    },
                    doc! {
                        "$set": {
                            "value": file_size_threshold
                        }
                    },
                    Some(options),
                )
                .await?;

            self.file_size_threshold = file_size_threshold;
        }

        Ok(())
    }

    /// Drop the database.
    #[inline]
    pub async fn drop_database(self) -> Result<(), FileCenterError> {
        self.db.drop(None).await?;

        Ok(())
    }

    /// Drop the file center. But remain the database.
    #[inline]
    pub async fn drop_file_center(self) -> Result<(), FileCenterError> {
        self.collections.files.drop(None).await?;
        self.collections.files_chunks.drop(None).await?;
        self.collections.settings.drop(None).await?;

        Ok(())
    }
}

impl FileCenter {
    async fn open_download_stream(
        &self,
        id: ObjectId,
    ) -> Result<impl Stream<Item = Result<Cursor<Vec<u8>>, io::Error>> + Unpin, FileCenterError>
    {
        let collection_files_chunks = &self.collections.files_chunks;

        let mut find_options = FindOptions::default();

        find_options.sort = Some(doc! {
            "n": 1
        });

        Ok(collection_files_chunks
            .find(
                doc! {
                    "file_id": id
                },
                find_options,
            )
            .await
            .unwrap()
            .map(|item| {
                item.map_err(|err| io::Error::new(ErrorKind::InvalidData, err)).and_then(|i| {
                    i.get_binary_generic("data")
                        .map(|v| Cursor::new(v.to_vec()))
                        .map_err(|err| io::Error::new(ErrorKind::InvalidData, err))
                })
            }))
    }

    async fn create_file_item(
        &self,
        mut document: Document,
    ) -> Result<
        FileItem,
        FileCenterError,
    > {
        let file_id = match document
            .remove("_id")
            .ok_or(FileCenterError::DocumentError(ValueAccessError::NotPresent))?
        {
            Bson::ObjectId(b) => b,
            _ => {
                return Err(FileCenterError::DocumentError(ValueAccessError::UnexpectedType));
            }
        };

        let create_time = match document
            .remove("create_time")
            .ok_or(FileCenterError::DocumentError(ValueAccessError::NotPresent))?
        {
            Bson::DateTime(b) => b,
            _ => {
                return Err(FileCenterError::DocumentError(ValueAccessError::UnexpectedType));
            }
        };

        let expire_at = match document.remove("expire_at") {
            Some(expire_at) => {
                match expire_at {
                    Bson::DateTime(b) => Some(b),
                    _ => {
                        return Err(FileCenterError::DocumentError(
                            ValueAccessError::UnexpectedType,
                        ));
                    }
                }
            }
            None => None,
        };

        let mime_type = match document
            .remove("mime_type")
            .ok_or(FileCenterError::DocumentError(ValueAccessError::NotPresent))?
        {
            Bson::String(b) => {
                Mime::from_str(&b)
                    .map_err(|_| FileCenterError::DocumentError(ValueAccessError::UnexpectedType))?
            }
            _ => {
                return Err(FileCenterError::DocumentError(ValueAccessError::UnexpectedType));
            }
        };

        let file_size = document.get_i64("file_size")? as u64;

        let file_name = match document
            .remove("file_name")
            .ok_or(FileCenterError::DocumentError(ValueAccessError::NotPresent))?
        {
            Bson::String(b) => b,
            _ => {
                return Err(FileCenterError::DocumentError(ValueAccessError::UnexpectedType));
            }
        };

        let file_data = match document.remove("file_data") {
            Some(file_data) => {
                match file_data {
                    Bson::Binary(b) => FileData::Buffer(b.bytes),
                    _ => {
                        return Err(FileCenterError::DocumentError(
                            ValueAccessError::UnexpectedType,
                        ));
                    }
                }
            }
            None => {
                match document
                    .remove("chunk_id")
                    .ok_or(FileCenterError::DocumentError(ValueAccessError::NotPresent))?
                {
                    Bson::ObjectId(_) => (),
                    _ => {
                        return Err(FileCenterError::DocumentError(
                            ValueAccessError::UnexpectedType,
                        ));
                    }
                };

                let stream = self.open_download_stream(file_id).await?;

                FileData::Stream(Box::new(stream))
            }
        };

        Ok(FileItem {
            file_id,
            create_time,
            expire_at,
            mime_type,
            file_size,
            file_name,
            file_data,
        })
    }

    /// Check whether the file exists or not. If the file is temporary, it will still remain in the database.
    pub async fn check_file_item_exist(&self, id: ObjectId) -> Result<bool, FileCenterError> {
        let mut options = FindOneOptions::default();
        options.projection = Some(file_exist_projection());

        let file_item = self
            .collections
            .files
            .find_one(
                Some(doc! {
                    "_id": id
                }),
                Some(options),
            )
            .await?;

        Ok(file_item.is_some())
    }

    /// Get the file item via an Object ID.
    pub async fn get_file_item_by_id(
        &self,
        id: ObjectId,
    ) -> Result<
        Option<FileItem>,
        FileCenterError,
    > {
        let collection_files = &self.collections.files;

        let mut options = FindOneOptions::default();
        options.projection = Some(file_item_projection());

        let file_item = collection_files
            .find_one(
                Some(doc! {
                    "_id": id
                }),
                Some(options),
            )
            .await?;

        match file_item {
            Some(file_item) => {
                if let Some(expire_at) = file_item.get("expire_at") {
                    match expire_at.as_datetime() {
                        Some(expire_at) => {
                            if collection_files
                                .delete_one(
                                    doc! {
                                        "_id": id
                                    },
                                    None,
                                )
                                .await
                                .is_err()
                            {}

                            if DateTime::now().gt(expire_at) {
                                return Ok(None);
                            }
                        }
                        None => {
                            return Err(FileCenterError::DocumentError(
                                ValueAccessError::UnexpectedType,
                            ))
                        }
                    }
                }

                let file_item = self.create_file_item(file_item).await?;

                Ok(Some(file_item))
            }
            None => Ok(None),
        }
    }

    /// Remove a file item via an Object ID.
    pub async fn delete_file_item_by_id(
        &self,
        file_id: ObjectId,
    ) -> Result<Option<u64>, FileCenterError> {
        let collection_files = &self.collections.files;

        let mut options = FindOneAndUpdateOptions::default();
        options.return_document = Some(ReturnDocument::After);
        options.projection = Some(file_item_delete_projection());

        let result = collection_files
            .find_one_and_update(
                doc! {
                   "_id": file_id,
                },
                doc! {
                    "$inc": {
                        "count": -1
                    }
                },
                Some(options),
            )
            .await?;

        match result {
            Some(result) => {
                let count = result.get_i32("count")?;
                let file_size = result.get_i64("file_size")? as u64;

                if count <= 0 {
                    collection_files
                        .delete_one(
                            doc! {
                                "_id": file_id
                            },
                            None,
                        )
                        .await?;

                    if result.get("chunk_id").is_some()
                        && self.delete_file_chunks(file_id).await.is_err()
                    {}
                }

                Ok(Some(file_size))
            }
            None => Ok(None),
        }
    }
}

impl FileCenter {
    #[inline]
    async fn delete_file_chunks(&self, file_id: ObjectId) -> Result<DeleteResult, FileCenterError> {
        Ok(self
            .collections
            .files_chunks
            .delete_many(
                doc! {
                    "file_id": file_id
                },
                None,
            )
            .await?)
    }
}

impl FileCenter {
    async fn upload_from_stream(
        &self,
        file_id: ObjectId,
        mut source: impl AsyncRead + Unpin,
    ) -> Result<ObjectId, FileCenterError> {
        let collection_files_chunks = &self.collections.files_chunks;

        let buffer_size = self.file_size_threshold as usize;

        let mut buffer: Vec<u8> = Vec::with_capacity(buffer_size);

        #[allow(clippy::uninit_vec)]
        unsafe {
            buffer.set_len(buffer_size);
        }

        let mut n = 0i64;

        let mut inserted_id = None;

        loop {
            let mut cc = 0;

            // read to full
            loop {
                let c = match source.read(&mut buffer[cc..]).await {
                    Ok(0) => break,
                    Ok(c) => c,
                    Err(ref e) if e.kind() == ErrorKind::Interrupted => continue,
                    Err(e) => return Err(e.into()),
                };

                cc += c;

                if cc == buffer_size {
                    break;
                }
            }

            // read nothing
            if cc == 0 {
                break;
            }

            let chunk = &buffer[..cc];

            let result = collection_files_chunks
                .insert_one(chunk_document(file_id, n, chunk.to_vec()), None)
                .await?;

            inserted_id = Some(match result.inserted_id.as_object_id() {
                Some(id) => id,
                None => {
                    return Err(FileCenterError::DocumentError(ValueAccessError::UnexpectedType));
                }
            });

            n += 1;
        }

        match inserted_id {
            Some(inserted_id) => Ok(inserted_id),
            None => {
                let result = collection_files_chunks
                    .insert_one(chunk_document(file_id, 0, Vec::new()), None)
                    .await?;

                match result.inserted_id.as_object_id() {
                    Some(id) => Ok(id),
                    None => Err(FileCenterError::DocumentError(ValueAccessError::UnexpectedType)),
                }
            }
        }
    }

    /// Input a file to the file center via a file path.
    pub async fn put_file_by_path<P: AsRef<Path>, S: Into<String>>(
        &self,
        file_path: P,
        file_name: Option<S>,
        mime_type: Option<Mime>,
    ) -> Result<ObjectId, FileCenterError> {
        let file_path = file_path.as_ref();

        let (hash_1, hash_2, hash_3, hash_4) = get_hash_by_path(file_path).await?;

        let mut options = FindOneAndUpdateOptions::default();
        options.return_document = Some(ReturnDocument::After);
        options.projection = Some(file_exist_projection());

        let result = self
            .collections
            .files
            .find_one_and_update(
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
            )
            .await?;

        match result {
            Some(result) => Ok(result.get_object_id("_id")?),
            None => {
                let file_name = match file_name {
                    Some(file_name) => file_name.into(),
                    None => file_path.file_name().unwrap().to_str().unwrap().to_string(),
                };

                let mut file = File::open(file_path).await?;

                let metadata = file.metadata().await?;

                let file_size = metadata.len();

                let file_id = ObjectId::new();

                let mut file_item_raw = doc! {
                    "_id": file_id,
                    "hash_1": hash_1,
                    "hash_2": hash_2,
                    "hash_3": hash_3,
                    "hash_4": hash_4,
                    "file_size": file_size as i64,
                    "file_name": file_name,
                    "count": 1i32
                };

                if file_size > self.file_size_threshold as u64 {
                    let chunk_id = match self.upload_from_stream(file_id, file).await {
                        Ok(id) => id,
                        Err(err) => {
                            if self.delete_file_chunks(file_id).await.is_err() {}

                            return Err(err);
                        }
                    };

                    file_item_raw.insert("chunk_id", chunk_id);
                } else {
                    let mut file_data = Vec::with_capacity(file_size as usize);

                    file.read_to_end(&mut file_data).await?;

                    file_item_raw.insert(
                        "file_data",
                        Bson::Binary(Binary {
                            subtype: BinarySubtype::Generic,
                            bytes: file_data,
                        }),
                    );

                    drop(file);
                }

                let mime_type = match mime_type {
                    Some(mime_type) => mime_type,
                    None => get_mime_by_path(file_path),
                };

                file_item_raw.insert("mime_type", mime_type.as_ref());

                file_item_raw.insert("create_time", DateTime::now());

                self.collections.files.insert_one(file_item_raw, None).await?;

                Ok(file_id)
            }
        }
    }

    /// Temporarily input a file to the file center via a file path.
    pub async fn put_file_by_path_temporarily<P: AsRef<Path>, S: Into<String>>(
        &self,
        file_path: P,
        file_name: Option<S>,
        mime_type: Option<Mime>,
    ) -> Result<ObjectId, FileCenterError> {
        let file_path = file_path.as_ref();

        let file_name = match file_name {
            Some(file_name) => file_name.into(),
            None => file_path.file_name().unwrap().to_str().unwrap().to_string(),
        };

        let mut file = File::open(file_path).await?;

        let metadata = file.metadata().await?;

        let file_size = metadata.len();

        let file_id = ObjectId::new();

        let mut file_item_raw = doc! {
            "_id": file_id,
            "file_size": file_size as i64,
            "file_name": file_name,
            "count": 1i32
        };

        let is_stream = file_size > self.file_size_threshold as u64;

        if is_stream {
            let chunk_id = match self.upload_from_stream(file_id, file).await {
                Ok(id) => id,
                Err(err) => {
                    if self.delete_file_chunks(file_id).await.is_err() {}

                    return Err(err);
                }
            };

            file_item_raw.insert("chunk_id", chunk_id);
        } else {
            let mut file_data = Vec::with_capacity(file_size as usize);

            file.read_to_end(&mut file_data).await?;

            file_item_raw.insert(
                "file_data",
                Bson::Binary(Binary {
                    subtype: BinarySubtype::Generic,
                    bytes: file_data,
                }),
            );

            drop(file);
        }

        let mime_type = match mime_type {
            Some(mime_type) => mime_type,
            None => get_mime_by_path(file_path),
        };

        file_item_raw.insert("mime_type", mime_type.as_ref());

        let now = DateTime::now();

        let expire = DateTime::from_millis(now.timestamp_millis() + TEMPORARY_LIFE_TIME);
        let expire_chunks =
            DateTime::from_millis(now.timestamp_millis() + TEMPORARY_CHUNK_LIFE_TIME);

        file_item_raw.insert("create_time", now);

        file_item_raw.insert("expire_at", expire);

        if is_stream {
            self.collections
                .files_chunks
                .update_many(
                    doc! {
                        "file_id": file_id
                    },
                    doc! {
                        "$set": {
                            "expire_at": expire_chunks
                        }
                    },
                    None,
                )
                .await?;
        }

        self.collections.files.insert_one(file_item_raw, None).await?;

        Ok(file_id)
    }
}

impl FileCenter {
    async fn upload_from_buffer(
        &self,
        file_id: ObjectId,
        source: &[u8],
    ) -> Result<ObjectId, FileCenterError> {
        let collection_files_chunks = &self.collections.files_chunks;

        let chunk_size = self.file_size_threshold as usize;

        let mut inserted_id = None;

        for (n, chunk) in source.chunks(chunk_size).enumerate() {
            let result = collection_files_chunks
                .insert_one(chunk_document(file_id, n as i64, chunk.to_vec()), None)
                .await?;

            inserted_id = Some(match result.inserted_id.as_object_id() {
                Some(id) => id,
                None => {
                    return Err(FileCenterError::DocumentError(ValueAccessError::UnexpectedType));
                }
            });
        }

        match inserted_id {
            Some(inserted_id) => Ok(inserted_id),
            None => {
                let result = collection_files_chunks
                    .insert_one(chunk_document(file_id, 0, Vec::new()), None)
                    .await?;

                match result.inserted_id.as_object_id() {
                    Some(id) => Ok(id),
                    None => Err(FileCenterError::DocumentError(ValueAccessError::UnexpectedType)),
                }
            }
        }
    }

    /// Input a file to the file center via a buffer.
    pub async fn put_file_by_buffer<B: AsRef<[u8]> + Into<Vec<u8>>, S: Into<String>>(
        &self,
        buffer: B,
        file_name: S,
        mime_type: Option<Mime>,
    ) -> Result<ObjectId, FileCenterError> {
        let (hash_1, hash_2, hash_3, hash_4) = get_hash_by_buffer(buffer.as_ref());

        let mut options = FindOneAndUpdateOptions::default();
        options.return_document = Some(ReturnDocument::After);
        options.projection = Some(file_exist_projection());

        let result = self
            .collections
            .files
            .find_one_and_update(
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
            )
            .await?;

        match result {
            Some(result) => Ok(result.get_object_id("_id")?),
            None => {
                let buffer = buffer.into();
                let file_name = file_name.into();

                let file_size = buffer.len();

                let file_id = ObjectId::new();

                let mut file_item_raw = doc! {
                    "_id": file_id,
                    "hash_1": hash_1,
                    "hash_2": hash_2,
                    "hash_3": hash_3,
                    "hash_4": hash_4,
                    "file_size": file_size as i64,
                    "file_name": file_name,
                    "count": 1i32
                };

                if file_size > self.file_size_threshold as usize {
                    let chunk_id = match self.upload_from_buffer(file_id, &buffer).await {
                        Ok(id) => id,
                        Err(err) => {
                            if self.delete_file_chunks(file_id).await.is_err() {}

                            return Err(err);
                        }
                    };

                    file_item_raw.insert("chunk_id", chunk_id);

                    drop(buffer);
                } else {
                    file_item_raw.insert(
                        "file_data",
                        Bson::Binary(Binary {
                            subtype: BinarySubtype::Generic,
                            bytes: buffer,
                        }),
                    );
                }

                let mime_type = mime_type.unwrap_or(DEFAULT_MIME_TYPE);

                file_item_raw.insert("mime_type", mime_type.as_ref());

                file_item_raw.insert("create_time", DateTime::now());

                self.collections.files.insert_one(file_item_raw, None).await?;

                Ok(file_id)
            }
        }
    }

    /// Temporarily input a file to the file center via a buffer.
    pub async fn put_file_by_buffer_temporarily<B: AsRef<[u8]> + Into<Vec<u8>>, S: Into<String>>(
        &self,
        buffer: B,
        file_name: S,
        mime_type: Option<Mime>,
    ) -> Result<ObjectId, FileCenterError> {
        let buffer = buffer.into();
        let file_name = file_name.into();

        let file_size = buffer.len();

        let file_id = ObjectId::new();

        let mut file_item_raw = doc! {
            "_id": file_id,
            "file_size": file_size as i64,
            "file_name": file_name,
            "count": 1i32
        };

        let is_stream = file_size > self.file_size_threshold as usize;

        if is_stream {
            let chunk_id = match self.upload_from_buffer(file_id, &buffer).await {
                Ok(id) => id,
                Err(err) => {
                    if self.delete_file_chunks(file_id).await.is_err() {}

                    return Err(err);
                }
            };

            file_item_raw.insert("chunk_id", chunk_id);

            drop(buffer);
        } else {
            file_item_raw.insert(
                "file_data",
                Bson::Binary(Binary {
                    subtype: BinarySubtype::Generic,
                    bytes: buffer,
                }),
            );
        }

        let mime_type = mime_type.unwrap_or(DEFAULT_MIME_TYPE);

        file_item_raw.insert("mime_type", mime_type.as_ref());

        let now = DateTime::now();

        let expire = DateTime::from_millis(now.timestamp_millis() + TEMPORARY_LIFE_TIME);
        let expire_chunks =
            DateTime::from_millis(now.timestamp_millis() + TEMPORARY_CHUNK_LIFE_TIME);

        file_item_raw.insert("create_time", now);

        file_item_raw.insert("expire_at", expire);

        if is_stream {
            self.collections
                .files_chunks
                .update_many(
                    doc! {
                        "file_id": file_id
                    },
                    doc! {
                        "$set": {
                            "expire_at": expire_chunks
                        }
                    },
                    None,
                )
                .await?;
        }

        self.collections.files.insert_one(file_item_raw, None).await?;

        Ok(file_id)
    }
}

impl FileCenter {
    async fn upload_from_stream_and_hash(
        &self,
        file_id: ObjectId,
        mut first_chunk_plus_one: Vec<u8>,
        mut source: impl AsyncRead + Unpin,
    ) -> Result<(ObjectId, i64, (i64, i64, i64, i64)), FileCenterError> {
        let collection_files_chunks = &self.collections.files_chunks;

        let buffer_size = self.file_size_threshold as usize;
        let mut buffer: Vec<u8> = Vec::with_capacity(buffer_size);

        #[allow(clippy::uninit_vec)]
        unsafe {
            buffer.set_len(buffer_size);
        }

        buffer[0] = first_chunk_plus_one[buffer_size];

        let mut hasher = Hasher::new();

        hasher.update(&first_chunk_plus_one[..buffer_size]);

        unsafe {
            first_chunk_plus_one.set_len(buffer_size);
        }

        let result = collection_files_chunks
            .insert_one(chunk_document(file_id, 0, first_chunk_plus_one), None)
            .await?;

        let mut inserted_id = match result.inserted_id.as_object_id() {
            Some(id) => id,
            None => {
                return Err(FileCenterError::DocumentError(ValueAccessError::UnexpectedType));
            }
        };

        let mut n = 1i64;
        let mut cc = 1;
        let mut file_size = buffer_size as i64;

        loop {
            // read to full
            loop {
                let c = match source.read(&mut buffer[cc..]).await {
                    Ok(0) => break,
                    Ok(c) => c,
                    Err(ref e) if e.kind() == ErrorKind::Interrupted => continue,
                    Err(e) => return Err(e.into()),
                };

                cc += c;

                if cc == buffer_size {
                    break;
                }
            }

            // read nothing
            if cc == 0 {
                break;
            }

            let chunk = &buffer[..cc];

            hasher.update(chunk);

            let result = collection_files_chunks
                .insert_one(chunk_document(file_id, n, chunk.to_vec()), None)
                .await?;

            inserted_id = match result.inserted_id.as_object_id() {
                Some(id) => id,
                None => {
                    return Err(FileCenterError::DocumentError(ValueAccessError::UnexpectedType));
                }
            };

            n += 1;
            file_size += cc as i64;

            cc = 0;
        }

        let hash = separate_hash(&hasher.finalize());

        Ok((inserted_id, file_size, hash))
    }

    async fn upload_from_stream_and_no_hash(
        &self,
        file_id: ObjectId,
        mut first_chunk_plus_one: Vec<u8>,
        mut source: impl AsyncRead + Unpin,
    ) -> Result<(ObjectId, i64), FileCenterError> {
        let collection_files_chunks = &self.collections.files_chunks;

        let buffer_size = self.file_size_threshold as usize;
        let mut buffer: Vec<u8> = Vec::with_capacity(buffer_size);

        #[allow(clippy::uninit_vec)]
        unsafe {
            buffer.set_len(buffer_size);
        }

        buffer[0] = first_chunk_plus_one[buffer_size];

        unsafe {
            first_chunk_plus_one.set_len(buffer_size);
        }

        let result = collection_files_chunks
            .insert_one(chunk_document(file_id, 0, first_chunk_plus_one), None)
            .await?;

        let mut inserted_id = match result.inserted_id.as_object_id() {
            Some(id) => id,
            None => {
                return Err(FileCenterError::DocumentError(ValueAccessError::UnexpectedType));
            }
        };

        let mut n = 1i64;
        let mut cc = 1;
        let mut file_size = buffer_size as i64;

        loop {
            // read to full
            loop {
                let c = match source.read(&mut buffer[cc..]).await {
                    Ok(0) => break,
                    Ok(c) => c,
                    Err(ref e) if e.kind() == ErrorKind::Interrupted => continue,
                    Err(e) => return Err(e.into()),
                };

                cc += c;

                if cc == buffer_size {
                    break;
                }
            }

            // read nothing
            if cc == 0 {
                break;
            }

            let chunk = &buffer[..cc];

            let result = collection_files_chunks
                .insert_one(chunk_document(file_id, n, chunk.to_vec()), None)
                .await?;

            inserted_id = match result.inserted_id.as_object_id() {
                Some(id) => id,
                None => {
                    return Err(FileCenterError::DocumentError(ValueAccessError::UnexpectedType));
                }
            };

            n += 1;
            file_size += cc as i64;

            cc = 0;
        }

        Ok((inserted_id, file_size))
    }

    /// Input a file to the file center via a reader.
    pub async fn put_file_by_reader<R: AsyncRead + Unpin, S: Into<String>>(
        &self,
        mut reader: R,
        file_name: S,
        mime_type: Option<Mime>,
    ) -> Result<ObjectId, FileCenterError> {
        let buffer_size = self.file_size_threshold as usize + 1;

        let mut file_data = Vec::with_capacity(buffer_size);

        #[allow(clippy::uninit_vec)]
        unsafe {
            file_data.set_len(buffer_size);
        }

        let mut cc = 0;

        // read to full
        loop {
            let c = match reader.read(&mut file_data[cc..]).await {
                Ok(0) => break,
                Ok(c) => c,
                Err(ref e) if e.kind() == ErrorKind::Interrupted => continue,
                Err(e) => return Err(e.into()),
            };

            cc += c;

            if cc == buffer_size {
                break;
            }
        }

        let cc = cc as i64;

        let file_name = file_name.into();

        let file_id = ObjectId::new();

        let mut file_item_raw = doc! {
            "_id": file_id,
            "file_name": file_name,
            "count": 1i32
        };

        let is_stream = cc == buffer_size as i64;

        let (hash_1, hash_2, hash_3, hash_4) = if is_stream {
            let (chunk_id, file_size, hash) =
                match self.upload_from_stream_and_hash(file_id, file_data, reader).await {
                    Ok(id) => id,
                    Err(err) => {
                        if self.delete_file_chunks(file_id).await.is_err() {}

                        return Err(err);
                    }
                };

            file_item_raw.insert("file_size", file_size);
            file_item_raw.insert("chunk_id", chunk_id);

            hash
        } else {
            unsafe {
                file_data.set_len(cc as usize);
            }

            let hash = get_hash_by_buffer(&file_data);

            file_item_raw.insert("file_size", cc);
            file_item_raw.insert(
                "file_data",
                Bson::Binary(Binary {
                    subtype: BinarySubtype::Generic,
                    bytes: file_data,
                }),
            );

            hash
        };

        let mut options = FindOneAndUpdateOptions::default();
        options.return_document = Some(ReturnDocument::After);
        options.projection = Some(file_exist_projection());

        let result = self
            .collections
            .files
            .find_one_and_update(
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
            )
            .await?;

        match result {
            Some(result) => {
                if is_stream && self.delete_file_chunks(file_id).await.is_err() {}

                Ok(result.get_object_id("_id")?)
            }
            None => {
                file_item_raw.insert("hash_1", hash_1);
                file_item_raw.insert("hash_2", hash_2);
                file_item_raw.insert("hash_3", hash_3);
                file_item_raw.insert("hash_4", hash_4);

                let mime_type = mime_type.unwrap_or(DEFAULT_MIME_TYPE);

                file_item_raw.insert("mime_type", mime_type.as_ref());

                file_item_raw.insert("create_time", DateTime::now());

                self.collections.files.insert_one(file_item_raw, None).await?;

                Ok(file_id)
            }
        }
    }

    /// Temporarily input a file to the file center via a reader.
    pub async fn put_file_by_reader_temporarily<R: AsyncRead + Unpin, S: Into<String>>(
        &self,
        mut reader: R,
        file_name: S,
        mime_type: Option<Mime>,
    ) -> Result<ObjectId, FileCenterError> {
        let buffer_size = self.file_size_threshold as usize + 1;

        let mut file_data = Vec::with_capacity(buffer_size);

        #[allow(clippy::uninit_vec)]
        unsafe {
            file_data.set_len(buffer_size);
        }

        let mut cc = 0;

        // read to full
        loop {
            let c = match reader.read(&mut file_data[cc..]).await {
                Ok(0) => break,
                Ok(c) => c,
                Err(ref e) if e.kind() == ErrorKind::Interrupted => continue,
                Err(e) => return Err(e.into()),
            };

            cc += c;

            if cc == buffer_size {
                break;
            }
        }

        let cc = cc as i64;

        let file_name = file_name.into();

        let file_id = ObjectId::new();

        let mut file_item_raw = doc! {
            "_id": file_id,
            "file_name": file_name,
            "count": 1i32
        };

        let is_stream = cc == buffer_size as i64;

        if is_stream {
            let (chunk_id, file_size) =
                match self.upload_from_stream_and_no_hash(file_id, file_data, reader).await {
                    Ok(id) => id,
                    Err(err) => {
                        if self.delete_file_chunks(file_id).await.is_err() {}

                        return Err(err);
                    }
                };

            file_item_raw.insert("file_size", file_size);
            file_item_raw.insert("chunk_id", chunk_id);
        } else {
            unsafe {
                file_data.set_len(cc as usize);
            }

            file_item_raw.insert("file_size", cc);
            file_item_raw.insert(
                "file_data",
                Bson::Binary(Binary {
                    subtype: BinarySubtype::Generic,
                    bytes: file_data,
                }),
            );
        };

        let mime_type = mime_type.unwrap_or(DEFAULT_MIME_TYPE);

        file_item_raw.insert("mime_type", mime_type.as_ref());

        let now = DateTime::now();

        let expire = DateTime::from_millis(now.timestamp_millis() + TEMPORARY_LIFE_TIME);
        let expire_chunks =
            DateTime::from_millis(now.timestamp_millis() + TEMPORARY_CHUNK_LIFE_TIME);

        file_item_raw.insert("create_time", now);

        file_item_raw.insert("expire_at", expire);

        if is_stream {
            self.collections
                .files_chunks
                .update_many(
                    doc! {
                        "file_id": file_id
                    },
                    doc! {
                        "$set": {
                            "expire_at": expire_chunks
                        }
                    },
                    None,
                )
                .await?;
        }

        self.collections.files.insert_one(file_item_raw, None).await?;

        Ok(file_id)
    }
}

impl FileCenter {
    /// Remove all unused file meta and file chunks in this file center.
    pub async fn clear_garbage(&self) -> Result<(), FileCenterError> {
        // unnecessary file items which have chunk_id but the target chunks do not exist
        {
            let mut result = self
                .collections
                .files
                .aggregate(
                    [
                        doc! {
                            "$match": {
                                "chunk_id": {
                                    "$exists": true
                                }
                            }
                        },
                        doc! {
                            "$lookup": {
                             "from": COLLECTION_FILES_CHUNKS_NAME,
                             "localField": "chunk_id",
                             "foreignField": "_id",
                             "as": "chunk"
                           }
                        },
                        doc! {
                            "$match": {
                                "chunk": []
                            }
                        },
                        doc! {
                            "$project": {
                                "_id": 1
                            }
                        },
                    ],
                    None,
                )
                .await?;

            let mut ids = Vec::new();

            while let Some(d) = result.try_next().await? {
                ids.push(d.get_object_id("_id")?);
            }

            if !ids.is_empty() {
                self.collections
                    .files
                    .delete_many(
                        doc! {
                                "_id": {
                                    "$in": ids
                            }
                        },
                        None,
                    )
                    .await?;
            }
        }

        // unnecessary file items whose count are smaller than or equal to 0
        {
            let mut result = self
                .collections
                .files
                .find(
                    doc! {
                        "count": {
                            "$lte": 0
                        }
                    },
                    None,
                )
                .await?;

            let mut ids = Vec::new();

            while let Some(d) = result.try_next().await? {
                ids.push(d.get_object_id("_id")?);
            }

            if !ids.is_empty() {
                self.collections
                    .files
                    .delete_many(
                        doc! {
                                "_id": {
                                    "$in": ids.clone()
                            }
                        },
                        None,
                    )
                    .await?;

                self.collections
                    .files_chunks
                    .delete_many(
                        doc! {
                                "file_id": {
                                    "$in": ids
                            }
                        },
                        None,
                    )
                    .await?;
            }
        }

        // unnecessary chunks which are not used in file items
        {
            let mut result = self
                .collections
                .files_chunks
                .aggregate(
                    [
                        doc! {
                            "$lookup": {
                             "from": COLLECTION_FILES_NAME,
                             "localField": "file_id",
                             "foreignField": "_id",
                             "as": "item"
                           }
                        },
                        doc! {
                            "$match": {
                                "item": []
                            }
                        },
                        doc! {
                            "$group": {
                                "_id": null,
                                "file_ids": {
                                    "$addToSet": "$file_id"
                                }
                            }
                        },
                        doc! {
                            "$unwind": "$file_ids"
                        },
                        doc! {
                            "$project": {
                                "file_id": "$file_ids"
                            }
                        },
                    ],
                    None,
                )
                .await?;

            let mut ids = Vec::new();

            while let Some(d) = result.try_next().await? {
                ids.push(d.get_object_id("file_id")?);
            }

            if !ids.is_empty() {
                self.collections
                    .files_chunks
                    .delete_many(
                        doc! {
                                "file_id": {
                                    "$in": ids
                            }
                        },
                        None,
                    )
                    .await?;
            }
        }

        Ok(())
    }
}

impl FileCenter {
    #[allow(clippy::missing_safety_doc)]
    /// Get the reference of MongoDB Database.
    #[inline]
    pub unsafe fn database(&self) -> &Database {
        &self.db
    }
}

impl FileCenter {
    /// Decrypt a ID token to an Object ID.
    pub fn decrypt_id_token<S: AsRef<str>>(
        &self,
        id_token: S,
    ) -> Result<ObjectId, FileCenterError> {
        let id_raw = self
            .short_crypt
            .decrypt_url_component(id_token)
            .map_err(FileCenterError::IDTokenError)?;

        let id_raw: [u8; 12] = {
            if id_raw.len() != 12 {
                return Err(FileCenterError::IDTokenError("ID needs to be 12 bytes"));
            }

            let mut fixed_raw = [0u8; 12];

            fixed_raw.copy_from_slice(&id_raw);

            fixed_raw
        };

        Ok(ObjectId::from_bytes(id_raw))
    }

    /// Encrypt an Object ID to an ID token.
    #[inline]
    pub fn encrypt_id(&self, id: ObjectId) -> IDToken {
        let id_raw = id.bytes();

        self.short_crypt.encrypt_to_url_component(&id_raw)
    }

    /// Encrypt an Object ID to an ID token.
    #[inline]
    pub fn encrypt_id_to_buffer(&self, id: ObjectId, buffer: String) -> String {
        let id_raw = id.bytes();

        self.short_crypt.encrypt_to_url_component_and_push_to_string(&id_raw, buffer)
    }
}
