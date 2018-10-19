#[macro_use(bson, doc)]
extern crate bson;

#[macro_use]
extern crate lazy_static;

extern crate mongodb;
extern crate chrono;
extern crate short_crypt;
extern crate sha3;
extern crate mime_guess;

use mongodb::{Client, ThreadedClient};
use mongodb::db::{Database, ThreadedDatabase};
use mongodb::coll::{Collection, options::{ReturnDocument, IndexOptions, FindOptions, UpdateOptions, FindOneAndUpdateOptions}};
use mongodb::gridfs::{self, Store, ThreadedStore};

use bson::oid::ObjectId;
use bson::{Bson, Document};
use bson::spec::BinarySubtype;

use chrono::prelude::*;

use short_crypt::ShortCrypt;

use sha3::{Digest, Sha3_256};

use std::path::Path;
use std::io::{self, Read, Write};
use std::fs::File;
use std::mem::transmute;
use std::fmt::{self, Debug, Formatter};

const FILE_BUFFER_SIZE: usize = 4096;
const DEFAULT_MIME_TYPE: &str = "application/octet-stream";

const COLLECTION_FILES_NAME: &str = "file_center";
const COLLECTION_SETTINGS_NAME: &str = "file_center_settings";
const MAX_FILE_SIZE_THRESHOLD: i32 = 16770000;

const SETTING_FILE_SIZE_THRESHOLD: &str = "file_size_threshold";
const SETTING_CREATE_TIME: &str = "create_time";

pub struct FileCenter {
    mongo_client_db: Database,
    file_size_threshold: i32,
    _create_time: DateTime<Utc>,
    short_crypt: ShortCrypt,
}

pub enum FileData {
    Collection(Vec<u8>),
    GridFS(gridfs::file::File),
}

impl FileData {
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

impl Debug for FileData {
    fn fmt(&self, f: &mut Formatter) -> Result<(), fmt::Error> {
        match self {
            FileData::Collection(_) => f.write_str("FileData::Collection")?,
            FileData::GridFS(_) => f.write_str("FileData::GridFS")?,
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct FileItem {
    id: ObjectId,
    create_time: DateTime<Utc>,
    expire_at: Option<DateTime<Utc>>,
    mime_type: String,
    file_size: u64,
    file_name: String,
    file_data: FileData,
}

impl FileItem {
    pub fn get_object_id(&self) -> &ObjectId {
        &self.id
    }

    pub fn get_create_time(&self) -> &DateTime<Utc> {
        &self.create_time
    }

    pub fn get_mime_type(&self) -> &str {
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

pub type IDToken = String;

#[derive(Debug)]
pub enum FileCenterError {
    MongoDBError(mongodb::Error),
    DocumentError(bson::ValueAccessError),
    IDTokenError(&'static str),
    FileSizeThresholdError,
    IOError(io::Error),
}

impl FileCenter {
    /// Create a new FileCenter instance.
    pub fn new(host: &str, port: u16, database: &str) -> Result<FileCenter, FileCenterError> {
        let mongodb_client = Client::connect(host, port).map_err(|err| FileCenterError::MongoDBError(err))?;

        let mongo_client_db = mongodb_client.db(database);

        let file_size_threshold;
        let create_time;

        {
            let collection_settings: Collection = mongo_client_db.collection(COLLECTION_SETTINGS_NAME);

            file_size_threshold = match collection_settings.find_one(Some(doc! {"_id": SETTING_FILE_SIZE_THRESHOLD}), None).map_err(|err| FileCenterError::MongoDBError(err))? {
                Some(file_size_threshold) => {
                    let file_size_threshold = file_size_threshold.get_i32("value").map_err(|err| FileCenterError::DocumentError(err))?;

                    if file_size_threshold > MAX_FILE_SIZE_THRESHOLD || file_size_threshold <= 0 {
                        return Err(FileCenterError::FileSizeThresholdError);
                    }

                    file_size_threshold
                }
                None => {
                    collection_settings.insert_one(doc! {"_id": SETTING_FILE_SIZE_THRESHOLD, "value": MAX_FILE_SIZE_THRESHOLD}, None).map_err(|err| FileCenterError::MongoDBError(err))?;
                    MAX_FILE_SIZE_THRESHOLD
                }
            };

            create_time = match collection_settings.find_one(Some(doc! {"_id": SETTING_CREATE_TIME}), None).map_err(|err| FileCenterError::MongoDBError(err))? {
                Some(file_size_threshold) => {
                    file_size_threshold.get_utc_datetime("value").map_err(|err| FileCenterError::DocumentError(err))?.clone()
                }
                None => {
                    let now = Utc::now();

                    collection_settings.insert_one(doc! {"_id": SETTING_CREATE_TIME, "value": now.clone()}, None).map_err(|err| FileCenterError::MongoDBError(err))?;

                    now
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
                collection_files.create_index(doc! {"hash_1": 1, "hash_2": 1, "hash_3": 1, "hash_4": 1}, None).unwrap();
            }
        }

        let short_crypt = ShortCrypt::new(&format!("FileCenter-{}", create_time.timestamp_millis()));

        Ok(FileCenter {
            mongo_client_db,
            file_size_threshold,
            _create_time: create_time,
            short_crypt,
        })
    }

    pub fn set_file_size_threshold(&mut self, file_size_threshold: i32) -> Result<(), FileCenterError> {
        let collection_settings: Collection = self.mongo_client_db.collection(COLLECTION_SETTINGS_NAME);

        if file_size_threshold > MAX_FILE_SIZE_THRESHOLD || file_size_threshold <= 0 {
            return Err(FileCenterError::FileSizeThresholdError);
        }

        if file_size_threshold != self.file_size_threshold {
            let mut options = UpdateOptions::new();
            options.upsert = Some(true);

            collection_settings.update_one(doc! {"_id": SETTING_FILE_SIZE_THRESHOLD}, doc! {"$set": {"value": file_size_threshold}}, Some(options)).map_err(|err| FileCenterError::MongoDBError(err))?;

            self.file_size_threshold = file_size_threshold;
        }

        Ok(())
    }

    /// Decrypt a ID token to an Object ID.
    pub fn decrypt_id_token(&self, id_token: &IDToken) -> Result<ObjectId, FileCenterError> {
        let id_raw = self.short_crypt.decrypt_url_component(id_token).map_err(|err| FileCenterError::IDTokenError(err))?;

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
        self.mongo_client_db.drop_database().map_err(|err| FileCenterError::MongoDBError(err))?;
        Ok(())
    }

    /// Drop the file center.
    pub fn drop_file_center(self) -> Result<(), FileCenterError> {
        self.mongo_client_db.drop_collection(COLLECTION_FILES_NAME).map_err(|err| FileCenterError::MongoDBError(err))?;
        self.mongo_client_db.drop_collection(COLLECTION_SETTINGS_NAME).map_err(|err| FileCenterError::MongoDBError(err))?;
        Ok(())
    }

    fn create_file_item(&self, mut document: Document) -> Result<FileItem, FileCenterError> {
        let collection_files: Collection = self.mongo_client_db.collection(COLLECTION_FILES_NAME);

        let id = match document.remove("_id")
            .ok_or(FileCenterError::DocumentError(bson::ValueAccessError::NotPresent))? {
            Bson::ObjectId(b) => b,
            _ => return Err(FileCenterError::DocumentError(bson::ValueAccessError::NotPresent))
        };

        let create_time = match document.remove("create_time")
            .ok_or(FileCenterError::DocumentError(bson::ValueAccessError::NotPresent))? {
            Bson::UtcDatetime(b) => b,
            _ => return Err(FileCenterError::DocumentError(bson::ValueAccessError::NotPresent))
        };

        let expire_at = match document.remove("expire_at") {
            Some(expire_at) => match expire_at {
                Bson::UtcDatetime(b) => {
                    collection_files.delete_one(doc! {"_id": id.clone()}, None).map_err(|err| FileCenterError::MongoDBError(err))?;
                    Some(b)
                }
                _ => return Err(FileCenterError::DocumentError(bson::ValueAccessError::NotPresent))
            }
            None => None
        };

        let mime_type = match document.remove("mime_type")
            .ok_or(FileCenterError::DocumentError(bson::ValueAccessError::NotPresent))? {
            Bson::String(b) => b,
            _ => return Err(FileCenterError::DocumentError(bson::ValueAccessError::NotPresent))
        };

        let file_size = document.get_i64("file_size").map_err(|err| FileCenterError::DocumentError(err))? as u64;

        let file_name = match document.remove("file_name")
            .ok_or(FileCenterError::DocumentError(bson::ValueAccessError::NotPresent))? {
            Bson::String(b) => b,
            _ => return Err(FileCenterError::DocumentError(bson::ValueAccessError::NotPresent))
        };

        let file_data = match document.remove("file_data") {
            Some(file_data) => match file_data {
                Bson::Binary(_, d) => FileData::Collection(d),
                _ => return Err(FileCenterError::DocumentError(bson::ValueAccessError::NotPresent))
            }
            None => {
                let file_id = match document.remove("file_id")
                    .ok_or(FileCenterError::DocumentError(bson::ValueAccessError::NotPresent))? {
                    Bson::ObjectId(b) => b,
                    _ => return Err(FileCenterError::DocumentError(bson::ValueAccessError::NotPresent))
                };

                let store = Store::with_db(self.mongo_client_db.clone());

                FileData::GridFS(store.open_id(file_id).map_err(|err| FileCenterError::MongoDBError(err))?)
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

    /// Get the file item searched via an Object ID.
    pub fn get_file_item_by_id(&self, id: &ObjectId) -> Result<Option<FileItem>, FileCenterError> {
        let collection_files: Collection = self.mongo_client_db.collection(COLLECTION_FILES_NAME);

        let mut options = FindOptions::new();
        options.projection = Some(
            doc! {
                "create_time": 1,
                "mime_type": 1,
                "file_size": 1,
                "file_name": 1,
                "file_data": 1,
                "file_id": 1,
                "expire_at": 1,
            }
        );

        let file_item = collection_files.find_one(Some(doc! {"_id": id.clone()}), Some(options)).map_err(|err| FileCenterError::MongoDBError(err))?;

        match file_item {
            Some(file_item) => {
                let file_item = self.create_file_item(file_item)?;

                Ok(Some(file_item))
            }
            None => Ok(None)
        }
    }

    /// Input a file to the file center via a file path.
    pub fn put_file_by_path<P: AsRef<Path>>(&self, file_path: P, file_name: Option<&str>, mime_type: Option<&str>) -> Result<FileItem, FileCenterError> {
        let file_path = file_path.as_ref();

        let (hash_1, hash_2, hash_3, hash_4) = get_hash_by_path(file_path).map_err(|err| FileCenterError::IOError(err))?;

        let collection_files: Collection = self.mongo_client_db.collection(COLLECTION_FILES_NAME);

        let mut options = FindOneAndUpdateOptions::new();
        options.return_document = Some(ReturnDocument::After);
        let result = collection_files.find_one_and_update(doc! {
            "hash_1": hash_1,
            "hash_2": hash_2,
            "hash_3": hash_3,
            "hash_4": hash_4,
         }, doc! {
            "$inc": {
                "count": 1
            }
        }, Some(options)).map_err(|err| FileCenterError::MongoDBError(err))?;

        match result {
            Some(result) => {
                Ok(self.create_file_item(result)?)
            }
            None => {
                let file_name = match file_name {
                    Some(file_name) => file_name,
                    None => {
                        file_path.file_name().unwrap().to_str().unwrap()
                    }
                };

                let mut file = File::open(file_path).map_err(|err| FileCenterError::IOError(err))?;

                let metadata = file.metadata().map_err(|err| FileCenterError::IOError(err))?;

                let file_size = metadata.len();

                let mut file_item_raw: Document = doc! {
                    "hash_1": hash_1,
                    "hash_2": hash_2,
                    "hash_3": hash_3,
                    "hash_4": hash_4,
                    "file_name": file_name,
                    "file_size": file_size,
                    "count": 1
                };

                if file_size >= self.file_size_threshold as u64 {
                    let store = Store::with_db(self.mongo_client_db.clone());
                    let mut store_file: gridfs::file::File = store.create(file_name.to_string()).map_err(|err| FileCenterError::MongoDBError(err))?;
                    let mut file = File::open(file_path).map_err(|err| FileCenterError::IOError(err))?;
                    let mut buffer = [0u8; FILE_BUFFER_SIZE];

                    loop {
                        let c = file.read(&mut buffer).map_err(|err| FileCenterError::IOError(err))?;

                        if c == 0 {
                            break;
                        }

                        store_file.write(&buffer[..c]).map_err(|err| FileCenterError::IOError(err))?;
                    }

                    let id = store_file.doc.id.clone();
                    file_item_raw.insert("file_id", id);
                    drop(file);
                } else {
                    let mut file_data = Vec::new();
                    file.read_to_end(&mut file_data).map_err(|err| FileCenterError::IOError(err))?;
                    file_item_raw.insert("file_data", Bson::Binary(BinarySubtype::Generic, file_data));
                    drop(file);
                }

                let mime_type = match mime_type {
                    Some(mime_type) => mime_type,
                    None => {
                        match file_path.extension() {
                            Some(extension) => {
                                match mime_guess::get_mime_type_str(&extension.to_str().unwrap().to_lowercase()) {
                                    Some(mime) => mime,
                                    None => DEFAULT_MIME_TYPE
                                }
                            }
                            None => DEFAULT_MIME_TYPE
                        }
                    }
                };

                file_item_raw.insert("mime_type", mime_type);

                file_item_raw.insert("create_time", Utc::now());

                let result = collection_files.insert_one(file_item_raw.clone(), None).map_err(|err| FileCenterError::MongoDBError(err))?;

                file_item_raw.insert("_id", result.inserted_id.unwrap());

                Ok(self.create_file_item(file_item_raw)?)
            }
        }
    }

    /// Input a file to the file center via a buffer.
    pub fn put_file_by_buffer(&self, buffer: Vec<u8>, file_name: &str, mime_type: Option<&str>) -> Result<FileItem, FileCenterError> {
        let (hash_1, hash_2, hash_3, hash_4) = get_hash_by_buffer(&buffer).map_err(|err| FileCenterError::IOError(err))?;

        let collection_files: Collection = self.mongo_client_db.collection(COLLECTION_FILES_NAME);

        let mut options = FindOneAndUpdateOptions::new();
        options.return_document = Some(ReturnDocument::After);
        let result = collection_files.find_one_and_update(doc! {
            "hash_1": hash_1,
            "hash_2": hash_2,
            "hash_3": hash_3,
            "hash_4": hash_4,
         }, doc! {
            "$inc": {
                "count": 1
            }
        }, Some(options)).map_err(|err| FileCenterError::MongoDBError(err))?;

        match result {
            Some(result) => {
                Ok(self.create_file_item(result)?)
            }
            None => {
                let file_size = buffer.len() as u64;

                let mut file_item_raw: Document = doc! {
                    "hash_1": hash_1,
                    "hash_2": hash_2,
                    "hash_3": hash_3,
                    "hash_4": hash_4,
                    "file_name": file_name,
                    "file_size": file_size,
                    "count": 1
                };

                if file_size >= self.file_size_threshold as u64 {
                    let store = Store::with_db(self.mongo_client_db.clone());

                    let mut store_file: gridfs::file::File = store.create(file_name.to_string()).map_err(|err| FileCenterError::MongoDBError(err))?;

                    store_file.write(&buffer).map_err(|err| FileCenterError::IOError(err))?;

                    let id = store_file.doc.id.clone();

                    file_item_raw.insert("file_id", id);
                } else {
                    file_item_raw.insert("file_data", Bson::Binary(BinarySubtype::Generic, buffer));
                }

                let mime_type = match mime_type {
                    Some(mime_type) => mime_type,
                    None => {
                        DEFAULT_MIME_TYPE
                    }
                };

                file_item_raw.insert("mime_type", mime_type);

                file_item_raw.insert("create_time", Utc::now());

                let result = collection_files.insert_one(file_item_raw.clone(), None).map_err(|err| FileCenterError::MongoDBError(err))?;

                file_item_raw.insert("_id", result.inserted_id.unwrap());

                Ok(self.create_file_item(file_item_raw)?)
            }
        }
    }
}

fn get_hash_by_path<P: AsRef<Path>>(file_path: P) -> Result<(i64, i64, i64, i64), io::Error> {
    let file_path = file_path.as_ref();

    let mut file = File::open(file_path)?;

    let mut sha3_256 = Sha3_256::new();

    let mut buffer = [0u8; FILE_BUFFER_SIZE];

    loop {
        let c = file.read(&mut buffer)?;

        if c == 0 {
            break;
        }

        sha3_256.input(&buffer[..c]);
    }

    let result = sha3_256.result();

    let mut hash_1 = [0u8; 8];
    let mut hash_2 = [0u8; 8];
    let mut hash_3 = [0u8; 8];
    let mut hash_4 = [0u8; 8];

    hash_1.copy_from_slice(&result[0..8]);
    hash_2.copy_from_slice(&result[8..16]);
    hash_3.copy_from_slice(&result[16..24]);
    hash_4.copy_from_slice(&result[24..32]);

    Ok((unsafe { transmute(hash_1) }, unsafe { transmute(hash_2) }, unsafe { transmute(hash_3) }, unsafe { transmute(hash_4) }))
}

fn get_hash_by_buffer<P: AsRef<[u8]>>(buffer: P) -> Result<(i64, i64, i64, i64), io::Error> {
    let buffer = buffer.as_ref();

    let mut sha3_256 = Sha3_256::new();

    sha3_256.input(buffer);

    let result = sha3_256.result();

    let mut hash_1 = [0u8; 8];
    let mut hash_2 = [0u8; 8];
    let mut hash_3 = [0u8; 8];
    let mut hash_4 = [0u8; 8];

    hash_1.copy_from_slice(&result[0..8]);
    hash_2.copy_from_slice(&result[8..16]);
    hash_3.copy_from_slice(&result[16..24]);
    hash_4.copy_from_slice(&result[24..32]);

    Ok((unsafe { transmute(hash_1) }, unsafe { transmute(hash_2) }, unsafe { transmute(hash_3) }, unsafe { transmute(hash_4) }))
}