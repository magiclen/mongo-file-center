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

use mongo_file_center::{FileCenter, FileData, mime};

const mongodb_uri: &str = "mongodb://localhost:27017/test_my_file_storage";

let file_center = FileCenter::new(mongodb_uri).unwrap();

let file = file_center.put_file_by_path("/path/to/file", Some("file_name"), Some(mime::IMAGE_JPEG)).unwrap();

let file_id = file.get_object_id();

let id_token = file_center.encrypt_id(&file_id); // this token is safe in public

let file_id = file_center.decrypt_id_token(&id_token).unwrap();

let r_file = file_center.get_file_item_by_id(&file_id).unwrap().unwrap();

match r_file.into_file_data() {
    FileData::GridFS(file) => {
        // do something
    }
    FileData::Collection(data) => {
        // do something
    }
}
```
*/

#[macro_use]
pub extern crate bson;

pub extern crate mime;

pub extern crate mongodb_cwal;

mod functions;

mod file_center_error;

mod file_data;

mod file_item;

mod file_center;

pub use file_center::*;
pub use file_center_error::*;
pub use file_data::*;
pub use file_item::*;

pub use bson::bson;
pub use bson::doc;

/// A string of an encrypted file ID which can be used as a URL component.
pub type IDToken = String;

const BUFFER_SIZE: usize = 4096;
