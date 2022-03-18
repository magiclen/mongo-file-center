File Center on MongoDB
====================

[![CI](https://github.com/magiclen/mongo-file-center/actions/workflows/ci.yml/badge.svg)](https://github.com/magiclen/mongo-file-center/actions/workflows/ci.yml)

This crate aims to build an easy-to-use and no-redundant file storage based on MongoDB.

For perennial files, each of them is unique in the database, and can be retrieved many times without limitation.

For temporary files, they are allowed to be duplicated, but each instance can be retrieved only one time in a minute after it is created.

The file data can be stored in a document or be separated into chunks to store in multiple documents. It depends on the size of data and the `file_size_threshold`. If the size is smaller than or equal to the threshold, it stores in a single document. The max threshold is **16770KB**. The default threshold is **256KiB**.

## Example

```rust,ignore
use mongo_file_center::{FileCenter, FileData, mime};

const mongodb_uri: &str = "mongodb://localhost:27017/test_my_file_storage";

let file_center = FileCenter::new(mongodb_uri).await.unwrap();

let file_id = file_center.put_file_by_path("/path/to/file", Some("file_name"), Some(mime::IMAGE_JPEG)).await.unwrap();

let id_token = file_center.encrypt_id(file_id); // this token is safe in public

let file_id = file_center.decrypt_id_token(id_token).unwrap();

let r_file = file_center.get_file_item_by_id(file_id).await.unwrap().unwrap();

match r_file.into_file_data() {
    FileData::Buffer(data) => {
        // do something
    }
    FileData::Stream(stream) => {
        // do something
    }
}
```

## Migration Limitation

The old file center should not be migrated to the file center 0.6+ because the structure and the hash algorithm have been changed extremely. You will need a lot of effort to do that by yourself.

## Crates.io

https://crates.io/crates/mongo-file-center

## Documentation

https://docs.rs/mongo-file-center

## License

[MIT](LICENSE)