extern crate mongo_file_center;
#[macro_use]
extern crate bson;
extern crate mime;

use std::fs::{self, File};
use std::io::Read;

use bson::oid::ObjectId;

use mongo_file_center::{mongodb::{db::ThreadedDatabase, coll::Collection}, FileCenter, FileData};

const HOST: &str = "localhost";
const PORT: u16 = 27017;

#[cfg(windows)]
const FILE_PATH: &str = r"tests\data\image.jpg";

#[cfg(not(windows))]
const FILE_PATH: &str = "tests/data/image.jpg";

const SIZE_THRESHOLD: i32 = 10 * 1024 * 1024;

#[test]
fn initialize() {
    let database = "test_initialize";
    {
        FileCenter::new(HOST, PORT, database).unwrap();
    }
    {
        FileCenter::new(HOST, PORT, database).unwrap().drop_database().unwrap();
    }
}

#[test]
fn crypt() {
    let database = "test_crypt";

    let file_center = FileCenter::new(HOST, PORT, database).unwrap();

    let oid = ObjectId::new().unwrap();

    let id_token = file_center.encrypt_id(&oid);

    let id_token_2 = String::new();

    let id_token_2 = file_center.encrypt_id_to_buffer(&oid, id_token_2);

    assert_eq!(id_token, id_token_2);

    let r_oid = file_center.decrypt_id_token(&id_token).unwrap();

    assert_eq!(oid, r_oid);

    file_center.drop_database().unwrap();
}

#[test]
fn input_output_collection() {
    let database = "test_input_output_collection";

    let mut file_center = FileCenter::new(HOST, PORT, database).unwrap();

    file_center.set_file_size_threshold(SIZE_THRESHOLD).unwrap();

    let file = file_center.put_file_by_path(FILE_PATH, None::<String>, None).unwrap();

    {
        let file_2 = file_center.put_file_by_reader(File::open(FILE_PATH).unwrap(), "", None).unwrap();

        assert_eq!(file.get_object_id(), file_2.get_object_id());

        let file_3 = file_center.put_file_by_buffer(file_2.into_file_data().into_vec_unchecked(), "", None).unwrap();

        assert_eq!(file.get_object_id(), file_3.get_object_id());
    }

    let r_file = file_center.get_file_item_by_id(file.get_object_id()).unwrap().unwrap();

    assert_eq!(file.get_object_id(), r_file.get_object_id());

    match r_file.into_file_data() {
        FileData::Collection(data) => {
            let o_data = fs::read(FILE_PATH).unwrap();

            assert_eq!(data, o_data);
        }
        _ => {
            panic!("Not from a collection!");
        }
    }

    file_center.drop_database().unwrap();
}

#[test]
fn input_output_gridfs() {
    let database = "test_input_output_gridfs";

    let file_center = FileCenter::new(HOST, PORT, database).unwrap();

    let file = file_center.put_file_by_path(FILE_PATH, None::<String>, Some(mime::IMAGE_JPEG)).unwrap();

    {
        let file_2 = file_center.put_file_by_reader(File::open(FILE_PATH).unwrap(), "", None).unwrap();

        assert_eq!(file.get_object_id(), file_2.get_object_id());

        let file_3 = file_center.put_file_by_buffer(file_2.into_file_data().into_vec_unchecked(), "", None).unwrap();

        assert_eq!(file.get_object_id(), file_3.get_object_id());
    }

    let r_file = file_center.get_file_item_by_id(file.get_object_id()).unwrap().unwrap();

    assert_eq!(file.get_object_id(), r_file.get_object_id());

    match r_file.into_file_data() {
        FileData::GridFS(mut file) => {
            let o_data = fs::read(FILE_PATH).unwrap();

            let mut data = Vec::new();

            file.read_to_end(&mut data).unwrap();

            assert_eq!(data, o_data);
        }
        _ => {
            panic!("Not from GridFS!");
        }
    }

    file_center.drop_database().unwrap();
}

#[test]
fn delete_collection() {
    let database = "test_delete_collection";

    let file_center = FileCenter::new_with_file_size_threshold(HOST, PORT, database, SIZE_THRESHOLD).unwrap();

    let file = file_center.put_file_by_path(FILE_PATH, None::<String>, None).unwrap();

    let file_id = file.get_object_id();

    assert_eq!(Some(file.get_file_size()), file_center.delete_file_item_by_id(file_id).unwrap());
    assert_eq!(None, file_center.delete_file_item_by_id(file_id).unwrap());

    file_center.put_file_by_path(FILE_PATH, None::<String>, None).unwrap();

    let file = file_center.put_file_by_path(FILE_PATH, None::<String>, None).unwrap();

    let file_id = file.get_object_id().clone();
    let file_size = file.get_file_size();

    assert_eq!(Some(file_size), file_center.delete_file_item_by_id(&file_id).unwrap());

    match file.into_file_data() {
        FileData::Collection(data) => {
            let o_data = fs::read(FILE_PATH).unwrap();

            assert_eq!(data, o_data);
        }
        _ => {
            panic!("Not from a collection!");
        }
    }

    assert_eq!(Some(file_size), file_center.delete_file_item_by_id(&file_id).unwrap());

    assert_eq!(None, file_center.delete_file_item_by_id(&file_id).unwrap());

    file_center.drop_database().unwrap();
}

#[test]
fn delete_gridfs() {
    let database = "test_delete_gridfs";

    let file_center = FileCenter::new(HOST, PORT, database).unwrap();

    let file = file_center.put_file_by_path(FILE_PATH, None::<String>, Some(mime::IMAGE_JPEG)).unwrap();

    let file_id = file.get_object_id();

    assert_eq!(Some(file.get_file_size()), file_center.delete_file_item_by_id(file_id).unwrap());
    assert_eq!(None, file_center.delete_file_item_by_id(file_id).unwrap());

    file_center.put_file_by_path(FILE_PATH, None::<String>, None).unwrap();

    let file = file_center.put_file_by_path(FILE_PATH, None::<String>, Some(mime::IMAGE_JPEG)).unwrap();

    let file_id = file.get_object_id().clone();
    let file_size = file.get_file_size();

    assert_eq!(Some(file_size), file_center.delete_file_item_by_id(&file_id).unwrap());

    match file.into_file_data() {
        FileData::GridFS(mut file) => {
            let o_data = fs::read(FILE_PATH).unwrap();

            let mut data = Vec::new();

            file.read_to_end(&mut data).unwrap();

            assert_eq!(data, o_data);
        }
        _ => {
            panic!("Not from GridFS!");
        }
    }

    assert_eq!(Some(file_size), file_center.delete_file_item_by_id(&file_id).unwrap());

    assert_eq!(None, file_center.delete_file_item_by_id(&file_id).unwrap());

    file_center.drop_database().unwrap();
}

#[test]
fn input_output_collection_temporarily() {
    let database = "test_input_output_collection_temporarily";

    let mut file_center = FileCenter::new(HOST, PORT, database).unwrap();

    file_center.set_file_size_threshold(SIZE_THRESHOLD).unwrap();

    let file = file_center.put_file_by_path_temporarily(FILE_PATH, None::<String>, None).unwrap();
    let file_2 = file_center.put_file_by_buffer_temporarily(fs::read(FILE_PATH).unwrap(), "", None).unwrap();
    let file_3 = file_center.put_file_by_reader_temporarily(File::open(FILE_PATH).unwrap(), "", None).unwrap();

    assert_ne!(file.get_object_id(), file_2.get_object_id());

    assert_ne!(file.get_object_id(), file_3.get_object_id());

    let r_file = file_center.get_file_item_by_id(file.get_object_id()).unwrap().unwrap();

    assert_eq!(file.get_object_id(), r_file.get_object_id());

    assert!(file_center.get_file_item_by_id(file.get_object_id()).unwrap().is_none());

    match r_file.into_file_data() {
        FileData::Collection(data) => {
            let o_data = fs::read(FILE_PATH).unwrap();

            assert_eq!(data, o_data);
        }
        _ => {
            panic!("Not from a collection!");
        }
    }

    file_center.drop_database().unwrap();
}

#[test]
fn input_output_gridfs_temporarily() {
    let database = "test_input_output_gridfs_temporarily";

    let file_center = FileCenter::new(HOST, PORT, database).unwrap();

    let file = file_center.put_file_by_path_temporarily(FILE_PATH, None::<String>, None).unwrap();
    let file_2 = file_center.put_file_by_buffer_temporarily(fs::read(FILE_PATH).unwrap(), "", None).unwrap();
    let file_3 = file_center.put_file_by_reader_temporarily(File::open(FILE_PATH).unwrap(), "", None).unwrap();

    assert_ne!(file.get_object_id(), file_2.get_object_id());

    assert_ne!(file.get_object_id(), file_3.get_object_id());

    let r_file = file_center.get_file_item_by_id(file.get_object_id()).unwrap().unwrap();

    assert_eq!(file.get_object_id(), r_file.get_object_id());

    assert!(file_center.get_file_item_by_id(file.get_object_id()).unwrap().is_none());

    match file.into_file_data() {
        FileData::GridFS(mut file) => {
            let o_data = fs::read(FILE_PATH).unwrap();

            let mut data = Vec::new();

            file.read_to_end(&mut data).unwrap();

            assert_eq!(data, o_data);
        }
        _ => {
            panic!("Not from GridFS!");
        }
    }

    file_center.drop_database().unwrap();
}

#[test]
fn clear_garbage() {
    let database = "test_clear_garbage";
    let file_center = FileCenter::new(HOST, PORT, database).unwrap();

    {
        let db = file_center.get_mongo_client_db();
        let fs_files: Collection = db.collection("fs.files");
        let collection_files: Collection = db.collection(mongo_file_center::COLLECTION_FILES_NAME);

        // unnecessary file items which have file_id but the target file does not exist
        {
            let file = file_center.put_file_by_path(FILE_PATH, None::<String>, None).unwrap();
            let object_id = file.get_object_id();
            file_center.clear_garbage().unwrap();
            collection_files.find_one(Some(doc! {
                "_id": object_id.clone()
            }), None).unwrap().unwrap();
            fs_files.delete_many(doc! {}, None).unwrap();
            file_center.clear_garbage().unwrap();
            assert!(collection_files.find_one(Some(doc! {
                "_id": object_id.clone()
            }), None).unwrap().is_none());
        }

        // unnecessary file items whose count are smaller than or equal to 0
        {
            let file = file_center.put_file_by_path(FILE_PATH, None::<String>, None).unwrap();
            let object_id = file.get_object_id();
            file_center.clear_garbage().unwrap();
            collection_files.find_one(Some(doc! {
                "_id": object_id.clone()
            }), None).unwrap().unwrap();
            collection_files.update_one(doc! {}, doc! {
                "$set": {
                    "count": 0
                }
            }, None).unwrap();
            file_center.clear_garbage().unwrap();
            assert!(collection_files.find_one(Some(doc! {
                "_id": object_id.clone()
            }), None).unwrap().is_none());
            assert_eq!(0, fs_files.count(Some(doc! {}), None).unwrap());
        }

        // unnecessary GridFS files which are not used in file items
        {
            let file = file_center.put_file_by_path(FILE_PATH, None::<String>, None).unwrap();
            let object_id = file.get_object_id();
            file_center.clear_garbage().unwrap();
            assert_eq!(1, fs_files.count(Some(doc! {}), None).unwrap());
            collection_files.delete_one(doc! {
                "_id": object_id.clone()
            }, None).unwrap();
            file_center.clear_garbage().unwrap();
            assert_eq!(0, fs_files.count(Some(doc! {}), None).unwrap());
        }
    }

    file_center.drop_database().unwrap();
}