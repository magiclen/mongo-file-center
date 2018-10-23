extern crate mongo_file_center;
extern crate bson;

use std::fs::{self, File};
use std::io::Read;

use bson::oid::ObjectId;

use mongo_file_center::{FileCenter, FileData};

const HOST: &str = "localhost";
const PORT: u16 = 27017;

const FILE_PATH: &str = "tests/data/image.jpg";

const SIZE_THRESHOLD: i32 = 10 * 1024 * 1024;

#[test]
fn test_initialize() {
    let database = "test_initialize";
    {
        FileCenter::new(HOST, PORT, database).unwrap();
    }
    {
        FileCenter::new(HOST, PORT, database).unwrap().drop_database().unwrap();
    }
}

#[test]
fn test_crypt() {
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
fn test_input_output_collection() {
    let database = "test_input_output_collection";

    let mut file_center = FileCenter::new(HOST, PORT, database).unwrap();

    file_center.set_file_size_threshold(SIZE_THRESHOLD).unwrap();

    let file = file_center.put_file_by_path(FILE_PATH, None, None).unwrap();

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
fn test_input_output_gridfs() {
    let database = "test_input_output_gridfs";

    let file_center = FileCenter::new(HOST, PORT, database).unwrap();

    let file = file_center.put_file_by_path(FILE_PATH, None, Some("image/jpeg")).unwrap();

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
fn test_delete_collection() {
    let database = "test_delete_collection";

    let file_center = FileCenter::new_with_file_size_threshold(HOST, PORT, database, SIZE_THRESHOLD).unwrap();

    let file = file_center.put_file_by_path(FILE_PATH, None, None).unwrap();

    let file_id = file.get_object_id();

    assert_eq!(Some(file.get_file_size()), file_center.delete_file_item_by_id(file_id).unwrap());
    assert_eq!(None, file_center.delete_file_item_by_id(file_id).unwrap());

    file_center.put_file_by_path(FILE_PATH, None, None).unwrap();

    let file = file_center.put_file_by_path(FILE_PATH, None, None).unwrap();

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
fn test_delete_gridfs() {
    let database = "test_delete_gridfs";

    let file_center = FileCenter::new(HOST, PORT, database).unwrap();

    let file = file_center.put_file_by_path(FILE_PATH, None, Some("image/jpeg")).unwrap();

    let file_id = file.get_object_id();

    assert_eq!(Some(file.get_file_size()), file_center.delete_file_item_by_id(file_id).unwrap());
    assert_eq!(None, file_center.delete_file_item_by_id(file_id).unwrap());

    file_center.put_file_by_path(FILE_PATH, None, None).unwrap();

    let file = file_center.put_file_by_path(FILE_PATH, None, Some("image/jpeg")).unwrap();

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
fn test_input_output_collection_temporarily() {
    let database = "test_input_output_collection_temporarily";

    let mut file_center = FileCenter::new(HOST, PORT, database).unwrap();

    file_center.set_file_size_threshold(SIZE_THRESHOLD).unwrap();

    let file = file_center.put_file_by_path_temporarily(FILE_PATH, None, None).unwrap();
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
fn test_input_output_gridfs_temporarily() {
    let database = "test_input_output_gridfs_temporarily";

    let file_center = FileCenter::new(HOST, PORT, database).unwrap();

    let file = file_center.put_file_by_path_temporarily(FILE_PATH, None, None).unwrap();
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
fn test_clear_garbage() {
    let database = "test_input_output_gridfs";

    let file_center = FileCenter::new(HOST, PORT, database).unwrap();

    // TODO: more cases

    file_center.clear_garbage().unwrap();

    file_center.drop_database().unwrap();
}