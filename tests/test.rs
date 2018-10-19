extern crate mongo_file_center;
extern crate bson;

use std::fs;
use std::io::Read;

use bson::oid::ObjectId;

use mongo_file_center::{FileCenter, FileData};

const HOST: &str = "localhost";
const PORT: u16 = 27017;

const FILE_PATH: &str = "tests/data/image.jpg";

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

    let file_center = FileCenter::new(HOST, PORT, database).unwrap();

    let file = file_center.put_file_by_path(FILE_PATH, None, None).unwrap();

    {
        let file_2 = file_center.put_file_by_path(FILE_PATH, None, None).unwrap();

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

    let mut file_center = FileCenter::new(HOST, PORT, database).unwrap();

    file_center.set_file_size_threshold(50).unwrap();

    let file = file_center.put_file_by_path(FILE_PATH, None, None).unwrap();

    {
        let file_2 = file_center.put_file_by_path(FILE_PATH, None, None).unwrap();

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
            panic!("Not from a collection!");
        }
    }

    file_center.drop_database().unwrap();
}