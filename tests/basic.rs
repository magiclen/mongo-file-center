mod common;

use mongo_file_center::bson::oid::ObjectId;
use mongo_file_center::{FileCenter, FileData, MAX_FILE_SIZE_THRESHOLD};

use common::*;

#[tokio::test]
async fn initialize() {
    let uri = get_mongodb_uri("test_initialize");

    {
        FileCenter::new(&uri).await.unwrap();
    }

    {
        FileCenter::new(uri).await.unwrap().drop_database().await.unwrap();
    }
}

#[tokio::test]
async fn crypt() {
    let uri = get_mongodb_uri("test_crypt");

    let file_center = FileCenter::new(uri).await.unwrap();

    let oid = ObjectId::new();

    let id_token = file_center.encrypt_id(oid);

    let id_token_2 = String::new();

    let id_token_2 = file_center.encrypt_id_to_buffer(oid, id_token_2);

    assert_eq!(id_token, id_token_2);

    let r_oid = file_center.decrypt_id_token(&id_token).unwrap();

    assert_eq!(oid, r_oid);

    file_center.drop_database().await.unwrap();
}

#[tokio::test]
async fn max_file_size_threshold() {
    let uri = get_mongodb_uri("max_file_size_threshold");

    let mut file_center = FileCenter::new(&uri).await.unwrap();

    file_center.set_file_size_threshold(MAX_FILE_SIZE_THRESHOLD).await.unwrap();

    let file_id = file_center
        .put_file_by_buffer(
            vec![0; MAX_FILE_SIZE_THRESHOLD as usize],
            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789",
            None,
        )
        .await
        .unwrap();

    let file_item = file_center.get_file_item_by_id(file_id).await.unwrap().unwrap();

    assert!(matches!(file_item.into_file_data(), FileData::Buffer(_)));

    file_center.drop_database().await.unwrap();
}
