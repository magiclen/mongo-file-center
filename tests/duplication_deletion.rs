mod common;

use tokio::fs::{self, File};

use mongo_file_center::{FileCenter, FileData, DEFAULT_FILE_SIZE_THRESHOLD};

use common::*;

#[tokio::test]
async fn duplication_deletion() {
    let uri = get_mongodb_uri("test_duplication_deletion");

    let file_center = FileCenter::new(uri).await.unwrap();

    let image_small = fs::read(IMAGE_SMALL_PATH).await.unwrap();
    let image_big = fs::read(IMAGE_BIG_PATH).await.unwrap();

    {
        let file_id_1 =
            file_center.put_file_by_path(IMAGE_SMALL_PATH, None::<&str>, None).await.unwrap();

        assert!(file_center.check_file_item_exist(file_id_1).await.unwrap());
        assert!(file_center.check_file_item_exist(file_id_1).await.unwrap());

        let file_id_2 =
            file_center.put_file_by_path(IMAGE_SMALL_PATH, None::<&str>, None).await.unwrap();

        assert_eq!(file_id_1, file_id_2);

        assert!(file_center.delete_file_item_by_id(file_id_1).await.unwrap().is_some());
        assert!(file_center.delete_file_item_by_id(file_id_1).await.unwrap().is_some());
        assert!(file_center.delete_file_item_by_id(file_id_1).await.unwrap().is_none());
    }

    {
        let file_id_1 =
            file_center.put_file_by_path(IMAGE_BIG_PATH, None::<&str>, None).await.unwrap();

        assert!(file_center.check_file_item_exist(file_id_1).await.unwrap());
        assert!(file_center.check_file_item_exist(file_id_1).await.unwrap());

        let file_id_2 =
            file_center.put_file_by_path(IMAGE_BIG_PATH, None::<&str>, None).await.unwrap();

        assert_eq!(file_id_1, file_id_2);

        assert!(file_center.delete_file_item_by_id(file_id_1).await.unwrap().is_some());
        assert!(file_center.delete_file_item_by_id(file_id_1).await.unwrap().is_some());
        assert!(file_center.delete_file_item_by_id(file_id_1).await.unwrap().is_none());
    }

    {
        let file_id_1 =
            file_center.put_file_by_buffer(image_small.clone(), "", None).await.unwrap();

        assert!(file_center.check_file_item_exist(file_id_1).await.unwrap());
        assert!(file_center.check_file_item_exist(file_id_1).await.unwrap());

        let file_id_2 =
            file_center.put_file_by_buffer(image_small.clone(), "", None).await.unwrap();

        assert_eq!(file_id_1, file_id_2);

        assert!(file_center.delete_file_item_by_id(file_id_1).await.unwrap().is_some());
        assert!(file_center.delete_file_item_by_id(file_id_1).await.unwrap().is_some());
        assert!(file_center.delete_file_item_by_id(file_id_1).await.unwrap().is_none());
    }

    {
        let file_id_1 = file_center.put_file_by_buffer(image_big.clone(), "", None).await.unwrap();

        assert!(file_center.check_file_item_exist(file_id_1).await.unwrap());
        assert!(file_center.check_file_item_exist(file_id_1).await.unwrap());

        let file_id_2 = file_center.put_file_by_buffer(image_big.clone(), "", None).await.unwrap();

        assert_eq!(file_id_1, file_id_2);

        assert!(file_center.delete_file_item_by_id(file_id_1).await.unwrap().is_some());
        assert!(file_center.delete_file_item_by_id(file_id_1).await.unwrap().is_some());
        assert!(file_center.delete_file_item_by_id(file_id_1).await.unwrap().is_none());
    }

    {
        let file_id_1 = file_center
            .put_file_by_reader(File::open(IMAGE_SMALL_PATH).await.unwrap(), "", None)
            .await
            .unwrap();

        assert!(file_center.check_file_item_exist(file_id_1).await.unwrap());
        assert!(file_center.check_file_item_exist(file_id_1).await.unwrap());

        let file_id_2 = file_center
            .put_file_by_reader(File::open(IMAGE_SMALL_PATH).await.unwrap(), "", None)
            .await
            .unwrap();

        assert_eq!(file_id_1, file_id_2);

        assert!(file_center.delete_file_item_by_id(file_id_1).await.unwrap().is_some());
        assert!(file_center.delete_file_item_by_id(file_id_1).await.unwrap().is_some());
        assert!(file_center.delete_file_item_by_id(file_id_1).await.unwrap().is_none());
    }

    {
        let file_id_1 = file_center
            .put_file_by_reader(File::open(IMAGE_BIG_PATH).await.unwrap(), "", None)
            .await
            .unwrap();

        assert!(file_center.check_file_item_exist(file_id_1).await.unwrap());
        assert!(file_center.check_file_item_exist(file_id_1).await.unwrap());

        let file_id_2 = file_center
            .put_file_by_reader(File::open(IMAGE_BIG_PATH).await.unwrap(), "", None)
            .await
            .unwrap();

        assert_eq!(file_id_1, file_id_2);

        assert!(file_center.delete_file_item_by_id(file_id_1).await.unwrap().is_some());
        assert!(file_center.delete_file_item_by_id(file_id_1).await.unwrap().is_some());
        assert!(file_center.delete_file_item_by_id(file_id_1).await.unwrap().is_none());
    }

    file_center.drop_database().await.unwrap();
}

#[tokio::test]
async fn duplication_different_threshold() {
    let uri = get_mongodb_uri("test_duplication_different_threshold");

    let mut file_center = FileCenter::new(uri).await.unwrap();

    let image_small = fs::read(IMAGE_SMALL_PATH).await.unwrap();

    {
        let file_id_1 =
            file_center.put_file_by_path(IMAGE_SMALL_PATH, None::<&str>, None).await.unwrap();

        file_center.set_file_size_threshold(1024).await.unwrap();

        let file_id_2 =
            file_center.put_file_by_path(IMAGE_SMALL_PATH, None::<&str>, None).await.unwrap();

        assert_eq!(file_id_1, file_id_2);

        let file_item = file_center.get_file_item_by_id(file_id_1).await.unwrap().unwrap();

        assert!(matches!(file_item.into_file_data(), FileData::Buffer(_)));
    }

    file_center.set_file_size_threshold(DEFAULT_FILE_SIZE_THRESHOLD).await.unwrap();

    {
        let file_id_1 =
            file_center.put_file_by_buffer(image_small.clone(), "", None).await.unwrap();

        file_center.set_file_size_threshold(1024).await.unwrap();

        let file_id_2 =
            file_center.put_file_by_buffer(image_small.clone(), "", None).await.unwrap();

        assert_eq!(file_id_1, file_id_2);

        let file_item = file_center.get_file_item_by_id(file_id_1).await.unwrap().unwrap();

        assert!(matches!(file_item.into_file_data(), FileData::Buffer(_)));
    }

    file_center.set_file_size_threshold(DEFAULT_FILE_SIZE_THRESHOLD).await.unwrap();

    {
        let file_id_1 = file_center
            .put_file_by_reader(File::open(IMAGE_SMALL_PATH).await.unwrap(), "", None)
            .await
            .unwrap();

        file_center.set_file_size_threshold(1024).await.unwrap();

        let file_id_2 = file_center
            .put_file_by_reader(File::open(IMAGE_SMALL_PATH).await.unwrap(), "", None)
            .await
            .unwrap();

        assert_eq!(file_id_1, file_id_2);

        let file_item = file_center.get_file_item_by_id(file_id_1).await.unwrap().unwrap();

        assert!(matches!(file_item.into_file_data(), FileData::Buffer(_)));
    }

    file_center.drop_database().await.unwrap();
}
