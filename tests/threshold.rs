extern crate tokio;
extern crate tokio_util;

extern crate mongo_file_center;

extern crate same_content;

mod common;

use tokio::fs::{self, File};

use mongo_file_center::{FileCenter, FileData};

use common::*;

#[tokio::test]
async fn threshold() {
    let uri = get_mongodb_uri("test_threshold");

    let mut file_center = FileCenter::new(uri).await.unwrap();

    let image_small = fs::read(IMAGE_SMALL_PATH).await.unwrap();

    file_center.set_file_size_threshold(261_120).await.unwrap();
    assert_eq!(261_120, file_center.get_file_size_threshold());

    {
        let file_id = file_center
            .put_file_by_path_temporarily(IMAGE_SMALL_PATH, None::<&str>, None)
            .await
            .unwrap();

        let file_item = file_center.get_file_item_by_id(file_id).await.unwrap().unwrap();

        assert!(matches!(file_item.into_file_data(), FileData::Buffer(_)));
    }

    {
        let file_id = file_center
            .put_file_by_buffer_temporarily(image_small.clone(), "", None)
            .await
            .unwrap();

        let file_item = file_center.get_file_item_by_id(file_id).await.unwrap().unwrap();

        assert!(matches!(file_item.into_file_data(), FileData::Buffer(_)));
    }

    {
        let file_id = file_center
            .put_file_by_reader_temporarily(File::open(IMAGE_SMALL_PATH).await.unwrap(), "", None)
            .await
            .unwrap();

        let file_item = file_center.get_file_item_by_id(file_id).await.unwrap().unwrap();

        assert!(matches!(file_item.into_file_data(), FileData::Buffer(_)));
    }

    {
        let file_id =
            file_center.put_file_by_path(IMAGE_SMALL_PATH, None::<&str>, None).await.unwrap();

        let file_item = file_center.get_file_item_by_id(file_id).await.unwrap().unwrap();

        assert!(matches!(file_item.into_file_data(), FileData::Buffer(_)));
        file_center.delete_file_item_by_id(file_id).await.unwrap();
    }

    {
        let file_id = file_center.put_file_by_buffer(image_small.clone(), "", None).await.unwrap();

        let file_item = file_center.get_file_item_by_id(file_id).await.unwrap().unwrap();

        assert!(matches!(file_item.into_file_data(), FileData::Buffer(_)));
        file_center.delete_file_item_by_id(file_id).await.unwrap();
    }

    {
        let file_id = file_center
            .put_file_by_reader(File::open(IMAGE_SMALL_PATH).await.unwrap(), "", None)
            .await
            .unwrap();

        let file_item = file_center.get_file_item_by_id(file_id).await.unwrap().unwrap();

        assert!(matches!(file_item.into_file_data(), FileData::Buffer(_)));
        file_center.delete_file_item_by_id(file_id).await.unwrap();
    }

    let new_threshold = (IMAGE_SMALL_SIZE - 1) as u32;
    file_center.set_file_size_threshold(new_threshold).await.unwrap();
    assert_eq!(new_threshold, file_center.get_file_size_threshold());

    {
        let file_id = file_center
            .put_file_by_path_temporarily(IMAGE_SMALL_PATH, None::<&str>, None)
            .await
            .unwrap();

        let file_item = file_center.get_file_item_by_id(file_id).await.unwrap().unwrap();

        assert!(matches!(file_item.into_file_data(), FileData::Stream(_)));
    }

    {
        let file_id = file_center
            .put_file_by_buffer_temporarily(image_small.clone(), "", None)
            .await
            .unwrap();

        let file_item = file_center.get_file_item_by_id(file_id).await.unwrap().unwrap();

        assert!(matches!(file_item.into_file_data(), FileData::Stream(_)));
    }

    {
        let file_id = file_center
            .put_file_by_reader_temporarily(File::open(IMAGE_SMALL_PATH).await.unwrap(), "", None)
            .await
            .unwrap();

        let file_item = file_center.get_file_item_by_id(file_id).await.unwrap().unwrap();

        assert!(matches!(file_item.into_file_data(), FileData::Stream(_)));
    }

    {
        let file_id =
            file_center.put_file_by_path(IMAGE_SMALL_PATH, None::<&str>, None).await.unwrap();

        let file_item = file_center.get_file_item_by_id(file_id).await.unwrap().unwrap();

        assert!(matches!(file_item.into_file_data(), FileData::Stream(_)));
        file_center.delete_file_item_by_id(file_id).await.unwrap();
    }

    {
        let file_id = file_center.put_file_by_buffer(image_small.clone(), "", None).await.unwrap();

        let file_item = file_center.get_file_item_by_id(file_id).await.unwrap().unwrap();

        assert!(matches!(file_item.into_file_data(), FileData::Stream(_)));
        file_center.delete_file_item_by_id(file_id).await.unwrap();
    }

    {
        let file_id = file_center
            .put_file_by_reader(File::open(IMAGE_SMALL_PATH).await.unwrap(), "", None)
            .await
            .unwrap();

        let file_item = file_center.get_file_item_by_id(file_id).await.unwrap().unwrap();

        assert!(matches!(file_item.into_file_data(), FileData::Stream(_)));
        file_center.delete_file_item_by_id(file_id).await.unwrap();
    }

    file_center.drop_database().await.unwrap();
}