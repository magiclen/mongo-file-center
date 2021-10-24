extern crate tokio;
extern crate tokio_util;

extern crate mongo_file_center;

extern crate same_content;

mod common;

use tokio::fs::{self, File};

use mongo_file_center::FileCenter;

use common::*;

#[tokio::test]
async fn temporary() {
    let uri = get_mongodb_uri("test_temporary");

    let file_center = FileCenter::new(uri).await.unwrap();

    let image_small = fs::read(IMAGE_SMALL_PATH).await.unwrap();
    let image_big = fs::read(IMAGE_BIG_PATH).await.unwrap();

    {
        let file_id = file_center
            .put_file_by_path_temporarily(IMAGE_SMALL_PATH, None::<&str>, None)
            .await
            .unwrap();

        assert!(file_center.get_file_item_by_id(file_id).await.unwrap().is_some());
        assert!(file_center.get_file_item_by_id(file_id).await.unwrap().is_none());

        let file_id_1 = file_center
            .put_file_by_path_temporarily(IMAGE_SMALL_PATH, None::<&str>, None)
            .await
            .unwrap();

        let file_id_2 = file_center
            .put_file_by_path_temporarily(IMAGE_SMALL_PATH, None::<&str>, None)
            .await
            .unwrap();

        assert_ne!(file_id_1, file_id_2);
    }

    {
        let file_id = file_center
            .put_file_by_path_temporarily(IMAGE_BIG_PATH, None::<&str>, None)
            .await
            .unwrap();

        assert!(file_center.get_file_item_by_id(file_id).await.unwrap().is_some());
        assert!(file_center.get_file_item_by_id(file_id).await.unwrap().is_none());

        let file_id_1 = file_center
            .put_file_by_path_temporarily(IMAGE_BIG_PATH, None::<&str>, None)
            .await
            .unwrap();

        let file_id_2 = file_center
            .put_file_by_path_temporarily(IMAGE_BIG_PATH, None::<&str>, None)
            .await
            .unwrap();

        assert_ne!(file_id_1, file_id_2);
    }

    {
        let file_id = file_center
            .put_file_by_buffer_temporarily(image_small.clone(), "", None)
            .await
            .unwrap();

        assert!(file_center.get_file_item_by_id(file_id).await.unwrap().is_some());
        assert!(file_center.get_file_item_by_id(file_id).await.unwrap().is_none());

        let file_id_1 = file_center
            .put_file_by_buffer_temporarily(image_small.clone(), "", None)
            .await
            .unwrap();

        let file_id_2 = file_center
            .put_file_by_buffer_temporarily(image_small.clone(), "", None)
            .await
            .unwrap();

        assert_ne!(file_id_1, file_id_2);
    }

    {
        let file_id =
            file_center.put_file_by_buffer_temporarily(image_big.clone(), "", None).await.unwrap();

        assert!(file_center.get_file_item_by_id(file_id).await.unwrap().is_some());
        assert!(file_center.get_file_item_by_id(file_id).await.unwrap().is_none());

        let file_id_1 =
            file_center.put_file_by_buffer_temporarily(image_big.clone(), "", None).await.unwrap();

        let file_id_2 =
            file_center.put_file_by_buffer_temporarily(image_big.clone(), "", None).await.unwrap();

        assert_ne!(file_id_1, file_id_2);
    }

    {
        let file_id = file_center
            .put_file_by_reader_temporarily(File::open(IMAGE_SMALL_PATH).await.unwrap(), "", None)
            .await
            .unwrap();

        assert!(file_center.get_file_item_by_id(file_id).await.unwrap().is_some());
        assert!(file_center.get_file_item_by_id(file_id).await.unwrap().is_none());

        let file_id_1 = file_center
            .put_file_by_reader_temporarily(File::open(IMAGE_SMALL_PATH).await.unwrap(), "", None)
            .await
            .unwrap();

        let file_id_2 = file_center
            .put_file_by_reader_temporarily(File::open(IMAGE_SMALL_PATH).await.unwrap(), "", None)
            .await
            .unwrap();

        assert_ne!(file_id_1, file_id_2);
    }

    {
        let file_id = file_center
            .put_file_by_reader_temporarily(File::open(IMAGE_BIG_PATH).await.unwrap(), "", None)
            .await
            .unwrap();

        assert!(file_center.get_file_item_by_id(file_id).await.unwrap().is_some());
        assert!(file_center.get_file_item_by_id(file_id).await.unwrap().is_none());

        let file_id_1 = file_center
            .put_file_by_reader_temporarily(File::open(IMAGE_BIG_PATH).await.unwrap(), "", None)
            .await
            .unwrap();

        let file_id_2 = file_center
            .put_file_by_reader_temporarily(File::open(IMAGE_BIG_PATH).await.unwrap(), "", None)
            .await
            .unwrap();

        assert_ne!(file_id_1, file_id_2);
    }

    file_center.drop_database().await.unwrap();
}
