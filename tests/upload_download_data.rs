mod common;

use tokio::fs::{self, File};
use tokio_util::io::StreamReader;

use mongo_file_center::{FileCenter, FileData};

use common::*;

#[tokio::test]
async fn upload_download_data() {
    let uri = get_mongodb_uri("test_upload_download");

    let file_center = FileCenter::new(uri).await.unwrap();

    let image_small = fs::read(IMAGE_SMALL_PATH).await.unwrap();
    let image_big = fs::read(IMAGE_BIG_PATH).await.unwrap();

    {
        let file_id = file_center
            .put_file_by_path_temporarily(IMAGE_SMALL_PATH, None::<&str>, None)
            .await
            .unwrap();

        let file_item = file_center.get_file_item_by_id(file_id).await.unwrap().unwrap();

        assert_eq!(IMAGE_SMALL_SIZE, file_item.get_file_size());

        match file_item.into_file_data() {
            FileData::Buffer(b) => {
                assert_eq!(image_small, b);
            }
            FileData::Stream(_) => panic!("should be a buffer"),
        }
    }

    {
        let file_id = file_center
            .put_file_by_path_temporarily(IMAGE_BIG_PATH, None::<&str>, None)
            .await
            .unwrap();

        let file_item = file_center.get_file_item_by_id(file_id).await.unwrap().unwrap();

        assert_eq!(IMAGE_BIG_SIZE, file_item.get_file_size());

        match file_item.into_file_data() {
            FileData::Buffer(_) => panic!("should be a stream"),
            FileData::Stream(s) => {
                assert!(same_content::same_content_from_readers_async(
                    &mut StreamReader::new(s),
                    &mut File::open(IMAGE_BIG_PATH).await.unwrap()
                )
                .await
                .unwrap());
            }
        }
    }

    {
        let file_id = file_center
            .put_file_by_buffer_temporarily(image_small.clone(), "", None)
            .await
            .unwrap();

        let file_item = file_center.get_file_item_by_id(file_id).await.unwrap().unwrap();

        assert_eq!(IMAGE_SMALL_SIZE, file_item.get_file_size());

        match file_item.into_file_data() {
            FileData::Buffer(b) => {
                assert_eq!(image_small, b);
            }
            FileData::Stream(_) => panic!("should be a buffer"),
        }
    }

    {
        let file_id =
            file_center.put_file_by_buffer_temporarily(image_big.clone(), "", None).await.unwrap();

        let file_item = file_center.get_file_item_by_id(file_id).await.unwrap().unwrap();

        assert_eq!(IMAGE_BIG_SIZE, file_item.get_file_size());

        match file_item.into_file_data() {
            FileData::Buffer(_) => panic!("should be a stream"),
            FileData::Stream(s) => {
                assert!(same_content::same_content_from_readers_async(
                    &mut StreamReader::new(s),
                    &mut File::open(IMAGE_BIG_PATH).await.unwrap()
                )
                .await
                .unwrap());
            }
        }
    }

    {
        let file_id = file_center
            .put_file_by_reader_temporarily(File::open(IMAGE_SMALL_PATH).await.unwrap(), "", None)
            .await
            .unwrap();

        let file_item = file_center.get_file_item_by_id(file_id).await.unwrap().unwrap();

        assert_eq!(IMAGE_SMALL_SIZE, file_item.get_file_size());

        match file_item.into_file_data() {
            FileData::Buffer(b) => {
                assert_eq!(image_small, b);
            }
            FileData::Stream(_) => panic!("should be a buffer"),
        }
    }

    {
        let file_id = file_center
            .put_file_by_reader_temporarily(File::open(IMAGE_BIG_PATH).await.unwrap(), "", None)
            .await
            .unwrap();

        let file_item = file_center.get_file_item_by_id(file_id).await.unwrap().unwrap();

        assert_eq!(IMAGE_BIG_SIZE, file_item.get_file_size());

        match file_item.into_file_data() {
            FileData::Buffer(_) => panic!("should be a stream"),
            FileData::Stream(s) => {
                assert!(same_content::same_content_from_readers_async(
                    &mut StreamReader::new(s),
                    &mut File::open(IMAGE_BIG_PATH).await.unwrap()
                )
                .await
                .unwrap());
            }
        }
    }

    {
        let file_id =
            file_center.put_file_by_path(IMAGE_SMALL_PATH, None::<&str>, None).await.unwrap();

        let file_item = file_center.get_file_item_by_id(file_id).await.unwrap().unwrap();

        assert_eq!(IMAGE_SMALL_SIZE, file_item.get_file_size());

        match file_item.into_file_data() {
            FileData::Buffer(b) => {
                assert_eq!(image_small, b);
            }
            FileData::Stream(_) => panic!("should be a buffer"),
        }

        file_center.delete_file_item_by_id(file_id).await.unwrap();
    }

    {
        let file_id =
            file_center.put_file_by_path(IMAGE_BIG_PATH, None::<&str>, None).await.unwrap();

        let file_item = file_center.get_file_item_by_id(file_id).await.unwrap().unwrap();

        assert_eq!(IMAGE_BIG_SIZE, file_item.get_file_size());

        match file_item.into_file_data() {
            FileData::Buffer(_) => panic!("should be a stream"),
            FileData::Stream(s) => {
                assert!(same_content::same_content_from_readers_async(
                    &mut StreamReader::new(s),
                    &mut File::open(IMAGE_BIG_PATH).await.unwrap()
                )
                .await
                .unwrap());
            }
        }

        file_center.delete_file_item_by_id(file_id).await.unwrap();
    }

    {
        let file_id = file_center.put_file_by_buffer(image_small.clone(), "", None).await.unwrap();

        let file_item = file_center.get_file_item_by_id(file_id).await.unwrap().unwrap();

        assert_eq!(IMAGE_SMALL_SIZE, file_item.get_file_size());

        match file_item.into_file_data() {
            FileData::Buffer(b) => {
                assert_eq!(image_small, b);
            }
            FileData::Stream(_) => panic!("should be a buffer"),
        }

        file_center.delete_file_item_by_id(file_id).await.unwrap();
    }

    {
        let file_id = file_center.put_file_by_buffer(image_big.clone(), "", None).await.unwrap();

        let file_item = file_center.get_file_item_by_id(file_id).await.unwrap().unwrap();

        assert_eq!(IMAGE_BIG_SIZE, file_item.get_file_size());

        match file_item.into_file_data() {
            FileData::Buffer(_) => panic!("should be a stream"),
            FileData::Stream(s) => {
                assert!(same_content::same_content_from_readers_async(
                    &mut StreamReader::new(s),
                    &mut File::open(IMAGE_BIG_PATH).await.unwrap()
                )
                .await
                .unwrap());
            }
        }

        file_center.delete_file_item_by_id(file_id).await.unwrap();
    }

    {
        let file_id = file_center
            .put_file_by_reader(File::open(IMAGE_SMALL_PATH).await.unwrap(), "", None)
            .await
            .unwrap();

        let file_item = file_center.get_file_item_by_id(file_id).await.unwrap().unwrap();

        assert_eq!(IMAGE_SMALL_SIZE, file_item.get_file_size());

        match file_item.into_file_data() {
            FileData::Buffer(b) => {
                assert_eq!(image_small, b);
            }
            FileData::Stream(_) => panic!("should be a buffer"),
        }

        file_center.delete_file_item_by_id(file_id).await.unwrap();
    }

    {
        let file_id = file_center
            .put_file_by_reader(File::open(IMAGE_BIG_PATH).await.unwrap(), "", None)
            .await
            .unwrap();

        let file_item = file_center.get_file_item_by_id(file_id).await.unwrap().unwrap();

        assert_eq!(IMAGE_BIG_SIZE, file_item.get_file_size());

        match file_item.into_file_data() {
            FileData::Buffer(_) => panic!("should be a stream"),
            FileData::Stream(s) => {
                assert!(same_content::same_content_from_readers_async(
                    &mut StreamReader::new(s),
                    &mut File::open(IMAGE_BIG_PATH).await.unwrap()
                )
                .await
                .unwrap());
            }
        }

        file_center.delete_file_item_by_id(file_id).await.unwrap();
    }

    file_center.drop_database().await.unwrap();
}
