mod common;

use common::*;
use mongo_file_center::{
    bson::{doc, Document},
    FileCenter, COLLECTION_FILES_CHUNKS_NAME, COLLECTION_FILES_NAME,
};

#[tokio::test]
async fn garbage() {
    let uri = get_mongodb_uri("test_garbage");

    let file_center = FileCenter::new(&uri).await.unwrap();

    let db = unsafe { file_center.database() };

    let collection_files = db.collection::<Document>(COLLECTION_FILES_NAME);
    let collection_files_chunks = db.collection::<Document>(COLLECTION_FILES_CHUNKS_NAME);

    let file_id_1 =
        file_center.put_file_by_path(IMAGE_SMALL_PATH, None::<&str>, None).await.unwrap();

    {
        let file_id_2 = file_center
            .put_file_by_path_temporarily(IMAGE_BIG_PATH, None::<&str>, None)
            .await
            .unwrap();

        let file_id_3 =
            file_center.put_file_by_path(IMAGE_BIG_PATH, None::<&str>, None).await.unwrap();

        collection_files_chunks
            .delete_many(
                doc! {
                    "file_id": file_id_3
                },
                None,
            )
            .await
            .unwrap();

        assert!(file_center.get_file_item_by_id(file_id_3).await.unwrap().is_some());

        file_center.clear_garbage().await.unwrap();

        assert!(file_center.get_file_item_by_id(file_id_1).await.unwrap().is_some());
        assert!(file_center.get_file_item_by_id(file_id_2).await.unwrap().is_some());
        assert!(file_center.get_file_item_by_id(file_id_3).await.unwrap().is_none());
        assert!(file_center.get_file_item_by_id(file_id_2).await.unwrap().is_none());
    }

    {
        let file_id_2 = file_center
            .put_file_by_path_temporarily(IMAGE_BIG_PATH, None::<&str>, None)
            .await
            .unwrap();

        let file_id_3 =
            file_center.put_file_by_path(IMAGE_BIG_PATH, None::<&str>, None).await.unwrap();

        assert!(file_center.get_file_item_by_id(file_id_3).await.unwrap().is_some());

        assert!(collection_files_chunks
            .find_one(
                doc! {
                    "file_id": file_id_3
                },
                None
            )
            .await
            .unwrap()
            .is_some());

        collection_files
            .delete_one(
                doc! {
                    "_id": file_id_3
                },
                None,
            )
            .await
            .unwrap();

        file_center.clear_garbage().await.unwrap();

        assert!(collection_files_chunks
            .find_one(
                doc! {
                    "file_id": file_id_3
                },
                None
            )
            .await
            .unwrap()
            .is_none());

        assert!(file_center.get_file_item_by_id(file_id_1).await.unwrap().is_some());
        assert!(file_center.get_file_item_by_id(file_id_2).await.unwrap().is_some());
        assert!(file_center.get_file_item_by_id(file_id_3).await.unwrap().is_none());
        assert!(file_center.get_file_item_by_id(file_id_2).await.unwrap().is_none());
    }

    file_center.drop_database().await.unwrap();
}
