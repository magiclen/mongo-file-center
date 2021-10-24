use std::io::{self, Cursor};

use crate::tokio_stream::{Stream, StreamExt};

/// To represent the file data retrieved from MongoDB.
#[derive(Educe)]
#[educe(Debug)]
pub enum FileData {
    Buffer(Vec<u8>),
    Stream(
        #[educe(Debug(ignore))]
        Box<dyn Stream<Item = Result<Cursor<Vec<u8>>, io::Error>> + Unpin + Send>,
    ),
}

impl FileData {
    /// Turn into a `Vec<u8>` instance.
    #[inline]
    pub async fn into_vec(self) -> Result<Vec<u8>, io::Error> {
        match self {
            FileData::Buffer(v) => Ok(v),
            FileData::Stream(mut f) => {
                let mut buffer = Vec::new();

                while let Some(chunk) = f.next().await {
                    let chunk = chunk?.into_inner();

                    buffer.extend_from_slice(&chunk);
                }

                Ok(buffer)
            }
        }
    }
}
