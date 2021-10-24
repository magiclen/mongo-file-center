use std::mem::transmute;
use std::path::Path;

use crate::tokio::fs::File;
use crate::tokio::io::{self, AsyncReadExt};

use crate::mime::Mime;

use crate::{Digest, Hasher, DEFAULT_MIME_TYPE};

const BUFFER_SIZE: usize = 4096;

pub(crate) fn get_mime_by_path<P: AsRef<Path>>(file_path: P) -> Mime {
    match file_path.as_ref().extension() {
        Some(extension) => {
            mime_guess::from_ext(extension.to_str().unwrap()).first_or_octet_stream()
        }
        None => DEFAULT_MIME_TYPE,
    }
}

pub(crate) async fn get_hash_by_path<P: AsRef<Path>>(
    file_path: P,
) -> Result<(i64, i64, i64, i64), io::Error> {
    let file_path = file_path.as_ref();

    let mut file = File::open(file_path).await?;

    let mut hasher = Hasher::new();

    let mut buffer = [0; BUFFER_SIZE];

    loop {
        let c = file.read(&mut buffer).await?;

        if c == 0 {
            break;
        }

        hasher.update(&buffer[..c]);
    }

    let result = hasher.finalize();

    Ok(separate_hash(&result))
}

pub(crate) fn get_hash_by_buffer<P: AsRef<[u8]>>(buffer: P) -> (i64, i64, i64, i64) {
    let buffer = buffer.as_ref();

    let mut hasher = Hasher::new();

    hasher.update(buffer);

    let result = hasher.finalize();

    separate_hash(&result)
}

pub(crate) fn separate_hash(hash: &[u8]) -> (i64, i64, i64, i64) {
    let mut hash_1 = [0u8; 8];
    let mut hash_2 = [0u8; 8];
    let mut hash_3 = [0u8; 8];
    let mut hash_4 = [0u8; 8];

    hash_1.copy_from_slice(&hash[0..8]);
    hash_2.copy_from_slice(&hash[8..16]);
    hash_3.copy_from_slice(&hash[16..24]);
    hash_4.copy_from_slice(&hash[24..32]);

    (
        unsafe { transmute(hash_1) },
        unsafe { transmute(hash_2) },
        unsafe { transmute(hash_3) },
        unsafe { transmute(hash_4) },
    )
}
