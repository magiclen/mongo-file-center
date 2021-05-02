extern crate rand;
extern crate sha3;

use std::fs::File;
use std::io::{self, Read};
use std::mem::transmute;
use std::path::Path;

use rand::Rng;
use sha3::{Digest, Sha3_256};

use crate::BUFFER_SIZE;

pub(crate) fn get_hash_by_path<P: AsRef<Path>>(
    file_path: P,
) -> Result<(i64, i64, i64, i64), io::Error> {
    let file_path = file_path.as_ref();

    let mut file = File::open(file_path)?;

    let mut sha3_256 = Sha3_256::new();

    let mut buffer = [0u8; BUFFER_SIZE];

    loop {
        let c = file.read(&mut buffer)?;

        if c == 0 {
            break;
        }

        sha3_256.update(&buffer[..c]);
    }

    let result = sha3_256.finalize();

    Ok(separate_hash(&result))
}

pub(crate) fn get_hash_by_buffer<P: AsRef<[u8]>>(
    buffer: P,
) -> Result<(i64, i64, i64, i64), io::Error> {
    let buffer = buffer.as_ref();

    let mut sha3_256 = Sha3_256::new();

    sha3_256.update(buffer);

    let result = sha3_256.finalize();

    Ok(separate_hash(&result))
}

#[inline]
pub(crate) fn get_hash_by_random() -> (i64, i64, i64, i64) {
    let mut rng = rand::thread_rng();

    (rng.gen(), rng.gen(), rng.gen(), rng.gen())
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
