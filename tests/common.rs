#![allow(dead_code)]

extern crate manifest_dir_macros;
extern crate slash_formatter;

use std::env;

const HOST_URI: &str = "mongodb://localhost:27017";

pub const IMAGE_BIG_PATH: &str = manifest_dir_macros::file_path!("tests", "data", "image-big.jpg");
pub const IMAGE_BIG_SIZE: u64 = 1312391;

pub const IMAGE_SMALL_PATH: &str =
    manifest_dir_macros::file_path!("tests", "data", "image-small.png");
pub const IMAGE_SMALL_SIZE: u64 = 11658;

#[inline]
pub fn get_mongodb_uri(database_name: &str) -> String {
    let mut host_uri = env::var("MONGODB_HOST_URI").unwrap_or_else(|_| HOST_URI.to_string());

    slash_formatter::concat_with_slash_in_place(&mut host_uri, database_name);

    host_uri
}
