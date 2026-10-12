#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    aws_sdk_s3_transfer_manager::__fuzz::buffer_pool_placement(data);
});
