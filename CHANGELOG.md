CHANGELOG
=========

v0.1.3
======

* Validate ranged GET requests content range and request count
* Validate field mappings between Transfer Manager and S3 input/output types
* Validate UploadPart requests content-length and part number alignment

vNext
======

* Use the default HTTPS client from SDK crate which is now based on `aws-lc`.

v0.1.1
======

* Fixed publishing on crate.io
    - Add README for the crate
    - Fix the repository URL
    - Add description, categories, keywords

v0.1.0
======

* Initial release - Developer Preview of a high performance Amazon S3 client for Rust.
