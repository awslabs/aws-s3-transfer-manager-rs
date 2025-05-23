/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! s3s integration layer.

use std::collections::HashMap;

use async_trait::async_trait;
use futures_util::stream;
use s3s::dto::StreamingBlob;
use s3s::dto::Timestamp;
use s3s::{S3Request, S3Response, S3Result};

use crate::error::Error;
use crate::storage::models::ObjectMetadata;
use crate::storage::StorageBackend;

/// Inner implementation of the s3s::S3 trait.
///
/// This struct implements the s3s::S3 trait, delegating to the storage backend
/// for data operations and managing metadata.
pub(crate) struct Inner<S: StorageBackend + 'static> {
    /// The storage backend.
    storage: S,

    /// Object metadata, keyed by object key.
    metadata: tokio::sync::RwLock<HashMap<String, ObjectMetadata>>,
}

impl<S: StorageBackend + 'static> Inner<S> {
    /// Create a new Inner with the given storage backend.
    pub(crate) fn new(storage: S) -> Self {
        Self {
            storage,
            metadata: tokio::sync::RwLock::new(HashMap::new()),
        }
    }
}

#[async_trait]
impl<S: StorageBackend + 'static> s3s::S3 for Inner<S> {
    async fn get_object(
        &self,
        req: S3Request<s3s::dto::GetObjectInput>,
    ) -> S3Result<S3Response<s3s::dto::GetObjectOutput>> {
        let input = req.input;
        let key = input.key;

        // Get metadata first to determine content length
        let metadata = {
            let metadata_map = self.metadata.read().await;
            match metadata_map.get(&key) {
                Some(metadata) => metadata.clone(),
                None => return Err(Error::NoSuchKey.into()),
            }
        };

        let range = input
            .range
            .as_ref()
            .map(|range_dto| {
                range_dto
                    .check(metadata.content_length)
                    .map_err(|_| Error::InvalidRange)
            })
            .transpose()?;

        let data = match self.storage.get_object_data(&key, range).await {
            Ok(data) => data,
            Err(err) => return Err(err.into()),
        };

        let mut output = s3s::dto::GetObjectOutput::default();

        // Create a streaming blob from the bytes
        let stream = stream::once(async move { Ok::<_, std::io::Error>(data) });
        output.body = Some(StreamingBlob::wrap(stream));

        output.content_length = Some(metadata.content_length as i64);
        output.e_tag = Some(metadata.etag);

        let timestamp = Timestamp::from(metadata.last_modified);
        output.last_modified = Some(timestamp);

        if let Some(content_type) = metadata.content_type {
            if let Ok(mime) = content_type.parse() {
                output.content_type = Some(mime);
            }
        }

        output.metadata = Some(metadata.user_metadata);
        // FIXME - add checksum support/storage

        Ok(S3Response::new(output))
    }
}
