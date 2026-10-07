/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! This module removes keys from a sync destination.
//!
//! A bucket removes up to 1,000 keys in one `DeleteObjects` request. A local tree removes one file
//! per key. Each destination reports one outcome per key, so the run knows which keys survived.

#[cfg(test)]
use std::sync::Arc;

use super::local_path_for_key;

// How many keys go in one delete request. `DeleteObjects` takes no more. Batching makes a large
// delete affordable: a thousand keys sent singly cost a thousand round trips and a thousand
// dispatch charges, where one batch costs one of each.
pub(super) const DELETE_BATCH: usize = 1000;

// How many times a batch asks again about keys S3 refused for load.
//
// A whole request failure and a per-key refusal need different retries. A whole request retries
// every key. A per-key retry sends only keys S3 still refuses. A batch can see both failures.
const DELETE_REFUSAL_ATTEMPTS: u32 = 3;

// Why one key was not removed: the category a caller acts on and the message a caller reads. The
// destination supplies the category because a bucket, a local tree, and an invalid key fail for
// different reasons.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Refusal {
    pub(super) kind: crate::error::ErrorKind,
    pub(super) why: String,
}

impl Refusal {
    pub(super) fn new(kind: crate::error::ErrorKind, why: impl Into<String>) -> Self {
        Self {
            kind,
            why: why.into(),
        }
    }

    #[cfg(test)]
    pub(super) fn why(&self) -> &str {
        &self.why
    }

    #[cfg(test)]
    pub(super) fn kind(&self) -> &crate::error::ErrorKind {
        &self.kind
    }
}

// What became of one key: removed, or refused with a reason naming it.
pub(super) type KeyOutcome = Result<String, Refusal>;

// The delete path asks before it sends a batch. A throttled key can wait before its next attempt.
// The destination asks again after that wait, so a stopped run starts no new attempt.
pub(crate) type StopCheck<'a> = &'a (dyn Fn() -> bool + Send + Sync);

// Where keys go when they leave the destination. A bucket removes up to 1,000 keys per request. A
// local tree removes one file per request.
pub(crate) enum Deleter {
    Bucket(DeleteFromBucket),
    LocalTree(DeleteFromLocalTree),
    #[cfg(test)]
    Recording(Arc<super::test_util::RecordDeletes>),
}

impl Deleter {
    // How many keys a destination collects before sending. A destination with no batch request
    // returns one.
    pub(crate) fn batch_size(&self) -> usize {
        match self {
            Deleter::Bucket(d) => d.batch_size(),
            Deleter::LocalTree(d) => d.batch_size(),
            #[cfg(test)]
            Deleter::Recording(d) => d.batch_size(),
        }
    }

    // Remove keys and report one outcome per key. A batch result alone cannot name the keys that
    // survived.
    pub(crate) async fn delete<K, I>(&self, keys: I, stopped: StopCheck<'_>) -> Vec<KeyOutcome>
    where
        K: Into<String>,
        I: IntoIterator<Item = K>,
    {
        let keys: Vec<String> = keys.into_iter().map(Into::into).collect();
        match self {
            Deleter::Bucket(d) => d.delete(keys, stopped).await,
            Deleter::LocalTree(d) => d.delete(keys).await,
            #[cfg(test)]
            Deleter::Recording(d) => d.delete(keys).await,
        }
    }
}

// Remove files from a local tree. Directories stay in place. Sync may not own a directory that
// becomes empty.
#[derive(Debug)]
pub(crate) struct DeleteFromLocalTree {
    root: std::path::PathBuf,
}

impl DeleteFromLocalTree {
    pub(crate) fn new(root: impl Into<std::path::PathBuf>) -> Self {
        Self { root: root.into() }
    }

    // Return the file a relative key names below the root. Reject paths outside the root and paths
    // that path cleaning changed.
    //
    // A local walk already produces cleaned paths. An S3 key can collapse onto another path after
    // cleaning. The check stops a future S3-to-local delete from removing the wrong file.
    pub(crate) fn file_path(&self, key: &str) -> Result<std::path::PathBuf, crate::error::Error> {
        let path = local_path_for_key(&self.root, key)?;
        // Containment and local deletion use the same remainder. The deletion path also requires
        // that the remainder still spells the original key.
        let named = crate::io::key::below_root(&self.root, &path)
            .and_then(std::path::Path::to_str)
            .is_some_and(|rest| rest == key.replace('/', std::path::MAIN_SEPARATOR_STR));
        if !named {
            return Err(crate::error::Error::new(
                crate::error::ErrorKind::InputInvalid,
                format!("the key '{key}' does not name the file this run would remove"),
            ));
        }
        Ok(path)
    }

    fn batch_size(&self) -> usize {
        1
    }

    pub(super) async fn delete(&self, keys: Vec<String>) -> Vec<KeyOutcome> {
        let mut outcomes = Vec::with_capacity(keys.len());
        for key in keys {
            let path = match self.file_path(&key) {
                Ok(path) => path,
                Err(err) => {
                    // Path derivation reports invalid input. Filesystem removal reports an input/output error.
                    outcomes.push(Err(Refusal::new(
                        err.kind().clone(),
                        format!("{key}: {err}"),
                    )));
                    continue;
                }
            };
            match tokio::fs::remove_file(&path).await {
                Ok(()) => outcomes.push(Ok(key)),
                // A missing file already has the delete result the run wanted. A second run reports
                // the same outcome.
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => outcomes.push(Ok(key)),
                Err(e) => outcomes.push(Err(Refusal::new(
                    crate::error::ErrorKind::IOError,
                    format!("{key}: {e}"),
                ))),
            }
        }
        outcomes
    }
}

// Delete keys from a bucket. Sync retries this request after throttling or a transient transport
// failure. A child gets retry behavior from its SDK client.
pub(crate) struct DeleteFromBucket {
    client: aws_sdk_s3::Client,
    bucket: String,
    // As in `SpawnUpload`: keys arrive relative to the run's root, so the root goes back on before
    // naming an object. Deleting a relative key would reach for something at the bucket root.
    root: String,
}

impl DeleteFromBucket {
    pub(crate) fn new(
        client: aws_sdk_s3::Client,
        bucket: impl Into<String>,
        prefix: Option<&str>,
    ) -> Self {
        Self {
            client,
            bucket: bucket.into(),
            root: crate::io::key::stream::root_prefix(prefix).into_owned(),
        }
    }

    // The object a relative key names under this run's root.
    pub(crate) fn object_key(&self, key: &str) -> String {
        format!("{}{}", self.root, key)
    }

    fn batch_size(&self) -> usize {
        DELETE_BATCH
    }

    async fn delete(&self, keys: Vec<String>, stopped: StopCheck<'_>) -> Vec<KeyOutcome> {
        // The object each relative key names, in the same order. The run matches a response entry
        // to the key it answers, and not to whatever sits at the same offset.
        let addressed: Vec<String> = keys.iter().map(|k| self.object_key(k)).collect();
        let mut at_object = std::collections::HashMap::with_capacity(addressed.len());
        for (at, object) in addressed.iter().enumerate() {
            at_object.insert(object.as_str(), at);
        }

        let unnamed = |at: usize, why: &str| {
            Err(Refusal::new(
                crate::error::ErrorKind::ServiceError,
                format!("{}: {why}", keys[at]),
            ))
        };
        let mut settled: Vec<Option<KeyOutcome>> = vec![None; keys.len()];
        // Keys that S3 still refuses. The next request contains only those keys.
        let mut outstanding: Vec<usize> = (0..keys.len()).collect();
        // The last refusal for each outstanding key. A stopped run returns that refusal to the
        // caller.
        let mut refused_with: std::collections::HashMap<usize, String> = Default::default();

        for attempt in 0..DELETE_REFUSAL_ATTEMPTS {
            if outstanding.is_empty() {
                break;
            }
            let last_attempt = attempt + 1 == DELETE_REFUSAL_ATTEMPTS;
            // Use the same backoff as a throttled request. An immediate retry adds to the load S3
            // is shedding.
            if attempt > 0 {
                let delay = crate::retry::Backoff::throttle().delay(attempt - 1, fastrand::f64());
                tokio::time::sleep(delay).await;
                // A stopped run starts no new attempt. Each outstanding key keeps the refusal that
                // earned its retry.
                if stopped() {
                    for &at in &outstanding {
                        let why = refused_with
                            .get(&at)
                            .map(String::as_str)
                            .unwrap_or("the run stopped before the service answered for this key");
                        settled[at].get_or_insert_with(|| unnamed(at, why));
                    }
                    break;
                }
            }

            let mut identifiers = Vec::with_capacity(outstanding.len());
            for &at in &outstanding {
                match aws_sdk_s3::types::ObjectIdentifier::builder()
                    .key(addressed[at].clone())
                    .build()
                {
                    Ok(id) => identifiers.push(id),
                    // Sync built that key, so an API refusal is a sync defect. The run records the
                    // key and continues with the batch.
                    Err(err) => settled[at] = Some(unnamed(at, &err.to_string())),
                }
            }
            if identifiers.is_empty() {
                break;
            }
            let delete = match aws_sdk_s3::types::Delete::builder()
                .set_objects(Some(identifiers))
                // The run asks for a loud response. A quiet response omits successful keys.
                .quiet(false)
                .build()
            {
                Ok(delete) => delete,
                Err(err) => {
                    for &at in &outstanding {
                        settled[at].get_or_insert_with(|| unnamed(at, &err.to_string()));
                    }
                    break;
                }
            };

            let sent = crate::retry::retry(crate::retry::classify_discovery_retry, |_| {
                let delete = delete.clone();
                async move {
                    self.client
                        .delete_objects()
                        .bucket(&self.bucket)
                        .delete(delete)
                        .send()
                        .await
                        // `Error::from` keeps the service metadata. The retry classifier reads that
                        // metadata.
                        .map_err(|err| {
                            crate::retry::GuardError::Inner(crate::error::Error::from(err))
                        })
                }
            })
            .await;

            match sent {
                // The response lists deleted keys and refused keys separately. The run reads both
                // lists.
                Ok(output) => {
                    for deleted in output.deleted() {
                        if let Some(&at) = deleted.key().and_then(|k| at_object.get(k)) {
                            settled[at] = Some(Ok(keys[at].clone()));
                        }
                    }
                    let mut refused_again = Vec::new();
                    for error in output.errors() {
                        let Some(&at) = error.key().and_then(|k| at_object.get(k)) else {
                            continue;
                        };
                        // Keep the service code and message. The code names the refusal; the
                        // message explains it.
                        let why = match (error.code(), error.message()) {
                            (Some(code), Some(message)) => format!("{code}: {message}"),
                            (Some(code), None) => code.to_string(),
                            (None, Some(message)) => message.to_string(),
                            (None, None) => "no reason given".to_string(),
                        };
                        // Retry only the throttle codes used by the request retry policy. A
                        // terminal delete stays for the next run.
                        if !last_attempt && crate::retry::is_throttle_code(error.code()) {
                            refused_with.insert(at, why);
                            refused_again.push(at);
                        } else {
                            settled[at] = Some(unnamed(at, &why));
                        }
                    }
                    outstanding = refused_again;
                }
                // The request exhausted its retries. The run returns that failure for every
                // outstanding key.
                Err(err) => {
                    let why = err.to_string();
                    for &at in &outstanding {
                        settled[at].get_or_insert_with(|| unnamed(at, &why));
                    }
                    break;
                }
            }
        }

        // The run names every requested key. A response that omits a key still needs an outcome.
        settled
            .into_iter()
            .enumerate()
            .map(|(at, outcome)| {
                outcome.unwrap_or_else(|| unnamed(at, "the response did not mention this key"))
            })
            .collect()
    }
}
