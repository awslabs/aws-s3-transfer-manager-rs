/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::collections::VecDeque;

use aws_sdk_s3::{
    error::SdkError,
    operation::list_objects_v2::{ListObjectsV2Error, ListObjectsV2Input, ListObjectsV2Output},
};
use aws_smithy_runtime_api::http::Response;
use std::mem;
use tracing::Instrument;

use super::DownloadObjectsContext;

/// Custom paginator for `ListObjectsV2` operation that handles
/// recursing over `CommonPrefixes` when a delimiter is set.
#[derive(Debug)]
struct ListObjectsPaginator {
    context: DownloadObjectsContext,
    state: Option<State>,
}

#[derive(Debug, PartialEq)]
enum State {
    Paginating {
        // next continuation token to use
        next_token: Option<String>,
        // next prefix to use
        prefix: Option<String>,
        // accumulated set of common prefixes to recurse
        common_prefixes: VecDeque<String>,
    },
    Done,
}

impl State {
    fn paginating(prefix: Option<String>) -> Self {
        State::Paginating {
            next_token: None,
            prefix,
            common_prefixes: VecDeque::new(),
        }
    }

    fn next_state(self, output: &ListObjectsV2Output) -> State {
        let prev_state = self;
        let is_truncated =
            output.is_truncated().unwrap_or(false) && output.next_continuation_token().is_some();
        let output_next_token = output.next_continuation_token.to_owned();
        let mut output_common_prefixes = output
            .common_prefixes
            .as_ref()
            .map(|prefixes| {
                prefixes
                    .iter()
                    .filter_map(|prefix| prefix.prefix.clone())
                    .collect::<VecDeque<_>>()
            })
            .unwrap_or_default();

        match prev_state {
            // more results with this prefix
            State::Paginating {
                prefix,
                mut common_prefixes,
                ..
            } if is_truncated => {
                // add new prefixes and keep going with same prefix
                common_prefixes.append(&mut output_common_prefixes);
                State::Paginating {
                    next_token: output_next_token,
                    prefix,
                    common_prefixes,
                }
            }

            // try next common prefix (if any)
            State::Paginating {
                mut common_prefixes,
                ..
            } => {
                common_prefixes.append(&mut output_common_prefixes);
                let prefix = common_prefixes.pop_front();
                match prefix {
                    Some(prefix) => State::Paginating {
                        next_token: None,
                        prefix: Some(prefix),
                        common_prefixes,
                    },
                    // no prefixes left
                    None => State::Done,
                }
            }
            State::Done => prev_state,
        }
    }
}

impl ListObjectsPaginator {
    fn new(context: DownloadObjectsContext) -> Self {
        let prefix = context.state.input.key_prefix.to_owned();
        Self {
            context,
            state: Some(State::paginating(prefix)),
        }
    }

    fn state(&self) -> &State {
        self.state.as_ref().expect("valid state")
    }

    async fn next_page(
        &mut self,
    ) -> Option<Result<ListObjectsV2Output, SdkError<ListObjectsV2Error, Response>>> {
        let input = &self.context.state.input;
        let request = match self.state() {
            State::Done => return None,
            State::Paginating {
                next_token, prefix, ..
            } => ListObjectsV2Input::builder()
                .set_bucket(input.bucket.to_owned())
                .set_prefix(prefix.clone())
                .set_continuation_token(next_token.clone())
                .set_delimiter(input.delimiter.to_owned()),
        };

        let list_result = request
            .send_with(self.context.client())
            .instrument(tracing::debug_span!("send-list-objects-v2"))
            .await;
        match list_result {
            Ok(output) => {
                let prev_state = self.state.take().expect("state set");
                let next_state = prev_state.next_state(&output);
                self.state.replace(next_state);
                Some(Ok(output))
            }
            Err(err) => Some(Err(err)),
        }
    }
}

/// Custom paginator for `ListObjectsV2` operation that handles
/// recursing over `CommonPrefixes` when a delimiter is set.
pub(crate) struct ListObjectsStream {
    paginator: ListObjectsPaginator,
    current_page: Option<std::vec::IntoIter<aws_sdk_s3::types::Object>>,
}

impl ListObjectsStream {
    pub(crate) fn new(context: DownloadObjectsContext) -> Self {
        Self {
            paginator: ListObjectsPaginator::new(context),
            current_page: None,
        }
    }

    pub(crate) async fn next(
        &mut self,
    ) -> Option<Result<aws_sdk_s3::types::Object, SdkError<ListObjectsV2Error, Response>>> {
        if let Err(err) = self.ensure_page().await {
            return Some(Err(err));
        }

        match &mut self.current_page {
            Some(ref mut page) => page.next().map(Ok),
            None => None,
        }
    }

    async fn set_next_page(&mut self) -> Result<(), SdkError<ListObjectsV2Error, Response>> {
        self.current_page.take();
        let next_page = self.paginator.next_page().await;
        if next_page.is_none() {
            return Ok(());
        }

        let mut next_page = next_page.unwrap()?;
        let mut contents = next_page.contents.take().map(|v| v.into_iter());
        mem::swap(&mut self.current_page, &mut contents);
        Ok(())
    }

    async fn ensure_page(&mut self) -> Result<(), SdkError<ListObjectsV2Error, Response>> {
        let remaining = match &mut self.current_page {
            Some(ref mut page) => page.size_hint().1.expect("known upper bound"),
            None => 0,
        };

        if remaining == 0 {
            self.set_next_page().await?
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use aws_sdk_s3::{
        operation::list_objects_v2::ListObjectsV2Output,
        types::{CommonPrefix, Object},
    };
    use aws_smithy_mocks::{mock, mock_client};

    use crate::operation::download_objects::{DownloadObjectsContext, DownloadObjectsInput};

    use super::{ListObjectsStream, State};

    /// Create a `ListObjectsV2` output
    fn list_resp(
        next_token: Option<&'static str>,
        prefix: &'static str,
        common_prefixes: Option<Vec<&'static str>>,
        keys: Vec<&'static str>,
    ) -> ListObjectsV2Output {
        let common_prefixes = common_prefixes.map(|p| {
            p.iter()
                .map(|v| CommonPrefix::builder().prefix(*v).build())
                .collect()
        });

        let contents = keys
            .iter()
            .map(|k| Object::builder().key(*k).build())
            .collect();

        ListObjectsV2Output::builder()
            .is_truncated(next_token.is_some())
            .set_next_continuation_token(next_token.map(str::to_owned))
            .prefix(prefix.to_owned())
            .set_common_prefixes(common_prefixes)
            .set_contents(Some(contents))
            .build()
    }

    /*
     *              initial-prefix
     * /   /  |  |     \              \
     * k1 k2 k3 k4   pre1             pre2
     *              / /   \ \          \  \
     *             k5 k6  k7 k8        k9 k10
     *
     *  Should see pages with following keys:
     *   * [k1, k2, k3, k4]
     *   * [pre1/k5, pre1/k6]
     *   * [pre1/k7, pre1/k8]
     *   * [pre2/k9, pre2/k10]
     */
    #[test]
    fn test_next_state() {
        let start = State::Paginating {
            next_token: None,
            prefix: Some("initial-prefix".to_string()),
            common_prefixes: VecDeque::new(),
        };

        let output1 = list_resp(
            Some("token1"),
            "initial-prefix",
            Some(vec!["pre1", "pre2"]),
            vec!["k1", "k2"],
        );
        let output2 = list_resp(None, "initial-prefix", None, vec!["k3", "k4"]);
        let output3 = list_resp(Some("token2"), "pre1", None, vec!["pre1/k5", "pre1/k6"]);
        let output4 = list_resp(None, "pre1", None, vec!["pre1/k7", "pre1/k8"]);
        let output5 = list_resp(None, "pre2", None, vec!["pre2/k9", "pre2/k10"]);

        let state2 = start.next_state(&output1);
        assert_eq!(
            state2,
            State::Paginating {
                next_token: Some("token1".to_owned()),
                prefix: Some("initial-prefix".to_owned()),
                common_prefixes: VecDeque::from_iter(vec!["pre1".to_owned(), "pre2".to_owned()]),
            }
        );

        let state3 = state2.next_state(&output2);
        assert_eq!(
            state3,
            State::Paginating {
                next_token: None,
                prefix: Some("pre1".to_owned()),
                common_prefixes: VecDeque::from_iter(vec!["pre2".to_owned()]),
            }
        );

        let state4 = state3.next_state(&output3);
        assert_eq!(
            state4,
            State::Paginating {
                next_token: Some("token2".to_owned()),
                prefix: Some("pre1".to_owned()),
                common_prefixes: VecDeque::from_iter(vec!["pre2".to_owned()]),
            }
        );

        let state5 = state4.next_state(&output4);
        assert_eq!(
            state5,
            State::Paginating {
                next_token: None,
                prefix: Some("pre2".to_owned()),
                common_prefixes: VecDeque::new(),
            }
        );

        let state6 = state5.next_state(&output5);
        assert_eq!(state6, State::Done);
    }

    #[tokio::test]
    async fn test_object_stream() {
        let resp1 = mock!(aws_sdk_s3::Client::list_objects_v2).then_output(|| {
            list_resp(
                Some("token1"),
                "initial-prefix",
                Some(vec!["pre1", "pre2"]),
                vec!["k1", "k2"],
            )
        });
        let resp2 = mock!(aws_sdk_s3::Client::list_objects_v2)
            .then_output(|| list_resp(None, "initial-prefix", None, vec!["k3", "k4"]));
        let resp3 = mock!(aws_sdk_s3::Client::list_objects_v2)
            .then_output(|| list_resp(Some("token2"), "pre1", None, vec!["pre1/k5", "pre1/k6"]));
        let resp4 = mock!(aws_sdk_s3::Client::list_objects_v2)
            .then_output(|| list_resp(None, "pre1", None, vec!["pre1/k7", "pre1/k8"]));
        let resp5 = mock!(aws_sdk_s3::Client::list_objects_v2)
            .then_output(|| list_resp(None, "pre2", None, vec!["pre2/k9", "pre2/k10"]));
        let client = mock_client!(aws_sdk_s3, &[&resp1, &resp2, &resp3, &resp4, &resp5]);

        let config = crate::Config::builder().client(client).build();
        let client = crate::Client::new(config);
        let input = DownloadObjectsInput::builder()
            .bucket("test-bucket")
            .destination("/tmp/test")
            .build()
            .unwrap();

        let ctx = DownloadObjectsContext::new(client.handle.clone(), input);
        let mut stream = ListObjectsStream::new(ctx);

        let mut keys = Vec::new();
        while let Some(res) = stream.next().await {
            let obj = res.unwrap();
            keys.push(obj.key.unwrap());
        }

        assert_eq!(
            keys,
            vec![
                "k1", "k2", "k3", "k4", "pre1/k5", "pre1/k6", "pre1/k7", "pre1/k8", "pre2/k9",
                "pre2/k10"
            ]
        );
    }
}
