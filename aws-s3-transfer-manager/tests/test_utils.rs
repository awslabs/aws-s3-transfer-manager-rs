use aws_s3_transfer_manager::{error::Error, operation::download::DownloadHandle};
use bytes::{BufMut, Bytes, BytesMut};

/// drain/consume the body
pub async fn drain(handle: &mut DownloadHandle) -> Result<Bytes, Error> {
    let body = handle.body_mut();
    let mut data = BytesMut::new();
    let mut error: Option<Error> = None;
    while let Some(chunk) = body.next().await {
        match chunk {
            Ok(chunk) => data.put(chunk.data.into_bytes()),
            Err(err) => {
                error.get_or_insert(err);
            }
        }
    }

    if let Some(error) = error {
        return Err(error);
    }
    Ok(data.into())
}
