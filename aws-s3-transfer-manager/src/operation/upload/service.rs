use tower::{service_fn, Service, ServiceBuilder, ServiceExt};


async fn upload_parts(
    ctx: UploadContext,
    reader: Arc<impl ReadPart>,
) -> Result<Vec<CompletedPart>, error::Error> {
}
