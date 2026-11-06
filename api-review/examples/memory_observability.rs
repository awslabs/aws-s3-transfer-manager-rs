//! Memory budgets, metrics, tracing, and runtime traces.
//!
//! Each function is the code a customer writes for one task. `main` runs them against a local S3
//! endpoint. To see transfer summaries, run with
//! `AWS_S3_TM_DIAGNOSTICS=transfer.detail=1 RUST_LOG=aws_sdk_s3_transfer_manager::transfer=debug`.

#[path = "support/mock.rs"]
mod mock;

use std::time::Instant;

use aws_config::SdkConfig;
use aws_sdk_s3_transfer_manager::io::InputStream;
use aws_sdk_s3_transfer_manager::memory::{BufferPool, MemoryBudgetConfig, MemoryConfig};
use aws_sdk_s3_transfer_manager::metrics::Throughput;
use aws_sdk_s3_transfer_manager::metrics::unit::ByteUnit;
use aws_sdk_s3_transfer_manager::{Client, Config};
use mock::BUCKET;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Cap the client's transfer memory at 256 MiB.
fn client_with_budget(sdk_config: &SdkConfig) -> Result<Client, BoxError> {
    let pool = BufferPool::builder()
        .memory_budget(MemoryBudgetConfig::Limit(256 * ByteUnit::Mebibyte.as_bytes_usize()))
        .build()?;
    let config = Config::builder()
        .s3_config(sdk_config)
        .memory(MemoryConfig::Explicit(pool))
        .build();
    Ok(Client::new(config))
}

/// Two clients, for example one per region, sharing one memory budget.
fn clients_sharing_a_budget(east: &SdkConfig, west: &SdkConfig) -> Result<(Client, Client), BoxError> {
    let pool = BufferPool::builder()
        .memory_budget(MemoryBudgetConfig::Fraction(0.10))
        .build()?;
    let client = |sdk_config: &SdkConfig| {
        Client::new(
            Config::builder()
                .s3_config(sdk_config)
                .memory(MemoryConfig::Explicit(pool.clone()))
                .build(),
        )
    };
    Ok((client(east), client(west)))
}

/// Report the client's memory and a transfer's throughput.
async fn report_metrics(client: &Client, key: &str) -> Result<String, BoxError> {
    let started = Instant::now();
    let handle = client
        .upload()
        .bucket(BUCKET)
        .key(key)
        .body(InputStream::from(vec![1u8; 24 * 1024 * 1024]))
        .initiate()?;
    let output = handle.join().await?;
    let sent = output.metrics().network_tx;
    let throughput = Throughput::new(sent, started.elapsed());

    let memory = client.metrics();
    let memory = memory.memory();
    Ok(format!(
        "sent={} at {}; pool capacity={} used={} charged={} prepared={} queued={}",
        ByteUnit::display(sent),
        throughput.display_as(ByteUnit::Megabit),
        ByteUnit::display(memory.configured_capacity_bytes()),
        memory.admission_used_bytes(),
        memory.charged_capacity_bytes(),
        memory.prepared_capacity_bytes(),
        memory.queued_reservations(),
    ))
}

/// Install a tracing subscriber that honours `RUST_LOG`.
fn install_tracing() {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_target(true)
        .init();
}

/// Record the client's runtime into a dial9 trace.
#[cfg(feature = "dial9")]
fn client_with_dial9(sdk_config: &SdkConfig, dir: &std::path::Path) -> Result<(Client, dial9::Recorder), BoxError> {
    let writer = dial9::DiskBuffer::builder()
        .base_path(dir)
        .max_file_size(64 * 1024 * 1024)
        .max_total_size(256 * 1024 * 1024)
        .build()?;
    let recorder = dial9::recorder(writer).build();
    let config = Config::builder()
        .s3_config(sdk_config)
        .dial9_handle(recorder.handle().clone())
        .build();
    Ok((Client::new(config), recorder))
}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    install_tracing();
    let mock = mock::Mock::start().await;
    let sdk_config = mock.handle.shared_config().await;

    let client = client_with_budget(&sdk_config)?;
    println!("client_with_budget: {}", report_metrics(&client, "budget.bin").await?);

    let (east, west) = clients_sharing_a_budget(&sdk_config, &sdk_config)?;
    println!("clients_sharing_a_budget: east {}", report_metrics(&east, "east.bin").await?);
    println!(
        "clients_sharing_a_budget: same pool capacity seen by both clients: {}",
        east.metrics().memory().configured_capacity_bytes() == west.metrics().memory().configured_capacity_bytes()
    );

    #[cfg(feature = "dial9")]
    {
        let dir = std::env::temp_dir().join(format!("tm-api-review-dial9-{}", std::process::id()));
        std::fs::create_dir_all(&dir)?;
        let (client, recorder) = client_with_dial9(&sdk_config, &dir)?;
        println!("client_with_dial9: {}", report_metrics(&client, "dial9.bin").await?);
        let completion = client.shutdown_completion();
        drop(client);
        if let Some(completion) = completion {
            completion.wait().await?;
        }
        recorder.graceful_shutdown(std::time::Duration::from_secs(10));
        let files = std::fs::read_dir(&dir)?.count();
        println!("client_with_dial9: {files} trace file(s) in {}", dir.display());
        std::fs::remove_dir_all(&dir)?;
    }
    Ok(())
}
