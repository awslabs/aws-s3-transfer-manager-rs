use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::runtime::Handle as RuntimeHandle;
use tokio_metrics::{TaskMetrics, TaskMonitor};

/// A complete snapshot of metrics at a point in time
#[derive(Debug)]
struct MetricsSnapshot {
    timestamp_ms: u64,
    runtime_metrics: tokio::runtime::RuntimeMetrics,
    task_metrics: HashMap<String, TaskMetrics>,
}

/// Simple metrics collector for Tokio runtime and tasks
#[derive(Debug)]
pub struct TokioMetricsCollector {
    runtime_handle: RuntimeHandle,
    task_monitors: Arc<Mutex<HashMap<String, TaskMonitor>>>,
    metrics_buffer: Arc<Mutex<Vec<MetricsSnapshot>>>,
}

impl TokioMetricsCollector {
    /// Create a new metrics collector
    pub fn new() -> Self {
        // Try to get the runtime handle - this will work if called from within a tokio runtime
        let runtime_handle = RuntimeHandle::try_current().unwrap();
        Self {
            runtime_handle,
            task_monitors: Arc::new(Mutex::new(HashMap::new())),
            metrics_buffer: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Register a task monitor with a name
    pub fn register_task(&self, name: &str) -> TaskMonitor {
        let monitor = TaskMonitor::new();

        let mut monitors = self.task_monitors.lock().unwrap();
        monitors.insert(name.to_string(), monitor.clone());

        monitor
    }

    /// Start collecting metrics at the specified interval
    pub fn start_collecting(&self, interval_ms: u64) {
        println!("waahm7 starting");
        // Clone what we need for the async task
        let metrics_buffer = Arc::clone(&self.metrics_buffer);
        let runtime_handle = self.runtime_handle.clone();

        // Copy the task monitors for the collector task
        let task_monitors = self.task_monitors.clone();

        // Use a single task for all metrics collection
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(interval_ms));

            loop {
                interval.tick().await;

                // Current timestamp for this collection cycle
                let timestamp_ms = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_else(|_| Duration::from_secs(0))
                    .as_millis() as u64;

                // Prepare snapshot data
                let mut snapshot = MetricsSnapshot {
                    timestamp_ms,
                    runtime_metrics: runtime_handle.metrics(),
                    task_metrics: HashMap::new(),
                };

                // Get task metrics - we need a separate HashMap for interval iterators
                // since we can't store iterators in a field
                let mut got_any_task_metrics = false;
                let task_monitors = task_monitors.lock().unwrap();
                for (name, monitor) in task_monitors.iter() {
                    // We'll add all monitors' intervals to a local HashMap
                    let mut monitor_intervals = monitor.intervals();
                    //println!("waahm7 here");
                    // Try to get the next interval from this monitor
                    if let Some(metrics) = monitor_intervals.next() {
                        //   println!("{:?}", metrics);
                        snapshot.task_metrics.insert(name.clone(), metrics);
                        got_any_task_metrics = true;
                    }
                }
                drop(task_monitors);

                let mut buffer = metrics_buffer.lock().unwrap();
                buffer.push(snapshot);
            }
        });
    }

    /// Get the current size of the metrics buffer
    pub fn buffer_size(&self) -> usize {
        self.metrics_buffer.lock().unwrap().len()
    }

    /// Flush buffered metrics to file in Prometheus format and clear buffer
    /// Returns the number of snapshots written
    pub fn flush_buffer_to_file(&self, path: &str) -> std::io::Result<usize> {
    let mut buffer = self.metrics_buffer.lock().unwrap();

    if buffer.is_empty() {
        return Ok(0);
    }

    let snapshot_count = buffer.len();
    let mut output = String::new();

    // === RUNTIME METRICS ===
    output.push_str("# HELP tokio_worker_threads Number of worker threads\n");
    output.push_str("# TYPE tokio_worker_threads gauge\n");
    for snapshot in buffer.iter() {
        output.push_str(&format!(
            "tokio_worker_threads {} {}\n",
            snapshot.runtime_metrics.num_workers(),
            snapshot.timestamp_ms
        ));
    }

    // === TASK METRICS ===
    let mut all_task_names = HashSet::new();
    for snapshot in buffer.iter() {
        for name in snapshot.task_metrics.keys() {
            all_task_names.insert(name.clone());
        }
    }

    // For each task, add its metrics
    for task_name in all_task_names {
        // Normalize name for Prometheus
        let norm_name = task_name.replace(['-', '.', ' '], "_");

        // Instrumentation count
        output.push_str(&format!(
            "# HELP tokio_task_instrumented_{} Number of tasks instrumented\n",
            norm_name
        ));
        output.push_str(&format!(
            "# TYPE tokio_task_instrumented_{} counter\n",
            norm_name
        ));

        for snapshot in buffer.iter() {
            if let Some(metrics) = snapshot.task_metrics.get(&task_name) {
                output.push_str(&format!(
                    "tokio_task_instrumented_{} {} {}\n",
                    norm_name, metrics.instrumented_count, snapshot.timestamp_ms
                ));
            }
        }

        // Poll count
        output.push_str(&format!(
            "# HELP tokio_task_polls_{} Total number of task polls\n",
            norm_name
        ));
        output.push_str(&format!(
            "# TYPE tokio_task_polls_{} counter\n",
            norm_name
        ));

        for snapshot in buffer.iter() {
            if let Some(metrics) = snapshot.task_metrics.get(&task_name) {
                output.push_str(&format!(
                    "tokio_task_polls_{} {} {}\n",
                    norm_name, metrics.total_poll_count, snapshot.timestamp_ms
                ));
            }
        }

        // Poll duration
        output.push_str(&format!(
            "# HELP tokio_task_poll_duration_{}_seconds Total duration spent polling tasks\n",
            norm_name
        ));
        output.push_str(&format!(
            "# TYPE tokio_task_poll_duration_{}_seconds counter\n",
            norm_name
        ));

        for snapshot in buffer.iter() {
            if let Some(metrics) = snapshot.task_metrics.get(&task_name) {
                output.push_str(&format!(
                    "tokio_task_poll_duration_{}_seconds {:.6} {}\n",
                    norm_name,
                    metrics.total_poll_duration.as_secs_f64(),
                    snapshot.timestamp_ms
                ));
            }
        }

        // Scheduled count
        output.push_str(&format!(
            "# HELP tokio_task_scheduled_{} Number of times tasks were scheduled\n",
            norm_name
        ));
        output.push_str(&format!(
            "# TYPE tokio_task_scheduled_{} counter\n",
            norm_name
        ));

        for snapshot in buffer.iter() {
            if let Some(metrics) = snapshot.task_metrics.get(&task_name) {
                output.push_str(&format!(
                    "tokio_task_scheduled_{} {} {}\n",
                    norm_name, metrics.total_scheduled_count, snapshot.timestamp_ms
                ));
            }
        }
    }

    // Write atomically using a temporary file
    let temp_path = format!("{}.tmp", path);
    std::fs::write(&temp_path, output)?;
    std::fs::rename(&temp_path, path)?;

    // Clear the buffer after successful write
    buffer.clear();

    Ok(snapshot_count)
}

pub fn flush_buffer_to_csv(&self, csv_path: &str) -> std::io::Result<usize> {
    let mut buffer = self.metrics_buffer.lock().unwrap();

    if buffer.is_empty() {
        return Ok(0);
    }

    let snapshot_count = buffer.len();
    
    // Build a list of all timestamps and metrics
    let mut all_timestamps = Vec::new();
    let mut all_metrics = HashMap::new();
    let mut metric_names = HashSet::new();

    // First, collect all timestamps and metric names
    for snapshot in buffer.iter() {
        all_timestamps.push(snapshot.timestamp_ms);
        
        // Add runtime metrics
        let worker_metric_name = "worker_threads".to_string();
        metric_names.insert(worker_metric_name.clone());
        all_metrics.insert(
            (snapshot.timestamp_ms, worker_metric_name),
            snapshot.runtime_metrics.num_workers().to_string()
        );

        // Add task metrics
        for (task_name, metrics) in &snapshot.task_metrics {
            let norm_name = task_name.replace(['-', '.', ' ', '/'], "_");
            
            let instrumented_name = format!("{}_instrumented", norm_name);
            let polls_name = format!("{}_polls", norm_name);
            let poll_duration_name = format!("{}_poll_duration", norm_name);
            let scheduled_name = format!("{}_scheduled", norm_name);
            
            metric_names.insert(instrumented_name.clone());
            metric_names.insert(polls_name.clone());
            metric_names.insert(poll_duration_name.clone());
            metric_names.insert(scheduled_name.clone());
            
            all_metrics.insert(
                (snapshot.timestamp_ms, instrumented_name),
                metrics.instrumented_count.to_string()
            );
            all_metrics.insert(
                (snapshot.timestamp_ms, polls_name),
                metrics.total_poll_count.to_string()
            );
            all_metrics.insert(
                (snapshot.timestamp_ms, poll_duration_name),
                format!("{:.6}", metrics.total_poll_duration.as_secs_f64())
            );
            all_metrics.insert(
                (snapshot.timestamp_ms, scheduled_name),
                metrics.total_scheduled_count.to_string()
            );
        }
    }

    // Sort timestamps and metric names
    all_timestamps.sort();
    let mut sorted_metric_names: Vec<String> = metric_names.into_iter().collect();
    sorted_metric_names.sort();
    
    // Create the CSV header
    let mut csv_content = String::from("timestamp");
    for metric_name in &sorted_metric_names {
        csv_content.push_str(&format!(",{}", metric_name));
    }
    csv_content.push('\n');
    
    // Add data rows
    for timestamp in all_timestamps {
        csv_content.push_str(&timestamp.to_string());
        
        for metric_name in &sorted_metric_names {
            let value = all_metrics.get(&(timestamp, metric_name.clone()))
                             .unwrap_or(&String::from("")).clone();
            csv_content.push_str(&format!(",{}", value));
        }
        
        csv_content.push('\n');
    }
    
    // Write the CSV file
    let parent_dir = std::path::Path::new(csv_path).parent();
    if let Some(dir) = parent_dir {
        std::fs::create_dir_all(dir)?;
    }
    
    std::fs::write(csv_path, csv_content)?;
    
    // Clear the buffer after successful write
    buffer.clear();
    
    Ok(snapshot_count)
}


}
