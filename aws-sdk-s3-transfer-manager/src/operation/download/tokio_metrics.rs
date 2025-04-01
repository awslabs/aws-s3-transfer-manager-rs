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
                let task_monitors = task_monitors.lock().unwrap();
                for (name, monitor) in task_monitors.iter() {
                    // We'll add all monitors' intervals to a local HashMap
                    let mut monitor_intervals = monitor.intervals();
                    //println!("waahm7 here");
                    // Try to get the next interval from this monitor
                    if let Some(metrics) = monitor_intervals.next() {
                        //   println!("{:?}", metrics);
                        snapshot.task_metrics.insert(name.clone(), metrics);
                    }
                }
                drop(task_monitors);

                let mut buffer = metrics_buffer.lock().unwrap();
                buffer.push(snapshot);
            }
        });
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
            output.push_str(&format!("# TYPE tokio_task_polls_{} counter\n", norm_name));

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
    pub fn flush_buffer_to_csv(&self, base_path: &str) -> std::io::Result<usize> {
        let mut buffer = self.metrics_buffer.lock().unwrap();

        if buffer.is_empty() {
            return Ok(0);
        }

        let snapshot_count = buffer.len();

        // Create paths for both CSV files
        let runtime_csv_path = format!("{}_runtime.csv", base_path);
        let tasks_csv_path = format!("{}_tasks.csv", base_path);

        // Ensure parent directory exists
        if let Some(dir) = std::path::Path::new(base_path).parent() {
            std::fs::create_dir_all(dir)?;
        }

        // Write runtime metrics to CSV
        self.write_runtime_metrics_to_csv(&runtime_csv_path, &buffer)?;

        // Write task metrics to CSV (wide format)
        self.write_task_metrics_to_csv_wide_format(&tasks_csv_path, &buffer)?;

        // Clear the buffer after successful writes
        buffer.clear();

        Ok(snapshot_count)
    }

    fn write_runtime_metrics_to_csv(
        &self,
        csv_path: &str,
        buffer: &Vec<MetricsSnapshot>,
    ) -> std::io::Result<()> {
        // Create a set of runtime metric names - start with timestamp as the first column
        let mut metric_names = vec!["timestamp".to_string()];

        // Add standard runtime metrics
        metric_names.push("num_workers".to_string());
        metric_names.push("num_alive_tasks".to_string());
        metric_names.push("global_queue_depth".to_string());
        metric_names.push("num_blocking_threads".to_string());
        metric_names.push("num_idle_blocking_threads".to_string());
        metric_names.push("blocking_queue_depth".to_string());
        metric_names.push("remote_schedule_count".to_string());
        metric_names.push("budget_forced_yield_count".to_string());
        metric_names.push("spawned_tasks_count".to_string());
        metric_names.push("io_driver_fd_registered_count".to_string());
        metric_names.push("io_driver_fd_deregistered_count".to_string());
        metric_names.push("io_driver_ready_count".to_string());

        // Get the maximum worker count across all snapshots for worker-specific metrics
        let max_worker_count = buffer
            .iter()
            .map(|s| s.runtime_metrics.num_workers())
            .max()
            .unwrap_or(0);

        // Add worker-specific metrics for each possible worker
        for worker_id in 0..max_worker_count {
            metric_names.push(format!("worker_{}_steal_count", worker_id));
            metric_names.push(format!("worker_{}_steal_operations", worker_id));
            metric_names.push(format!("worker_{}_local_queue_depth", worker_id));
            metric_names.push(format!("worker_{}_local_schedule_count", worker_id));
            metric_names.push(format!("worker_{}_overflow_count", worker_id));
            metric_names.push(format!("worker_{}_poll_count", worker_id));
            metric_names.push(format!("worker_{}_park_count", worker_id));
            metric_names.push(format!("worker_{}_park_unpark_count", worker_id));
            metric_names.push(format!("worker_{}_noop_count", worker_id));
            metric_names.push(format!("worker_{}_total_busy_duration", worker_id));
            metric_names.push(format!("worker_{}_mean_poll_time", worker_id));
        }

        // Create CSV content
        let mut csv_content = String::new();

        // Write header
        csv_content.push_str(&metric_names.join(","));
        csv_content.push('\n');

        // Write values for each snapshot
        for snapshot in buffer {
            let mut row = Vec::with_capacity(metric_names.len());

            for metric_name in &metric_names {
                let value = match metric_name.as_str() {
                    "timestamp" => snapshot.timestamp_ms.to_string(),
                    "num_workers" => snapshot.runtime_metrics.num_workers().to_string(),
                    "num_alive_tasks" => snapshot.runtime_metrics.num_alive_tasks().to_string(),
                    "global_queue_depth" => {
                        snapshot.runtime_metrics.global_queue_depth().to_string()
                    }
                    "num_blocking_threads" => {
                        snapshot.runtime_metrics.num_blocking_threads().to_string()
                    }
                    "num_idle_blocking_threads" => snapshot
                        .runtime_metrics
                        .num_idle_blocking_threads()
                        .to_string(),
                    "blocking_queue_depth" => {
                        snapshot.runtime_metrics.blocking_queue_depth().to_string()
                    }
                    "remote_schedule_count" => {
                        snapshot.runtime_metrics.remote_schedule_count().to_string()
                    }
                    "budget_forced_yield_count" => snapshot
                        .runtime_metrics
                        .budget_forced_yield_count()
                        .to_string(),
                    "spawned_tasks_count" => {
                        snapshot.runtime_metrics.spawned_tasks_count().to_string()
                    }
                    "io_driver_fd_registered_count" => snapshot
                        .runtime_metrics
                        .io_driver_fd_registered_count()
                        .to_string(),
                    "io_driver_fd_deregistered_count" => snapshot
                        .runtime_metrics
                        .io_driver_fd_deregistered_count()
                        .to_string(),
                    "io_driver_ready_count" => {
                        snapshot.runtime_metrics.io_driver_ready_count().to_string()
                    }
                    _ => {
                        if let Some(worker_metric) = metric_name.strip_prefix("worker_") {
                            if let Some(parts) = worker_metric.split_once('_') {
                                if let Ok(worker_id) = parts.0.parse::<usize>() {
                                    if worker_id < snapshot.runtime_metrics.num_workers() {
                                        match parts.1 {
                                            "steal_count" => snapshot
                                                .runtime_metrics
                                                .worker_steal_count(worker_id)
                                                .to_string(),
                                            "steal_operations" => snapshot
                                                .runtime_metrics
                                                .worker_steal_operations(worker_id)
                                                .to_string(),
                                            "local_queue_depth" => snapshot
                                                .runtime_metrics
                                                .worker_local_queue_depth(worker_id)
                                                .to_string(),
                                            "local_schedule_count" => snapshot
                                                .runtime_metrics
                                                .worker_local_schedule_count(worker_id)
                                                .to_string(),
                                            "overflow_count" => snapshot
                                                .runtime_metrics
                                                .worker_overflow_count(worker_id)
                                                .to_string(),
                                            "poll_count" => snapshot
                                                .runtime_metrics
                                                .worker_poll_count(worker_id)
                                                .to_string(),
                                            "park_count" => snapshot
                                                .runtime_metrics
                                                .worker_park_count(worker_id)
                                                .to_string(),
                                            "park_unpark_count" => snapshot
                                                .runtime_metrics
                                                .worker_park_unpark_count(worker_id)
                                                .to_string(),
                                            "noop_count" => snapshot
                                                .runtime_metrics
                                                .worker_noop_count(worker_id)
                                                .to_string(),
                                            "total_busy_duration" => format!(
                                                "{:.6}",
                                                snapshot
                                                    .runtime_metrics
                                                    .worker_total_busy_duration(worker_id)
                                                    .as_secs_f64()
                                            ),
                                            "mean_poll_time" => format!(
                                                "{:.6}",
                                                snapshot
                                                    .runtime_metrics
                                                    .worker_mean_poll_time(worker_id)
                                                    .as_secs_f64()
                                            ),
                                            _ => String::new(),
                                        }
                                    } else {
                                        String::new() // This worker doesn't exist in this snapshot
                                    }
                                } else {
                                    String::new()
                                }
                            } else {
                                String::new()
                            }
                        } else {
                            String::new()
                        }
                    }
                };

                row.push(value);
            }

            csv_content.push_str(&row.join(","));
            csv_content.push('\n');
        }

        // Write the runtime CSV file
        std::fs::write(csv_path, csv_content)
    }

    fn write_task_metrics_to_csv_wide_format(
        &self,
        csv_path: &str,
        buffer: &Vec<MetricsSnapshot>,
    ) -> std::io::Result<()> {
        // First, collect all unique task names and metrics
        let mut task_metric_columns = HashSet::new();

        // Define all possible metric names
        let metric_names = vec![
            "instrumented_count",
            "dropped_count",
            "first_poll_count",
            "total_first_poll_delay",
            "total_idled_count",
            "total_idle_duration",
            "total_scheduled_count",
            "total_scheduled_duration",
            "total_poll_count",
            "total_poll_duration",
            "total_fast_poll_count",
            "total_fast_poll_duration",
            "total_slow_poll_count",
            "total_slow_poll_duration",
            "total_short_delay_count",
            "total_long_delay_count",
            "total_short_delay_duration",
            "total_long_delay_duration",
            "mean_first_poll_delay",
            "mean_idle_duration",
            "mean_scheduled_duration",
            "mean_poll_duration",
            "slow_poll_ratio",
            "long_delay_ratio",
            "mean_fast_poll_duration",
            "mean_short_delay_duration",
            "mean_slow_poll_duration",
            "mean_long_delay_duration",
        ];

        // Collect all task names across all snapshots
        let mut task_names = HashSet::new();
        for snapshot in buffer {
            for (task_name, _) in &snapshot.task_metrics {
                let norm_name = task_name.replace(['-', '.', ' ', '/'], "_");
                task_names.insert(norm_name);
            }
        }

        // Generate all task-metric column combinations
        for task_name in &task_names {
            for metric_name in &metric_names {
                task_metric_columns.insert(format!("{}_{}", task_name, metric_name));
            }
        }

        // Sort the columns for consistent output
        let mut sorted_columns: Vec<String> = task_metric_columns.into_iter().collect();
        sorted_columns.sort();

        // Prepare the CSV content
        let mut csv_content = String::new();

        // Write header: timestamp + all task metrics
        csv_content.push_str("timestamp");
        for column in &sorted_columns {
            csv_content.push_str(&format!(",{}", column));
        }
        csv_content.push('\n');

        // Write data for each snapshot
        for snapshot in buffer {
            // Start with timestamp
            csv_content.push_str(&snapshot.timestamp_ms.to_string());

            // Add values for each task-metric combination
            for column in &sorted_columns {
                // Split the column name to get task name and metric name
                let parts: Vec<&str> = column.split('_').collect();
                if parts.len() >= 2 {
                    // Task name could contain underscores, so join all parts except the last one
                    let metric_name = parts.last().unwrap();
                    let task_name_parts = &parts[0..parts.len() - 1];
                    let task_name = task_name_parts.join("_");

                    // Find the corresponding task metrics
                    let mut value = String::new();
                    for (t_name, metrics) in &snapshot.task_metrics {
                        let norm_name = t_name.replace(['-', '.', ' ', '/'], "_");
                        if norm_name == task_name {
                            value = match *metric_name {
                                // Direct fields
                                "instrumented_count" => metrics.instrumented_count.to_string(),
                                "dropped_count" => metrics.dropped_count.to_string(),
                                "first_poll_count" => metrics.first_poll_count.to_string(),
                                "total_first_poll_delay" => {
                                    format!("{:.6}", metrics.total_first_poll_delay.as_secs_f64())
                                }
                                "total_idled_count" => metrics.total_idled_count.to_string(),
                                "total_idle_duration" => {
                                    format!("{:.6}", metrics.total_idle_duration.as_secs_f64())
                                }
                                "total_scheduled_count" => {
                                    metrics.total_scheduled_count.to_string()
                                }
                                "total_scheduled_duration" => {
                                    format!("{:.6}", metrics.total_scheduled_duration.as_secs_f64())
                                }
                                "total_poll_count" => metrics.total_poll_count.to_string(),
                                "total_poll_duration" => {
                                    format!("{:.6}", metrics.total_poll_duration.as_secs_f64())
                                }
                                "total_fast_poll_count" => {
                                    metrics.total_fast_poll_count.to_string()
                                }
                                "total_fast_poll_duration" => {
                                    format!("{:.6}", metrics.total_fast_poll_duration.as_secs_f64())
                                }
                                "total_slow_poll_count" => {
                                    metrics.total_slow_poll_count.to_string()
                                }
                                "total_slow_poll_duration" => {
                                    format!("{:.6}", metrics.total_slow_poll_duration.as_secs_f64())
                                }
                                "total_short_delay_count" => {
                                    metrics.total_short_delay_count.to_string()
                                }
                                "total_long_delay_count" => {
                                    metrics.total_long_delay_count.to_string()
                                }
                                "total_short_delay_duration" => format!(
                                    "{:.6}",
                                    metrics.total_short_delay_duration.as_secs_f64()
                                ),
                                "total_long_delay_duration" => format!(
                                    "{:.6}",
                                    metrics.total_long_delay_duration.as_secs_f64()
                                ),

                                // Calculated metrics
                                "mean_first_poll_delay" => {
                                    format!("{:.6}", metrics.mean_first_poll_delay().as_secs_f64())
                                }
                                "mean_idle_duration" => {
                                    format!("{:.6}", metrics.mean_idle_duration().as_secs_f64())
                                }
                                "mean_scheduled_duration" => format!(
                                    "{:.6}",
                                    metrics.mean_scheduled_duration().as_secs_f64()
                                ),
                                "mean_poll_duration" => {
                                    format!("{:.6}", metrics.mean_poll_duration().as_secs_f64())
                                }
                                "slow_poll_ratio" => format!("{:.6}", metrics.slow_poll_ratio()),
                                "long_delay_ratio" => format!("{:.6}", metrics.long_delay_ratio()),
                                "mean_fast_poll_duration" => format!(
                                    "{:.6}",
                                    metrics.mean_fast_poll_duration().as_secs_f64()
                                ),
                                "mean_short_delay_duration" => format!(
                                    "{:.6}",
                                    metrics.mean_short_delay_duration().as_secs_f64()
                                ),
                                "mean_slow_poll_duration" => format!(
                                    "{:.6}",
                                    metrics.mean_slow_poll_duration().as_secs_f64()
                                ),
                                "mean_long_delay_duration" => format!(
                                    "{:.6}",
                                    metrics.mean_long_delay_duration().as_secs_f64()
                                ),

                                _ => String::new(),
                            };
                            break;
                        }
                    }
                    csv_content.push_str(&format!(",{}", value));
                } else {
                    csv_content.push_str(",");
                }
            }

            csv_content.push('\n');
        }

        // Write the tasks CSV file
        std::fs::write(csv_path, csv_content)
    }
}
