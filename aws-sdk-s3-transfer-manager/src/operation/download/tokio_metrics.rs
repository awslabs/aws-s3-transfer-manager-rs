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
    task_monitors: Mutex<HashMap<String, TaskMonitor>>,
    metrics_buffer: Arc<Mutex<Vec<MetricsSnapshot>>>,
}

impl TokioMetricsCollector {
    /// Create a new metrics collector
    pub fn new() -> Self {
        // Try to get the runtime handle - this will work if called from within a tokio runtime
        let runtime_handle = RuntimeHandle::try_current().unwrap();
        Self {
            runtime_handle,
            task_monitors: Mutex::new(HashMap::new()),
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
        // Clone what we need for the async task
        let metrics_buffer = Arc::clone(&self.metrics_buffer);
        let runtime_handle = self.runtime_handle.clone();

        // Copy the task monitors for the collector task
        let task_monitors = {
            let guard = self.task_monitors.lock().unwrap();
            guard.clone()
        };

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

                for (name, monitor) in &task_monitors {
                    // We'll add all monitors' intervals to a local HashMap
                    let mut monitor_intervals = monitor.intervals();

                    // Try to get the next interval from this monitor
                    if let Some(metrics) = monitor_intervals.next() {
                        snapshot.task_metrics.insert(name.clone(), metrics);
                        got_any_task_metrics = true;
                    }
                }

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

        // Build Prometheus output
        let mut output = String::new();

        // === RUNTIME METRICS ===

        output.push_str("# HELP tokio_worker_threads Number of worker threads\n");
        output.push_str("# TYPE tokio_worker_threads gauge\n");
        for snapshot in buffer.iter() {
            output.push_str(&format!(
                "tokio_worker_threads{{timestamp_ms=\"{}\"}} {}\n",
                snapshot.timestamp_ms,
                snapshot.runtime_metrics.num_workers()
            ));
        }

        // output.push_str("# HELP tokio_blocking_threads_count Number of blocking threads\n");
        // output.push_str("# TYPE tokio_blocking_threads_count gauge\n");
        // for snapshot in buffer.iter() {
        //     if let Some(runtime_metrics) = &snapshot.runtime_metrics {
        //         output.push_str(&format!(
        //             "tokio_blocking_threads_count{{timestamp_ms=\"{}\"}} {}\n",
        //             snapshot.timestamp_ms, runtime_metrics.num_blocking_threads()
        //         ));
        //     }
        // }

        // output.push_str("# HELP tokio_active_tasks Number of active tasks\n");
        // output.push_str("# TYPE tokio_active_tasks gauge\n");
        // for snapshot in buffer.iter() {
        //     if let Some(runtime_metrics) = &snapshot.runtime_metrics {
        //         output.push_str(&format!(
        //             "tokio_active_tasks{{timestamp_ms=\"{}\"}} {}\n",
        //             snapshot.timestamp_ms, runtime_metrics.active_tasks()
        //         ));
        //     }
        // }

        // output.push_str("# HELP tokio_idle_worker_threads Number of idle worker threads\n");
        // output.push_str("# TYPE tokio_idle_worker_threads gauge\n");
        // for snapshot in buffer.iter() {
        //     if let Some(runtime_metrics) = &snapshot.runtime_metrics {
        //         output.push_str(&format!(
        //             "tokio_idle_worker_threads{{timestamp_ms=\"{}\"}} {}\n",
        //             snapshot.timestamp_ms, runtime_metrics.quiescent_workers()
        //         ));
        //     }
        // }

        // === TASK METRICS ===

        // Find all unique task names
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
                        "tokio_task_instrumented_{}{{timestamp_ms=\"{}\"}} {}\n",
                        norm_name, snapshot.timestamp_ms, metrics.instrumented_count
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
                        "tokio_task_polls_{}{{timestamp_ms=\"{}\"}} {}\n",
                        norm_name, snapshot.timestamp_ms, metrics.total_poll_count
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
                        "tokio_task_poll_duration_{}_seconds{{timestamp_ms=\"{}\"}} {:.6}\n",
                        norm_name,
                        snapshot.timestamp_ms,
                        metrics.total_poll_duration.as_secs_f64()
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
                        "tokio_task_scheduled_{}{{timestamp_ms=\"{}\"}} {}\n",
                        norm_name, snapshot.timestamp_ms, metrics.total_scheduled_count
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
}
