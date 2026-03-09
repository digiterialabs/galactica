use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

/// Simple metrics registry for tracking counters, gauges, and histograms.
///
/// This is a skeleton implementation — will be replaced with
/// OpenTelemetry/Prometheus backends in a future step.
#[derive(Debug, Clone)]
pub struct MetricsRegistry {
    counters: Arc<RwLock<HashMap<String, Arc<AtomicU64>>>>,
}

impl MetricsRegistry {
    pub fn new() -> Self {
        Self {
            counters: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Increment a counter by the given value.
    pub fn increment_counter(&self, name: &str, value: u64) {
        let counters = self.counters.read().unwrap();
        if let Some(counter) = counters.get(name) {
            counter.fetch_add(value, Ordering::Relaxed);
            return;
        }
        drop(counters);

        let mut counters = self.counters.write().unwrap();
        let counter = counters
            .entry(name.to_string())
            .or_insert_with(|| Arc::new(AtomicU64::new(0)));
        counter.fetch_add(value, Ordering::Relaxed);
    }

    /// Get the current value of a counter.
    pub fn get_counter(&self, name: &str) -> u64 {
        let counters = self.counters.read().unwrap();
        counters
            .get(name)
            .map(|c| c.load(Ordering::Relaxed))
            .unwrap_or(0)
    }

    /// Record a gauge value (just stores the latest value).
    pub fn set_gauge(&self, name: &str, value: u64) {
        let mut counters = self.counters.write().unwrap();
        let entry = counters
            .entry(name.to_string())
            .or_insert_with(|| Arc::new(AtomicU64::new(0)));
        entry.store(value, Ordering::Relaxed);
    }

    /// Record a histogram observation (placeholder — just increments a count).
    pub fn record_histogram(&self, name: &str, _value: f64) {
        self.increment_counter(&format!("{name}_count"), 1);
    }
}

impl Default for MetricsRegistry {
    fn default() -> Self {
        Self::new()
    }
}
