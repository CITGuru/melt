use std::time::Instant;

/// RAII span that records elapsed time to a histogram on drop.
///
/// ```ignore
/// let _t = TimedSpan::new(melt_metrics::PROXY_LATENCY);
/// // ... do work ...
/// // dropped here → histogram updated.
/// ```
pub struct TimedSpan {
    name: &'static str,
    start: Instant,
}

impl TimedSpan {
    pub fn new(name: &'static str) -> Self {
        Self {
            name,
            start: Instant::now(),
        }
    }
}

impl Drop for TimedSpan {
    fn drop(&mut self) {
        metrics::histogram!(self.name).record(self.start.elapsed().as_secs_f64());
    }
}
