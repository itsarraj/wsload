use std::time::Duration;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LatencyStats {
    pub count: usize,
    pub min_ms: f64,
    pub p50_ms: f64,
    pub p75_ms: f64,
    pub p90_ms: f64,
    pub p95_ms: f64,
    pub p99_ms: f64,
    pub p999_ms: f64,
    pub max_ms: f64,
    pub mean_ms: f64,
    pub stddev_ms: f64,
}

pub fn compute_latency_stats(samples: &[Duration]) -> Option<LatencyStats> {
    if samples.is_empty() {
        return None;
    }

    let mut millis: Vec<f64> = samples.iter().map(|d| d.as_secs_f64() * 1000.0).collect();
    millis.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

    let n = millis.len();
    let min_ms = millis[0];
    let max_ms = millis[n - 1];

    let sum: f64 = millis.iter().sum();
    let mean_ms = sum / (n as f64);

    let variance: f64 = if n > 1 {
        let sq_diff_sum: f64 = millis.iter().map(|&x| (x - mean_ms).powi(2)).sum();
        sq_diff_sum / (n as f64)
    } else {
        0.0
    };
    let stddev_ms = variance.sqrt();

    let percentile = |p: f64| -> f64 {
        if n == 1 {
            return millis[0];
        }
        let rank = (p / 100.0) * ((n - 1) as f64);
        let low = rank.floor() as usize;
        let high = rank.ceil() as usize;
        let weight = rank - (low as f64);
        millis[low] * (1.0 - weight) + millis[high] * weight
    };

    Some(LatencyStats {
        count: n,
        min_ms,
        p50_ms: percentile(50.0),
        p75_ms: percentile(75.0),
        p90_ms: percentile(90.0),
        p95_ms: percentile(95.0),
        p99_ms: percentile(99.0),
        p999_ms: percentile(99.9),
        max_ms,
        mean_ms,
        stddev_ms,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_samples() {
        assert!(compute_latency_stats(&[]).is_none());
    }

    #[test]
    fn test_single_sample() {
        let samples = vec![Duration::from_millis(50)];
        let stats = compute_latency_stats(&samples).unwrap();
        assert_eq!(stats.count, 1);
        assert!((stats.min_ms - 50.0).abs() < 1e-4);
        assert!((stats.max_ms - 50.0).abs() < 1e-4);
        assert!((stats.mean_ms - 50.0).abs() < 1e-4);
        assert!((stats.p50_ms - 50.0).abs() < 1e-4);
        assert!((stats.p99_ms - 50.0).abs() < 1e-4);
        assert!((stats.stddev_ms - 0.0).abs() < 1e-4);
    }

    #[test]
    fn test_uniform_100_samples() {
        // 1ms to 100ms
        let samples: Vec<Duration> = (1..=100).map(Duration::from_millis).collect();
        let stats = compute_latency_stats(&samples).unwrap();

        assert_eq!(stats.count, 100);
        assert!((stats.min_ms - 1.0).abs() < 1e-4);
        assert!((stats.max_ms - 100.0).abs() < 1e-4);
        assert!((stats.mean_ms - 50.5).abs() < 1e-4);
        assert!((stats.p50_ms - 50.5).abs() < 1.0);
        assert!((stats.p90_ms - 90.1).abs() < 1.0);
        assert!((stats.p99_ms - 99.01).abs() < 1.0);
    }
}
