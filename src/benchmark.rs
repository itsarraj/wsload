use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::Result;
use clap::ValueEnum;
use colored::*;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

use crate::stats::{compute_latency_stats, LatencyStats};

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Serialize, Deserialize)]
pub enum BenchmarkMode {
    Echo,
    Ping,
    Fanout,
}

#[derive(Debug, Clone)]
pub struct BenchmarkConfig {
    pub url: String,
    pub connections: usize,
    pub duration: Duration,
    pub ramp_duration: Duration,
    pub mode: BenchmarkMode,
    pub interval: Duration,
    pub payload: Option<String>,
    pub max_messages: Option<usize>,
    pub quiet: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkResult {
    pub url: String,
    pub mode: BenchmarkMode,
    pub target_connections: usize,
    pub elapsed_secs: f64,
    pub handshakes_ok: usize,
    pub handshakes_err: usize,
    pub messages_sent: usize,
    pub messages_recv: usize,
    pub bytes_sent: usize,
    pub bytes_recv: usize,
    pub errors: usize,
    pub msgs_per_sec: f64,
    pub bytes_per_sec: f64,
    pub handshake_stats: Option<LatencyStats>,
    pub rtt_stats: Option<LatencyStats>,
    pub fanout_stats: Option<LatencyStats>,
}

impl BenchmarkResult {
    pub fn print_summary(&self) {
        println!("\n{}", "==================== Benchmark Summary ====================".bold().cyan());
        println!("Target URL:             {}", self.url.bold());
        println!("Benchmark Mode:         {:?}", self.mode);
        println!("Target Connections:     {}", self.target_connections);
        println!("Test Duration:          {:.2}s", self.elapsed_secs);
        println!(
            "Handshake Success Rate: {}/{} ({:.1}%)",
            self.handshakes_ok,
            self.target_connections,
            if self.target_connections > 0 {
                (self.handshakes_ok as f64 / self.target_connections as f64) * 100.0
            } else {
                0.0
            }
        );
        println!(
            "Messages Sent / Recv:   {} / {}",
            self.messages_sent, self.messages_recv
        );
        println!(
            "Message Throughput:     {:.1} msgs/sec ({:.2} MB/s)",
            self.msgs_per_sec,
            self.bytes_per_sec / (1024.0 * 1024.0)
        );
        println!("Total Errors / Drops:   {}", self.errors);

        if let Some(ref hs) = self.handshake_stats {
            println!("\n{}", "--- Connection Handshake Latency ---".bold().yellow());
            println!("  Min:    {:>8.2} ms", hs.min_ms);
            println!("  p50:    {:>8.2} ms", hs.p50_ms);
            println!("  p90:    {:>8.2} ms", hs.p90_ms);
            println!("  p99:    {:>8.2} ms", hs.p99_ms);
            println!("  Max:    {:>8.2} ms", hs.max_ms);
            println!("  Mean:   {:>8.2} ms (± {:.2} ms)", hs.mean_ms, hs.stddev_ms);
        }

        if let Some(ref rtt) = self.rtt_stats {
            println!("\n{}", "--- Message Round-Trip Latency (RTT) ---".bold().green());
            println!("  Count:  {}", rtt.count);
            println!("  Min:    {:>8.2} ms", rtt.min_ms);
            println!("  p50:    {:>8.2} ms", rtt.p50_ms);
            println!("  p75:    {:>8.2} ms", rtt.p75_ms);
            println!("  p90:    {:>8.2} ms", rtt.p90_ms);
            println!("  p95:    {:>8.2} ms", rtt.p95_ms);
            println!("  p99:    {:>8.2} ms", rtt.p99_ms);
            println!("  p99.9:  {:>8.2} ms", rtt.p999_ms);
            println!("  Max:    {:>8.2} ms", rtt.max_ms);
            println!("  Mean:   {:>8.2} ms (± {:.2} ms)", rtt.mean_ms, rtt.stddev_ms);
        }

        if let Some(ref fo) = self.fanout_stats {
            println!("\n{}", "--- Broadcast Fan-Out Delivery Latency ---".bold().magenta());
            println!("  Deliveries: {}", fo.count);
            println!("  Min:        {:>8.2} ms", fo.min_ms);
            println!("  p50:        {:>8.2} ms", fo.p50_ms);
            println!("  p90:        {:>8.2} ms", fo.p90_ms);
            println!("  p99:        {:>8.2} ms", fo.p99_ms);
            println!("  Max:        {:>8.2} ms", fo.max_ms);
            println!("  Mean:       {:>8.2} ms (± {:.2} ms)", fo.mean_ms, fo.stddev_ms);
        }

        println!("{}\n", "===========================================================".bold().cyan());
    }
}

pub struct BenchmarkRunner {
    config: BenchmarkConfig,
}

impl BenchmarkRunner {
    pub fn new(config: BenchmarkConfig) -> Self {
        Self { config }
    }

    pub async fn run(&self) -> Result<BenchmarkResult> {
        let total_connections = self.config.connections.max(1);
        let handshakes_ok = Arc::new(AtomicUsize::new(0));
        let handshakes_err = Arc::new(AtomicUsize::new(0));
        let active_connections = Arc::new(AtomicUsize::new(0));
        let messages_sent = Arc::new(AtomicUsize::new(0));
        let messages_recv = Arc::new(AtomicUsize::new(0));
        let bytes_sent = Arc::new(AtomicUsize::new(0));
        let bytes_recv = Arc::new(AtomicUsize::new(0));
        let total_errors = Arc::new(AtomicUsize::new(0));

        let handshake_durations = Arc::new(Mutex::new(Vec::with_capacity(total_connections)));
        let rtt_durations = Arc::new(Mutex::new(Vec::new()));
        let fanout_durations = Arc::new(Mutex::new(Vec::new()));

        let stop_flag = Arc::new(AtomicBool::new(false));
        let benchmark_start = Instant::now();
        let end_time = benchmark_start + self.config.duration;

        let ramp_interval = if !self.config.ramp_duration.is_zero() && total_connections > 1 {
            self.config.ramp_duration / (total_connections as u32)
        } else {
            Duration::ZERO
        };

        // Live status reporter task
        let quiet = self.config.quiet;
        let r_active = Arc::clone(&active_connections);
        let r_hs_ok = Arc::clone(&handshakes_ok);
        let r_sent = Arc::clone(&messages_sent);
        let r_recv = Arc::clone(&messages_recv);
        let r_err = Arc::clone(&total_errors);
        let r_stop = Arc::clone(&stop_flag);

        let reporter_handle = tokio::spawn(async move {
            if quiet {
                return;
            }
            while !r_stop.load(Ordering::Relaxed) {
                tokio::time::sleep(Duration::from_millis(500)).await;
                if r_stop.load(Ordering::Relaxed) {
                    break;
                }
                let active = r_active.load(Ordering::Relaxed);
                let hs = r_hs_ok.load(Ordering::Relaxed);
                let sent = r_sent.load(Ordering::Relaxed);
                let recv = r_recv.load(Ordering::Relaxed);
                let err = r_err.load(Ordering::Relaxed);

                eprint!(
                    "\r[wsload] Active: {:>4} | Handshakes: {:>4}/{} | Sent: {:>5} | Recv: {:>5} | Errors: {:>3}   ",
                    active, hs, total_connections, sent, recv, err
                );
            }
            eprint!("\r\x1b[2K"); // Clear line on finish
        });

        // Launch worker tasks
        let mut worker_handles = Vec::with_capacity(total_connections);

        for conn_idx in 0..total_connections {
            if conn_idx > 0 && !ramp_interval.is_zero() {
                tokio::time::sleep(ramp_interval).await;
            }

            if Instant::now() >= end_time {
                break;
            }

            let url = self.config.url.clone();
            let mode = self.config.mode;
            let interval = self.config.interval;
            let custom_payload = self.config.payload.clone();
            let max_msgs = self.config.max_messages;
            let c_stop = Arc::clone(&stop_flag);

            let c_hs_ok = Arc::clone(&handshakes_ok);
            let c_hs_err = Arc::clone(&handshakes_err);
            let c_active = Arc::clone(&active_connections);
            let c_sent = Arc::clone(&messages_sent);
            let c_recv = Arc::clone(&messages_recv);
            let c_b_sent = Arc::clone(&bytes_sent);
            let c_b_recv = Arc::clone(&bytes_recv);
            let c_err = Arc::clone(&total_errors);

            let c_hs_dur = Arc::clone(&handshake_durations);
            let c_rtt_dur = Arc::clone(&rtt_durations);
            let c_fo_dur = Arc::clone(&fanout_durations);

            let is_publisher = mode == BenchmarkMode::Fanout && conn_idx == 0;

            let handle = tokio::spawn(async move {
                let hs_start = Instant::now();
                let connect_res = connect_async(&url).await;
                let hs_elapsed = hs_start.elapsed();

                let (ws_stream, _) = match connect_res {
                    Ok(res) => {
                        c_hs_ok.fetch_add(1, Ordering::Relaxed);
                        c_active.fetch_add(1, Ordering::Relaxed);
                        c_hs_dur.lock().await.push(hs_elapsed);
                        res
                    }
                    Err(_) => {
                        c_hs_err.fetch_add(1, Ordering::Relaxed);
                        c_err.fetch_add(1, Ordering::Relaxed);
                        return;
                    }
                };

                let (mut write, mut read) = ws_stream.split();
                let mut msg_seq: usize = 0;

                match mode {
                    BenchmarkMode::Ping => {
                        while !c_stop.load(Ordering::Relaxed) && Instant::now() < end_time {
                            if let Some(limit) = max_msgs {
                                if msg_seq >= limit {
                                    break;
                                }
                            }

                            msg_seq += 1;
                            let ping_payload = format!("wsload-ping-{conn_idx}-{msg_seq}").into_bytes();
                            let ping_len = ping_payload.len();
                            let sent_time = Instant::now();

                            if write.send(Message::Ping(ping_payload.clone())).await.is_err() {
                                c_err.fetch_add(1, Ordering::Relaxed);
                                break;
                            }
                            c_sent.fetch_add(1, Ordering::Relaxed);
                            c_b_sent.fetch_add(ping_len, Ordering::Relaxed);

                            // Wait for matching pong
                            let pong_res = tokio::time::timeout(Duration::from_secs(5), async {
                                while let Some(Ok(frame)) = read.next().await {
                                    if let Message::Pong(ref data) = frame {
                                        if data == &ping_payload {
                                            return Ok(sent_time.elapsed());
                                        }
                                    }
                                }
                                Err(())
                            })
                            .await;

                            match pong_res {
                                Ok(Ok(rtt)) => {
                                    c_recv.fetch_add(1, Ordering::Relaxed);
                                    c_b_recv.fetch_add(ping_len, Ordering::Relaxed);
                                    c_rtt_dur.lock().await.push(rtt);
                                }
                                _ => {
                                    c_err.fetch_add(1, Ordering::Relaxed);
                                    break;
                                }
                            }

                            tokio::time::sleep(interval).await;
                        }
                    }
                    BenchmarkMode::Echo => {
                        while !c_stop.load(Ordering::Relaxed) && Instant::now() < end_time {
                            if let Some(limit) = max_msgs {
                                if msg_seq >= limit {
                                    break;
                                }
                            }

                            msg_seq += 1;
                            let payload = match custom_payload {
                                Some(ref p) => p.replace("{{seq}}", &msg_seq.to_string()),
                                None => format!(
                                    r#"{{"_wsload_conn":{},"_wsload_seq":{},"_wsload_ts":{}}}"#,
                                    conn_idx,
                                    msg_seq,
                                    current_time_micros()
                                ),
                            };
                            let payload_len = payload.len();
                            let sent_time = Instant::now();

                            if write.send(Message::Text(payload)).await.is_err() {
                                c_err.fetch_add(1, Ordering::Relaxed);
                                break;
                            }
                            c_sent.fetch_add(1, Ordering::Relaxed);
                            c_b_sent.fetch_add(payload_len, Ordering::Relaxed);

                            let reply = tokio::time::timeout(Duration::from_secs(5), read.next()).await;
                            match reply {
                                Ok(Some(Ok(msg))) => {
                                    let rtt = sent_time.elapsed();
                                    c_recv.fetch_add(1, Ordering::Relaxed);
                                    let len = match msg {
                                        Message::Text(ref t) => t.len(),
                                        Message::Binary(ref b) => b.len(),
                                        _ => 0,
                                    };
                                    c_b_recv.fetch_add(len, Ordering::Relaxed);
                                    c_rtt_dur.lock().await.push(rtt);
                                }
                                _ => {
                                    c_err.fetch_add(1, Ordering::Relaxed);
                                    break;
                                }
                            }

                            tokio::time::sleep(interval).await;
                        }
                    }
                    BenchmarkMode::Fanout => {
                        if is_publisher {
                            // Publisher connection
                            while !c_stop.load(Ordering::Relaxed) && Instant::now() < end_time {
                                if let Some(limit) = max_msgs {
                                    if msg_seq >= limit {
                                        break;
                                    }
                                }
                                msg_seq += 1;
                                let now_micros = current_time_micros();
                                let broadcast_payload = format!(
                                    r#"{{"_wsload_seq":{},"_wsload_ts_micros":{}}}"#,
                                    msg_seq, now_micros
                                );
                                let len = broadcast_payload.len();

                                if write.send(Message::Text(broadcast_payload)).await.is_err() {
                                    c_err.fetch_add(1, Ordering::Relaxed);
                                    break;
                                }
                                c_sent.fetch_add(1, Ordering::Relaxed);
                                c_b_sent.fetch_add(len, Ordering::Relaxed);

                                tokio::time::sleep(interval).await;
                            }
                        } else {
                            // Subscriber connection
                            while !c_stop.load(Ordering::Relaxed) && Instant::now() < end_time {
                                let frame_res = tokio::time::timeout(Duration::from_secs(10), read.next()).await;
                                match frame_res {
                                    Ok(Some(Ok(Message::Text(text)))) => {
                                        c_recv.fetch_add(1, Ordering::Relaxed);
                                        c_b_recv.fetch_add(text.len(), Ordering::Relaxed);

                                        if let Some(delay) = extract_fanout_delay(&text) {
                                            c_fo_dur.lock().await.push(delay);
                                        }
                                    }
                                    Ok(Some(Ok(Message::Binary(bin)))) => {
                                        c_recv.fetch_add(1, Ordering::Relaxed);
                                        c_b_recv.fetch_add(bin.len(), Ordering::Relaxed);
                                    }
                                    Ok(Some(Ok(Message::Close(_)))) | Ok(None) => {
                                        break;
                                    }
                                    _ => {
                                        c_err.fetch_add(1, Ordering::Relaxed);
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }

                c_active.fetch_sub(1, Ordering::Relaxed);
            });

            worker_handles.push(handle);
        }

        // Wait until benchmark duration completes
        let remaining = end_time.saturating_duration_since(Instant::now());
        if !remaining.is_zero() {
            tokio::time::sleep(remaining).await;
        }

        stop_flag.store(true, Ordering::SeqCst);
        let _ = reporter_handle.await;

        for h in worker_handles {
            h.abort();
        }

        let elapsed = benchmark_start.elapsed();
        let elapsed_secs = elapsed.as_secs_f64();

        let ok_handshakes = handshakes_ok.load(Ordering::Relaxed);
        let err_handshakes = handshakes_err.load(Ordering::Relaxed);
        let sent_msgs = messages_sent.load(Ordering::Relaxed);
        let recv_msgs = messages_recv.load(Ordering::Relaxed);
        let sent_b = bytes_sent.load(Ordering::Relaxed);
        let recv_b = bytes_recv.load(Ordering::Relaxed);
        let errs = total_errors.load(Ordering::Relaxed);

        let hs_durations = handshake_durations.lock().await;
        let hs_stats = compute_latency_stats(&hs_durations);

        let rtts = rtt_durations.lock().await;
        let rtt_stats = compute_latency_stats(&rtts);

        let fos = fanout_durations.lock().await;
        let fanout_stats = compute_latency_stats(&fos);

        let msgs_per_sec = if elapsed_secs > 0.0 {
            (sent_msgs + recv_msgs) as f64 / elapsed_secs
        } else {
            0.0
        };

        let bytes_per_sec = if elapsed_secs > 0.0 {
            (sent_b + recv_b) as f64 / elapsed_secs
        } else {
            0.0
        };

        Ok(BenchmarkResult {
            url: self.config.url.clone(),
            mode: self.config.mode,
            target_connections: total_connections,
            elapsed_secs,
            handshakes_ok: ok_handshakes,
            handshakes_err: err_handshakes,
            messages_sent: sent_msgs,
            messages_recv: recv_msgs,
            bytes_sent: sent_b,
            bytes_recv: recv_b,
            errors: errs,
            msgs_per_sec,
            bytes_per_sec,
            handshake_stats: hs_stats,
            rtt_stats,
            fanout_stats,
        })
    }
}

pub fn current_time_micros() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros()
}

pub fn extract_fanout_delay(text: &str) -> Option<Duration> {
    // Quick parse: find "_wsload_ts_micros":<num>
    let needle = "\"_wsload_ts_micros\":";
    if let Some(pos) = text.find(needle) {
        let after = &text[pos + needle.len()..];
        let digits: String = after
            .chars()
            .take_while(|c| c.is_ascii_digit())
            .collect();
        if let Ok(ts_micros) = digits.parse::<u128>() {
            let now = current_time_micros();
            let delta = now.saturating_sub(ts_micros);
            return Some(Duration::from_micros(delta as u64));
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_fanout_delay() {
        let now = current_time_micros() - 1500; // 1.5ms ago
        let json = format!(r#"{{"_wsload_seq":1,"_wsload_ts_micros":{}}}"#, now);
        let delay = extract_fanout_delay(&json).unwrap();
        assert!(delay.as_micros() >= 1500);
    }
}
