use std::time::Duration;
use clap::Parser;
use wsload::{BenchmarkConfig, BenchmarkMode, BenchmarkRunner};

#[derive(Parser, Debug)]
#[command(
    name = "wsload",
    about = "High-concurrency WebSocket load tester and broadcast fan-out latency benchmark",
    version
)]
struct Args {
    /// WebSocket URL to benchmark (e.g. ws://127.0.0.1:8080 or wss://echo.websocket.org)
    url: String,

    /// Number of concurrent WebSocket connections to maintain
    #[arg(short = 'c', long, default_value_t = 50)]
    connections: usize,

    /// Benchmark test duration in seconds
    #[arg(short = 'd', long, default_value_t = 10)]
    duration: u64,

    /// Gradual ramp-up duration for opening connections (in milliseconds)
    #[arg(long, default_value_t = 0)]
    ramp_ms: u64,

    /// Benchmark mode: echo (round-trip message test), ping (RFC 6455 Ping/Pong RTT), or fanout (1 publisher broadcasts to N-1 subscribers)
    #[arg(short = 'm', long, value_enum, default_value = "echo")]
    mode: BenchmarkMode,

    /// Interval between messages per connection in milliseconds
    #[arg(short = 'i', long, default_value_t = 1000)]
    interval_ms: u64,

    /// Custom payload string to send in echo mode (use {{seq}} for sequential counter)
    #[arg(short = 'p', long)]
    payload: Option<String>,

    /// Maximum messages per connection before remaining idle
    #[arg(long)]
    max_messages: Option<usize>,

    /// Output results as JSON for CI or automation
    #[arg(long)]
    json: bool,

    /// Quiet mode: suppress live progress updates
    #[arg(short, long)]
    quiet: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let config = BenchmarkConfig {
        url: args.url,
        connections: args.connections,
        duration: Duration::from_secs(args.duration),
        ramp_duration: Duration::from_millis(args.ramp_ms),
        mode: args.mode,
        interval: Duration::from_millis(args.interval_ms),
        payload: args.payload,
        max_messages: args.max_messages,
        quiet: args.quiet || args.json,
    };

    let runner = BenchmarkRunner::new(config);
    let result = runner.run().await?;

    if args.json {
        println!("{}", serde_json::to_string_pretty(&result)?);
    } else {
        result.print_summary();
    }

    Ok(())
}
