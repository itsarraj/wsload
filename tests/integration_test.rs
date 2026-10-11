use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, Mutex};
use tokio_tungstenite::tungstenite::Message;
use wsload::{BenchmarkConfig, BenchmarkMode, BenchmarkRunner};

async fn spawn_echo_server() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            tokio::spawn(async move {
                if let Ok(mut ws) = tokio_tungstenite::accept_async(stream).await {
                    while let Some(Ok(msg)) = ws.next().await {
                        match msg {
                            Message::Text(t) => {
                                if ws.send(Message::Text(t)).await.is_err() {
                                    break;
                                }
                            }
                            Message::Ping(p) => {
                                if ws.send(Message::Pong(p)).await.is_err() {
                                    break;
                                }
                            }
                            Message::Close(_) => break,
                            _ => {}
                        }
                    }
                }
            });
        }
    });

    addr
}

async fn spawn_broadcast_server() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let clients = Arc::new(Mutex::new(HashMap::<usize, mpsc::UnboundedSender<Message>>::new()));
    let next_id = Arc::new(AtomicUsize::new(1));

    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let id = next_id.fetch_add(1, Ordering::Relaxed);
            let clients = Arc::clone(&clients);

            tokio::spawn(async move {
                if let Ok(ws) = tokio_tungstenite::accept_async(stream).await {
                    let (mut write, mut read) = ws.split();
                    let (tx, mut rx) = mpsc::unbounded_channel::<Message>();

                    clients.lock().await.insert(id, tx);

                    // Writer task for this client
                    let write_task = tokio::spawn(async move {
                        while let Some(msg) = rx.recv().await {
                            if write.send(msg).await.is_err() {
                                break;
                            }
                        }
                    });

                    // Reader task: broadcast to all other clients
                    while let Some(Ok(msg)) = read.next().await {
                        if matches!(msg, Message::Close(_)) {
                            break;
                        }
                        if matches!(msg, Message::Text(_)) {
                            let peer_list: Vec<mpsc::UnboundedSender<Message>> = {
                                let guard = clients.lock().await;
                                guard
                                    .iter()
                                    .filter(|(&cid, _)| cid != id)
                                    .map(|(_, s)| s.clone())
                                    .collect()
                            };
                            for peer in peer_list {
                                let _ = peer.send(msg.clone());
                            }
                        }
                    }

                    clients.lock().await.remove(&id);
                    write_task.abort();
                }
            });
        }
    });

    addr
}

#[tokio::test]
async fn test_load_echo_mode() {
    let addr = spawn_echo_server().await;
    let url = format!("ws://{addr}");

    let config = BenchmarkConfig {
        url,
        connections: 8,
        duration: Duration::from_millis(1200),
        ramp_duration: Duration::from_millis(100),
        mode: BenchmarkMode::Echo,
        interval: Duration::from_millis(80),
        payload: None,
        max_messages: None,
        quiet: true,
    };

    let runner = BenchmarkRunner::new(config);
    let result = runner.run().await.unwrap();

    assert_eq!(result.target_connections, 8);
    assert_eq!(result.handshakes_ok, 8);
    assert_eq!(result.handshakes_err, 0);
    assert!(result.messages_sent > 0);
    assert_eq!(result.messages_sent, result.messages_recv);
    assert!(result.rtt_stats.is_some());
    let rtt = result.rtt_stats.unwrap();
    assert!(rtt.min_ms >= 0.0);
    assert!(rtt.mean_ms >= 0.0);
}

#[tokio::test]
async fn test_load_ping_mode() {
    let addr = spawn_echo_server().await;
    let url = format!("ws://{addr}");

    let config = BenchmarkConfig {
        url,
        connections: 5,
        duration: Duration::from_millis(1000),
        ramp_duration: Duration::ZERO,
        mode: BenchmarkMode::Ping,
        interval: Duration::from_millis(100),
        payload: None,
        max_messages: None,
        quiet: true,
    };

    let runner = BenchmarkRunner::new(config);
    let result = runner.run().await.unwrap();

    assert_eq!(result.handshakes_ok, 5);
    assert_eq!(result.handshakes_err, 0);
    assert!(result.messages_sent > 0);
    assert_eq!(result.messages_sent, result.messages_recv);
    assert!(result.rtt_stats.is_some());
}

#[tokio::test]
async fn test_load_fanout_mode() {
    let addr = spawn_broadcast_server().await;
    let url = format!("ws://{addr}");

    let config = BenchmarkConfig {
        url,
        connections: 6, // 1 publisher + 5 subscribers
        duration: Duration::from_millis(1500),
        ramp_duration: Duration::from_millis(150),
        mode: BenchmarkMode::Fanout,
        interval: Duration::from_millis(100),
        payload: None,
        max_messages: None,
        quiet: true,
    };

    let runner = BenchmarkRunner::new(config);
    let result = runner.run().await.unwrap();

    assert_eq!(result.handshakes_ok, 6);
    assert_eq!(result.handshakes_err, 0);
    assert!(result.messages_sent > 0);
    assert!(result.messages_recv > 0);
    assert!(result.fanout_stats.is_some());

    let fo = result.fanout_stats.unwrap();
    assert!(fo.count > 0);
    assert!(fo.min_ms >= 0.0);
}

#[tokio::test]
async fn test_load_unreachable_endpoint() {
    let config = BenchmarkConfig {
        url: "ws://127.0.0.1:49876".to_string(), // Unbound port
        connections: 4,
        duration: Duration::from_millis(500),
        ramp_duration: Duration::ZERO,
        mode: BenchmarkMode::Ping,
        interval: Duration::from_millis(100),
        payload: None,
        max_messages: None,
        quiet: true,
    };

    let runner = BenchmarkRunner::new(config);
    let result = runner.run().await.unwrap();

    assert_eq!(result.handshakes_ok, 0);
    assert_eq!(result.handshakes_err, 4);
    assert_eq!(result.messages_sent, 0);
}
