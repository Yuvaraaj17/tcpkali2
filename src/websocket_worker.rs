#![allow(unused_assignments)]

use crate::command::Config;
use crate::error::TcpKaliError;
use crate::stats::Stats;

use crossbeam_deque::Injector;
use futures::{SinkExt, StreamExt};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};
use tokio::time;
use tokio_tungstenite::connect_async_with_config;
use tungstenite::Message;

/// Optimized error handling macro
/// Reduces repeated stats.is_shutting_down() && !config.quiet checks
macro_rules! log_error {
    ($stats:expr, $config:expr, $($arg:tt)*) => {
        if !$stats.is_shutting_down() && !$config.quiet {
            eprintln!($($arg)*);
        }
    };
}

/// WebSocket worker main function
///
/// # Arguments
/// * `target` - Target server address
/// * `config` - Configuration object
/// * `stats` - Statistics object
/// * `shutdown` - Shutdown signal receiver
///
/// # Returns
/// * `Result<(), TcpKaliError>` - Execution result
pub async fn websocket_worker(
    target: &str,
    config: Arc<Config>,
    stats: Arc<Stats>,
    shutdown: tokio::sync::broadcast::Receiver<()>,
) -> Result<(), TcpKaliError> {
    if config.pipeline {
        websocket_worker_pipeline(target, config, stats, shutdown).await
    } else {
        websocket_worker_pingpong(target, config, stats, shutdown).await
    }
}

/// WebSocket ping-pong mode worker
///
/// Implements request-response pattern where each message expects a response.
///
/// # Arguments
/// * `target` - Target server address
/// * `config` - Configuration object
/// * `stats` - Statistics object
/// * `shutdown` - Shutdown signal receiver
///
/// # Returns
/// * `Result<(), TcpKaliError>` - Execution result
pub async fn websocket_worker_pingpong(
    target: &str,
    config: Arc<Config>,
    stats: Arc<Stats>,
    mut shutdown: tokio::sync::broadcast::Receiver<()>,
) -> Result<(), TcpKaliError> {
    stats.total_connections.fetch_add(1, Ordering::Relaxed);

    // Build WebSocket URL - add ws:// prefix if not already present
    let ws_url = if target.starts_with("ws://") || target.starts_with("wss://") {
        target.to_string()
    } else {
        format!("ws://{}", target)
    };

    // Connect to WebSocket server
    let ws_stream = match time::timeout(
        config.connect_timeout,
        connect_async_with_config(&ws_url, None, !config.nagle),
    )
    .await
    {
        Ok(Ok(ws)) => ws.0,
        Ok(Err(e)) => {
            log_error!(stats, config, "Failed to connect to WebSocket {}: {}", ws_url, e);
            stats.record_connection_error();
            return Ok(());
        }
        Err(_) => {
            log_error!(stats, config, "WebSocket connection timeout to {}", ws_url);
            stats.record_connection_error();
            return Ok(());
        }
    };

    let (mut write, mut read) = ws_stream.split();

    // Handle first message if configured
    if let Some(first_msg) = &config.first_message {
        let start = Instant::now();
        if let Err(e) = write.send(Message::Binary(first_msg.clone())).await {
            if !stats.is_shutting_down() && !config.quiet {
                eprintln!("Failed to send first WebSocket message: {}", e);
            }
            stats.record_connection_error();
            return Ok(());
        }

        // Wait for first message response
        match read.next().await {
            Some(Ok(Message::Binary(data))) => {
                let latency = start.elapsed().as_micros() as u64;
                stats.record_latency(latency, 0);
                stats.record_request(first_msg.len(), data.len());
            }
            Some(Err(e)) => {
                if !stats.is_shutting_down() && !config.quiet {
                    eprintln!("Failed to read first message response: {}", e);
                }
                stats.record_connection_error();
                return Ok(());
            }
            _ => {
                if !stats.is_shutting_down() && !config.quiet {
                    eprintln!("Unexpected response to first message");
                }
                stats.record_connection_error();
                return Ok(());
            }
        }
    }

    // Prepare payload for main benchmark
    let message = config.message.as_ref().expect("Message must be provided");
    // Pre-create Message object and share with Arc to avoid repeated creation in loop
    let shared_message = std::sync::Arc::new(Message::Binary(message.clone()));

    // Writer task
    let start_time = Instant::now();
    // Pre-calculate end time to avoid repeated calculation in loop
    // channel-lifetime starts after warmup ends
    let end_time = config.channel_lifetime.map(|lifetime| start_time + config.warmup_duration + lifetime);
    let mut counter = 0;

    // If message rate is 0 or message size is 0, only wait without sending messages
    if config.message_rate == Some(0) || message.len() == 0 {
        // Wait loop: only check connection lifetime and shutdown signal
        loop {
            if let Some(end) = end_time {
                if Instant::now() >= end {
                    break;
                }
            }

            tokio::select! {
                _ = tokio::time::sleep(Duration::from_millis(100)) => {
                    // Brief sleep to avoid busy waiting
                    continue;
                }
                _ = shutdown.recv() => break,
            }
        }
    } else {
        // Original message sending loop
        loop {
            if let Some(end) = end_time {
                if Instant::now() >= end {
                    break;
                }
            }

            tokio::select! {
                    _ = async {
                        if stats.is_shutting_down() {
                            return;
                        }
                        // Time-based yield: yield every 10ms
                        #[allow(unused_assignments)]
                        let mut last_yield_time = Instant::now();
                        const YIELD_INTERVAL: Duration = Duration::from_millis(10);

                        // Record send time before actual write
                        let send_time = Instant::now();

                        // Perform write operation - clone Message object using Arc
                        if let Err(e) = write.send((*shared_message).clone()).await {
                            if !stats.is_shutting_down() && !config.quiet {
                                eprintln!("WebSocket send error: {}", e);
                            }
                            stats.record_connection_error();
                            return;
                        }

                        counter += 1;

                        if let Some(msg) = read.next().await {
                            match msg {
                                Ok(Message::Binary(data)) => {
                                    // Cache elapsed result to avoid repeated calculation
                                    let elapsed = send_time.elapsed();
                                    let latency = elapsed.as_micros() as u64;
                                    stats.record_latency(latency, counter);
                                    stats.record_request(message.len(), data.len());

                                    // Optimize message rate control: use cached elapsed result
                                    if let Some(rate) = config.message_rate {
                                        if rate > 0 {
                                            // Pre-calculate target interval (nanosecond precision)
                                            const NANOS_PER_SEC: u64 = 1_000_000_000;
                                            let target_interval_ns = NANOS_PER_SEC / rate;
                                            let elapsed_ns = elapsed.as_nanos() as u64;

                                            if elapsed_ns < target_interval_ns {
                                                let sleep_ns = target_interval_ns - elapsed_ns;
                                                if sleep_ns > 1_000_000 { // Only sleep when exceeding 1ms
                                                    time::sleep(Duration::from_nanos(sleep_ns)).await;
                                                }
                                            }
                                        }
                                        // If rate == 0, no rate control
                                    }
                                }
                                Ok(_) => {}
                                Err(e) => {
                                    if !stats.is_shutting_down() && !config.quiet {
                                        eprintln!("WebSocket receive error: {}", e);
                                    }
                                    stats.record_connection_error();
                                    return;
                                }
                            }
                        } else {
                            // If no response received, still need to check message rate control
                            if let Some(rate) = config.message_rate {
                                if rate > 0 {
                                    let elapsed = send_time.elapsed();
                                    const NANOS_PER_SEC: u64 = 1_000_000_000;
                                    let target_interval_ns = NANOS_PER_SEC / rate;
                                    let elapsed_ns = elapsed.as_nanos() as u64;

                                    if elapsed_ns < target_interval_ns {
                                        let sleep_ns = target_interval_ns - elapsed_ns;
                                        if sleep_ns > 1_000_000 {
                                            time::sleep(Duration::from_nanos(sleep_ns)).await;
                                        }
                                    }
                                }
                                // If rate == 0, no rate control
                            }
                        }

                        // Time-based yield: check every 10ms if yield is needed
                        if last_yield_time.elapsed() >= YIELD_INTERVAL {
                            tokio::task::yield_now().await;
                            last_yield_time = Instant::now();
                        }

                } => {},
                _ = shutdown.recv() => break,
            }
        }
    }
    // Clean up reader task
    let _ = write.send(Message::Close(None)).await;
    drop(write);
    stats.success_connections.fetch_add(1, Ordering::Relaxed);
    Ok(())
}

/// WebSocket pipeline mode worker
///
/// Implements pipeline pattern where multiple messages can be sent without waiting for responses.
///
/// # Arguments
/// * `target` - Target server address
/// * `config` - Configuration object
/// * `stats` - Statistics object
/// * `shutdown` - Shutdown signal receiver
///
/// # Returns
/// * `Result<(), TcpKaliError>` - Execution result
pub async fn websocket_worker_pipeline(
    target: &str,
    config: Arc<Config>,
    stats: Arc<Stats>,
    mut shutdown: tokio::sync::broadcast::Receiver<()>,
) -> Result<(), TcpKaliError> {
    stats.total_connections.fetch_add(1, Ordering::Relaxed);

    // Build WebSocket URL - add ws:// prefix if not already present
    let ws_url = if target.starts_with("ws://") || target.starts_with("wss://") {
        target.to_string()
    } else {
        format!("ws://{}", target)
    };

    // Connect to WebSocket server
    let ws_stream = match time::timeout(
        config.connect_timeout,
        connect_async_with_config(&ws_url, None, !config.nagle),
    )
    .await
    {
        Ok(Ok(ws)) => ws.0,
        Ok(Err(e)) => {
            log_error!(stats, config, "Failed to connect to WebSocket {}: {}", ws_url, e);
            stats.record_connection_error();
            return Ok(());
        }
        Err(_) => {
            log_error!(stats, config, "WebSocket connection timeout to {}", ws_url);
            stats.record_connection_error();
            return Ok(());
        }
    };

    let (mut write, mut read) = ws_stream.split();
    // Use cross-thread Injector queue to implement pipeline mode
    let sent_times = Arc::new(Injector::<Instant>::new());

    // Handle first message if configured
    if let Some(first_msg) = &config.first_message {
        let start = Instant::now();
        if let Err(e) = write.send(Message::Binary(first_msg.clone())).await {
            if !stats.is_shutting_down() && !config.quiet {
                eprintln!("Failed to send first WebSocket message: {}", e);
            }
            stats.record_connection_error();
            return Ok(());
        }

        // Wait for first message response
        match read.next().await {
            Some(Ok(Message::Binary(data))) => {
                let latency = start.elapsed().as_micros() as u64;
                stats.record_latency(latency, 0);
                stats.record_request(first_msg.len(), data.len());
            }
            Some(Err(e)) => {
                if !stats.is_shutting_down() && !config.quiet {
                    eprintln!("Failed to read first message response: {}", e);
                }
                stats.record_connection_error();
                return Ok(());
            }
            _ => {
                if !stats.is_shutting_down() && !config.quiet {
                    eprintln!("Unexpected response to first message");
                }
                stats.record_connection_error();
                return Ok(());
            }
        }
    }

    // Spawn reader task for full-duplex operation
    let reader_stats = stats.clone();
    let reader_config = config.clone();
    let reader_sent_times = sent_times.clone();
    let mut counter: usize = 0;

    let reader_handle = tokio::spawn(async move {
        while let Some(msg) = read.next().await {
            match msg {
                Ok(Message::Binary(data)) => {
                    counter += 1;
                    if let crossbeam_deque::Steal::Success(sent_time) = reader_sent_times.steal() {
                        let latency = sent_time.elapsed().as_micros() as u64;
                        reader_stats.record_latency(latency, counter);
                        reader_stats.record_request(reader_config.message_size, data.len());
                    }
                }
                Ok(_) => continue, // Ignore non-binary messages
                Err(e) => {
                    if !reader_stats.is_shutting_down() && !reader_config.quiet {
                        eprintln!("WebSocket receive error: {}", e);
                    }
                    reader_stats.record_connection_error();
                    break;
                }
            }
        }
    });

    // Prepare payload for main benchmark
    let message = config.message.as_ref().expect("Message must be provided");
    // Pre-create Message object and share with Arc to avoid repeated creation in loop
    let shared_message = std::sync::Arc::new(Message::Binary(message.clone()));

    // Writer task
    let start_time = Instant::now();
    // Pre-calculate end time to avoid repeated calculation in loop
    // channel-lifetime starts after warmup ends
    let end_time = config.channel_lifetime.map(|lifetime| start_time + config.warmup_duration + lifetime);
    let mut counter = 0;

    // If message rate is 0 or message size is 0, only wait without sending messages
    if config.message_rate == Some(0) || message.len() == 0 {
        // Wait loop: only check connection lifetime and shutdown signal
        loop {
            if let Some(end) = end_time {
                if Instant::now() >= end {
                    break;
                }
            }

            tokio::select! {
                _ = tokio::time::sleep(Duration::from_millis(100)) => {
                    // Brief sleep to avoid busy waiting
                    continue;
                }
                _ = shutdown.recv() => break,
            }
        }
    } else {
        // Original message sending loop
        loop {
            if let Some(end) = end_time {
                if Instant::now() >= end {
                    break;
                }
            }

            tokio::select! {
                _ = async {
                    if stats.is_shutting_down() {
                        return;
                    }
                    // Time-based yield: yield every 10ms
                    #[allow(unused_assignments)]
                    let mut last_yield_time = Instant::now();
                    const YIELD_INTERVAL: Duration = Duration::from_millis(10);

                    // Record send time before actual write
                    let send_time = Instant::now();
                    sent_times.push(send_time);

                    // Perform write operation - clone Message object using Arc
                    if let Err(e) = write.send((*shared_message).clone()).await {
                        if !stats.is_shutting_down() && !config.quiet {
                            eprintln!("WebSocket send error: {}", e);
                        }
                        stats.record_connection_error();
                        return;
                    }

                    counter += 1;
                    // Time-based yield: check every 10ms if yield is needed
                    if last_yield_time.elapsed() >= YIELD_INTERVAL {
                        tokio::task::yield_now().await;
                        last_yield_time = Instant::now();
                    }

                    // Optimize message rate control: pre-calculate target interval, reduce repeated calculation
                    if let Some(rate) = config.message_rate {
                        if rate > 0 {
                            // Pre-calculate target interval (nanosecond precision)
                            const NANOS_PER_SEC: u64 = 1_000_000_000;
                            let target_interval_ns = NANOS_PER_SEC / rate;
                            let elapsed_ns = send_time.elapsed().as_nanos() as u64;

                            if elapsed_ns < target_interval_ns {
                                let sleep_ns = target_interval_ns - elapsed_ns;
                                if sleep_ns > 1_000_000 { // Only sleep when exceeding 1ms
                                    time::sleep(Duration::from_nanos(sleep_ns)).await;
                                }
                            }
                        }
                        // If rate == 0, no rate control
                    }
                } => {},
                _ = shutdown.recv() => break,
            }
        }
    }

    // Clean up reader task
    let _ = write.send(Message::Close(None)).await;
    drop(write);
    let _ = reader_handle.await;

    stats.success_connections.fetch_add(1, Ordering::Relaxed);
    Ok(())
}
