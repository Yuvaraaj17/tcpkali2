#![allow(unused_assignments)]

use crate::command::Config;
use crate::error::TcpKaliError;
use crate::stats::Stats;

use crossbeam_deque::Injector;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time;

/// Optimized error handling macro
/// Reduces repeated stats.is_shutting_down() && !config.quiet checks
macro_rules! log_error {
    ($stats:expr, $config:expr, $($arg:tt)*) => {
        if !$stats.is_shutting_down() && !$config.quiet {
            eprintln!($($arg)*);
        }
    };
}



/// TCP worker main function
///
/// # Arguments
/// * `target` - Target server address
/// * `config` - Configuration object
/// * `stats` - Statistics object
/// * `shutdown` - Shutdown signal receiver
///
/// # Returns
/// * `Result<(), TcpKaliError>` - Execution result
pub async fn tcp_worker(
    target: &str,
    config: Arc<Config>,
    stats: Arc<Stats>,
    shutdown: tokio::sync::broadcast::Receiver<()>,
) -> Result<(), TcpKaliError> {
    if config.pipeline {
        tcp_worker_pipeline(target, config, stats, shutdown).await
    } else {
        tcp_worker_pingpong(target, config, stats, shutdown).await
    }
}

/// TCP ping-pong mode worker
///
/// # Arguments
/// * `target` - Target server address
/// * `config` - Configuration object
/// * `stats` - Statistics object
/// * `shutdown` - Shutdown signal receiver
///
/// # Returns
/// * `Result<(), TcpKaliError>` - Execution result
pub async fn tcp_worker_pingpong(
    target: &str,
    config: Arc<Config>,
    stats: Arc<Stats>,
    mut shutdown: tokio::sync::broadcast::Receiver<()>,
) -> Result<(), TcpKaliError> {
    stats.total_connections.fetch_add(1, Ordering::Relaxed);

    // Connect to server
    let stream = match time::timeout(config.connect_timeout, TcpStream::connect(target)).await {
        Ok(Ok(stream)) => stream,
        Ok(Err(e)) => {
            log_error!(stats, config, "Failed to connect to {}: {}", target, e);
            stats.record_connection_error();
            return Ok(());
        }
        Err(_) => {
            log_error!(stats, config, "Connection timeout to {}", target);
            stats.record_connection_error();
            return Ok(());
        }
    };

    // Whether Nagle algorithm is enabled
    if config.nagle {
        stream.set_nodelay(false)?;
    }

    let (mut reader, mut writer) = stream.into_split();

    // Handle first message if configured
    if let Some(first_msg) = &config.first_message {
        let start = Instant::now();
        // Use write instead of write_all to reduce system calls
        match writer.write(first_msg).await {
            Ok(n) if n == first_msg.len() => { /* Successfully wrote complete message */ }
            Ok(_) => {
                if !stats.is_shutting_down() && !config.quiet {
                    eprintln!("Partial write of first message");
                }
                stats.record_connection_error();
                return Ok(());
            }
            Err(e) => {
                if !stats.is_shutting_down() && !config.quiet {
                    eprintln!("Failed to send first message: {}", e);
                }
                stats.record_connection_error();
                return Ok(());
            }
        }

        // Wait for first message response
        let mut response_buf = vec![0u8; first_msg.len()];
        match reader.read_exact(&mut response_buf).await {
            Ok(_) => {
                let latency = start.elapsed().as_micros() as u64;
                stats.record_latency(latency, 0);
                stats.record_request(first_msg.len(), first_msg.len());
            }
            Err(e) => {
                if !stats.is_shutting_down() && !config.quiet {
                    eprintln!("Failed to read first message response: {}", e);
                }
                stats.record_connection_error();
                return Ok(());
            }
        }
    }

    // Prepare payload for main benchmark
    let message = config.message.as_ref().expect("Message must be provided");

    let message_size = message.len();

    // Writer task
    let start_time = Instant::now();
    // Pre-calculate end time to avoid repeated calculation in loop
    // channel-lifetime starts after warmup ends
    let end_time = config.channel_lifetime.map(|lifetime| start_time + config.warmup_duration + lifetime);
    let mut counter = 0;
    // Time-based yield: yield every 10ms to avoid frequent modulo operations
    #[allow(unused_assignments)]
    let mut last_yield_time = Instant::now();
    const YIELD_INTERVAL: Duration = Duration::from_millis(10);

    // Pre-allocate buffer and reuse in loop to avoid repeated allocation
    let mut buf = vec![0u8; message_size];
    let expected_len = message_size;

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

                    // Record send time before actual write
                    let send_time = Instant::now();

                    // Perform write operation immediately
                    // Use write instead of write_all to reduce system calls overhead
                    match writer.write(message).await {
                        Ok(n) if n == message.len() => {
                            // Successfully wrote complete message
                        }
                        Ok(_) => {
                            if !stats.is_shutting_down() && !config.quiet {
                                eprintln!("Partial write");
                            }
                            stats.record_connection_error();
                            return;
                        }
                        Err(e) => {
                            if !stats.is_shutting_down() && !config.quiet {
                                eprintln!("Write error: {}", e);
                            }
                            stats.record_connection_error();
                            return;
                        }
                    }

                    counter += 1;

                    match reader.read_exact(&mut buf[..expected_len]).await {
                    Ok(n) if n == expected_len => {
                        // Cache elapsed result to avoid repeated calculation
                        let elapsed = send_time.elapsed();
                        let latency = elapsed.as_micros() as u64;
                        stats.record_latency(latency, counter);
                        stats.record_request(expected_len, expected_len);

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
                            // If rate == 0, no rate control and no message sending?
                            // Current loop continues to send messages, but user may expect no messages.
                            // For backward compatibility, we still send messages (may be empty).
                        }
                    }
                    Ok(_) => return, // Unexpected EOF
                    Err(e) => {
                        if !stats.is_shutting_down() && !config.quiet {
                            eprintln!("Read error: {}", e);
                        }
                        stats.record_connection_error();
                        return;
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
    drop(writer);
    stats.success_connections.fetch_add(1, Ordering::Relaxed);
    Ok(())
}

/// TCP pipeline mode worker
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
pub async fn tcp_worker_pipeline(
    target: &str,
    config: Arc<Config>,
    stats: Arc<Stats>,
    mut shutdown: tokio::sync::broadcast::Receiver<()>,
) -> Result<(), TcpKaliError> {
    stats.total_connections.fetch_add(1, Ordering::Relaxed);

    // Connect to server
    let stream = match time::timeout(config.connect_timeout, TcpStream::connect(target)).await {
        Ok(Ok(stream)) => stream,
        Ok(Err(e)) => {
            log_error!(stats, config, "Failed to connect to {}: {}", target, e);
            stats.record_connection_error();
            return Ok(());
        }
        Err(_) => {
            log_error!(stats, config, "Connection timeout to {}", target);
            stats.record_connection_error();
            return Ok(());
        }
    };

    // Whether Nagle algorithm is enabled
    if config.nagle {
        stream.set_nodelay(false)?;
    }

    let (mut reader, mut writer) = stream.into_split();
    // Use cross-thread Injector queue to implement pipeline mode
    let sent_times = Arc::new(Injector::<Instant>::new());

    // Handle first message if configured
    if let Some(first_msg) = &config.first_message {
        let start = Instant::now();
        // Use write instead of write_all to reduce system calls
        match writer.write(first_msg).await {
            Ok(n) if n == first_msg.len() => { /* Successfully wrote complete message */ }
            Ok(_) => {
                if !stats.is_shutting_down() && !config.quiet {
                    eprintln!("Partial write of first message");
                }
                stats.record_connection_error();
                return Ok(());
            }
            Err(e) => {
                if !stats.is_shutting_down() && !config.quiet {
                    eprintln!("Failed to send first message: {}", e);
                }
                stats.record_connection_error();
                return Ok(());
            }
        }

        // Wait for first message response
        let mut response_buf = vec![0u8; first_msg.len()];
        match reader.read_exact(&mut response_buf).await {
            Ok(_) => {
                let latency = start.elapsed().as_micros() as u64;
                stats.record_latency(latency, 0);
                stats.record_request(first_msg.len(), first_msg.len());
            }
            Err(e) => {
                if !stats.is_shutting_down() && !config.quiet {
                    eprintln!("Failed to read first message response: {}", e);
                }
                stats.record_connection_error();
                return Ok(());
            }
        }
    }

    // Prepare payload for main benchmark
    let message = config.message.as_ref().expect("Message must be provided");

    let message_size = message.len();

    // Spawn reader task for full-duplex operation
    let reader_stats = stats.clone();
    let reader_config = config.clone();
    let reader_sent_times = sent_times.clone();
    let reader_handle = tokio::spawn(async move {
        // Pre-allocate buffer and reuse in loop to avoid repeated allocation
        let mut buf = vec![0u8; message_size];
        let expected_len = message_size;
        let mut counter: usize = 0;

        loop {
            tokio::select! {
                result = reader.read_exact(&mut buf[..expected_len]) => {
                    match result {
                        Ok(n) if n == expected_len => {
                            counter += 1;
                            // Use Injector's steal method to get elements
                            if let crossbeam_deque::Steal::Success(sent_time) = reader_sent_times.steal() {
                                let latency = sent_time.elapsed().as_micros() as u64;
                                reader_stats.record_request(expected_len, expected_len);
                                reader_stats.record_latency(latency, counter);
                            }
                        }
                        Ok(_) => break, // Unexpected EOF
                        Err(e) => {
                            if !reader_stats.is_shutting_down() && !reader_config.quiet {
                                eprintln!("Read error: {}", e);
                            }
                            reader_stats.record_connection_error();
                            break;
                        }
                    }
                }
                _ = tokio::signal::ctrl_c() => {
                    // Handle shutdown signal
                    break;
                }
            }
        }
    });

    // Writer task
    let start_time = Instant::now();
    // Pre-calculate end time to avoid repeated calculation in loop
    // channel-lifetime starts after warmup ends
    let end_time = config.channel_lifetime.map(|lifetime| start_time + config.warmup_duration + lifetime);
    let mut counter = 0;
    // Time-based yield: yield every 10ms to avoid frequent modulo operations
    #[allow(unused_assignments)]
    let mut last_yield_time = Instant::now();
    const YIELD_INTERVAL: Duration = Duration::from_millis(10);

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

                    // Record send time before actual write
                    let send_time = Instant::now();
                    sent_times.push(send_time);

                    // Perform write operation immediately
                    // Use write instead of write_all to reduce system calls overhead
                    match writer.write(message).await {
                        Ok(n) if n == message.len() => {
                            // Successfully wrote complete message
                        }
                        Ok(_) => {
                            if !stats.is_shutting_down() && !config.quiet {
                                eprintln!("Partial write");
                            }
                            stats.record_connection_error();
                            return;
                        }
                        Err(e) => {
                            if !stats.is_shutting_down() && !config.quiet {
                                eprintln!("Write error: {}", e);
                            }
                            stats.record_connection_error();
                            return;
                        }
                    }

                    counter += 1;
                    // Time-based yield: check every 10ms if yield is needed
                    if last_yield_time.elapsed() >= YIELD_INTERVAL {
                        tokio::task::yield_now().await;
                        last_yield_time = Instant::now();
                    }

                    if let Some(rate) = config.message_rate {
                        if rate > 0 {
                            let target_duration = Duration::from_secs_f64(1.0 / rate as f64);
                            let elapsed = send_time.elapsed();
                            if elapsed < target_duration {
                                time::sleep(target_duration - elapsed).await;
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
    drop(writer);
    let _ = reader_handle.await;

    stats.success_connections.fetch_add(1, Ordering::Relaxed);
    Ok(())
}
