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

/// 优化的错误处理宏
/// 减少重复的 stats.is_shutting_down() && !config.quiet 检查
macro_rules! log_error {
    ($stats:expr, $config:expr, $($arg:tt)*) => {
        if !$stats.is_shutting_down() && !$config.quiet {
            eprintln!($($arg)*);
        }
    };
}

/// WebSocket 工作线程主函数
/// WebSocket worker main function
///
/// # Arguments
/// * `target` - 目标服务器地址 / Target server address
/// * `config` - 配置对象 / Configuration object
/// * `stats` - 统计对象 / Statistics object
/// * `shutdown` - 关闭信号接收器 / Shutdown signal receiver
///
/// # Returns
/// * `Result<(), TcpKaliError>` - 执行结果 / Execution result
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
    // 预创建Message对象并使用Arc共享，避免循环中重复创建
    let shared_message = std::sync::Arc::new(Message::Binary(message.clone()));

    // Writer task
    let start_time = Instant::now();
    // 预计算结束时间，避免循环中重复计算
    let end_time = config.channel_lifetime.map(|lifetime| start_time + lifetime);
    let mut counter = 0;

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
                    // 基于时间的yield：每10ms yield一次
                    #[allow(unused_assignments)]
                    let mut last_yield_time = Instant::now();
                    const YIELD_INTERVAL: Duration = Duration::from_millis(10);

                    // Record send time before actual write
                    let send_time = Instant::now();

                    // Perform write operation - 使用Arc克隆Message对象
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
                                // 缓存elapsed结果，避免重复计算
                                let elapsed = send_time.elapsed();
                                let latency = elapsed.as_micros() as u64;
                                stats.record_latency(latency, counter);
                                stats.record_request(message.len(), data.len());
                                
                                // 优化消息速率控制：使用缓存的elapsed结果
                                if let Some(rate) = config.message_rate {
                                    // 预计算目标间隔（纳秒精度）
                                    const NANOS_PER_SEC: u64 = 1_000_000_000;
                                    let target_interval_ns = NANOS_PER_SEC / rate;
                                    let elapsed_ns = elapsed.as_nanos() as u64;
                                    
                                    if elapsed_ns < target_interval_ns {
                                        let sleep_ns = target_interval_ns - elapsed_ns;
                                        if sleep_ns > 1_000_000 { // 只sleep超过1ms的情况
                                            time::sleep(Duration::from_nanos(sleep_ns)).await;
                                        }
                                    }
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
                        // 如果没有收到响应，仍然需要检查消息速率控制
                        if let Some(rate) = config.message_rate {
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
                    }

                    // 基于时间的yield：每10ms检查一次是否需要yield
                    if last_yield_time.elapsed() >= YIELD_INTERVAL {
                        tokio::task::yield_now().await;
                        last_yield_time = Instant::now();
                    }

            } => {},
            _ = shutdown.recv() => break,
        }
    }
    // Clean up reader task
    let _ = write.send(Message::Close(None)).await;
    drop(write);
    stats.success_connections.fetch_add(1, Ordering::Relaxed);
    Ok(())
}

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
    // 使用可跨线程使用的Injector队列实现pipeline模式
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
    // 预创建Message对象并使用Arc共享，避免循环中重复创建
    let shared_message = std::sync::Arc::new(Message::Binary(message.clone()));

    // Writer task
    let start_time = Instant::now();
    // 预计算结束时间，避免循环中重复计算
    let end_time = config.channel_lifetime.map(|lifetime| start_time + lifetime);
    let mut counter = 0;

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
                // 基于时间的yield：每10ms yield一次
                #[allow(unused_assignments)]
                let mut last_yield_time = Instant::now();
                const YIELD_INTERVAL: Duration = Duration::from_millis(10);

                // Record send time before actual write
                let send_time = Instant::now();
                sent_times.push(send_time);

                // Perform write operation - 使用Arc克隆Message对象
                if let Err(e) = write.send((*shared_message).clone()).await {
                    if !stats.is_shutting_down() && !config.quiet {
                        eprintln!("WebSocket send error: {}", e);
                    }
                    stats.record_connection_error();
                    return;
                }

                counter += 1;
                // 基于时间的yield：每10ms检查一次是否需要yield
                if last_yield_time.elapsed() >= YIELD_INTERVAL {
                    tokio::task::yield_now().await;
                    last_yield_time = Instant::now();
                }

                // 优化消息速率控制：预计算目标间隔，减少重复计算
                if let Some(rate) = config.message_rate {
                    // 预计算目标间隔（纳秒精度）
                    const NANOS_PER_SEC: u64 = 1_000_000_000;
                    let target_interval_ns = NANOS_PER_SEC / rate;
                    let elapsed_ns = send_time.elapsed().as_nanos() as u64;
                    
                    if elapsed_ns < target_interval_ns {
                        let sleep_ns = target_interval_ns - elapsed_ns;
                        if sleep_ns > 1_000_000 { // 只sleep超过1ms的情况
                            time::sleep(Duration::from_nanos(sleep_ns)).await;
                        }
                    }
                }
            } => {},
            _ = shutdown.recv() => break,
        }
    }

    // Clean up reader task
    let _ = write.send(Message::Close(None)).await;
    drop(write);
    let _ = reader_handle.await;

    stats.success_connections.fetch_add(1, Ordering::Relaxed);
    Ok(())
}
