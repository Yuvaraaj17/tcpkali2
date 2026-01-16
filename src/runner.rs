//! 负载测试运行器模块
//! 
//! 负责管理异步任务的执行、统计收集和结果报告

use crate::command::Config;
use crate::error::TcpKaliError;
use crate::stats::Stats;
use crate::tcp_worker::tcp_worker;
use crate::websocket_worker::websocket_worker;

use futures::StreamExt;
use futures::stream::FuturesUnordered;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};
use tokio::time;

/// 异步主函数，负责执行负载测试
/// 
/// # 参数
/// - `matches`: 命令行参数匹配结果
/// 
/// # 返回
/// - `Result<(), TcpKaliError>`: 执行结果
pub async fn async_main(matches: clap::ArgMatches) -> Result<(), TcpKaliError> {
    let config = crate::command::parse_config(&matches);
    let stats = Arc::new(Stats::new());
    let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel(1);

    // 定时输出实时统计数据
    if !config.quiet {
        spawn_stats_printer(stats.clone(), shutdown_rx.resubscribe());
    }

    let mut tasks = FuturesUnordered::new();

    // 优化连接速率控制：预计算延迟值，避免每个任务重复计算
    let connect_delay = if config.connect_rate > 0 {
        Some(Duration::from_secs(1) / config.connect_rate as u32)
    } else {
        None
    };

    // 准备任务
    for target in matches.get_many::<String>("host:port").unwrap() {
        for i in 0..config.connections {
            let task_config = config.clone();
            let task_stats = stats.clone();
            let target = target.clone();
            let shutdown = shutdown_tx.subscribe();
            let connect_delay = connect_delay.clone();

            tasks.push(tokio::spawn(async move {
                // 应用连接速率控制
                if let Some(delay) = connect_delay {
                    // 使用索引来错开连接时间，避免所有连接同时开始
                    let staggered_delay = delay * (i % task_config.connect_rate.max(1) as u64) as u32;
                    time::sleep(staggered_delay).await;
                }

                if task_config.use_websocket {
                    websocket_worker(&target, task_config, task_stats, shutdown).await
                } else {
                    tcp_worker(&target, task_config, task_stats, shutdown).await
                }
            }));
        }
    }

    // 热身阶段
    warmup_phase(&config, &stats).await;

    let start_time = Instant::now();

    // 执行压力测试
    execute_benchmark(&config, &stats, &shutdown_tx, &mut tasks).await;

    // 输出统计结果
    Stats::print_final_stats(&stats, start_time.elapsed(), !config.quiet);
    Ok(())
}

/// 生成统计信息打印任务
fn spawn_stats_printer(stats: Arc<Stats>, mut shutdown_rx: tokio::sync::broadcast::Receiver<()>) {
    tokio::spawn(async move {
        // 等待热身结束
        while stats.is_warmup() {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        
        // 热身结束后等待1秒，让压测有足够时间开始
        tokio::time::sleep(Duration::from_secs(1)).await;
        
        // 重置统计打印时间，避免热身结束后的第一次统计出现NaN
        let now = crate::utils::unix_timestamp_millis();
        stats.last_print_time.store(now, std::sync::atomic::Ordering::Relaxed);
        stats.last_print_count.store(stats.total_requests.load(std::sync::atomic::Ordering::Relaxed), std::sync::atomic::Ordering::Relaxed);
        
        let mut interval = time::interval(Duration::from_millis(1000));
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let qps = stats.get_qps();
                    let current_count = stats.total_requests.load(Ordering::Relaxed);

                    let hist = stats.latency_histogram.lock();
                    // 检查直方图是否有数据
                    let p50 = if hist.len() > 0 { hist.value_at_percentile(50.0) } else { 0 };
                    let p95 = if hist.len() > 0 { hist.value_at_percentile(95.0) } else { 0 };
                    let p99 = if hist.len() > 0 { hist.value_at_percentile(99.0) } else { 0 };
                    
                    println!(
                        "[Live] QPS: {:.0} | Req: {} | Latency(us): P50={} P95={} P99={}",
                        qps,
                        current_count,
                        p50,
                        p95,
                        p99
                    );
                }
                _ = shutdown_rx.recv() => break,
            }
        }
    });
}

/// 热身阶段处理
async fn warmup_phase(config: &Config, stats: &Arc<Stats>) {
    if !config.quiet {
        println!(
            "Warming up for {} seconds...",
            config.warmup_duration.as_secs()
        );
    }
    // 等待热身完成
    time::sleep(config.warmup_duration).await;
    // 重置统计数据
    stats.end_warmup();
    // 输出日志
    if !config.quiet {
        println!(
            "Warmup completed. Starting benchmark for {} seconds...",
            config.duration.as_secs()
        );
    }
}

/// 执行基准测试
async fn execute_benchmark(
    config: &Config,
    stats: &Arc<Stats>,
    shutdown_tx: &tokio::sync::broadcast::Sender<()>,
    tasks: &mut FuturesUnordered<tokio::task::JoinHandle<Result<(), TcpKaliError>>>,
) {
    // 等待压力测试完成
    time::sleep(config.duration).await;
    // 标记压测结束
    stats.set_shutting_down();
    // 通知压测结束
    let _ = shutdown_tx.send(());
    // 等待所有任务完成
    while let Some(result) = tasks.next().await {
        match result {
            Ok(task_result) => {
                if let Err(e) = task_result {
                    if !config.quiet {
                        eprintln!("Task error: {}", e);
                    }
                }
            }
            Err(e) => {
                if !config.quiet {
                    eprintln!("Task join error: {}", e);
                }
            }
        }
    }
}