use crate::utils::unix_timestamp_millis;
use hdrhistogram::Histogram;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

/// 本地统计缓存，用于批量更新以减少原子操作
/// Local statistics cache for batch updates to reduce atomic operations
#[derive(Default, Clone, Debug)]
#[allow(dead_code)]
pub struct LocalStatsCache {
    /// 本地请求计数 / Local request count
    pub requests: u64,
    /// 本地发送字节数 / Local bytes sent
    pub bytes_sent: u64,
    /// 本地接收字节数 / Local bytes received
    pub bytes_received: u64,
}

#[allow(dead_code)]
impl LocalStatsCache {
    /// 创建新的本地缓存
    /// Create new local cache
    pub fn new() -> Self {
        Self::default()
    }
    
    /// 记录请求到本地缓存
    /// Record request to local cache
    pub fn record_request(&mut self, bytes_sent: usize, bytes_received: usize) {
        self.requests += 1;
        self.bytes_sent += bytes_sent as u64;
        self.bytes_received += bytes_received as u64;
    }
    
    /// 将本地缓存提交到全局统计
    /// Commit local cache to global statistics
    pub fn commit_to(&self, stats: &Stats) {
        if self.requests > 0 {
            stats.total_requests.fetch_add(self.requests, Ordering::Relaxed);
            stats.total_bytes_sent.fetch_add(self.bytes_sent, Ordering::Relaxed);
            stats.total_bytes_received.fetch_add(self.bytes_received, Ordering::Relaxed);
        }
    }
    
    /// 重置本地缓存
    /// Reset local cache
    pub fn reset(&mut self) {
        self.requests = 0;
        self.bytes_sent = 0;
        self.bytes_received = 0;
    }
}

/// 性能统计数据结构
/// Performance statistics data structure
/// 使用64字节对齐优化缓存行效率，减少伪共享
#[repr(align(64))]
#[derive(Debug)]
pub struct Stats {
    /// 总连接数 / Total connections
    pub total_connections: AtomicU64,
    /// 成功连接数 / Successful connections
    pub success_connections: AtomicU64,
    /// 总请求数 / Total requests
    pub total_requests: AtomicU64,
    /// 总发送字节数 / Total bytes sent
    pub total_bytes_sent: AtomicU64,
    /// 总接收字节数 / Total bytes received
    pub total_bytes_received: AtomicU64,
    /// 延迟直方图 / Latency histogram
    pub latency_histogram: parking_lot::Mutex<Histogram<u64>>,
    /// 是否处于热身阶段 / Whether in warmup phase
    pub is_warmup: AtomicBool,
    /// 是否正在关闭 / Whether shutting down
    pub is_shutting_down: AtomicBool,
    /// 上次打印时间 / Last print time
    pub last_print_time: AtomicU64,
    /// 上次打印计数 / Last print count
    pub last_print_count: AtomicU64,
    /// 连接错误数 / Connection errors
    pub connection_errors: AtomicU64,
}

impl Stats {
    /// 创建新的统计对象
    /// Create new statistics object
    ///
    /// # Returns
    /// * `Self` - 新的统计对象 / New statistics object
    pub fn new() -> Self {
        let hist = Histogram::<u64>::new_with_bounds(1, 60_000_000, 3)
            .expect("Failed to create histogram");

        Self {
            total_connections: AtomicU64::new(0),
            success_connections: AtomicU64::new(0),
            total_requests: AtomicU64::new(0),
            total_bytes_sent: AtomicU64::new(0),
            total_bytes_received: AtomicU64::new(0),
            latency_histogram: parking_lot::Mutex::new(hist),
            is_warmup: AtomicBool::new(true),
            is_shutting_down: AtomicBool::new(false),
            last_print_time: AtomicU64::new(unix_timestamp_millis()),
            last_print_count: AtomicU64::new(0),
            connection_errors: AtomicU64::new(0),
        }
    }

    /// 记录延迟数据
    /// Record latency data
    ///
    /// # Arguments
    /// * `latency_us` - 延迟时间（微秒）/ Latency in microseconds
    /// * `sample_count` - 样本计数 / Sample count
    pub fn record_latency(&self, latency_us: u64, sample_count: usize) {
        // 优化采样策略：使用位运算检查是否为2的幂次方的倍数，减少分支预测失败
        // 同时增加采样间隔到256，减少锁竞争
        if (sample_count & 0xFF) == 0 && !self.is_warmup() {
            let mut hist = self.latency_histogram.lock();
            hist.record(latency_us).unwrap_or_else(|e| {
                if !self.is_shutting_down() {
                    eprintln!("Failed to record latency: {}", e);
                }
            });
        }
    }

    /// 记录请求数据
    /// Record request data
    ///
    /// # Arguments
    /// * `bytes_sent` - 发送字节数 / Bytes sent
    /// * `bytes_received` - 接收字节数 / Bytes received
    pub fn record_request(&self, bytes_sent: usize, bytes_received: usize) {
        if !self.is_warmup() {
            self.total_requests.fetch_add(1, Ordering::Relaxed);
            self.total_bytes_sent
                .fetch_add(bytes_sent as u64, Ordering::Relaxed);
            self.total_bytes_received
                .fetch_add(bytes_received as u64, Ordering::Relaxed);
        }
    }
    
    /// 批量记录请求数据（使用本地缓存）
    /// Batch record request data (using local cache)
    ///
    /// # Arguments
    /// * `cache` - 本地统计缓存 / Local statistics cache
    #[allow(dead_code)]
    pub fn record_request_batch(&self, cache: &LocalStatsCache) {
        if !self.is_warmup() && cache.requests > 0 {
            self.total_requests.fetch_add(cache.requests, Ordering::Relaxed);
            self.total_bytes_sent.fetch_add(cache.bytes_sent, Ordering::Relaxed);
            self.total_bytes_received.fetch_add(cache.bytes_received, Ordering::Relaxed);
        }
    }

    /// 记录连接错误
    /// Record connection error
    pub fn record_connection_error(&self) {
        self.connection_errors.fetch_add(1, Ordering::Relaxed);
    }

    pub fn end_warmup(&self) {
        self.total_requests.store(0, Ordering::Relaxed);
        self.total_bytes_sent.store(0, Ordering::Relaxed);
        self.total_bytes_received.store(0, Ordering::Relaxed);
        self.latency_histogram.lock().reset();
        self.last_print_count.store(0, Ordering::Relaxed);
        self.last_print_time
            .store(unix_timestamp_millis(), Ordering::Relaxed);
        self.is_warmup.store(false, Ordering::Relaxed);
    }

    pub fn is_warmup(&self) -> bool {
        self.is_warmup.load(Ordering::Relaxed)
    }

    pub fn set_shutting_down(&self) {
        self.is_shutting_down.store(true, Ordering::Relaxed);
    }

    pub fn is_shutting_down(&self) -> bool {
        self.is_shutting_down.load(Ordering::Relaxed)
    }

    pub fn get_qps(&self) -> f64 {
        let now = unix_timestamp_millis();
        let current_count = self.total_requests.load(Ordering::Relaxed);
        let last_time = self.last_print_time.swap(now, Ordering::Relaxed);
        let last_count = self.last_print_count.swap(current_count, Ordering::Relaxed);

        let elapsed_ms = now - last_time;
        
        // 避免除零错误：如果时间间隔小于1毫秒，返回0.0
        if elapsed_ms < 1 {
            return 0.0;
        }
        
        let elapsed = elapsed_ms as f64 / 1000.0;
        (current_count - last_count) as f64 / elapsed
    }

    /// 打印最终统计结果
    ///
    /// # 参数
    /// - `stats`: 统计数据结构
    /// - `duration`: 测试持续时间
    /// - `show_output`: 是否显示输出
    pub fn print_final_stats(stats: &Stats, duration: Duration, show_output: bool) {
        if !show_output {
            return;
        }

        let hist = stats.latency_histogram.lock();
        let total_bytes_sent = stats.total_bytes_sent.load(Ordering::Relaxed);
        let total_bytes_received = stats.total_bytes_received.load(Ordering::Relaxed);
        let total_bytes = total_bytes_sent + total_bytes_received;
        let total_requests = stats.total_requests.load(Ordering::Relaxed);
        let qps = if duration.as_secs_f64() > 0.0 {
            total_requests as f64 / duration.as_secs_f64()
        } else {
            0.0
        };
        let success_connections = stats.success_connections.load(Ordering::Relaxed) as f64;
        let total_connections = stats.total_connections.load(Ordering::Relaxed) as f64;
        let success_rate = if total_connections > 0.0 {
            success_connections / total_connections * 100.0
        } else {
            0.0
        };
        let error_rate = if total_requests > 0 {
            stats.connection_errors.load(Ordering::Relaxed) as f64 / (total_requests as f64 * 100.0)
        } else {
            0.0
        };

        println!("\n=== Final Results ===");
        println!("Duration:          {:.2}s", duration.as_secs_f64());
        println!("Total Connections: {}", total_connections);
        println!("Success Rate:      {:.1}%", success_rate);
        println!("Total Requests:    {}", total_requests);
        println!("Error Rate:        {:.2}%", error_rate);
        println!("Requests Rate:     {:.2} req/s", qps,);
        println!(
            "Throughput:        {:.2} MB",
            total_bytes as f64 / 1_000_000.0
        );
        println!(
            "Bandwidth:         {:.2} MB/s",
            total_bytes as f64 / duration.as_secs_f64() / 1_000_000.0
        );
        println!(
            "Traffic:           {:.2}↓, {:.2}↑ Mbps",
            total_bytes_received * 8 / 1000000,
            total_bytes_sent * 8 / 1000000
        );
        println!("Latency Distribution (us):");
        println!("  Avg: {:8.1}  Min: {:8}", hist.mean(), hist.min());
        println!(
            "  P50: {:8}  P90: {:8}",
            hist.value_at_percentile(50.0),
            hist.value_at_percentile(90.0)
        );
        println!(
            "  P95: {:8}  P99: {:8}",
            hist.value_at_percentile(95.0),
            hist.value_at_percentile(99.0)
        );
        println!("  Max: {:8}", hist.max());
    }
}
