use crate::utils::{generate_payload, get_file_arg, get_message_arg, parse_duration, parse_rate};
use bytes::Bytes;
use clap::{Arg, ArgAction, Command, value_parser};
use std::sync::Arc;
use std::time::Duration;

/// 负载测试配置
/// 使用64字节对齐优化缓存行效率
#[repr(align(64))]
#[derive(Clone, Debug)]
pub struct Config {
    /// 测试持续时间
    pub duration: Duration,
    /// 热身持续时间
    pub warmup_duration: Duration,
    /// 消息大小（字节）
    pub message_size: usize,
    /// 是否静默模式
    pub quiet: bool,
    /// 是否启用 Nagle 算法
    pub nagle: bool,
    /// 是否启用管道模式
    pub pipeline: bool,
    /// 连接数
    pub connections: u64,
    /// 连接速率（连接数/秒）
    pub connect_rate: u64,
    /// 连接超时时间
    pub connect_timeout: Duration,
    /// 通道生命周期
    pub channel_lifetime: Option<Duration>,
    /// 第一条消息
    pub first_message: Option<Bytes>,
    /// 测试消息
    pub message: Option<Bytes>,
    /// 消息发送速率（消息数/秒）
    pub message_rate: Option<u64>,
    /// 是否使用 WebSocket
    pub use_websocket: bool,
}

/// 解析命令行参数并创建配置
/// Parse command line arguments and create configuration
///
/// # Arguments
/// * `matches` - 命令行参数匹配结果 / Command line argument matches
///
/// # Returns
/// * `Arc<Config>` - 共享的配置对象 / Shared configuration object
pub fn parse_config(matches: &clap::ArgMatches) -> Arc<Config> {
    let unescape = matches.get_flag("unescape-message-args");

    let message_size = *matches.get_one::<usize>("message-size").unwrap();

    let config = Config {
        duration: *matches.get_one::<Duration>("duration").unwrap(),
        warmup_duration: Duration::from_secs(5),
        quiet: matches.get_flag("quiet"),
        nagle: matches.get_flag("nagle"),
        pipeline: matches.get_flag("pipeline"),
        connections: *matches.get_one::<u64>("connections").unwrap(),
        connect_rate: *matches.get_one::<u64>("connect-rate").unwrap(),
        connect_timeout: *matches.get_one::<Duration>("connect-timeout").unwrap(),
        channel_lifetime: matches.get_one::<Duration>("channel-lifetime").cloned(),
        first_message: get_message_arg(matches, "first-message", unescape)
            .or_else(|| get_file_arg(matches, "first-message-file", unescape)),
        message: get_message_arg(matches, "message", unescape)
            .or_else(|| get_file_arg(matches, "message-file", unescape))
            .or_else(|| generate_payload(message_size)),
        message_size,
        message_rate: matches.get_one::<u64>("message-rate").cloned(),
        use_websocket: matches.get_flag("websocket"),
    };

    Arc::new(config)
}

/// 创建命令行参数解析器
/// Create command line argument parser
///
/// # Returns
/// * `clap::ArgMatches` - 解析后的命令行参数 / Parsed command line arguments
pub fn new_command() -> clap::ArgMatches {
    Command::new("tcpkali2")
        .version("0.1.1")
        .about("A load testing tool for WebSocket and TCP server")
        .arg(
            Arg::new("host:port")
                .required(true)
                .num_args(1)
                .help("Target server in host:port format"),
        )
        .arg(
            Arg::new("websocket")
                .long("websocket")
                .alias("ws")
                .action(ArgAction::SetTrue)
                .help("Use RFC6455 WebSocket transport"),
        )
        .arg(
            Arg::new("connections")
                .short('c')
                .long("connections")
                .value_name("N")
                .default_value("1")
                .value_parser(value_parser!(u64))
                .help("Connections to keep open to the destinations"),
        )
        .arg(
            Arg::new("connect-rate")
                .long("connect-rate")
                .value_name("R")
                .default_value("100")
                .value_parser(parse_rate)
                .help("Limit number of new connections per second"),
        )
        .arg(
            Arg::new("connect-timeout")
                .long("connect-timeout")
                .value_name("T")
                .default_value("1s")
                .value_parser(parse_duration)
                .help("Limit time spent in a connection attempt"),
        )
        .arg(
            Arg::new("channel-lifetime")
                .long("channel-lifetime")
                .value_name("T")
                .value_parser(parse_duration)
                .help("Shut down each connection after T seconds"),
        )
        .arg(
            Arg::new("workers")
                .short('w')
                .long("workers")
                .value_name("N")
                .default_value("8")
                .value_parser(value_parser!(usize))
                .help("Number of Tokio worker threads to use"),
        )
        .arg(
            Arg::new("nagle")
                .long("nagle")
                .action(ArgAction::SetTrue)
                .help("Control Nagle algorithm (set TCP_NODELAY)"),
        )
        .arg(
            Arg::new("pipeline")
                .short('p')
                .long("pipeline")
                .action(ArgAction::SetTrue)
                .help("Use pipeline client to send messages"),
        )
        .arg(
            Arg::new("duration")
                .short('T')
                .long("duration")
                .value_name("T")
                .default_value("15s")
                .value_parser(parse_duration)
                .help("Load test for the specified amount of time"),
        )
        .arg(
            Arg::new("unescape-message-args")
                .short('e')
                .long("unescape-message-args")
                .action(ArgAction::SetTrue)
                .help("Unescape the following {-m|-f|--first-*} arguments"),
        )
        .arg(
            Arg::new("first-message")
                .long("first-message")
                .value_name("string")
                .help("Send this message first, once"),
        )
        .arg(
            Arg::new("first-message-file")
                .long("first-message-file")
                .value_name("name")
                .help("Read the first message from a file"),
        )
        .arg(
            Arg::new("message")
                .short('m')
                .long("message")
                .value_name("string")
                .help("Message to repeatedly send to the remote"),
        )
        .arg(
            Arg::new("message-size")
                .short('s')
                .long("message-size")
                .default_value("128")
                .value_parser(value_parser!(usize))
                .help("Random message to repeatedly send to the remote"),
        )
        .arg(
            Arg::new("message-file")
                .short('f')
                .long("message-file")
                .value_name("name")
                .help("Read message to send from a file"),
        )
        .arg(
            Arg::new("message-rate")
                .short('r')
                .long("message-rate")
                .value_name("R")
                .value_parser(parse_rate)
                .help("Messages per second to send in a connection"),
        )
        .arg(
            Arg::new("quiet")
                .short('q')
                .action(ArgAction::SetTrue)
                .help("Suppress real-time output"),
        )
        .get_matches()
}
