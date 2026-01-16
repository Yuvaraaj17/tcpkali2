//! 错误处理模块
//! 
//! 定义项目特定的错误类型和错误处理逻辑

use thiserror::Error;

/// 项目错误类型
#[derive(Error, Debug)]
pub enum TcpKaliError {
    /// IO 错误
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    
    /// 网络连接错误
    #[error("Network connection error: {0}")]
    Network(String),
    
    /// WebSocket 错误
    #[error("WebSocket error: {0}")]
    WebSocket(String),
    
    /// 配置错误
    #[error("Configuration error: {0}")]
    Config(String),
    
    /// 统计记录错误
    #[error("Statistics recording error: {0}")]
    #[allow(dead_code)]
    Stats(String),
    
    /// 任务执行错误
    #[error("Task execution error: {0}")]
    Task(String),
    
    /// 超时错误
    #[error("Timeout error: {0}")]
    Timeout(String),
    
    /// 解析错误
    #[error("Parse error: {0}")]
    Parse(String),
}

/// 项目结果类型别名
#[allow(dead_code)]
pub type Result<T> = std::result::Result<T, TcpKaliError>;

/// 从 tungstenite 错误转换
impl From<tungstenite::Error> for TcpKaliError {
    fn from(err: tungstenite::Error) -> Self {
        TcpKaliError::WebSocket(err.to_string())
    }
}

/// 从地址解析错误转换
impl From<std::net::AddrParseError> for TcpKaliError {
    fn from(err: std::net::AddrParseError) -> Self {
        TcpKaliError::Parse(err.to_string())
    }
}

/// 方便的错误创建函数
#[allow(dead_code)]
impl TcpKaliError {
    /// 创建网络错误
    pub fn network(msg: impl Into<String>) -> Self {
        TcpKaliError::Network(msg.into())
    }
    
    /// 创建配置错误
    pub fn config(msg: impl Into<String>) -> Self {
        TcpKaliError::Config(msg.into())
    }
    
    /// 创建任务错误
    pub fn task(msg: impl Into<String>) -> Self {
        TcpKaliError::Task(msg.into())
    }
    
    /// 创建超时错误
    pub fn timeout(msg: impl Into<String>) -> Self {
        TcpKaliError::Timeout(msg.into())
    }
}