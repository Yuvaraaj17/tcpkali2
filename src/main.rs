mod command;
mod error;
mod runner;
mod stats;
mod tcp_worker;
mod utils;
mod websocket_worker;

use crate::command::new_command;
use crate::error::TcpKaliError;
use crate::runner::async_main;

/// TCPKali2 主函数
/// TCPKali2 main function
///
/// # Returns
/// * `Result<(), TcpKaliError>` - 执行结果 / Execution result
fn main() -> Result<(), TcpKaliError> {
    let matches = new_command();
    let workers = *matches.get_one::<usize>("workers").unwrap();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers)
        .enable_all()
        .build()?;

    rt.block_on(async_main(matches))
}
