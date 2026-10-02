//! Exact-plan helper entrypoint. Physical workspace and ceilings arrive through
//! host argv; stdin cannot replace that configuration or select a raw fallback.

use std::io::Write;
use std::path::PathBuf;

use kolyan_tool_worker::{WorkerConfig, WorkerError, bootstrap_check, execute_request};
use kolyan_tools::FileOperationLimits;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let arguments = std::env::args_os().skip(1).collect::<Vec<_>>();
    if matches!(arguments.as_slice(), [flag] if flag == "--bootstrap-check") {
        bootstrap_check(std::io::stdout().lock())?;
        return Ok(());
    }
    if arguments.len() != 4 {
        return Err(WorkerError::Configuration.into());
    }
    let parse_limit = |index: usize| -> Result<usize, WorkerError> {
        arguments[index]
            .to_str()
            .and_then(|value| value.parse().ok())
            .ok_or(WorkerError::Configuration)
    };
    let config = WorkerConfig {
        workspace: PathBuf::from(&arguments[0]),
        max_input_bytes: parse_limit(1)?,
        file_limits: FileOperationLimits {
            max_read_bytes: parse_limit(2)?,
            max_write_bytes: parse_limit(3)?,
        },
    };
    let result = execute_request(&config, std::io::stdin().lock())?;
    let mut output = std::io::stdout().lock();
    serde_json::to_writer(&mut output, &result)?;
    output.write_all(b"\n")?;
    Ok(())
}
