//! Transport selection and process lifecycle. Execution assembly is shared.
mod assembly;
mod config;
mod http;

use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path =
        std::env::var_os("KOLYAN_SERVER_CONFIG").ok_or("KOLYAN_SERVER_CONFIG is required")?;
    let path = std::path::PathBuf::from(path);
    let config: config::Config = serde_json::from_slice(&std::fs::read(&path)?)?;
    let transport = config.http.clone();
    let app = assembly::App::new(config, &path)?;
    if let Some(transport) = transport {
        return http::serve(app, transport).await;
    }
    let host = Arc::new(app.rpc());
    let (sender, mut receiver) = tokio::sync::mpsc::channel::<String>(64);
    let writer = tokio::spawn(async move {
        let mut stdout = tokio::io::stdout();
        while let Some(response) = receiver.recv().await {
            stdout.write_all(response.as_bytes()).await?;
            stdout.write_all(b"\n").await?;
            stdout.flush().await?;
        }
        Ok::<_, std::io::Error>(())
    });
    let mut lines = tokio::io::BufReader::new(tokio::io::stdin()).lines();
    let mut tasks = tokio::task::JoinSet::new();
    while let Some(line) = lines.next_line().await? {
        let host = host.clone();
        let sender = sender.clone();
        tasks.spawn(async move {
            let response = host.handle_json(&line).await;
            // Reader disconnect never rolls back durable execution.
            let _ = sender.send(response).await;
        });
        while let Some(result) = tasks.try_join_next() {
            result?;
        }
    }
    drop(sender);
    while let Some(result) = tasks.join_next().await {
        result?;
    }
    writer.await??;
    Ok(())
}
