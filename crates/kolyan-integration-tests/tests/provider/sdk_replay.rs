//! Replay actual official-SDK responses through Kolyan without another network call.

#[path = "../common/mod.rs"]
mod common;

use common::*;
use futures_util::StreamExt;
use kolyan_model::{ModelEvent, ModelProvider, ProviderErrorKind};
use serde_json::{Value, json};
use std::{
    fs,
    io::{Read, Write},
    net::TcpListener,
    path::Path,
};

#[tokio::test]
#[ignore = "requires KOLYAN_SDK_EVIDENCE directory produced by sdk_reference.py; loopback only"]
async fn captured_sdk_responses_have_the_same_schema_outcome_in_kolyan() {
    let root =
        std::env::var("KOLYAN_SDK_EVIDENCE").expect("set the captured SDK evidence directory");
    let root = Path::new(&root);
    let output = tempfile::Builder::new()
        .prefix("kolyan-sdk-replay-")
        .tempdir()
        .unwrap()
        .keep();
    let config = load_config();
    let mut count = 0;
    for (family, mut cfg) in [
        ("minimax", config.minimax_openai),
        ("qwen", config.qwen_openai),
    ] {
        for entry in cfg.model_matrix.clone() {
            for variant in ["original", "closed"] {
                let label = format!("{family}-openai-{}-{variant}", entry.model);
                let evidence = root.join(&label);
                let (url, server) = serve(&evidence, output.join(format!("{label}-request.json")));
                cfg.base_url = url;
                replay(
                    build_openai_provider(&cfg, "local-fixture"),
                    family,
                    &entry.model,
                    &evidence,
                    &output.join(format!("{label}-events.jsonl")),
                    "openai",
                )
                .await;
                server.join().unwrap();
                compare_request(
                    &evidence,
                    &output.join(format!("{label}-request.json")),
                    "openai",
                );
                count += 1;
            }
        }
    }
    for (family, mut cfg) in [
        ("minimax", config.minimax_anthropic),
        ("qwen", config.qwen_anthropic),
    ] {
        for entry in cfg.model_matrix.clone() {
            for variant in ["original", "closed"] {
                let label = format!("{family}-anthropic-{}-{variant}", entry.model);
                let evidence = root.join(&label);
                let (url, server) = serve(&evidence, output.join(format!("{label}-request.json")));
                cfg.base_url = url;
                replay(
                    build_anthropic_provider(&cfg, "local-fixture"),
                    family,
                    &entry.model,
                    &evidence,
                    &output.join(format!("{label}-events.jsonl")),
                    "anthropic",
                )
                .await;
                server.join().unwrap();
                compare_request(
                    &evidence,
                    &output.join(format!("{label}-request.json")),
                    "anthropic",
                );
                count += 1;
            }
        }
    }
    let reference: Vec<Value> =
        serde_json::from_slice(&fs::read(root.join("summary.json")).unwrap()).unwrap();
    assert_eq!(
        count,
        reference.len(),
        "every captured matrix row must be replayed"
    );
    println!(
        "PASS {count} captured SDK response replays: {}",
        output.display()
    );
}

fn compare_request(evidence: &Path, actual: &Path, protocol: &str) {
    let mut expected: Value =
        serde_json::from_slice(&fs::read(evidence.join("request.json")).unwrap()).unwrap();
    let actual: Value = serde_json::from_slice(&fs::read(actual).unwrap()).unwrap();
    // Both are documented SDK forms: omitted auto and a single text system block.
    if protocol == "openai" {
        expected["tool_choice"] = json!("auto");
    } else {
        expected["tool_choice"] = json!({"type":"auto"});
        if let Some(text) = expected["system"].as_str() {
            expected["system"] = json!([{"type":"text","text":text}]);
        }
    }
    assert_eq!(
        actual,
        expected,
        "wire request differs from SDK at {}",
        evidence.display()
    );
}

async fn replay<P: ModelProvider>(
    provider: P,
    family: &str,
    model: &str,
    evidence: &Path,
    output: &Path,
    protocol: &str,
) {
    let wire: Value =
        serde_json::from_slice(&fs::read(evidence.join("request.json")).unwrap()).unwrap();
    let expected: Value =
        serde_json::from_slice(&fs::read(evidence.join("result.json")).unwrap()).unwrap();
    assert_eq!(
        expected["transport"], "completed",
        "SDK transport must have succeeded"
    );
    let mut request = build_request(
        family,
        model,
        &load_fixture("structured_output"),
        "replay".into(),
        None,
    );
    request.output_format.as_mut().unwrap().schema = if protocol == "openai" {
        wire["text"]["format"]["schema"].clone()
    } else {
        wire["output_config"]["format"]["schema"].clone()
    };
    let mut stream = provider.stream(request).await.unwrap();
    let mut file = fs::File::create(output).unwrap();
    let mut terminal = false;
    let mut invalid_schema = false;
    let mut text = String::new();
    while let Some(event) = stream.next().await {
        writeln!(file, "{}", json!({"event_debug":format!("{event:?}")})).unwrap();
        match event {
            Ok(ModelEvent::TextDelta(delta)) => text.push_str(&delta),
            Ok(ModelEvent::Completed(_)) => {
                assert!(!terminal);
                terminal = true;
            }
            Err(error) => {
                assert_eq!(error.kind, ProviderErrorKind::InvalidOutput);
                assert!(
                    error.message.contains("structured output"),
                    "unexpected error: {error}"
                );
                invalid_schema = true;
            }
            _ => {}
        }
    }
    file.flush().unwrap();
    assert_eq!(
        text,
        fs::read_to_string(evidence.join("output.txt")).unwrap(),
        "decoded text differs from official SDK: {}",
        evidence.display()
    );
    assert_eq!(
        terminal,
        expected["schema_valid"].as_bool().unwrap(),
        "schema result differs: {}",
        evidence.display()
    );
    assert_eq!(invalid_schema, !terminal);
}

fn serve(
    evidence: &Path,
    request_path: std::path::PathBuf,
) -> (String, std::thread::JoinHandle<()>) {
    let body = fs::read(evidence.join("response.bin")).unwrap();
    let http: Value =
        serde_json::from_slice(&fs::read(evidence.join("http.json")).unwrap()).unwrap();
    assert_eq!(
        http["capture_format"], 2,
        "regenerate evidence with streaming capture"
    );
    let chunks = fs::read_to_string(evidence.join("chunks.jsonl"))
        .unwrap()
        .lines()
        .map(|line| {
            serde_json::from_str::<Value>(line).unwrap()["length"]
                .as_u64()
                .unwrap() as usize
        })
        .collect::<Vec<_>>();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = std::thread::spawn(move || {
        let (mut socket, _) = listener.accept().unwrap();
        socket
            .set_read_timeout(Some(std::time::Duration::from_secs(10)))
            .unwrap();
        let mut input = vec![];
        let boundary = loop {
            let mut byte = [0];
            socket.read_exact(&mut byte).unwrap();
            input.push(byte[0]);
            assert!(input.len() < 65536, "unexpectedly large request headers");
            if input.ends_with(b"\r\n\r\n") {
                break input.len();
            }
        };
        let headers = String::from_utf8_lossy(&input);
        let length = headers
            .lines()
            .find_map(|line| {
                line.split_once(':')
                    .filter(|(key, _)| key.eq_ignore_ascii_case("content-length"))
                    .map(|(_, value)| value.trim().parse::<usize>().unwrap())
            })
            .unwrap();
        input.resize(boundary + length, 0);
        socket.read_exact(&mut input[boundary..]).unwrap();
        // Deliberately never persist request headers (including even dummy authorization).
        fs::write(request_path, &input[boundary..]).unwrap();
        write!(socket, "HTTP/1.1 {} Recorded\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n", http["status"].as_u64().unwrap(), http["headers"]["content-type"].as_str().unwrap_or("application/octet-stream"), body.len()).unwrap();
        if let Some(encoding) = http["headers"]["content-encoding"].as_str() {
            write!(socket, "Content-Encoding: {encoding}\r\n").unwrap();
        }
        write!(socket, "\r\n").unwrap();
        let mut offset = 0;
        for length in chunks {
            socket.write_all(&body[offset..offset + length]).unwrap();
            offset += length;
        }
        assert_eq!(offset, body.len());
    });
    (url, server)
}
