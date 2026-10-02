//! Transport regression probes retain the scenario dataset's scripted output.

use super::*;

#[path = "tests/socket_modes.rs"]
mod socket_modes;

fn outputs() -> Vec<Value> {
    let cases: Value = serde_json::from_str(include_str!(
        "../../../fixtures/server_http_environment_tools.json"
    ))
    .unwrap();
    vec![cases[0]["outputs"][0].clone()]
}

fn directory() -> std::path::PathBuf {
    tempfile::Builder::new()
        .prefix("kolyan-http-fixture-transport-")
        .tempdir()
        .unwrap()
        .keep()
}

#[tokio::test]
async fn idle_connections_do_not_block_or_consume_scripted_responses() {
    let root = directory();
    let provider = Provider::start(&root, outputs());
    let address = provider.url.strip_prefix("http://").unwrap();
    let idle = TcpStream::connect(address).unwrap();
    let response = reqwest::Client::new()
        .post(format!("{}/v1/responses", provider.url))
        .json(&json!({"input":"transport probe"}))
        .send()
        .await
        .unwrap();
    let status = response.status().as_u16();
    let body = response.text().await.unwrap();
    super::super::append(
        &root.join("probe.jsonl"),
        &json!({"status":status,"body":body}),
    );
    let count = provider.finish().unwrap();
    let model = std::fs::read_to_string(root.join("model.jsonl")).unwrap();
    let trace = std::fs::read_to_string(root.join("connections.jsonl")).unwrap();
    assert_eq!(status, 200);
    assert_eq!(count, 1);
    assert_eq!(model.lines().count(), 1);
    let events: Vec<Value> = trace
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert!(
        events
            .iter()
            .filter(|event| event["phase"] == "accepted")
            .count()
            >= 2
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| event["phase"] == "request_parsed")
            .count(),
        1
    );
    assert!(
        events.iter().any(|event| event["phase"] == "read_stopped"
            && event["detail"]["partial_bytes"] == json!([]))
    );
    drop(idle);
    eprintln!("Idle connection evidence: {}", root.display());
}

#[cfg(target_os = "macos")]
#[test]
fn accepted_nonblocking_socket_timeout_does_not_make_read_line_blocking() {
    use std::io::{BufRead, BufReader};

    let root = directory();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let mut client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    // Listener readiness is not socket read readiness. Establish the connection
    // with blocking accept, then explicitly select the mode under examination.
    let (accepted, _) = listener.accept().unwrap();
    accepted.set_nonblocking(true).unwrap();
    accepted
        .set_read_timeout(Some(Duration::from_secs(30)))
        .unwrap();
    let mut reader = BufReader::new(accepted);
    let started = Instant::now();
    let mut line = String::new();
    let result = reader.read_line(&mut line);
    let elapsed = started.elapsed();
    super::super::append(
        &root.join("probe.jsonl"),
        &json!({"read_timeout_seconds":30,"elapsed_us":elapsed.as_micros(),"result":result.as_ref().map_err(|error|json!({"kind":format!("{:?}",error.kind()),"errno":error.raw_os_error()})),"partial_line":line,"first_byte_sent":false}),
    );
    let error = result.unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::WouldBlock);
    assert_eq!(error.raw_os_error(), Some(35));
    assert!(elapsed < Duration::from_secs(1), "not a 30-second timeout");
    reader.get_ref().set_nonblocking(false).unwrap();
    client
        .write_all(b"POST /v1/responses HTTP/1.1\r\n")
        .unwrap();
    let read = reader.read_line(&mut line).unwrap();
    super::super::append(
        &root.join("probe.jsonl"),
        &json!({"explicit_blocking":true,"read":read,"line":line}),
    );
    assert_eq!(line, "POST /v1/responses HTTP/1.1\r\n");
    eprintln!("Explicit nonblocking Rust evidence: {}", root.display());
}

fn wait_for_trace(root: &Path, predicate: impl Fn(&Value) -> bool) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let contents = std::fs::read(root.join("connections.jsonl")).unwrap();
        let events = completed_trace_lines(&contents).expect("completed JSONL lines must be valid");
        if events.iter().any(&predicate) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "expected transport trace: {}",
            String::from_utf8_lossy(&contents)
        );
        thread::sleep(Duration::from_millis(2));
    }
}

fn completed_trace_lines(bytes: &[u8]) -> Result<Vec<Value>, serde_json::Error> {
    let mut events = Vec::new();
    for line in bytes.split_inclusive(|byte| *byte == b'\n') {
        // The writer may be between payload and newline; only that final fragment is pending.
        if line.last() != Some(&b'\n') {
            break;
        }
        events.push(serde_json::from_slice(&line[..line.len() - 1])?);
    }
    Ok(events)
}

#[test]
fn completed_trace_corruption_is_not_ignored_even_before_partial_tail() {
    let result = completed_trace_lines(b"{\"valid\":true}\nnot-json\n{\"pending\":");
    assert!(result.is_err());
}

#[test]
fn only_unterminated_final_trace_fragment_is_pending() {
    let events = completed_trace_lines(b"{\"valid\":true}\n{\"pending\":").unwrap();
    assert_eq!(events, vec![json!({"valid":true})]);
    assert!(completed_trace_lines(b"{\"valid\":true}\n{\"pending\":\n").is_err());
}

#[test]
fn delayed_first_byte_and_fragmented_headers_body_preserve_one_request() {
    let root = directory();
    let provider = Provider::start(&root, outputs());
    let mut socket = TcpStream::connect(provider.url.strip_prefix("http://").unwrap()).unwrap();
    socket
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    wait_for_trace(&root, |event| {
        event["phase"] == "would_block" && event["detail"]["partial_bytes"] == 0
    });
    let body = json!({"input":"fragmented transport probe"}).to_string();
    let header = format!(
        "POST /v1/responses HTTP/1.1\r\nContent-Length: {}\r\n\r\n",
        body.len()
    );
    let fragments = [
        &header.as_bytes()[..7],
        &header.as_bytes()[7..header.len() - 2],
        &header.as_bytes()[header.len() - 2..],
        &body.as_bytes()[..3],
        &body.as_bytes()[3..],
    ];
    let mut total = 0;
    for fragment in fragments {
        socket.write_all(fragment).unwrap();
        total += fragment.len();
        wait_for_trace(&root, |event| {
            event["phase"] == "read" && event["detail"]["total"] == total
        });
        if total < header.len() + body.len() {
            // Acknowledged WouldBlock, not a sleep, separates every incomplete fragment.
            wait_for_trace(&root, |event| {
                event["phase"] == "would_block" && event["detail"]["partial_bytes"] == total
            });
        }
    }
    let mut response = String::new();
    socket.read_to_string(&mut response).unwrap();
    super::super::append(
        &root.join("probe.jsonl"),
        &json!({"response":response,"fragments":fragments.len()}),
    );
    let count = provider.finish().unwrap();
    let model = std::fs::read_to_string(root.join("model.jsonl")).unwrap();
    let rows: Vec<Value> = model
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(count, 1);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["index"], 0);
    assert_eq!(
        rows[0]["request"],
        serde_json::from_str::<Value>(&body).unwrap()
    );
    assert!(response.starts_with("HTTP/1.1 200 OK"));
    eprintln!("Fragmented request evidence: {}", root.display());
}

#[test]
fn parsed_headers_with_truncated_body_fail_explicit_finish() {
    let root = directory();
    let provider = Provider::start(&root, outputs());
    let mut socket = TcpStream::connect(provider.url.strip_prefix("http://").unwrap()).unwrap();
    socket
        .write_all(b"POST /v1/responses HTTP/1.1\r\nContent-Length: 100\r\n\r\n{}")
        .unwrap();
    socket.shutdown(std::net::Shutdown::Write).unwrap();
    socket
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let mut bytes = Vec::new();
    socket.read_to_end(&mut bytes).unwrap();
    let result = provider.finish();
    super::super::append(&root.join("probe.jsonl"), &json!({"finish":result}));
    assert!(result.unwrap_err().contains("truncated request"));
    let trace = std::fs::read_to_string(root.join("connections.jsonl")).unwrap();
    assert!(trace.contains("headers_parsed") && trace.contains("request_error"));
    assert!(!root.join("model.jsonl").exists());
    eprintln!("Truncated request evidence: {}", root.display());
}

#[test]
fn drop_preserves_original_unwind_instead_of_panicking_again() {
    let root = directory();
    let result = std::panic::catch_unwind(|| {
        let _provider = Provider::start(&root, outputs());
        panic!("original fixture assertion");
    });
    let error = panic_message(result.unwrap_err());
    assert_eq!(error, "original fixture assertion");
    let trace = std::fs::read_to_string(root.join("connections.jsonl")).unwrap();
    assert!(trace.contains("\"phase\":\"drop\"") && trace.contains("\"unwinding\":true"));
    eprintln!("Unwind cleanup evidence: {}", root.display());
}
