//! Fault boundaries use a held Provider response, not timing assumptions.
use super::*;

fn fixture() -> Value {
    serde_json::from_str(include_str!("../../fixtures/server_http_faults.json")).unwrap()
}
fn held_provider(
    directory: &Path,
) -> (
    String,
    mpsc::Receiver<()>,
    mpsc::Sender<()>,
    std::thread::JoinHandle<()>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let directory = directory.to_owned();
    let (ready_sender, ready) = mpsc::channel();
    let (release, release_receiver) = mpsc::channel();
    let worker = std::thread::spawn(move || {
        let (mut socket, _) = listener.accept().unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(15)))
            .unwrap();
        let mut reader = BufReader::new(socket.try_clone().unwrap());
        let mut size = 0;
        loop {
            let mut line = String::new();
            reader.read_line(&mut line).unwrap();
            if line == "\r\n" {
                break;
            }
            if let Some(value) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                size = value.trim().parse().unwrap();
            }
        }
        let mut bytes = vec![0; size];
        reader.read_exact(&mut bytes).unwrap();
        fs::write(directory.join("actual-model-request.json"), bytes).unwrap();
        ready_sender.send(()).unwrap();
        let _ = release_receiver.recv_timeout(Duration::from_secs(30));
        let event = json!({"type":"response.completed","response":{"id":"held","status":"completed","output":fixture()["output"]}});
        let body = format!("event: response.completed\ndata: {event}\n\n");
        // The interrupted case deliberately closes the peer before release.
        let _ = write!(
            socket,
            "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
    });
    (url, ready, release, worker)
}
fn assert_fields(actual: &Value, expected: &Value) {
    for (name, value) in expected.as_object().unwrap() {
        assert_eq!(&actual[name], value, "field {name}, actual {actual}");
    }
}
fn assert_facts(directory: &Path) {
    let events = SqliteLedger::open(directory.join("ledger.sqlite"))
        .unwrap()
        .events_after(0)
        .unwrap();
    for (kind, count) in fixture()["counts"].as_object().unwrap() {
        let actual = events
            .iter()
            .filter(|event| serde_json::to_value(event.kind).unwrap() == *kind)
            .count() as u64;
        assert_eq!(actual, count.as_u64().unwrap(), "{kind}");
    }
}
fn start_request(process: &Process) -> tokio::task::JoinHandle<reqwest::Response> {
    let client = process.client.clone();
    let base = process.base.clone();
    tokio::spawn(async move {
        client
            .post(format!("{base}/v1/sessions/s/turns"))
            .bearer_auth("local-test-token-not-a-secret")
            .json(&json!({"turn_id":"t","input":fixture()["input"]}))
            .send()
            .await
            .unwrap()
    })
}
async fn ready(receiver: mpsc::Receiver<()>) {
    tokio::task::spawn_blocking(move || receiver.recv_timeout(Duration::from_secs(15)).unwrap())
        .await
        .unwrap();
}

#[tokio::test]
async fn disconnected_execution_can_be_cancelled_without_blocking_status() {
    let directory = tempfile::Builder::new()
        .prefix("kolyan-http-cancel-")
        .tempdir()
        .unwrap()
        .keep();
    let (url, started, release, worker) = held_provider(&directory);
    setup(&directory, &url);
    let config_path = directory.join("server.json");
    let mut config: Value = serde_json::from_slice(&fs::read(&config_path).unwrap()).unwrap();
    config["http"]["max_active_turns"] = json!(1);
    fs::write(config_path, serde_json::to_vec_pretty(&config).unwrap()).unwrap();
    let process = Process::start(&directory);
    for session in ["s", "other"] {
        process
            .call(
                Method::POST,
                "/v1/sessions",
                Some(json!({"session_id":session})),
                201,
            )
            .await;
    }
    let request = start_request(&process);
    ready(started).await;
    let path = "/v1/sessions/s/turns/t";
    assert_fields(
        &process.call(Method::GET, path, None, 200).await,
        &fixture()["active"],
    );
    process
        .call(
            Method::POST,
            "/v1/sessions/s/turns",
            Some(json!({"turn_id":"duplicate","input":"no"})),
            409,
        )
        .await;
    process
        .call(
            Method::POST,
            "/v1/sessions/other/turns",
            Some(json!({"turn_id":"capacity","input":"no"})),
            429,
        )
        .await;
    process
        .call(Method::GET, "/v1/sessions/other/turns/t", None, 404)
        .await;
    request.abort();
    assert_fields(
        &process
            .call(
                Method::POST,
                &format!("{path}/cancel"),
                Some(json!({})),
                200,
            )
            .await,
        &fixture()["cancel_intent"],
    );
    release.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let status = process.call(Method::GET, path, None, 200).await;
            if status["execution_stopped"] == true {
                assert_fields(&status, &fixture()["cancelled"]);
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    worker.join().unwrap();
    assert_facts(&directory);
    drop(process);
    eprintln!("HTTP cancellation evidence: {}", directory.display());
}

#[tokio::test]
async fn interrupted_worker_is_not_replayed_on_restart() {
    let directory = tempfile::Builder::new()
        .prefix("kolyan-http-interrupted-")
        .tempdir()
        .unwrap()
        .keep();
    let (url, started, release, worker) = held_provider(&directory);
    setup(&directory, &url);
    let process = Process::start(&directory);
    process
        .call(
            Method::POST,
            "/v1/sessions",
            Some(json!({"session_id":"s"})),
            201,
        )
        .await;
    let request = start_request(&process);
    ready(started).await;
    assert_fields(
        &process
            .call(Method::GET, "/v1/sessions/s/turns/t", None, 200)
            .await,
        &fixture()["active"],
    );
    request.abort();
    drop(process);
    release.send(()).unwrap();
    worker.join().unwrap();
    let restarted = Process::start(&directory);
    assert_fields(
        &restarted
            .call(Method::GET, "/v1/sessions/s/turns/t", None, 200)
            .await,
        &fixture()["interrupted"],
    );
    restarted
        .call(
            Method::POST,
            "/v1/sessions/s/turns",
            Some(json!({"turn_id":"t","input":fixture()["input"]})),
            409,
        )
        .await;
    assert_facts(&directory);
    eprintln!("HTTP interrupted evidence: {}", directory.display());
}
