//! Synthetic transport evidence; no process lifecycle or authorization is invented.

use std::io::Write;

use serde::Deserialize;
use serde_json::{Value, json};

use super::*;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Dataset {
    cases: Vec<Case>,
    concurrent: Concurrent,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Concurrent {
    capacity: usize,
    producers: usize,
    pairs: u8,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    capacity: usize,
    valid: bool,
    actions: Vec<Action>,
    observations: Vec<Value>,
    lifecycle_dropped: u64,
    output_dropped: u64,
}

#[derive(Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
enum Action {
    Emit { sender: usize, event: Event },
    Receive { asynchronous: bool },
    CloseSenders,
    CloseReceiver,
    RefuseContext,
}

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum Event {
    Spawned,
    Output { stream: String, text: String },
    Capture { stream: String },
    Reaped,
}
impl Event {
    fn actual(self) -> SandboxProcessEvent {
        let pipe = |name: &str| match name {
            "stdout" => SandboxOutputStream::Stdout,
            "stderr" => SandboxOutputStream::Stderr,
            other => panic!("unknown fixture pipe: {other}"),
        };
        match self {
            Self::Spawned => SandboxProcessEvent::Spawned,
            Self::Output { stream, text } => SandboxProcessEvent::OutputObserved {
                stream: pipe(&stream),
                bytes: text.into_bytes(),
            },
            Self::Capture { stream } => SandboxProcessEvent::CaptureCompleted {
                stream: pipe(&stream),
                retained_bytes: 1,
                error: None,
            },
            Self::Reaped => SandboxProcessEvent::Reaped {
                exit_code: Some(0),
                signal: None,
            },
        }
    }
}

#[tokio::test]
async fn independent_queues_preserve_order_and_classified_loss() {
    let cases = serde_json::from_str::<Dataset>(include_str!("transport_cases.json"))
        .unwrap()
        .cases;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("actual.jsonl");
    let mut file = std::fs::File::create(&path).unwrap();
    let mut comparisons = Vec::new();
    for case in cases {
        let channel = sandbox_process_observation_channel(case.capacity);
        let mut actual = Vec::new();
        let mut counters_consistent = true;
        let valid = channel.is_ok();
        let (mut lifecycle_dropped, mut output_dropped) = (0, 0);
        if let Ok((sender, receiver)) = channel {
            let transport = sender.transport.clone();
            let mut senders = vec![Some(sender.clone()), sender.bind_context("second".into())];
            drop(sender);
            let mut launches: Vec<_> = senders
                .iter()
                .map(|s| Some(s.as_ref().unwrap().launch(123, 123)))
                .collect();
            let mut receiver = Some(receiver);
            for action in case.actions {
                match action {
                    Action::Emit { sender, event } => {
                        launches[sender].as_ref().unwrap().emit(event.actual())
                    }
                    Action::Receive { asynchronous } => {
                        let receiver = receiver.as_mut().unwrap();
                        let event = if asynchronous {
                            match tokio::time::timeout(
                                std::time::Duration::from_secs(1),
                                receiver.recv(),
                            )
                            .await
                            {
                                Ok(Some(event)) => Ok(event),
                                Ok(None) => Err("disconnected"),
                                Err(_) => Err("timeout"),
                            }
                        } else {
                            receiver.try_recv().map_err(|error| match error {
                                mpsc::error::TryRecvError::Empty => "empty",
                                mpsc::error::TryRecvError::Disconnected => "disconnected",
                            })
                        };
                        let row = match event {
                            Ok(event) => json!({"context":event.context,"event":event.event}),
                            Err(error) => json!({"error":error}),
                        };
                        writeln!(
                            file,
                            "{}",
                            json!({"case":case.id,"event":"received","actual":row})
                        )
                        .unwrap();
                        actual.push(row);
                    }
                    Action::CloseSenders => {
                        senders.clear();
                        launches.clear();
                    }
                    Action::CloseReceiver => {
                        receiver.take();
                    }
                    Action::RefuseContext => {
                        counters_consistent &= senders[0]
                            .as_ref()
                            .unwrap()
                            .bind_context("x".repeat(4097))
                            .is_none();
                        senders[0].as_ref().unwrap().record_loss();
                    }
                }
                let life = transport.lifecycle_dropped.load(Ordering::Relaxed);
                let output = transport.output_dropped.load(Ordering::Relaxed);
                let total = life + output;
                for sender in senders.iter().flatten() {
                    counters_consistent &= (
                        sender.dropped(),
                        sender.lifecycle_dropped(),
                        sender.output_dropped(),
                    ) == (total, life, output);
                }
                if let Some(receiver) = &receiver {
                    counters_consistent &= (
                        receiver.dropped(),
                        receiver.lifecycle_dropped(),
                        receiver.output_dropped(),
                    ) == (total, life, output);
                }
            }
            lifecycle_dropped = transport.lifecycle_dropped.load(Ordering::Relaxed);
            output_dropped = transport.output_dropped.load(Ordering::Relaxed);
        }
        let observed = json!({"valid":valid,"observations":actual,"lifecycle_dropped":lifecycle_dropped,"output_dropped":output_dropped,"getters_consistent":counters_consistent});
        let expected = json!({"valid":case.valid,"observations":case.observations,"lifecycle_dropped":case.lifecycle_dropped,"output_dropped":case.output_dropped,"getters_consistent":true});
        writeln!(
            file,
            "{}",
            json!({"case":case.id,"event":"comparison_input","actual":observed,"expected":expected})
        )
        .unwrap();
        comparisons.push((case.id, observed, expected));
    }
    file.sync_all().unwrap();
    let retained = directory.keep();
    println!(
        "OBSERVATION_TRANSPORT_TRACE={}",
        retained.join("actual.jsonl").display()
    );
    for (id, actual, expected) in comparisons {
        assert_eq!(actual, expected, "{id}");
    }
}

#[tokio::test]
async fn pending_receiver_wakes_for_each_queue_and_sender_closure() {
    let directory = tempfile::tempdir().unwrap();
    let mut file = std::fs::File::create(directory.path().join("actual.jsonl")).unwrap();
    let mut observations = Vec::new();
    for output in [false, true] {
        let (sender, mut receiver) = sandbox_process_observation_channel(1).unwrap();
        let worker = tokio::spawn(async move {
            tokio::task::yield_now().await;
            sender.launch(123, 123).emit(if output {
                SandboxProcessEvent::OutputObserved {
                    stream: SandboxOutputStream::Stdout,
                    bytes: vec![1],
                }
            } else {
                SandboxProcessEvent::Spawned
            });
        });
        let row = tokio::time::timeout(std::time::Duration::from_secs(1), receiver.recv())
            .await
            .unwrap();
        worker.await.unwrap();
        let closed = receiver.recv().await.is_none();
        let received_output = row
            .as_ref()
            .is_some_and(|r| matches!(r.event, SandboxProcessEvent::OutputObserved { .. }));
        let actual = json!({"expected_output":output,"received_output":received_output,"received":row,"closed":closed,"dropped":receiver.dropped()});
        writeln!(file, "{actual}").unwrap();
        observations.push((
            output,
            received_output,
            row.is_some(),
            closed,
            receiver.dropped(),
        ));
    }
    file.sync_all().unwrap();
    println!(
        "OBSERVATION_WAKE_TRACE={}",
        directory.keep().join("actual.jsonl").display()
    );
    for (output, received_output, received, closed, dropped) in observations {
        assert_eq!(output, received_output);
        assert!(received && closed);
        assert_eq!(dropped, 0);
    }
}

#[tokio::test]
async fn concurrent_producers_merge_while_receiver_drains() {
    let config = serde_json::from_str::<Dataset>(include_str!("transport_cases.json"))
        .unwrap()
        .concurrent;
    let directory = tempfile::tempdir().unwrap();
    let mut file = std::fs::File::create(directory.path().join("actual.jsonl")).unwrap();
    let (sender, mut receiver) = sandbox_process_observation_channel(config.capacity).unwrap();
    let barrier = Arc::new(std::sync::Barrier::new(config.producers + 1));
    let mut workers = Vec::new();
    for producer in 0..config.producers {
        let launch = sender
            .bind_context(producer.to_string())
            .unwrap()
            .launch(123, 123);
        let barrier = barrier.clone();
        let pairs = config.pairs;
        workers.push(std::thread::spawn(move || {
            barrier.wait();
            for index in 0..pairs {
                launch.emit(SandboxProcessEvent::OutputObserved {
                    stream: SandboxOutputStream::Stdout,
                    bytes: vec![index],
                });
                std::thread::yield_now();
                launch.emit(SandboxProcessEvent::CaptureCompleted {
                    stream: SandboxOutputStream::Stdout,
                    retained_bytes: usize::from(index),
                    error: None,
                });
            }
        }));
    }
    drop(sender);
    barrier.wait();
    let mut rows = Vec::new();
    let mut timeout = false;
    let mut use_async = false;
    loop {
        use_async = !use_async;
        let row = if use_async {
            match tokio::time::timeout(std::time::Duration::from_secs(2), receiver.recv()).await {
                Ok(row) => row,
                Err(_) => {
                    timeout = true;
                    break;
                }
            }
        } else {
            match receiver.try_recv() {
                Ok(row) => Some(row),
                Err(mpsc::error::TryRecvError::Empty) => {
                    tokio::task::yield_now().await;
                    continue;
                }
                Err(mpsc::error::TryRecvError::Disconnected) => None,
            }
        };
        let Some(row) = row else {
            break;
        };
        writeln!(file, "{}", serde_json::to_string(&row).unwrap()).unwrap();
        rows.push(row);
    }
    for worker in workers {
        worker.join().unwrap();
    }
    writeln!(
        file,
        "{}",
        json!({"event":"summary","timeout":timeout,"dropped":receiver.dropped(),"rows":rows.len()})
    )
    .unwrap();
    file.sync_all().unwrap();
    println!(
        "OBSERVATION_CONCURRENT_TRACE={}",
        directory.keep().join("actual.jsonl").display()
    );
    assert!(!timeout);
    assert_eq!(receiver.dropped(), 0);
    assert_eq!(rows.len(), config.producers * usize::from(config.pairs) * 2);
    for producer in 0..config.producers {
        let actual: Vec<_> = rows
            .iter()
            .filter(|r| r.context == producer.to_string())
            .map(|r| match &r.event {
                SandboxProcessEvent::OutputObserved { bytes, .. } => {
                    ("output", usize::from(bytes[0]))
                }
                SandboxProcessEvent::CaptureCompleted { retained_bytes, .. } => {
                    ("capture", *retained_bytes)
                }
                other => panic!("unexpected event: {other:?}"),
            })
            .collect();
        let expected: Vec<_> = (0..config.pairs)
            .flat_map(|n| [("output", usize::from(n)), ("capture", usize::from(n))])
            .collect();
        assert_eq!(actual, expected);
    }
}
