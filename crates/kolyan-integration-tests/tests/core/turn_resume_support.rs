use futures_util::StreamExt;
use kolyan_core::*;
use kolyan_model::*;
use kolyan_policy::*;
use serde::Deserialize;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

#[derive(Clone, Default)]
pub struct Records(pub Arc<Mutex<Vec<Value>>>);
impl Records {
    pub fn push(&self, value: Value) {
        self.0.lock().unwrap().push(value);
    }
    pub fn all(&self) -> Vec<Value> {
        self.0.lock().unwrap().clone()
    }
    pub fn count(&self, event: &str) -> usize {
        self.0
            .lock()
            .unwrap()
            .iter()
            .filter(|row| row["event"] == event)
            .count()
    }
    pub fn write(&self, label: &str) -> PathBuf {
        let id = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root =
            std::env::temp_dir().join(format!("kolyan-turn-resume-{}-{id}", std::process::id()));
        std::fs::create_dir(&root).unwrap();
        let file = root.join("actual.jsonl");
        let mut data = String::new();
        for record in self.all() {
            data.push_str(&serde_json::to_string(&record).unwrap());
            data.push('\n');
        }
        std::fs::write(&file, data).unwrap();
        eprintln!("TRACE [{label}] {}", file.display());
        file
    }
}

pub fn contains(actual: &Value, expected: &Value) -> bool {
    match expected {
        Value::Object(fields) => fields.iter().all(|(key, value)| {
            actual
                .get(key)
                .is_some_and(|actual| contains(actual, value))
        }),
        _ => actual == expected,
    }
}

pub fn compare(file: &std::path::Path, expected: &str, label: &str) {
    let actual: Vec<Value> = std::fs::read_to_string(file)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    let mut offset = 0;
    for line in expected.lines().filter(|line| !line.trim().is_empty()) {
        let expected: Value = serde_json::from_str(line).unwrap();
        let found = actual[offset..]
            .iter()
            .position(|row| contains(row, &expected));
        assert!(
            found.is_some(),
            "[{label}] missing {expected}; actual: {}",
            file.display()
        );
        offset += found.unwrap() + 1;
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct Trigger {
    pub boundary: String,
    pub occurrence: usize,
}

#[derive(Default)]
struct GateState {
    cancelled: bool,
    terminal: bool,
    counts: HashMap<String, usize>,
}

#[derive(Clone, Default)]
pub struct MemoryGate {
    state: Arc<Mutex<GateState>>,
    pub cancel_at: Option<Trigger>,
    pub fail_at: Option<Trigger>,
    pub records: Records,
}

impl MemoryGate {
    pub fn new(records: Records, cancel_at: Option<Trigger>, fail_at: Option<Trigger>) -> Self {
        Self {
            records,
            cancel_at,
            fail_at,
            ..Default::default()
        }
    }
    pub fn cancel(&self) -> bool {
        let mut state = self.state.lock().unwrap();
        if state.terminal {
            return false;
        }
        state.cancelled = true;
        self.records.push(json!({"event":"cancel_requested"}));
        true
    }
}

impl TurnBoundaryControl for MemoryGate {
    fn admit(&self, boundary: TurnBoundary) -> TurnBoundaryFuture<'_> {
        Box::pin(async move {
            let mut state = self.state.lock().unwrap();
            let (kind, detail) = match &boundary.kind {
                TurnBoundaryKind::Step { step_id } => ("step", json!({"step_id":step_id})),
                TurnBoundaryKind::Tool { step_id, call_id } => {
                    ("tool", json!({"step_id":step_id,"call_id":call_id}))
                }
                TurnBoundaryKind::AwaitingApproval { approval_id } => {
                    ("approval", json!({"approval_id":approval_id}))
                }
                TurnBoundaryKind::ResumeApproval { approval_id } => {
                    ("resume", json!({"approval_id":approval_id}))
                }
                TurnBoundaryKind::Terminal { reason } => {
                    ("terminal", json!({"reason":format!("{reason:?}")}))
                }
            };
            let count = state.counts.entry(kind.into()).or_default();
            *count += 1;
            let matches = |trigger: &Option<Trigger>| {
                trigger
                    .as_ref()
                    .is_some_and(|t| t.boundary == kind && t.occurrence == *count)
            };
            let fail = matches(&self.fail_at);
            let cancel = matches(&self.cancel_at);
            if cancel {
                state.cancelled = true;
                self.records.push(json!({"event":"cancel_requested"}));
            }
            let allowed = !state.cancelled && !state.terminal && !fail;
            self.records.push(
                json!({"event":"boundary","boundary":kind,"detail":detail,"allowed":allowed}),
            );
            if fail {
                return Err(TurnError::BoundaryControl {
                    message: "injected admission failure".into(),
                });
            }
            if state.cancelled {
                return Err(TurnError::Cancelled);
            }
            if state.terminal {
                return Err(TurnError::BoundaryControl {
                    message: "Turn is terminal".into(),
                });
            }
            if kind == "terminal" {
                state.terminal = true;
            }
            Ok(())
        })
    }
}

pub struct RecordingProvider<P> {
    pub inner: Arc<P>,
    pub records: Records,
}
impl<P> Clone for RecordingProvider<P> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            records: self.records.clone(),
        }
    }
}
impl<P: ModelProvider + 'static> ModelProvider for RecordingProvider<P> {
    fn stream(&self, request: ModelRequest) -> ProviderFuture<'_> {
        let step_id = request.request_id.clone();
        self.records
            .push(json!({"event":"model_request","step_id":step_id,"request":request}));
        let future = self.inner.stream(request);
        let records = self.records.clone();
        Box::pin(async move {
            let stream = future.await?;
            Ok(Box::pin(stream.map(move |event| {
                match &event {
                    Ok(event) => records
                        .push(json!({"event":"model_event","step_id":step_id,"content":event})),
                    Err(error) => {
                        records.push(json!({"event":"model_error","error":error.to_string()}))
                    }
                }
                event
            })) as ModelEventStream)
        })
    }
}

pub struct RecordingTool<T> {
    pub inner: T,
    pub records: Records,
}
impl<T: ToolExecutor> ToolExecutor for RecordingTool<T> {
    fn execute(&self, call: ToolCall) -> ToolFuture<'_> {
        self.run(call, None)
    }
    fn execute_with_grant(&self, call: ToolCall, grant: ExecutionGrant) -> ToolFuture<'_> {
        self.run(call, Some(grant))
    }
}
impl<T: ToolExecutor> RecordingTool<T> {
    fn run(&self, call: ToolCall, grant: Option<ExecutionGrant>) -> ToolFuture<'_> {
        Box::pin(async move {
            self.records.push(json!({"event":"tool_start","call":call}));
            let id = call.id.clone();
            let result = match grant {
                Some(grant) => self.inner.execute_with_grant(call, grant).await,
                None => self.inner.execute(call).await,
            };
            match &result {
                Ok(result) => self
                    .records
                    .push(json!({"event":"tool_result","result":result})),
                Err(error) => self
                    .records
                    .push(json!({"event":"tool_error","call_id":id,"error":error.to_string()})),
            }
            result
        })
    }
}

pub fn policy(approvals: &[String], denied: &[String]) -> Arc<PolicyEngine> {
    let mut policy = PolicyEngine::default();
    for (name, capability, effect) in [
        ("file.write", Capability::FilesystemWrite, Effect::Update),
        ("file.read", Capability::FilesystemRead, Effect::Read),
    ] {
        policy.register(ToolManifest {
            tool_name: name.into(),
            capabilities: [capability].into_iter().collect(),
            effects: [effect].into_iter().collect(),
            path_scopes: vec![PathScope::new("safe")],
            idempotency: Idempotency::Unknown,
            approval: if approvals.iter().any(|item| item == name) {
                ApprovalMode::Always
            } else {
                ApprovalMode::Never
            },
        });
    }
    for name in denied {
        policy.deny_tool(name);
    }
    Arc::new(policy)
}
