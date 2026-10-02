//! Delays and one-shot failures around real stores, never fake execution ports.

use super::*;
use kolyan_ledger::{LedgerError, LedgerQuery};
use kolyan_storage::{SessionContextPolicy, SessionInitialization, SessionRecord, SessionTurn};

#[derive(Clone)]
pub(super) struct Timeline {
    pub events: Arc<Mutex<Vec<Value>>>,
    origin: std::time::Instant,
}
impl Timeline {
    pub fn new() -> Self {
        Self {
            events: Arc::new(Mutex::new(vec![])),
            origin: std::time::Instant::now(),
        }
    }
    pub fn mark(&self, phase: &str) {
        self.events.lock().unwrap().push(
            json!({"phase":phase,"elapsed_ns":self.origin.elapsed().as_nanos(),"unix_ms":now_ms()}),
        );
    }
}

#[derive(Clone)]
pub(super) struct SlowLedger<L> {
    pub inner: L,
    pub delay_kind: Option<LedgerEventKind>,
    pub delay_ms: u64,
    pub delayed: Arc<AtomicBool>,
    pub timeline: Timeline,
}
impl<L: LedgerStore> LedgerStore for SlowLedger<L> {
    fn append(&self, event: LedgerEvent) -> Result<LedgerEvent, LedgerError> {
        self.timeline
            .mark(&format!("ledger:{:?}:enter", event.kind));
        if Some(event.kind) == self.delay_kind && !self.delayed.swap(true, Ordering::SeqCst) {
            std::thread::sleep(Duration::from_millis(self.delay_ms));
        }
        let kind = event.kind;
        let result = self.inner.append(event);
        self.timeline.mark(&format!("ledger:{kind:?}:return"));
        result
    }
    fn append_unless_cancelled(&self, event: LedgerEvent) -> Result<LedgerEvent, LedgerError> {
        self.inner.append_unless_cancelled(event)
    }
    fn query(&self, query: &LedgerQuery) -> Result<Vec<LedgerEvent>, LedgerError> {
        self.inner.query(query)
    }
    fn events_after(&self, cursor: u64) -> Result<Vec<LedgerEvent>, LedgerError> {
        self.inner.events_after(cursor)
    }
    fn claim(&self, key: &str) -> Result<bool, LedgerError> {
        self.inner.claim(key)
    }
}

#[derive(Clone)]
pub(super) struct SlowSessionStore {
    pub inner: FileSessionStore,
    pub load_ms: u64,
    pub begin_ms: u64,
    pub loaded: Arc<AtomicBool>,
    pub fail_commit: Arc<AtomicBool>,
    pub timeline: Timeline,
    pub after_begin: Arc<dyn Fn() + Send + Sync>,
}
impl SessionStore for SlowSessionStore {
    fn initialize(
        &self,
        id: &str,
        input: &SessionInitialization,
    ) -> Result<SessionRecord, StorageError> {
        self.inner.initialize(id, input)
    }
    fn create(&self, id: &str) -> Result<SessionRecord, StorageError> {
        self.inner.create(id)
    }
    fn load(&self, id: &str) -> Result<SessionRecord, StorageError> {
        self.timeline.mark("session_load_enter");
        if !self.loaded.swap(true, Ordering::SeqCst) {
            std::thread::sleep(Duration::from_millis(self.load_ms));
        }
        let result = self.inner.load(id);
        self.timeline.mark("session_load_return");
        result
    }
    fn begin_turn_with_projection(
        &self,
        id: &str,
        turn: SessionTurn,
        version: u64,
        messages: Vec<Message>,
        policy: SessionContextPolicy,
    ) -> Result<SessionRecord, StorageError> {
        self.timeline.mark("session_begin_enter");
        let saved = self
            .inner
            .begin_turn_with_projection(id, turn, version, messages, policy)?;
        std::thread::sleep(Duration::from_millis(self.begin_ms));
        (self.after_begin)();
        self.timeline.mark("session_begin_return");
        Ok(saved)
    }
    fn begin_turn_with_input(
        &self,
        id: &str,
        turn: SessionTurn,
        version: u64,
        messages: Vec<Message>,
    ) -> Result<SessionRecord, StorageError> {
        self.inner
            .begin_turn_with_input(id, turn, version, messages)
    }
    fn begin_turn(&self, id: &str, turn: SessionTurn) -> Result<SessionRecord, StorageError> {
        self.inner.begin_turn(id, turn)
    }
    fn update_turn_with_context(
        &self,
        id: &str,
        turn: &str,
        status: SessionTurnStatus,
        messages: Vec<Message>,
        context: Vec<Message>,
    ) -> Result<SessionRecord, StorageError> {
        if self.fail_commit.swap(false, Ordering::SeqCst) {
            return Err(StorageError::Conflict("fixture commit failure".into()));
        }
        self.inner
            .update_turn_with_context(id, turn, status, messages, context)
    }
    fn update_turn(
        &self,
        id: &str,
        turn: &str,
        status: SessionTurnStatus,
        messages: Vec<Message>,
    ) -> Result<SessionRecord, StorageError> {
        self.inner.update_turn(id, turn, status, messages)
    }
    fn append_turn(
        &self,
        id: &str,
        turn: SessionTurn,
        messages: Vec<Message>,
    ) -> Result<SessionRecord, StorageError> {
        self.inner.append_turn(id, turn, messages)
    }
}

pub(super) struct Hook {
    pub seen: Arc<Mutex<Vec<Value>>>,
    pub timeline: Timeline,
}
impl TurnPreparationHook for Hook {
    fn prepare<'a>(
        &'a self,
        execution: &'a ExecutionRef,
        version: u64,
        request: &'a TurnRequest,
    ) -> TurnPreparationFuture<'a> {
        self.timeline.mark("hook_prepare");
        self.seen.lock().unwrap().push(json!({"execution":execution,"version":version,
            "request":request.model_request,"deadline_ns":request.config.deadline.map(|value|value.as_nanos())}));
        Box::pin(async move { Ok(request.model_request.clone()) })
    }
}

pub(super) struct Provider {
    pub seen: Arc<Mutex<Vec<ModelRequest>>>,
    pub approval: bool,
    pub prepared: PreparedCall,
    pub timeline: Timeline,
}
impl ModelProvider for Provider {
    fn stream(&self, request: ModelRequest) -> ProviderFuture<'_> {
        self.timeline.mark("model_open");
        let mut seen = self.seen.lock().unwrap();
        let first = seen.is_empty();
        seen.push(request.clone());
        let response = ModelResponse {
            id: "actual".into(),
            model: request.model,
            content: if self.approval && first {
                vec![ContentBlock::ToolCall {
                    call: self.prepared.call().clone(),
                }]
            } else {
                vec![ContentBlock::Text {
                    text: "done".into(),
                }]
            },
            structured_output: None,
            stop_reason: if self.approval && first {
                StopReason::ToolUse
            } else {
                StopReason::EndTurn
            },
            usage: TokenUsage::default(),
            metadata: Value::Null,
        };
        Box::pin(async move {
            Ok(
                Box::pin(futures_util::stream::iter(vec![Ok(ModelEvent::Completed(
                    response,
                ))])) as ModelEventStream,
            )
        })
    }
}
pub(super) struct Tools {
    pub prepared: PreparedCall,
    pub calls: Arc<AtomicUsize>,
}
impl ToolExecutor for Tools {
    fn prepare(&self, call: kolyan_model::ToolCall) -> ToolPreparationFuture<'_> {
        let prepared = self.prepared.clone();
        Box::pin(async move {
            if prepared.call() != &call {
                return Err(kolyan_core::ToolError::Unavailable { name: call.name });
            }
            Ok(prepared)
        })
    }
    fn execute_invocation(&self, invocation: ToolInvocation) -> ToolFuture<'_> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            Ok(ToolOutcome::Completed(kolyan_model::ToolResult {
                call_id: invocation.prepared.call().id.clone(),
                content: "actual".into(),
                is_error: false,
            }))
        })
    }
}
