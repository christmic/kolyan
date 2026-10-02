//! Test-only owned-plan provider; counts invocation and never uses ordinary stream.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use kolyan_core::TurnControl;
use kolyan_model::{
    ContextProtocol, CountCoverage, CountProfile, MappingIdentity, ModelProvider, ModelRequest,
    PreparedContextWire, PreparedModelGeneration, PreparedModelProvider, ProviderCountFuture,
    ProviderError, ProviderErrorKind, ProviderErrorPhase, ProviderFuture,
};
use serde_json::{Value, json};

#[derive(Default)]
pub(super) struct Counters {
    pub prepare: AtomicUsize,
    pub count: AtomicUsize,
    pub generation: AtomicUsize,
    pub events: Mutex<Vec<Value>>,
}
pub(super) struct FixtureProvider {
    pub owner: Arc<()>,
    pub mode: String,
    pub control: TurnControl,
    pub counters: Arc<Counters>,
}
pub(super) struct Prepared {
    wire: PreparedContextWire,
}
impl PreparedModelGeneration for Prepared {
    fn wire(&self) -> &PreparedContextWire {
        &self.wire
    }
}
impl ModelProvider for FixtureProvider {
    fn stream(&self, _: ModelRequest) -> ProviderFuture<'_> {
        Box::pin(async { panic!("ordinary stream fallback forbidden") })
    }
}
impl PreparedModelProvider for FixtureProvider {
    type Prepared = Prepared;
    fn prepare_generation(&self, request: &ModelRequest) -> Result<Prepared, ProviderError> {
        self.counters.prepare.fetch_add(1, Ordering::SeqCst);
        let identity = MappingIdentity::new(
            "a".repeat(64),
            ContextProtocol::OpenAiResponses,
            request.model.clone(),
            "fixture-mapping-v1".into(),
            "fixture-coverage-v1".into(),
        )?;
        let profile = if self.mode == "unsupported" {
            CountProfile::default()
        } else {
            CountProfile::registered(identity.clone(), "fixture-counter-v1".into())?
        };
        let wire = PreparedContextWire::new(
            self.owner.clone(),
            identity,
            profile,
            request,
            json!({"model":request.model.model,"input":request.messages}),
            json!({"model":request.model.model,"input":request.messages}),
            CountCoverage::new(Vec::new()),
        )?;
        self.counters.events.lock().unwrap().push(json!({"type":"prepared","body":wire.generation_body(),"digest":wire.generation_wire_digest()}));
        Ok(Prepared { wire })
    }
    fn count_prepared<'a>(
        &'a self,
        prepared: &'a Prepared,
        _: Duration,
    ) -> ProviderCountFuture<'a> {
        Box::pin(async move {
            self.counters.count.fetch_add(1, Ordering::SeqCst);
            self.counters
                .events
                .lock()
                .unwrap()
                .push(json!({"type":"count","body":prepared.wire.count_body()}));
            if self.mode == "cancel_count" {
                self.control.cancel();
            }
            if self.mode == "slow_count" {
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            prepared.wire.reported_count(3)
        })
    }
    fn stream_prepared(&self, prepared: Prepared) -> ProviderFuture<'_> {
        Box::pin(async move {
            self.counters.generation.fetch_add(1, Ordering::SeqCst);
            self.counters.events.lock().unwrap().push(json!({"type":"gen","body":prepared.wire.generation_body(),"digest":prepared.wire.generation_wire_digest()}));
            if self.mode == "gen_error" {
                return Err(ProviderError::new(
                    ProviderErrorKind::Transport,
                    ProviderErrorPhase::Open,
                    "fixture opening uncertainty",
                ));
            }
            Ok(Box::pin(futures_util::stream::empty()) as kolyan_model::ModelEventStream)
        })
    }
}
