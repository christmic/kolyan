use kolyan_agent::{
    AgentCatalog, AgentDefinition, AgentDefinitionInput, AgentInvocationBinding,
    AgentInvocationBindingStore, AgentPermissions, AgentSelector, BindingContextKind,
    hooks::{
        HookAccessPolicy, HookAccessRule, HookCatalog, HookKey, HookManifest, HookPhase, HookScope,
        NativeHookHost,
    },
};
use kolyan_ledger::{FactDraft, FactError, FactJournal, FactRecord, SqliteFactJournal};
use kolyan_model::ModelRef;
use kolyan_policy::{Capability, Effect};
use kolyan_trace::ArtifactStore;
use std::{
    collections::BTreeSet,
    fs,
    os::unix::fs::PermissionsExt,
    sync::{Arc, Mutex},
};

pub struct Journal {
    inner: SqliteFactJournal,
    streams: Mutex<BTreeSet<String>>,
    pub fail_completion: std::sync::atomic::AtomicBool,
}
impl Journal {
    pub fn records(&self) -> Vec<FactRecord> {
        let streams = self.streams.lock().unwrap().clone();
        streams
            .into_iter()
            .flat_map(|s| self.inner.read(&s, 0, 1024).unwrap())
            .collect()
    }
}
impl FactJournal for Journal {
    fn read(&self, stream: &str, after: u64, limit: usize) -> Result<Vec<FactRecord>, FactError> {
        self.streams.lock().unwrap().insert(stream.into());
        self.inner.read(stream, after, limit)
    }
    fn append(
        &self,
        stream: &str,
        head: u64,
        drafts: Vec<FactDraft>,
    ) -> Result<Vec<FactRecord>, FactError> {
        self.streams.lock().unwrap().insert(stream.into());
        if self
            .fail_completion
            .load(std::sync::atomic::Ordering::SeqCst)
            && drafts.iter().any(|d| d.kind == "agent.hook.completed")
        {
            return Err(FactError::Storage(
                "explicit completion-storage fault after native execution".into(),
            ));
        }
        self.inner.append(stream, head, drafts)
    }
}
pub struct Host {
    pub journal: Arc<Journal>,
    pub artifacts: Arc<ArtifactStore>,
    pub catalog: HookCatalog,
    pub native: NativeHookHost,
    pub scope: HookScope,
    pub ownership: kolyan_ledger::FactRef,
    pub policy: HookAccessPolicy,
}
impl Host {
    pub fn open(root: &std::path::Path, phase: HookPhase) -> Self {
        for name in ["installation", "cwd"] {
            let path = root.join(name);
            fs::create_dir(&path).unwrap();
            fs::set_permissions(path, fs::Permissions::from_mode(0o700)).unwrap();
        }
        let journal = Arc::new(Journal {
            inner: SqliteFactJournal::open(root.join("journal.sqlite")).unwrap(),
            streams: Mutex::default(),
            fail_completion: std::sync::atomic::AtomicBool::new(false),
        });
        let artifacts = Arc::new(ArtifactStore::new(root.join("artifacts"), 1024 * 1024).unwrap());
        let catalog =
            HookCatalog::new(journal.clone(), artifacts.clone(), "native-fixture".into()).unwrap();
        let permissions = AgentPermissions::default();
        let definition = AgentDefinition::new(AgentDefinitionInput {
            definition_id: "native-agent".into(),
            revision: "1".into(),
            display_name: None,
            model: ModelRef {
                provider: "fixture".into(),
                model: "no-model".into(),
            },
            instructions: "Native hook fixture only".into(),
            permissions: permissions.clone(),
        })
        .unwrap();
        let snapshot = AgentCatalog::new(1)
            .unwrap()
            .resolve(
                &AgentSelector::Inline(definition),
                "native-instance",
                &permissions,
                &permissions,
            )
            .unwrap();
        let binding = AgentInvocationBinding {
            task_id: "task".into(),
            invocation_id: "root".into(),
            logical_session_id: "logical".into(),
            private_session_id: "logical".into(),
            context_kind: BindingContextKind::Root,
            snapshot,
        };
        let ownership = AgentInvocationBindingStore::new(journal.clone())
            .save(&binding)
            .unwrap();
        let scope = HookScope::from_binding(&binding, "execution".into(), "turn".into()).unwrap();
        let policy = HookAccessPolicy::new(
            "1".into(),
            vec![HookAccessRule {
                agent: binding.snapshot.definition().key(),
                logical_session_id: "logical".into(),
                task_id: Some("task".into()),
                invocation_id: Some("root".into()),
                hooks: BTreeSet::from([Self::key()]),
                phases: BTreeSet::from([phase]),
            }],
        )
        .unwrap();
        let native = NativeHookHost::new(root.join("installation"), root.join("cwd")).unwrap();
        Self {
            journal,
            artifacts,
            catalog,
            native,
            scope,
            ownership,
            policy,
        }
    }
    pub fn key() -> HookKey {
        HookKey {
            id: "native-check".into(),
            revision: "1".into(),
        }
    }
    pub fn manifest(phase: HookPhase, timeout_ms: u64, output: usize) -> HookManifest {
        HookManifest {
            key: Self::key(),
            phases: BTreeSet::from([phase]),
            tool_names: BTreeSet::new(),
            capabilities: BTreeSet::from([Capability::ProcessExecute, Capability::FilesystemRead]),
            effects: BTreeSet::from([Effect::Execute, Effect::Read]),
            timeout_ms,
            max_input_bytes: 4096,
            max_output_bytes: output,
        }
    }
}
