//! Trusted fixture configuration for the actual production Host, not factories.

use std::{collections::BTreeSet, path::Path, time::Duration};

use kolyan_agent::{
    AgentDefinition, AgentDelegationConfig, AgentPermissions, AgentSelector, InvokePrepareLimits,
    context::{BudgetMode, ContextPolicy},
};
use kolyan_agent_host::{AgentHostConfig, DeploymentProtocol, HostDeployment, HostStartRequest};
use kolyan_core::{ToolErrorPolicy, TurnConfig};
use kolyan_model::{ModelDescriptor, ModelFeature, ModelRef, ModelRequest, ParameterTable};
use kolyan_policy::{ApprovalMode, Capability, Effect, Idempotency, PathScope, ToolManifest};
use kolyan_server::{CancellationPolicy, TaskLimits};
use kolyan_tools::{
    FileOperationLimits, IsolatedFileConfig, IsolatedShellConfig, IsolatedToolSetConfig,
};
use serde_json::json;

use super::Case;

pub(super) fn directories(root: &Path) -> std::io::Result<()> {
    use std::os::unix::fs::DirBuilderExt;
    std::fs::create_dir_all(root)?;
    for name in ["workspace", "state", "staging"] {
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(root.join(name))?;
    }
    std::fs::write(root.join("workspace/input.txt"), "seed\n")
}

fn permissions() -> AgentPermissions {
    serde_json::from_value(json!({"tools":["file.read","file.write","file.edit","shell"],
        "delegation":{"named_targets":[{"definition_id":"child","revision":"1"}],"allow_inline":true,"allow_self":true}})).unwrap()
}

fn definition(id: &str, model: &str, child: bool) -> AgentDefinition {
    let mut authority = serde_json::to_value(permissions()).unwrap();
    if child {
        authority["delegation"] =
            json!({"named_targets":[],"allow_inline":false,"allow_self":false});
    }
    serde_json::from_value(json!({"definition_id":id,"revision":"1","display_name":null,
        "model":{"provider":"host-loopback","model":model},"instructions":"Execute only the supplied bounded task and use actual tool results.","permissions":authority})).unwrap()
}

pub(super) fn request(case: &Case) -> Result<HostStartRequest, String> {
    Ok(HostStartRequest {
        task_id: case.id.clone(),
        invocation_id: "root".into(),
        attempt_id: "root-attempt".into(),
        turn_id: format!("turn-{}", case.id),
        execution_id: format!("execution-{}", case.id),
        selector: if case.named {
            AgentSelector::Named(definition("root-agent", "fixture-model", false).key())
        } else {
            AgentSelector::Inline(definition("inline-agent", "fixture-model", false))
        },
        requested_permissions: permissions(),
        objective: case.input.clone(),
        messages: serde_json::from_value(
            json!([{"role":"user","content":[{"type":"text","text":case.input}]}]),
        )
        .map_err(|e| e.to_string())?,
        goals: case.goals.clone(),
    })
}

pub(super) fn configuration(
    root: &Path,
    protocol: &str,
    endpoint: &str,
    case: &Case,
    revoke: bool,
    different_model: bool,
) -> Result<AgentHostConfig, String> {
    let model = if different_model {
        "changed-model"
    } else {
        "fixture-model"
    };
    let features = BTreeSet::from([
        ModelFeature::TextInput,
        ModelFeature::TextOutput,
        ModelFeature::ToolUse,
        ModelFeature::Streaming,
    ]);
    let table: ParameterTable = serde_json::from_value(json!({"provider":"host-loopback","protocol":protocol,
        "features":features,"defaults":{"max_output_tokens":{"support":"supported","omittable":false},
            "tool_choice":{"support":"supported"}},"models":{model:{}}})).map_err(|e|e.to_string())?;
    let workspace = root
        .join("workspace")
        .canonicalize()
        .map_err(|e| e.to_string())?;
    let approval = if case.approval {
        ApprovalMode::Always
    } else {
        ApprovalMode::Never
    };
    let manifests = [
        (
            "file.read",
            vec![Capability::FilesystemRead],
            vec![Effect::Read],
            Idempotency::Idempotent,
        ),
        (
            "file.write",
            vec![Capability::FilesystemWrite],
            vec![Effect::Create, Effect::Update],
            Idempotency::NonIdempotent,
        ),
        (
            "file.edit",
            vec![Capability::FilesystemRead, Capability::FilesystemWrite],
            vec![Effect::Read, Effect::Update],
            Idempotency::NonIdempotent,
        ),
        (
            "shell",
            vec![
                Capability::FilesystemRead,
                Capability::FilesystemWrite,
                Capability::ProcessExecute,
            ],
            vec![
                Effect::Read,
                Effect::Create,
                Effect::Update,
                Effect::Delete,
                Effect::Execute,
            ],
            Idempotency::NonIdempotent,
        ),
    ]
    .into_iter()
    .map(|(name, capabilities, effects, idempotency)| ToolManifest {
        tool_name: name.into(),
        capabilities: capabilities.into_iter().collect(),
        effects: effects.into_iter().collect(),
        path_scopes: vec![PathScope::new(workspace.to_str().unwrap())],
        approval,
        idempotency,
    })
    .collect();
    let mut ceiling = permissions();
    if revoke {
        ceiling.tools.clear();
    }
    Ok(AgentHostConfig {
        state_root:root.join("state"),host_id:"production-host-test".into(),
        deployment:HostDeployment {
            protocol:match protocol { "openai_responses"=>DeploymentProtocol::OpenaiResponses,
                "anthropic_messages"=>DeploymentProtocol::AnthropicMessages,_=>return Err("unsupported fixture protocol".into()) },
            descriptor:ModelDescriptor { reference:ModelRef::new("host-loopback",model),context_window:Some(65536),
                max_output_tokens:Some(16384),features },
            base_url:endpoint.into(),api_key_env:"KOLYAN_HOST_LOOPBACK_KEY".into(),timeout_secs:30,
            http_retry:Default::default(),parameter_table:table,
        },
        context:ContextPolicy {id:"host-inspection".into(),revision:"1".into(),mode:BudgetMode::Inspect,
            max_serialized_bytes:2*1024*1024,max_messages:256,max_content_blocks:1024,
            context_limit_tokens:None,output_reserve_tokens:2048},
        catalog:vec![definition("root-agent",model,false),definition("child",model,true)],permissions:ceiling,
        environment:IsolatedToolSetConfig {
            files:IsolatedFileConfig {workspace:workspace.clone(),worker:env!("CARGO_BIN_EXE_kolyan-test-tool-worker").into(),
                staging_root:root.join("staging"),protected_roots:vec![],file_limits:FileOperationLimits {max_read_bytes:8192,max_write_bytes:8192},
                max_output_bytes:65536,timeout:Duration::from_secs(30)},
            shell:IsolatedShellConfig {workspace,protected_roots:vec![],max_command_bytes:8192,
                max_output_bytes:65536,timeout:Duration::from_secs(30)},
        },
        tool_manifests:manifests,tool_scope:".".into(),allow_shell:true,
        delegation:AgentDelegationConfig {limits:InvokePrepareLimits {max_children:4,max_parallel:1,
            max_child_input_bytes:4096,max_output_bytes:65536,admission_timeout_ms:5000},approval:ApprovalMode::Never},
        tool_error_policy:ToolErrorPolicy::ContinueBatch,max_parallel:1,
        template:serde_json::from_value::<ModelRequest>(json!({"request_id":"template","model":{"provider":"host-loopback","model":model},
            "system":[],"messages":[],"tools":[],"tool_choice":"auto","output_format":null,"prompt_cache":null,
            "reasoning":null,"max_output_tokens":2048,"extensions":{}})).map_err(|e|e.to_string())?,
        turn_config:TurnConfig {max_steps:8,max_tool_calls:Some(16),deadline:None},
        task_limits:TaskLimits {max_depth:4,max_invocations:8,max_attempts:16,max_tokens:None,max_steps_per_turn:8},
        cancellation_policy:CancellationPolicy::AllInvocations,
    })
}
