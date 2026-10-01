//! Tool registration and execution boundaries.

mod exact_file;
mod file_operations;
mod isolated;
mod isolated_file;
mod isolated_shell;
mod workspace;
pub use exact_file::{
    ExactDirectoryBinding, ExactFileBinding, ExactFileIdentity, ExactFileStaging,
    ExactFileWorkerRequest, execute_exact,
};
pub use isolated::{IsolatedToolSet, IsolatedToolSetConfig, IsolatedToolSetError};
pub use isolated_file::{IsolatedFileConfig, IsolatedFileError, IsolatedFileTools};
pub use isolated_shell::{
    IsolatedShellConfig, IsolatedShellError, IsolatedShellTool, ShellArguments,
};

pub use file_operations::{
    EditArguments, FileOperation, FileOperationError, FileOperationLimits, FileOperationResult,
    FileOperations, ReadArguments, WriteArguments,
};

use kolyan_core::{ToolError, ToolExecutor, ToolFuture, ToolInvocation, ToolPreparationFuture};
use kolyan_model::{ToolCall, ToolDefinition, ToolResult};
use kolyan_policy::{
    InvocationClaim, PolicyContext, PolicyDecisionKind, PolicyResolver, PreparedCall,
    ResourceClaim, ToolManifest, ToolRequirements,
};
use serde_json::Value;
use std::path::PathBuf;
use std::sync::Arc;
use workspace::Workspace;

/// A deliberately small, non-shell command tool for V0 Turn integration.
///
/// It accepts only structured query commands and never invokes a shell or
/// interprets arbitrary command strings.
#[derive(Debug, Clone)]
pub struct RestrictedShellTool {
    root: Workspace,
}

impl RestrictedShellTool {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: Workspace::new(root),
        }
    }

    pub fn tool_definition() -> ToolDefinition {
        ToolDefinition {
            name: "shell.query".into(),
            description: Some(
                "Run a restricted filesystem query: pwd, list, count_lines, or count_entries."
                    .into(),
            ),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "command": {
                        "type": "string",
                        "enum": ["pwd", "list", "count_lines", "count_entries"]
                    },
                    "path": {"type": "string"}
                },
                "required": ["command"],
                "additionalProperties": false
            }),
        }
    }

    pub fn tool_manifest() -> ToolManifest {
        ToolManifest {
            tool_name: "shell.query".into(),
            capabilities: [kolyan_policy::Capability::ProcessInspect]
                .into_iter()
                .collect(),
            effects: [kolyan_policy::Effect::Read].into_iter().collect(),
            path_scopes: Vec::new(),
            idempotency: kolyan_policy::Idempotency::Idempotent,
            approval: kolyan_policy::ApprovalMode::Never,
        }
    }

    fn execute_query(&self, call: ToolCall) -> Result<ToolResult, ToolError> {
        if call.name != "shell.query" {
            return Err(ToolError::Unavailable { name: call.name });
        }

        let (content, is_error) = match self.run_query(&call.arguments) {
            Ok(content) => (content, false),
            Err(error) => (error, true),
        };
        Ok(ToolResult {
            call_id: call.id,
            content,
            is_error,
        })
    }

    fn run_query(&self, arguments: &Value) -> Result<String, String> {
        let object = arguments
            .as_object()
            .ok_or_else(|| "arguments must be an object".to_string())?;
        let command = object
            .get("command")
            .and_then(Value::as_str)
            .ok_or_else(|| "command must be a string".to_string())?;
        let path = object.get("path").and_then(Value::as_str).unwrap_or(".");

        match command {
            "pwd" => Ok(self.root.path.display().to_string()),
            "list" => self.list(path),
            "count_lines" => self.count_lines(path),
            "count_entries" => self.count_entries(path),
            other => Err(format!("unsupported shell.query command: {other}")),
        }
    }

    fn list(&self, relative: &str) -> Result<String, String> {
        let path = Workspace::relative(relative)?;
        let mut names = self
            .root
            .directory()?
            .read_dir(path)
            .map_err(|error| error.to_string())?
            .map(|entry| entry.map(|entry| entry.file_name().to_string_lossy().into_owned()))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| error.to_string())?;
        names.sort();
        Ok(names.join("\n"))
    }

    fn count_lines(&self, relative: &str) -> Result<String, String> {
        let path = Workspace::relative(relative)?;
        let content = self
            .root
            .directory()?
            .read_to_string(path)
            .map_err(|error| error.to_string())?;
        Ok(content.lines().count().to_string())
    }

    fn count_entries(&self, relative: &str) -> Result<String, String> {
        let path = Workspace::relative(relative)?;
        let count = self
            .root
            .directory()?
            .read_dir(path)
            .map_err(|error| error.to_string())?
            .count();
        Ok(count.to_string())
    }
}

impl ToolExecutor for RestrictedShellTool {
    fn prepare(&self, call: ToolCall) -> ToolPreparationFuture<'_> {
        Box::pin(async move { prepare_restricted(&self.root, call, Self::tool_manifest()) })
    }

    fn execute_invocation(&self, invocation: ToolInvocation) -> ToolFuture<'_> {
        Box::pin(async move {
            validate_restricted(self, &invocation).await?;
            bounded_result(
                self.execute_query(invocation.prepared.call().clone())?,
                &invocation,
            )
        })
    }
}

/// A deliberately small workspace-scoped file tool.
///
/// It only reads and writes UTF-8 files below the configured root. It does
/// not create directories, execute commands, or resolve paths outside root.
#[derive(Debug, Clone)]
pub struct RestrictedFileTool {
    root: Workspace,
}

impl RestrictedFileTool {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: Workspace::new(root),
        }
    }

    pub fn tool_definitions() -> Vec<ToolDefinition> {
        vec![
            ToolDefinition {
                name: "file.read".into(),
                description: Some("Read a UTF-8 file below the configured workspace root.".into()),
                input_schema: serde_json::json!({
                    "type": "object",
                    "properties": {"path": {"type": "string"}},
                    "required": ["path"],
                    "additionalProperties": false
                }),
            },
            ToolDefinition {
                name: "file.write".into(),
                description: Some(
                    "Write UTF-8 content to a file below the configured workspace root.".into(),
                ),
                input_schema: serde_json::json!({
                    "type": "object",
                    "properties": {
                        "path": {"type": "string"},
                        "content": {"type": "string"}
                    },
                    "required": ["path", "content"],
                    "additionalProperties": false
                }),
            },
        ]
    }

    pub fn tool_manifests() -> Vec<ToolManifest> {
        vec![
            ToolManifest {
                tool_name: "file.read".into(),
                capabilities: [kolyan_policy::Capability::FilesystemRead]
                    .into_iter()
                    .collect(),
                effects: [kolyan_policy::Effect::Read].into_iter().collect(),
                path_scopes: Vec::new(),
                idempotency: kolyan_policy::Idempotency::Idempotent,
                approval: kolyan_policy::ApprovalMode::Never,
            },
            ToolManifest {
                tool_name: "file.write".into(),
                capabilities: [kolyan_policy::Capability::FilesystemWrite]
                    .into_iter()
                    .collect(),
                effects: [kolyan_policy::Effect::Create, kolyan_policy::Effect::Update]
                    .into_iter()
                    .collect(),
                path_scopes: Vec::new(),
                idempotency: kolyan_policy::Idempotency::NonIdempotent,
                approval: kolyan_policy::ApprovalMode::Never,
            },
        ]
    }

    fn execute_file(&self, call: ToolCall) -> Result<ToolResult, ToolError> {
        let result = self.run_file(&call.name, &call.arguments);
        match result {
            Ok(content) => Ok(ToolResult {
                call_id: call.id,
                content,
                is_error: false,
            }),
            Err(content) => Ok(ToolResult {
                call_id: call.id,
                content,
                is_error: true,
            }),
        }
    }

    fn run_file(&self, name: &str, arguments: &Value) -> Result<String, String> {
        if name != "file.read" && name != "file.write" {
            return Err(format!("unsupported file tool: {name}"));
        }
        let object = arguments
            .as_object()
            .ok_or_else(|| "arguments must be an object".to_string())?;
        let path = object
            .get("path")
            .and_then(Value::as_str)
            .ok_or_else(|| "path must be a string".to_string())?;
        let path = Workspace::relative(path)?;
        let directory = self.root.directory()?;

        match name {
            "file.read" => directory
                .read_to_string(path)
                .map_err(|error| error.to_string()),
            "file.write" => {
                let content = object
                    .get("content")
                    .and_then(Value::as_str)
                    .ok_or_else(|| "content must be a string".to_string())?;
                directory
                    .write(path, content)
                    .map_err(|error| error.to_string())?;
                Ok(format!("wrote {} bytes", content.len()))
            }
            _ => unreachable!(),
        }
    }
}

/// Adds the policy decision and grant check at the tool execution boundary.
/// The wrapped executor remains unaware of policy and cannot accidentally
/// bypass the resolver through a normal Turn execution.
pub struct PolicyEnforcingTool<T, R> {
    inner: T,
    resolver: Arc<R>,
}

impl<T, R> PolicyEnforcingTool<T, R> {
    pub fn new(inner: T, resolver: Arc<R>) -> Self {
        Self { inner, resolver }
    }
}

impl<T, R> ToolExecutor for PolicyEnforcingTool<T, R>
where
    T: ToolExecutor,
    R: PolicyResolver + 'static,
{
    fn prepare(&self, call: ToolCall) -> ToolPreparationFuture<'_> {
        self.inner.prepare(call)
    }

    fn execute_invocation(&self, invocation: ToolInvocation) -> ToolFuture<'_> {
        Box::pin(async move {
            let decision = self.resolver.decide_prepared(
                &invocation.prepared,
                &PolicyContext {
                    turn_id: Some(invocation.scope.execution.turn_id.clone()),
                    ..Default::default()
                },
            );
            invocation
                .grant
                .validate(
                    &invocation.prepared,
                    &decision.policy_version,
                    &invocation.scope,
                )
                .map_err(|error| ToolError::PolicyDenied {
                    message: error.to_string(),
                })?;
            if decision.kind == PolicyDecisionKind::Deny
                || decision.policy_version != invocation.policy_revision
            {
                return Err(ToolError::PolicyDenied {
                    message: decision.reason,
                });
            }
            self.inner.execute_invocation(invocation).await
        })
    }
}

impl ToolExecutor for RestrictedFileTool {
    fn prepare(&self, call: ToolCall) -> ToolPreparationFuture<'_> {
        Box::pin(async move {
            let manifest = Self::tool_manifests()
                .into_iter()
                .find(|manifest| manifest.tool_name == call.name)
                .ok_or_else(|| ToolError::Unavailable {
                    name: call.name.clone(),
                })?;
            prepare_restricted(&self.root, call, manifest)
        })
    }

    fn execute_invocation(&self, invocation: ToolInvocation) -> ToolFuture<'_> {
        Box::pin(async move {
            validate_restricted(self, &invocation).await?;
            bounded_result(
                self.execute_file(invocation.prepared.call().clone())?,
                &invocation,
            )
        })
    }
}

// These deliberately non-process primitives remain explicit host adapters for
// existing in-process fixtures. They are not the default sandboxed Agent inventory.
fn prepare_restricted(
    root: &Workspace,
    call: ToolCall,
    manifest: ToolManifest,
) -> Result<PreparedCall, ToolError> {
    if call.name != manifest.tool_name {
        return Err(ToolError::Unavailable { name: call.name });
    }
    let object = call
        .arguments
        .as_object()
        .ok_or_else(|| ToolError::Failed {
            message: "tool arguments must be an object".into(),
        })?;
    let path = object.get("path").and_then(Value::as_str).unwrap_or(".");
    let relative = Workspace::relative(path).map_err(|message| ToolError::Failed { message })?;
    root.directory()
        .map_err(|message| ToolError::Failed { message })?;
    let resource = root.path.join(relative).to_string_lossy().into_owned();
    let revision = format!("restricted-v2/{}", root.path.display());
    PreparedCall::new(
        call,
        revision,
        InvocationClaim {
            tool_name: manifest.tool_name,
            capabilities: manifest.capabilities,
            effects: manifest.effects,
            resource: ResourceClaim {
                path: Some(resource),
            },
            idempotency: manifest.idempotency,
        },
        ToolRequirements {
            process_sandbox: false,
            max_output_bytes: 1024 * 1024,
            timeout_ms: 30_000,
        },
    )
    .map_err(|error| ToolError::Failed {
        message: error.to_string(),
    })
}

async fn validate_restricted<T: ToolExecutor>(
    tool: &T,
    invocation: &ToolInvocation,
) -> Result<(), ToolError> {
    if invocation.control.is_cancelled() {
        return Err(ToolError::Cancelled);
    }
    let current = tool.prepare(invocation.prepared.call().clone()).await?;
    if current != invocation.prepared || current.requirements().process_sandbox {
        return Err(ToolError::PolicyDenied {
            message: "restricted adapter preparation mismatch".into(),
        });
    }
    invocation
        .grant
        .validate(&current, &invocation.policy_revision, &invocation.scope)
        .map_err(|error| ToolError::PolicyDenied {
            message: error.to_string(),
        })
}

fn bounded_result(
    result: ToolResult,
    invocation: &ToolInvocation,
) -> Result<ToolResult, ToolError> {
    if result.content.len() as u64
        > invocation
            .grant
            .constraints()
            .max_output_bytes
            .expect("validated limit")
    {
        return Err(ToolError::Failed {
            message: "tool result exceeds its granted byte limit".into(),
        });
    }
    Ok(result)
}

#[cfg(test)]
mod tests;
