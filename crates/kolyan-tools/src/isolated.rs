//! Exactly four isolated environment tools under one host-selected workspace.
//! Shared control paths, including staging, are denied to every sibling shell.

use kolyan_core::{ToolError, ToolExecutor, ToolFuture, ToolInvocation, ToolPreparationFuture};
use kolyan_model::{ToolCall, ToolDefinition};
use serde_json::json;

use crate::{
    IsolatedFileConfig, IsolatedFileError, IsolatedFileTools, IsolatedShellConfig,
    IsolatedShellError, IsolatedShellTool,
};

/// Independent adapter limits are explicit; both adapters must select the same
/// physical workspace. Construction unions all protected paths for both adapters.
#[derive(Debug, Clone)]
pub struct IsolatedToolSetConfig {
    pub files: IsolatedFileConfig,
    pub shell: IsolatedShellConfig,
}

#[derive(Debug)]
pub enum IsolatedToolSetError {
    Invalid(String),
    Io(std::io::Error),
    File(IsolatedFileError),
    Shell(IsolatedShellError),
}

impl std::fmt::Display for IsolatedToolSetError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Invalid(reason) => write!(formatter, "invalid isolated tool set: {reason}"),
            Self::Io(error) => error.fmt(formatter),
            Self::File(error) => error.fmt(formatter),
            Self::Shell(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for IsolatedToolSetError {}

/// Default environment inventory for Agent assembly. This does not issue grants,
/// select Agent permissions, register policy ceilings or schedule child Agents.
#[derive(Clone)]
pub struct IsolatedToolSet {
    files: IsolatedFileTools,
    shell: IsolatedShellTool,
}

impl IsolatedToolSet {
    /// Fail closed when isolation, workspace identity or protected paths are
    /// unavailable. Blocking filesystem admission belongs in host assembly.
    pub fn new(mut config: IsolatedToolSetConfig) -> Result<Self, IsolatedToolSetError> {
        let workspace = config
            .files
            .workspace
            .canonicalize()
            .map_err(IsolatedToolSetError::Io)?;
        if workspace
            != config
                .shell
                .workspace
                .canonicalize()
                .map_err(IsolatedToolSetError::Io)?
        {
            return Err(IsolatedToolSetError::Invalid(
                "file and shell workspaces differ".into(),
            ));
        }
        let staging = config
            .files
            .staging_root
            .canonicalize()
            .map_err(IsolatedToolSetError::Io)?;
        let mut protected = config.files.protected_roots;
        protected.extend(config.shell.protected_roots);
        protected.push(staging.clone());
        let mut protected = protected
            .into_iter()
            .map(|path| path.canonicalize().map_err(IsolatedToolSetError::Io))
            .collect::<Result<Vec<_>, _>>()?;
        protected.sort();
        protected.dedup();
        config.files.workspace = workspace.clone();
        config.shell.workspace = workspace;
        config.files.staging_root = staging;
        config.files.protected_roots = protected.clone();
        config.shell.protected_roots = protected;
        Ok(Self {
            files: IsolatedFileTools::new(config.files).map_err(IsolatedToolSetError::File)?,
            shell: IsolatedShellTool::new(config.shell).map_err(IsolatedToolSetError::Shell)?,
        })
    }

    /// Advertised schemas match the strict typed adapter inputs. The host must
    /// filter this inventory using the effective Agent tool ceiling; advertising
    /// a definition is never authority to invoke it.
    pub fn tool_definitions() -> Vec<ToolDefinition> {
        vec![
            definition(
                "file.read",
                "Read bounded UTF-8 content from a workspace-relative file.",
                json!({"path":{"type":"string"}}),
                &["path"],
            ),
            definition(
                "file.write",
                "Atomically replace a workspace-relative file with UTF-8 content; its parent must exist.",
                json!({"path":{"type":"string"},"content":{"type":"string"}}),
                &["path", "content"],
            ),
            definition(
                "file.edit",
                "Replace exactly one matching UTF-8 span; optionally require the current SHA-256 digest.",
                json!({"path":{"type":"string"},"old_text":{"type":"string","minLength":1},
                    "new_text":{"type":"string"},"expected_sha256":{"type":"string","pattern":"^[0-9a-fA-F]{64}$"}}),
                &["path", "old_text", "new_text"],
            ),
            IsolatedShellTool::tool_definition(),
        ]
    }
}

impl ToolExecutor for IsolatedToolSet {
    fn prepare(&self, call: ToolCall) -> ToolPreparationFuture<'_> {
        match call.name.as_str() {
            "file.read" | "file.write" | "file.edit" => ToolExecutor::prepare(&self.files, call),
            "shell" => ToolExecutor::prepare(&self.shell, call),
            _ => Box::pin(async move { Err(ToolError::Unavailable { name: call.name }) }),
        }
    }

    fn execute_invocation(&self, invocation: ToolInvocation) -> ToolFuture<'_> {
        match invocation.prepared.call().name.as_str() {
            "file.read" | "file.write" | "file.edit" => self.files.execute_invocation(invocation),
            "shell" => self.shell.execute_invocation(invocation),
            _ => Box::pin(async move {
                Err(ToolError::Unavailable {
                    name: invocation.prepared.call().name.clone(),
                })
            }),
        }
    }
}

fn definition(
    name: &str,
    description: &str,
    properties: serde_json::Value,
    required: &[&str],
) -> ToolDefinition {
    ToolDefinition {
        name: name.into(),
        description: Some(description.into()),
        input_schema: json!({"type":"object","properties":properties,
            "required":required,"additionalProperties":false}),
    }
}

#[cfg(test)]
mod tests;
