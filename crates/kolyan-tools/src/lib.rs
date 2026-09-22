//! Tool registration and execution boundaries.

use kolyan_core::{ToolError, ToolExecutor, ToolFuture};
use kolyan_model::{ToolCall, ToolDefinition, ToolResult};
use serde_json::Value;
use std::fs;
use std::path::{Component, Path, PathBuf};

/// A deliberately small, non-shell command tool for V0 Turn integration.
///
/// It accepts only structured query commands and never invokes a shell or
/// interprets arbitrary command strings.
#[derive(Debug, Clone)]
pub struct RestrictedShellTool {
    root: PathBuf,
}

impl RestrictedShellTool {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
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
            "pwd" => Ok(self.root.display().to_string()),
            "list" => self.list(path),
            "count_lines" => self.count_lines(path),
            "count_entries" => self.count_entries(path),
            other => Err(format!("unsupported shell.query command: {other}")),
        }
    }

    fn resolve(&self, relative: &str) -> Result<PathBuf, String> {
        let path = Path::new(relative);
        if path.is_absolute()
            || path
                .components()
                .any(|component| matches!(component, Component::ParentDir | Component::RootDir))
        {
            return Err("path must stay below the configured tool root".into());
        }
        Ok(self.root.join(path))
    }

    fn list(&self, relative: &str) -> Result<String, String> {
        let path = self.resolve(relative)?;
        let mut names = fs::read_dir(path)
            .map_err(|error| error.to_string())?
            .map(|entry| entry.map(|entry| entry.file_name().to_string_lossy().into_owned()))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| error.to_string())?;
        names.sort();
        Ok(names.join("\n"))
    }

    fn count_lines(&self, relative: &str) -> Result<String, String> {
        let path = self.resolve(relative)?;
        let content = fs::read_to_string(path).map_err(|error| error.to_string())?;
        Ok(content.lines().count().to_string())
    }

    fn count_entries(&self, relative: &str) -> Result<String, String> {
        let path = self.resolve(relative)?;
        let count = fs::read_dir(path)
            .map_err(|error| error.to_string())?
            .count();
        Ok(count.to_string())
    }
}

impl ToolExecutor for RestrictedShellTool {
    fn execute(&self, call: ToolCall) -> ToolFuture<'_> {
        Box::pin(async move { self.execute_query(call) })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::FutureExt;
    use std::fs;

    #[test]
    fn exposes_only_restricted_query_commands() {
        let definition = RestrictedShellTool::tool_definition();
        assert_eq!(definition.name, "shell.query");
        assert!(
            definition.input_schema["properties"]["command"]["enum"]
                .as_array()
                .is_some_and(|commands| commands.len() == 4)
        );
    }

    #[test]
    fn counts_lines_without_executing_a_shell() {
        let root = std::env::temp_dir().join(format!("kolyan-tool-{}", std::process::id()));
        fs::create_dir_all(&root).expect("temporary root should be created");
        fs::write(root.join("input.txt"), "one\ntwo\n").expect("fixture should be written");
        let tool = RestrictedShellTool::new(&root);
        let result = tool
            .execute(ToolCall {
                id: "call-1".into(),
                name: "shell.query".into(),
                arguments: serde_json::json!({"command":"count_lines","path":"input.txt"}),
            })
            .now_or_never()
            .expect("query should complete")
            .expect("query should succeed");

        assert_eq!(result.content, "2");
        assert!(!result.is_error);
        fs::remove_file(root.join("input.txt")).expect("fixture should be removed");
        fs::remove_dir(root).expect("temporary root should be removed");
    }

    #[test]
    fn rejects_parent_paths() {
        let tool = RestrictedShellTool::new(std::env::temp_dir());
        let result = tool
            .execute(ToolCall {
                id: "call-2".into(),
                name: "shell.query".into(),
                arguments: serde_json::json!({"command":"list","path":"../"}),
            })
            .now_or_never()
            .expect("query should complete")
            .expect("tool should return a ToolResult error");

        assert!(result.is_error);
    }
}
