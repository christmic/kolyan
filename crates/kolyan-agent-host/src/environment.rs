//! One real native inventory and explicit host policy, reused by all saved agents.

use std::sync::Arc;

use kolyan_agent::{AgentSnapshot, EnvironmentToolFactory, RunnerError, RunnerToolSet};
use kolyan_policy::PolicyEngine;
use kolyan_server::ExecutionRef;
use kolyan_tools::IsolatedToolSet;

#[derive(Clone)]
pub(crate) struct HostEnvironment {
    pub tools: IsolatedToolSet,
    pub policy: Arc<PolicyEngine>,
}

impl EnvironmentToolFactory for HostEnvironment {
    type Executor = IsolatedToolSet;

    fn build(
        &self,
        _: &AgentSnapshot,
        _: &ExecutionRef,
    ) -> Result<RunnerToolSet<IsolatedToolSet>, RunnerError> {
        // Runner independently narrows advertised and executed tools against the
        // exact saved snapshot. Native adapters enforce grants and OS isolation.
        Ok(RunnerToolSet {
            executor: self.tools.clone(),
            definitions: IsolatedToolSet::tool_definitions(),
            policy: self.policy.clone(),
        })
    }
}
