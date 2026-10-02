//! Reusable production Agent assembly, independent of HTTP and CLI transports.
//! Provider selection is exact; persisted ownership remains the Runner's authority.

mod approvals;
mod environment;
mod host;
mod operations;
mod providers;
mod skills;

pub use approvals::ApprovalDecision;
pub use host::{AgentHost, AgentHostConfig};
pub use operations::{
    FileGoalInput, HostAttemptView, HostStartRequest, HostTaskView, PendingApproval,
};
pub use providers::{
    DeploymentProtocol, HostDeployment, HostPreparedGeneration, HostProvider, HostProviderFactory,
};
pub use skills::HostSkillsConfig;
