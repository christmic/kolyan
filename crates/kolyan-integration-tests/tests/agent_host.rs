//! Production Host, localhost official-protocol SSE and actual native effects.
//! Scripted localhost replies are not real-provider/model acceptance.

#[cfg(target_os = "macos")]
#[path = "agent_host/mod.rs"]
mod agent_host;
