use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PromptCacheConfig {
    pub key: Option<String>,
    pub retention: Option<CacheRetention>,
    pub breakpoints: Vec<CacheBreakpoint>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CacheRetention {
    InMemory,
    TwentyFourHours,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CacheBreakpoint {
    System,
    Tools,
    Messages,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cache_config_serializes_as_provider_neutral_data() {
        let config = PromptCacheConfig {
            key: Some("agent-system-v1".into()),
            retention: Some(CacheRetention::TwentyFourHours),
            breakpoints: vec![CacheBreakpoint::System, CacheBreakpoint::Tools],
        };
        let value = serde_json::to_value(config).unwrap();
        assert_eq!(value["retention"], "twenty_four_hours");
        assert_eq!(value["breakpoints"][0], "system");
    }
}
