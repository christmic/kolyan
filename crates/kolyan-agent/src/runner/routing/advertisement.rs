//! Discovery narrows model-visible choices, never grants authority or rewrites
//! model output. Immutable resumed requests must still match current discovery.

use crate::{
    AGENT_INVOKE_NAME, AgentCatalog, AgentDefinition, AgentError, AgentPermissions, AgentSnapshot,
};
use kolyan_model::{
    ModelProvider, ModelRequest, ProviderError, ProviderErrorKind, ProviderErrorPhase,
    ProviderFuture, ToolDefinition,
};
use serde_json::{Value, json};

pub(in crate::runner) struct AdvertisementProvider<P> {
    pub inner: P,
    pub expected: Option<ToolDefinition>,
    pub skills: Option<(
        std::sync::Arc<crate::SkillRuntime>,
        crate::VerifiedSkillBinding,
    )>,
}
impl<P: ModelProvider> ModelProvider for AdvertisementProvider<P> {
    fn stream(&self, request: ModelRequest) -> ProviderFuture<'_> {
        let mut advertisements = request
            .tools
            .iter()
            .filter(|tool| tool.name == AGENT_INVOKE_NAME);
        if advertisements.next() != self.expected.as_ref() || advertisements.next().is_some() {
            return Box::pin(async {
                Err(ProviderError::new(
                    ProviderErrorKind::InvalidRequest,
                    ProviderErrorPhase::Validate,
                    "saved Agent invocation advertisement differs from current admitted targets",
                ))
            });
        }
        // Core's recorded request is dispatched unchanged, or rejected. A changed
        // catalog on resume cannot silently rewrite immutable checkpoint history.
        let skills = self.skills.clone();
        Box::pin(async move {
            let expected = skills
                .as_ref()
                .and_then(|(_, binding)| crate::skills::skill_load_definition(binding));
            let mut schemas = request
                .tools
                .iter()
                .filter(|d| d.name == crate::skills::SKILL_LOAD_NAME);
            if schemas.next() != expected.as_ref() || schemas.next().is_some() {
                return Err(ProviderError::new(
                    ProviderErrorKind::InvalidRequest,
                    ProviderErrorPhase::Validate,
                    "saved Skill advertisement differs from exact source binding",
                ));
            }
            if let Some((runtime, binding)) = skills {
                tokio::task::spawn_blocking(move || runtime.validate_current(&binding))
                    .await
                    .map_err(|_| {
                        ProviderError::new(
                            ProviderErrorKind::InvalidRequest,
                            ProviderErrorPhase::Validate,
                            "Skill guard worker failed",
                        )
                    })?
                    .map_err(|error| {
                        ProviderError::new(
                            ProviderErrorKind::InvalidRequest,
                            ProviderErrorPhase::Validate,
                            error.to_string(),
                        )
                    })?;
            }
            self.inner.stream(request).await
        })
    }
}

pub(super) fn definition(
    parent: &AgentSnapshot,
    host: &AgentPermissions,
    catalog: &AgentCatalog,
) -> Result<Option<ToolDefinition>, AgentError> {
    let authority = parent.permissions().intersection(host)?;
    let named = catalog.admitted_named_definitions(parent, host)?;
    let base = super::schema::input_schema();
    let template = base["properties"]["children"]["items"].clone();
    let targets = template["properties"]["target"]["oneOf"]
        .as_array()
        .expect("static schema targets");
    let mut children = Vec::new();
    if authority.delegation.allow_self {
        let ceiling = authority.intersection(parent.definition().permissions())?;
        children.push(child(&template, targets[0].clone(), &ceiling, &named));
    }
    for target in &named {
        let key = target.key();
        let mut branch = targets[1].clone();
        branch["properties"]["value"] = json!({"type":"object","additionalProperties":false,
            "required":["definition_id","revision"],"properties":{
                "definition_id":{"const":key.definition_id},"revision":{"const":key.revision}}});
        let ceiling = authority.intersection(target.permissions())?;
        children.push(child(&template, branch, &ceiling, &named));
    }
    if authority.delegation.allow_inline {
        children.push(child(&template, targets[2].clone(), &authority, &named));
    }
    if children.is_empty() {
        return Ok(None);
    }
    let mut input = base;
    input["properties"]["parallel"]["description"] = json!(
        "A scheduling request, not a concurrency grant. The host enforces resource safety and its concurrency budget."
    );
    input["properties"]["children"]["items"] = json!({"oneOf":children});
    Ok(Some(ToolDefinition{name:AGENT_INVOKE_NAME.into(),description:Some(
        "Invoke only the advertised exact named revisions or admitted inline/self targets; select child permissions within their advertised ceilings. Await durable child results.".into()),input_schema:input}))
}

fn child(
    template: &Value,
    target: Value,
    ceiling: &AgentPermissions,
    named: &[AgentDefinition],
) -> Value {
    let mut branch = template.clone();
    branch["properties"]["target"] = target;
    branch["properties"]["target"]["description"] = json!(
        "The Agent invoked by this call. This target does not grant the child permission to invoke the same target later."
    );
    if branch["properties"]["target"]["properties"]["kind"]["const"] == "inline" {
        branch["properties"]["target"]["properties"]["value"]["description"] = json!(
            "An immutable Agent definition with a static permission ceiling, not an execution grant. The separate child permissions select this invocation's effective authority."
        );
    }
    let permissions = &mut branch["properties"]["permissions"];
    permissions["description"] = json!(
        "Requested ceiling for this child and its descendants, bounded by the advertised target ceiling. Descendants cannot regain omitted tools. If an intermediate child must not read itself but must delegate reading, retain file.read here and express the no-direct-read task in its input."
    );
    let tools: Vec<_> = ceiling.tools.iter().map(|tool| tool.name()).collect();
    permissions["properties"]["tools"]["description"] = json!(
        "Environment tools available to the child and as an upper bound for its descendants. agent.invoke is not an environment tool; delegation is configured separately. Use [] for no tools, never an empty string."
    );
    permissions["properties"]["tools"]["items"] = if tools.is_empty() {
        json!(false)
    } else {
        json!({"enum":tools})
    };
    if tools.is_empty() {
        permissions["properties"]["tools"]["maxItems"] = json!(0);
        permissions["properties"]["tools"]["examples"] = json!([[]]);
    }
    let keys: Vec<_> = named
        .iter()
        .map(AgentDefinition::key)
        .filter(|key| ceiling.delegation.named_targets.contains(key))
        .collect();
    let delegation = &mut permissions["properties"]["delegation"]["properties"];
    delegation["named_targets"]["description"] = json!(
        "Exact named revisions this child may invoke later, not the target of this call. Choose only advertised entries. Use [] for no named delegation, never an empty string; do not copy the current target unless independently allowed here."
    );
    delegation["named_targets"]["items"] = if keys.is_empty() {
        json!(false)
    } else {
        json!({"enum":keys})
    };
    if keys.is_empty() {
        delegation["named_targets"]["maxItems"] = json!(0);
        delegation["named_targets"]["examples"] = json!([[]]);
    }
    if !ceiling.delegation.allow_self {
        delegation["allow_self"] = json!({"type":"boolean","const":false});
    }
    if !ceiling.delegation.allow_inline {
        delegation["allow_inline"] = json!({"type":"boolean","const":false});
    }
    delegation["allow_self"]["description"] = json!(
        "Whether this child may invoke its own saved definition later. false removes that authority; true is allowed only within the advertised ceiling."
    );
    delegation["allow_inline"]["description"] = json!(
        "Whether this child may invoke inline Agent definitions later. false removes that authority; true is allowed only within the advertised ceiling."
    );
    branch
}

#[cfg(test)]
mod tests;
