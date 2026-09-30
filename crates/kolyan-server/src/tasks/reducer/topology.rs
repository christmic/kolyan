//! Task topology transitions; shared admission and replay invariants.

use super::*;

pub(super) fn admit(
    state: &mut TaskSnapshot,
    definition: InvocationDefinition,
) -> Result<(), TaskError> {
    available_id(state, &definition.invocation_id)?;
    agent(&definition.agent)?;
    digest(&definition.constraints_digest)?;
    if state.invocations.len() as u64 >= state.definition.limits.max_invocations {
        return Err(transition("shared invocation budget exhausted"));
    }
    let parent = definition.parent_invocation_id.as_ref();
    match definition.role {
        InvocationRole::Root => {
            if parent.is_some()
                || state
                    .invocations
                    .values()
                    .any(|inv| inv.definition.role == InvocationRole::Root)
                || definition.agent != state.definition.agent
                || definition.constraints_digest != state.definition.constraints_digest
            {
                return Err(invalid(
                    "root identity/constraints mismatch or duplicate root",
                ));
            }
        }
        _ => {
            let parent = parent.ok_or_else(|| invalid("non-root requires explicit parent"))?;
            let owner = state
                .invocations
                .get(parent)
                .ok_or_else(|| invalid("unknown parent"))?;
            if definition.role == InvocationRole::Continuation {
                if owner.state != InvocationState::Completed
                    || !definition.dependencies.contains(parent)
                {
                    return Err(transition(
                        "continuation requires a completed predecessor and explicit result dependency",
                    ));
                }
            } else if matches!(
                owner.state,
                InvocationState::Completed | InvocationState::Cancelled | InvocationState::Failed
            ) {
                return Err(transition("parent is terminal"));
            }
            if definition.role == InvocationRole::SelfCall
                && definition.agent.definition_id != owner.definition.agent.definition_id
            {
                return Err(invalid("self-call must use the same agent definition"));
            }
            let mut depth = u32::from(definition.role != InvocationRole::Continuation);
            let mut ancestor = Some(owner);
            while let Some(invocation) = ancestor {
                if invocation.definition.parent_invocation_id.is_some()
                    && invocation.definition.role != InvocationRole::Continuation
                {
                    depth += 1;
                }
                ancestor = invocation
                    .definition
                    .parent_invocation_id
                    .as_ref()
                    .and_then(|id| state.invocations.get(id));
            }
            if depth > state.definition.limits.max_depth {
                return Err(transition("recursion budget exhausted"));
            }
        }
    }
    let mut dependencies = BTreeSet::new();
    for id in &definition.dependencies {
        if !state.invocations.contains_key(id) || !dependencies.insert(id) {
            return Err(invalid("missing or duplicate dependency"));
        }
    }
    state.invocations.insert(
        definition.invocation_id.clone(),
        InvocationSnapshot {
            definition,
            state: InvocationState::Admitted,
            attempts: Vec::new(),
            consumed_results: Default::default(),
            completion_fact: None,
            cancellation_requested: false,
        },
    );
    Ok(())
}

pub(super) fn add_dependency(
    state: &mut TaskSnapshot,
    invocation: &str,
    dependency: &str,
) -> Result<(), TaskError> {
    let owner = state
        .invocations
        .get(invocation)
        .ok_or_else(|| invalid("unknown invocation"))?;
    if !matches!(
        owner.state,
        InvocationState::Admitted | InvocationState::Running | InvocationState::Suspended
    ) || !state.invocations.contains_key(dependency)
        || owner
            .definition
            .dependencies
            .iter()
            .any(|id| id == dependency)
        || reaches(state, dependency, invocation, &mut BTreeSet::new())
    {
        return Err(transition("invalid dependency or cyclic result topology"));
    }
    state
        .invocations
        .get_mut(invocation)
        .unwrap()
        .definition
        .dependencies
        .push(dependency.into());
    Ok(())
}

pub(super) fn reaches(
    state: &TaskSnapshot,
    from: &str,
    target: &str,
    seen: &mut BTreeSet<String>,
) -> bool {
    if from == target {
        return true;
    }
    if !seen.insert(from.to_owned()) {
        return false;
    }
    state.invocations.get(from).is_some_and(|inv| {
        inv.definition
            .dependencies
            .iter()
            .any(|id| reaches(state, id, target, seen))
    })
}

pub(super) fn required_results(state: &TaskSnapshot, invocation: &str) -> BTreeSet<String> {
    let mut ids: BTreeSet<String> = state.invocations[invocation]
        .definition
        .dependencies
        .iter()
        .cloned()
        .collect();
    ids.extend(
        state
            .invocations
            .iter()
            .filter(|(_, inv)| {
                inv.definition.parent_invocation_id.as_deref() == Some(invocation)
                    && inv.definition.role != InvocationRole::Continuation
            })
            .map(|(id, _)| id.clone()),
    );
    ids
}

pub(super) fn consume(
    state: &mut TaskSnapshot,
    invocation: &str,
    result: ConsumedResult,
    record: &FactRecord,
) -> Result<(), TaskError> {
    let inv = state
        .invocations
        .get(invocation)
        .ok_or_else(|| invalid("unknown result consumer"))?;
    if matches!(
        inv.state,
        InvocationState::Completed | InvocationState::Cancelled | InvocationState::Failed
    ) || !required_results(state, invocation).contains(&result.child_invocation_id)
        || inv
            .consumed_results
            .contains_key(&result.child_invocation_id)
    {
        return Err(transition(
            "not an admitted result edge or result already consumed",
        ));
    }
    let child = state
        .invocations
        .get(&result.child_invocation_id)
        .ok_or_else(|| invalid("unknown child"))?;
    let actual = child
        .attempts
        .last()
        .and_then(|id| state.attempts[id].observation.as_ref());
    let Some(AttemptObservation {
        outcome: AttemptOutcome::Completed { evidence },
        ..
    }) = actual
    else {
        return Err(transition("child has no completed attempt"));
    };
    if child.completion_fact.as_ref() != Some(&result.completion_fact)
        || result.evidence != *evidence
        || !record.draft.causes.contains(&result.completion_fact)
    {
        return Err(invalid(
            "result must reference exact child completion and evidence",
        ));
    }
    state
        .invocations
        .get_mut(invocation)
        .unwrap()
        .consumed_results
        .insert(result.child_invocation_id.clone(), result);
    Ok(())
}
