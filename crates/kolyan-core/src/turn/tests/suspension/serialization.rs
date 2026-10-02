use super::*;

#[tokio::test]
async fn unified_envelope_and_approval_reason_reject_missing_or_unknown_fields() {
    let (executor, _) = executor(
        vec![call("approved", "file.write", "safe/a")],
        ScenarioTools::new(&[]),
        ToolDispatchMode::Serial,
        true,
    );
    let waiting = suspended(executor.start_resumable(input(1)).await.unwrap());
    let value = serde_json::to_value(&waiting).unwrap();
    for (pointer, key) in [
        ("", "waiting"),
        ("/waiting", "external_waits"),
        ("/waiting/approvals/0", "expires_at_ms"),
        ("/checkpoint/approvals/0", "reason"),
    ] {
        let mut bad = value.clone();
        bad.pointer_mut(pointer)
            .unwrap()
            .as_object_mut()
            .unwrap()
            .remove(key);
        assert!(serde_json::from_value::<TurnSuspension>(bad).is_err());
    }
    for pointer in [
        "",
        "/waiting",
        "/waiting/approvals/0",
        "/checkpoint/dispatch",
    ] {
        let mut bad = value.clone();
        bad.pointer_mut(pointer)
            .unwrap()
            .as_object_mut()
            .unwrap()
            .insert("unknown_critical".into(), Value::Bool(true));
        assert!(serde_json::from_value::<TurnSuspension>(bad).is_err());
    }
    let mut forged = waiting.clone();
    forged.waiting.approvals.clear();
    assert!(forged.validate(&waiting.checkpoint.scope).is_err());
}

#[tokio::test]
async fn confirmed_scope_digest_revision_and_cancel_are_checked_before_any_effect_or_model() {
    let tools = ScenarioTools::new(&[]);
    let (executor, requests) = executor(
        vec![call("approved", "file.write", "a")],
        tools.clone(),
        ToolDispatchMode::Serial,
        true,
    );
    let waiting = suspended(executor.start_resumable(input(1)).await.unwrap());
    let saved = &waiting.checkpoint.approvals[0];
    let original = ApprovalConfirmation {
        approval_id: saved.approval_id.clone(),
        prepared_digest: saved.prepared.digest().into(),
        policy_revision: saved.policy_revision.clone(),
        scope: saved.scope.clone(),
        evidence_id: "trusted-evidence".into(),
    };
    for index in 0..4 {
        let mut bad = original.clone();
        match index {
            0 => bad.scope.execution.session_id.push('x'),
            1 => bad.prepared_digest.push('x'),
            2 => bad.policy_revision.push('x'),
            _ => bad.approval_id.push('x'),
        }
        assert!(
            executor
                .merge_resume_with_control(
                    waiting.clone(),
                    ResumeInput::ApprovalConfirmed(bad),
                    waiting.checkpoint.scope.clone(),
                    TurnControl::default()
                )
                .is_err()
        );
    }
    let control = TurnControl::default();
    control.cancel();
    assert!(matches!(
        executor.merge_resume_with_control(
            waiting.clone(),
            ResumeInput::ApprovalConfirmed(original),
            waiting.checkpoint.scope.clone(),
            control
        ),
        Err(TurnError::Cancelled)
    ));
    assert_eq!(tools.executed().len(), 0);
    assert_eq!(requests.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn checkpoint_reconstruction_uses_completed_step_and_exact_receipt_states_without_polling() {
    let tools = ScenarioTools::new(&[("child", Behavior::Wait)]);
    let (executor, requests) = executor(
        vec![call("child", "file.read", "a")],
        tools.clone(),
        ToolDispatchMode::Serial,
        false,
    );
    let waiting = suspended(executor.start_resumable(input(1)).await.unwrap());
    let source = waiting.checkpoint;
    let reconstructed = TurnCheckpoint::reconstruct(
        CheckpointReconstruction {
            scope: source.scope.clone(),
            model_request: source.model_request.clone(),
            input_message_count: source.input_message_count,
            steps: source.steps.clone(),
            calls: source.calls.clone(),
            stages: source.stages.clone(),
            stage_index: source.stage_index,
            approvals: source.approvals.clone(),
            budget: source.budget.clone(),
            dispatch: source.dispatch,
        },
        &source.scope,
    )
    .unwrap();
    assert_eq!(reconstructed, source);
    let again = suspended(
        executor
            .resume_checkpoint_with_control(
                reconstructed,
                source.scope.clone(),
                TurnControl::default(),
            )
            .await
            .unwrap(),
    );
    assert_eq!(again.waiting.external_waits.len(), 1);
    assert_eq!(tools.executed(), ["child"]);
    assert_eq!(requests.lock().unwrap().len(), 1);
}
