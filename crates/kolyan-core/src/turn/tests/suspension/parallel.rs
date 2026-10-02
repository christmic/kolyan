use super::*;

#[tokio::test]
async fn parallel_current_stage_drains_short_outcomes_and_partial_results_block_conflict_tail() {
    let tools = ScenarioTools::new(&[("a", Behavior::Wait), ("b", Behavior::Wait)]);
    let (executor, requests) = executor(
        vec![
            call("a", "file.read", "shared"),
            call("b", "file.read", "independent"),
            call("tail", "file.write", "shared"),
        ],
        tools.clone(),
        ToolDispatchMode::Parallel,
        false,
    );
    let waiting = suspended(executor.start_resumable(input(3)).await.unwrap());
    assert_eq!(tools.executed(), ["a", "b"]);
    assert_eq!(waiting.waiting.external_waits.len(), 2);
    assert_eq!(waiting.checkpoint.budget.tool_calls_used, 2);
    let counts = [tools.prepare_count("a"), tools.prepare_count("b")];
    let checkpoint = resolve(&executor, waiting, &["b"]);
    let scope = checkpoint.scope.clone();
    let partial = suspended(
        executor
            .resume_checkpoint_with_control(checkpoint, scope, TurnControl::default())
            .await
            .unwrap(),
    );
    assert_eq!(partial.waiting.external_waits.len(), 1);
    assert_eq!(partial.waiting.external_waits[0].call_id, "a");
    assert_eq!(tools.executed(), ["a", "b"]);
    assert_eq!(requests.lock().unwrap().len(), 1);
    let checkpoint = resolve(&executor, partial, &["a"]);
    let scope = checkpoint.scope.clone();
    assert!(matches!(
        executor
            .resume_checkpoint_with_control(checkpoint, scope, TurnControl::default())
            .await
            .unwrap(),
        ResumableTurn::Completed(_)
    ));
    assert_eq!(tools.executed(), ["a", "b", "tail"]);
    assert_eq!([tools.prepare_count("a"), tools.prepare_count("b")], counts);
    assert_eq!(requests.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn mixed_approval_and_external_wait_summary_is_rederived_after_either_partial_merge() {
    for approval_first in [true, false] {
        let tools = ScenarioTools::new(&[("child", Behavior::Wait)]);
        let (executor, requests) = executor(
            vec![
                call("child", "file.read", "child"),
                call("approved", "file.write", "other"),
            ],
            tools.clone(),
            ToolDispatchMode::Parallel,
            true,
        );
        let waiting = suspended(executor.start_resumable(input(2)).await.unwrap());
        assert_eq!(waiting.waiting.approvals.len(), 1);
        assert_eq!(waiting.waiting.external_waits.len(), 1);
        assert_eq!(tools.executed(), ["child"]);
        let scope = waiting.checkpoint.scope.clone();
        let saved = waiting.checkpoint.approvals[0].clone();
        let confirmation = ResumeInput::ApprovalConfirmed(ApprovalConfirmation {
            approval_id: saved.approval_id.clone(),
            prepared_digest: saved.prepared.digest().into(),
            policy_revision: saved.policy_revision.clone(),
            scope: scope.clone(),
            evidence_id: "trusted-confirmation".into(),
        });
        let checkpoint = if approval_first {
            executor
                .merge_resume_with_control(
                    waiting,
                    confirmation.clone(),
                    scope.clone(),
                    TurnControl::default(),
                )
                .unwrap()
        } else {
            resolve(&executor, waiting, &["child"])
        };
        assert_eq!(requests.lock().unwrap().len(), 1);
        let partial = suspended(
            executor
                .resume_checkpoint_with_control(checkpoint, scope.clone(), TurnControl::default())
                .await
                .unwrap(),
        );
        assert_eq!(
            partial.waiting.approvals.len(),
            usize::from(!approval_first)
        );
        assert_eq!(
            partial.waiting.external_waits.len(),
            usize::from(approval_first)
        );
        assert_eq!(tools.executed(), ["child"]);
        let checkpoint = if approval_first {
            resolve(&executor, partial, &["child"])
        } else {
            executor
                .merge_resume_with_control(
                    partial,
                    confirmation,
                    scope.clone(),
                    TurnControl::default(),
                )
                .unwrap()
        };
        assert!(matches!(
            executor
                .resume_checkpoint_with_control(checkpoint, scope, TurnControl::default())
                .await
                .unwrap(),
            ResumableTurn::Completed(_)
        ));
        assert_eq!(tools.executed(), ["child", "approved"]);
        assert_eq!(requests.lock().unwrap().len(), 2);
    }
}

#[tokio::test]
async fn confirmed_preparation_changes_halt_before_any_effect() {
    let tools = ScenarioTools::new(&[]);
    let (executor, requests) = executor(
        vec![
            call("write", "file.write", "a"),
            call("tail", "file.write", "b"),
        ],
        tools.clone(),
        ToolDispatchMode::Serial,
        true,
    );
    let waiting = suspended(executor.start_resumable(input(2)).await.unwrap());
    let old_tail = waiting
        .checkpoint
        .approvals
        .iter()
        .find(|approval| approval.prepared.call().id == "tail")
        .unwrap()
        .clone();
    let scope = waiting.checkpoint.scope.clone();
    let saved = waiting.checkpoint.approvals[0].clone();
    let checkpoint = executor
        .merge_resume_with_control(
            waiting,
            ResumeInput::ApprovalConfirmed(ApprovalConfirmation {
                approval_id: saved.approval_id,
                prepared_digest: saved.prepared.digest().into(),
                policy_revision: saved.policy_revision,
                scope: scope.clone(),
                evidence_id: "head-confirmed".into(),
            }),
            scope.clone(),
            TurnControl::default(),
        )
        .unwrap();
    let partial = suspended(
        executor
            .resume_checkpoint_with_control(checkpoint, scope.clone(), TurnControl::default())
            .await
            .unwrap(),
    );
    assert_eq!(tools.executed().len(), 0);
    tools.version.store(1, Ordering::SeqCst);
    // The unconfirmed tail may be refreshed; the already confirmed head cannot
    // silently inherit changed preparation. This must halt before either effect.
    let error = executor
        .resume_checkpoint_with_control(partial.checkpoint, scope, TurnControl::default())
        .await
        .unwrap_err();
    assert!(matches!(error, TurnError::InvalidRequest { .. }));
    assert_eq!(tools.executed().len(), 0);
    assert_eq!(requests.lock().unwrap().len(), 1);
    assert!(old_tail.evidence_id.is_none());
}

#[tokio::test]
async fn ready_tail_gets_fresh_binding_after_completed_write_and_completed_head_is_not_reprepared()
{
    let tools = ScenarioTools::new(&[("tail", Behavior::Wait)]);
    let (executor, requests) = executor(
        vec![
            call("write", "file.write", "a"),
            call("tail", "file.read", "a"),
        ],
        tools.clone(),
        ToolDispatchMode::Serial,
        false,
    );
    let waiting = suspended(executor.start_resumable(input(2)).await.unwrap());
    assert_eq!(tools.executed(), ["write", "tail"]);
    {
        let issued = tools.issued.lock().unwrap();
        assert_eq!(issued[0].execution_binding()["observed_version"], 0);
        assert_eq!(issued[1].execution_binding()["observed_version"], 1);
        assert_ne!(issued[0].digest(), issued[1].digest());
    }
    let before = tools.prepare_count("write");
    let checkpoint = resolve(&executor, waiting, &["tail"]);
    let scope = checkpoint.scope.clone();
    assert!(matches!(
        executor
            .resume_checkpoint_with_control(checkpoint, scope, TurnControl::default())
            .await
            .unwrap(),
        ResumableTurn::Completed(_)
    ));
    assert_eq!(tools.prepare_count("write"), before);
    assert_eq!(tools.executed(), ["write", "tail"]);
    assert_eq!(requests.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn changed_unconfirmed_preparation_gets_a_new_host_approval_identity_without_effects() {
    let tools = ScenarioTools::new(&[]);
    let (executor, requests) = executor(
        vec![call("approved", "file.write", "safe/a")],
        tools.clone(),
        ToolDispatchMode::Serial,
        true,
    );
    let old = suspended(executor.start_resumable(input(1)).await.unwrap());
    tools.version.store(1, Ordering::SeqCst);
    let scope = old.checkpoint.scope.clone();
    let new = suspended(
        executor
            .resume_checkpoint_with_control(
                old.checkpoint.clone(),
                scope.clone(),
                TurnControl::default(),
            )
            .await
            .unwrap(),
    );
    assert_ne!(
        new.waiting.approvals[0].approval_id,
        old.waiting.approvals[0].approval_id
    );
    assert_ne!(
        new.checkpoint.approvals[0].prepared.digest(),
        old.checkpoint.approvals[0].prepared.digest()
    );
    assert_eq!(new.checkpoint.checkpoint_id, old.checkpoint.checkpoint_id);
    let saved = &old.checkpoint.approvals[0];
    let old_confirmation = ApprovalConfirmation {
        approval_id: saved.approval_id.clone(),
        prepared_digest: saved.prepared.digest().into(),
        policy_revision: saved.policy_revision.clone(),
        scope: scope.clone(),
        evidence_id: "old-confirmation".into(),
    };
    assert!(
        executor
            .merge_resume_with_control(
                new,
                ResumeInput::ApprovalConfirmed(old_confirmation),
                scope,
                TurnControl::default()
            )
            .is_err()
    );
    assert_eq!(tools.executed().len(), 0);
    assert_eq!(requests.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn changed_policy_revision_gets_a_distinct_approval_identity_in_actual_loop() {
    let tools = ScenarioTools::new(&[]);
    let (executor, requests) = executor(
        vec![call("approved", "file.write", "safe/a")],
        tools.clone(),
        ToolDispatchMode::Serial,
        true,
    );
    let old = suspended(executor.start_resumable(input(1)).await.unwrap());
    let mut policy = (**executor.policy_engine.as_ref().unwrap()).clone();
    policy.restrict_workspace("safe");
    let executor = executor.with_policy_engine(Arc::new(policy));
    let scope = old.checkpoint.scope.clone();
    let new = suspended(
        executor
            .resume_checkpoint_with_control(old.checkpoint.clone(), scope, TurnControl::default())
            .await
            .unwrap(),
    );
    assert_eq!(
        new.checkpoint.approvals[0].prepared.digest(),
        old.checkpoint.approvals[0].prepared.digest()
    );
    assert_ne!(
        new.checkpoint.approvals[0].policy_revision,
        old.checkpoint.approvals[0].policy_revision
    );
    assert_ne!(
        new.waiting.approvals[0].approval_id,
        old.waiting.approvals[0].approval_id
    );
    assert_eq!(tools.executed().len(), 0);
    assert_eq!(requests.lock().unwrap().len(), 1);
}
