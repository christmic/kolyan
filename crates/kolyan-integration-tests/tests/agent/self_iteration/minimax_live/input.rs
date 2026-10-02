//! Actual archived task and system instructions; never rewrite model output.

use kolyan_model::{ModelRequest, SystemInstruction};
use serde_json::{Value, json};

use super::{Plan, Stage, baseline::Inventory};

pub(super) fn specification(
    plan: &Plan,
    stage: &Stage,
    initial: &Inventory,
    current: &Inventory,
) -> Result<Value, String> {
    let mut spec = super::super::input::specification(plan, &stage.legacy(), initial, current)?;
    spec["workflow"] = json!({"kind":plan.workflow,"reserved_invocations":6,"repair_limit":1,"stage_kind":stage.kind});
    spec["candidate_observation_contract"] = super::observations::contract(&plan.expected_states);
    spec["review_contract"] = review_schema(plan);
    spec["local_output_validation"] = json!({"native_schema_enforcement":false,"strict_final_text":true,"four_fresh_reads_required":true});
    Ok(spec)
}
pub(super) fn prompt(spec: &Value, stage: &Stage) -> Result<String, String> {
    super::super::input::prompt(spec, &stage.legacy())
}
fn review_schema(plan: &Plan) -> Value {
    let paths = plan
        .allowlist
        .iter()
        .map(|p| {
            (
                p.clone(),
                json!({"type":"string","pattern":"^[0-9a-f]{64}$"}),
            )
        })
        .collect::<serde_json::Map<_, _>>();
    json!({"type":"object","additionalProperties":false,
        "required":["schema_version","candidate_digests","verdict","findings"],
        "properties":{"schema_version":{"type":"integer","const":1},
        "candidate_digests":{"type":"object","additionalProperties":false,"required":plan.allowlist,"properties":paths},
        "verdict":{"type":"string","enum":["accept","repair"]},
        "findings":{"type":"array","maxItems":16,"items":{"type":"object","additionalProperties":false,
        "required":["path","issue"],"properties":{"path":{"type":"string","enum":plan.allowlist},"issue":{"type":"string","minLength":1}}}}}})
}
pub(super) fn configure(
    plan: &Plan,
    stage: &Stage,
    request: &mut ModelRequest,
) -> Result<(), String> {
    if request.output_format.is_some()
        || request.model.provider != plan.family
        || request.model.model != plan.model
    {
        return Err("LocalStrict does not request native output or another deployment".into());
    }
    if stage.kind.structured_review() {
        request.system.push(SystemInstruction {
            text:format!("This is a host-selected LOCAL review contract, not server-enforced structured generation. Read all four candidate paths with actual file.read in this stage. The final Text must be exactly one raw JSON object, <=32768 UTF-8 bytes, with no prose/fences/unknown or duplicate keys. Reasoning is not a verdict. Use actual read SHA-256 values. Accept has no findings; repair has 1..16 findings. Schema: {}",review_schema(plan)),
            cache:false });
    }
    Ok(())
}
