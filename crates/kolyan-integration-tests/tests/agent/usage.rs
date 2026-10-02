//! Observed Task input includes reported base input and both reported cache parts.
//! Missing source fields stay missing; partial sums never prove complete usage.

mod tests;

use kolyan_model::TokenUsage;
use serde::Serialize;

#[derive(Debug, PartialEq, Serialize)]
pub struct ObservedUsage {
    pub observed_input_tokens: u64,
    pub observed_output_tokens: u64,
    pub unreported_steps: u64,
    pub unreported_fields: Vec<UnreportedFields>,
    pub source: Vec<TokenUsage>,
}

#[derive(Debug, PartialEq, Serialize)]
pub struct UnreportedFields {
    pub step_index: usize,
    pub fields: Vec<&'static str>,
}

pub fn observe<'a>(
    values: impl IntoIterator<Item = &'a TokenUsage>,
) -> Result<ObservedUsage, &'static str> {
    let mut total = ObservedUsage {
        observed_input_tokens: 0,
        observed_output_tokens: 0,
        unreported_steps: 0,
        unreported_fields: vec![],
        source: vec![],
    };
    for (step_index, usage) in values.into_iter().enumerate() {
        for reported in [
            usage.input_tokens,
            usage.cache_read_tokens,
            usage.cache_write_tokens,
        ]
        .into_iter()
        .flatten()
        {
            total.observed_input_tokens = total
                .observed_input_tokens
                .checked_add(reported)
                .ok_or("input usage overflow")?;
        }
        if let Some(reported) = usage.output_tokens {
            total.observed_output_tokens = total
                .observed_output_tokens
                .checked_add(reported)
                .ok_or("output usage overflow")?;
        }
        // Match Server's unreported-Step contract independently of optional cache
        // and reasoning fields; retain every missing field in diagnostic evidence.
        if usage.input_tokens.is_none() || usage.output_tokens.is_none() {
            total.unreported_steps = total
                .unreported_steps
                .checked_add(1)
                .ok_or("unreported Step overflow")?;
        }
        let fields = [
            ("input_tokens", usage.input_tokens),
            ("output_tokens", usage.output_tokens),
            ("cache_read_tokens", usage.cache_read_tokens),
            ("cache_write_tokens", usage.cache_write_tokens),
            ("reasoning_tokens", usage.reasoning_tokens),
        ]
        .into_iter()
        .filter_map(|(field, value)| value.is_none().then_some(field))
        .collect::<Vec<_>>();
        if !fields.is_empty() {
            total
                .unreported_fields
                .push(UnreportedFields { step_index, fields });
        }
        total.source.push(usage.clone());
    }
    Ok(total)
}
