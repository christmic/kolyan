# kolyan-integration-tests

Real-network live regression tests for Kolyan's two provider adapters, run
against both **MiniMax China-region** and the **Qwen (千问) platform**
(which hosts Qwen, DeepSeek, and Zhipu models behind one base URL).

The two adapter surfaces exercised are:

* OpenAI-compatible — `OpenAiClient` posts to `${base_url}/v1/responses`
* Anthropic-compatible — `AnthropicClient` posts to `${base_url}/v1/messages`

## What this crate is for

The unit tests in `kolyan-provider-*` use mocked protocol clients. This
crate exists to catch the failure modes that mocks cannot:

* Wrong base URL → request goes to the wrong host.
* Wrong auth header → server returns 401/403.
* Wrong model identifier → server returns 404 or silently downgrades.
* Wrong stream-to-event mapping → events arrive but never produce a
  `ModelEvent::Completed` and `aggregate_stream` errors out.
* Wrong `usage` field mapping → token accounting silently lies.
* Prompt-cache contract drift → `cache_read_tokens` never moves off zero
  on the second request.
* Server-emitted events drift → e.g. some servers omit `response.completed`
  on certain code paths; the provider must cope.

## Layered separation

```
        test code (this crate, tests/provider/ + tests/core/)
              │
              ▼  reads
        local config (tests/config/live-tests.toml — committed, no API key)
              │
              ▼  injected via
        env vars (KOLYAN_*_API_KEY — never committed)
```

The API key never enters the repository. To change the key, set the env
var in your shell; nothing inside this directory needs to be edited.

## Layered data / config / code

A test case has three layered pieces, all kept separate:

```
  tests/fixtures/<name>.json     ← inputs + expected outputs (data)
  tests/config/live-tests.toml   ← provider × model matrix (config)
  tests/provider/ + tests/core/  ← runners (code)
```

To **add a new test case**: drop a JSON file under `tests/fixtures/` and
register its name in `common::load_fixture()`. No test code changes.

To **add a new provider**: add a `[provider.<family>.<surface>]` section
to `tests/config/live-tests.toml`; provider protocol runners live under
`tests/provider/`. No fixture or assertion code changes.

To **add a new model** under an existing provider: add a
`[[provider.<family>.<surface>.model_matrix]]` entry. No code changes.

To **add a new assertion** for an existing case: edit the fixture's
`expectations` block. No code changes.

## Running the live tests

```bash
# 1. Export API keys for the families you want to exercise.
export KOLYAN_MINIMAX_API_KEY="<your-minimax-key>"
export KOLYAN_QWEN_API_KEY="<your-qwen-key>"

# 2. From the Kolyan workspace root, run all live tests.
cargo test -p kolyan-integration-tests -- --ignored --nocapture

# 3. Or run a single scenario.
cargo test -p kolyan-integration-tests openai_single_shot_matrix -- --ignored --nocapture
cargo test -p kolyan-integration-tests anthropic_prompt_cache_matrix -- --ignored --nocapture

# 4. Diagnostic dumps — see exactly what each event looks like on the wire.
cargo test -p kolyan-integration-tests openai_dump_first_text -- --ignored --nocapture

# 5. Run the Step layer against MiniMax using both protocol surfaces.
cargo test -p kolyan-integration-tests --test core_step -- --ignored --nocapture
```

Turn integration tests (one-step across every configured model, plus a
multi-step `shell.query` ToolCall → ToolResult loop across both protocols):

```bash
cargo test -p kolyan-integration-tests --test core_turn -- --ignored --nocapture
```

The dedicated Turn event-stream matrix is separate from the existing tests and
consumes `TurnEventStream` for every configured model/protocol combination:

```bash
cargo test -p kolyan-integration-tests --test core_turn \
  turn_event_stream_completes_across_configured_models -- --ignored --nocapture
```

To run the long multi-step case for selected models only, set a comma-separated
model filter, for example:

```bash
KOLYAN_TURN_MODEL_FILTER=MiniMax-M3 \
  cargo test -p kolyan-integration-tests --test core_turn \
  turn_ten_step_tool_loop_runs_across_all_configured_models -- --ignored --nocapture
```

The multi-step case records the provider event stream as a temporary JSONL
trace. It compares that trace with
`tests/expected/turn/ten_step.jsonl` as an order-preserving contract: tool
names and terminal stop reasons are checked, while generated text, reasoning,
provider metadata, tool-call ids, and arguments remain visible in the actual
temporary trace without being compared as exact strings.

If either env var is unset, matrix rows for that provider are reported as
`[SKIP ...]` with the required variable name. Once a row has a key and starts
running, Provider or aggregation errors fail the row; live execution never
silently converts an adapter error into a passing test.

## Provider matrix

Configured in `tests/config/live-tests.toml`:

| Family  | Surface             | Base URL (host part — `/v1/...` is appended by the client) | API key env var        |
|---------|---------------------|------------------------------------------------------------|------------------------|
| minimax | openai_compat       | `https://api.minimaxi.com`                                 | `KOLYAN_MINIMAX_API_KEY` |
| minimax | anthropic_compat    | `https://api.minimaxi.com/anthropic`                       | `KOLYAN_MINIMAX_API_KEY` |
| qwen    | openai_compat       | `https://token-plan.maas.qianwenaiapi.com/compatible-mode` | `KOLYAN_QWEN_API_KEY`  |
| qwen    | anthropic_compat    | `https://token-plan.maas.qianwenaiapi.com/apps/anthropic`  | `KOLYAN_QWEN_API_KEY`  |

⚠️ **Do NOT add a trailing `/v1` to `base_url`** — the protocol clients
append `/v1/responses` or `/v1/messages` themselves. A stray `/v1` produces
`/v1/v1/...` and the server returns 404. The TOML file documents this
inline at the top.

Models covered by `model_matrix` (Qwen platform — same base URL serves
all of these via `model_id`):

| Model                | Text | Reasoning | Vision |
|----------------------|:----:|:---------:|:------:|
| qwen3.8-max          |  ✓   |    ✓      |   ✓    |
| qwen3.8-flash        |  ✓   |    ✓      |   ✓    |
| qwen3.7-plus         |  ✓   |    ✓      |   ✓    |
| qwen3.7-max          |  ✓   |    ✓      |        |
| deepseek-v4.1-flash  |  ✓   |    ✓      |        |
| deepseek-v4-pro-0813 |  ✓   |    ✓      |        |
| deepseek-v4-pro      |  ✓   |    ✓      |        |
| deepseek-v4-flash-0731 |  ✓   |    ✓      |        |
| glm-5.3              |  ✓   |    ✓      |        |
| glm-5.2              |  ✓   |    ✓      |        |

**Out of scope for this crate** (they need a non-text `ModelProvider`
trait variant): image generation (`qwen-image-3.0-pro`, `wan2.7-image*`),
video generation (`happyhorse-1.1-*`), speech recognition
(`qwen-audio-3.0-asr-flash`), TTS / realtime (`qwen-audio-3.0-tts-plus`,
`qwen-audio-3.0-realtime-plus`).

## Test cases (fixtures)

| Fixture name          | What it exercises                                              |
|-----------------------|----------------------------------------------------------------|
| `text`                | Plain text Q&A; asserts non-empty content + `EndTurn` stop.     |
| `tool_call`           | `get_weather` tool use; asserts `ToolUse` stop + JSON args.   |
| `structured_output`   | JSON schema for city info; asserts parsed `structured_output`.|
| `prompt_cache`        | Long Python GIL prefix × 2 requests; asserts cache warming.    |

Each fixture has shape:

```json
{
  "request": {
    "system": "...",
    "messages": [...],
    "tools": [...],               // optional
    "output_format": { ... },     // optional
    "prompt_cache": { ... },      // optional
    "max_output_tokens": 256
  },
  "expectations": {
    "stop_reason": "end_turn",     // optional
    "text": { "min_chars": 1 },    // optional
    "tool_call": {                  // optional
      "name": "get_weather",
      "arguments": { "type": "object", "required": ["city"], "properties": { ... } }
    },
    "structured_output": { ... },  // optional JSON-schema-like shape
    "prompt_cache": { "second_cache_read_tokens_gt_first": true },  // optional
    "requires": ["cache_control_ephemeral"]  // capability names the provider must have
  }
}
```

Assertion fields supported by the runner:

* `stop_reason` — `"end_turn" | "tool_use" | "max_output_tokens" | "refusal"`
* `text.min_chars` — minimum character count of concatenated text content
* `tool_call.name` — exact name match
* `tool_call.arguments` — JSON-schema-like shape (`type`, `required`, `properties`, `min_len`, `contains_any`)
* `structured_output` — top-level type/required/properties
* `prompt_cache.second_cache_read_tokens_gt_first` — `cache_read_tokens` on the second request must exceed the first
* `requires` — list of capability names from `Capabilities`; if any is false for the model, that (model, fixture) row is skipped with a `[SKIP ...]` line

## Capabilities

Each `(family, surface)` and each `model_matrix` entry has a `capabilities`
block. The runner uses these to choose how strictly to assert on the
`TokenUsage` fields and which fixtures to skip:

| Capability                 | When `false`                                                                            |
|----------------------------|-----------------------------------------------------------------------------------------|
| `input_tokens_reporting`   | `usage.input_tokens` is not asserted; `None` or `Some(0)` accepted (logged as `WARN`). |
| `output_tokens_reporting`  | Same semantics for `usage.output_tokens`.                                               |
| `cache_control_ephemeral`  | `prompt_cache` fixture is skipped (server doesn't report cache hit tokens).             |
| `server_emits_completed`   | Informational — the OpenAI provider layer already synthesizes a terminal `Completed` if missing, so this flag doesn't gate any test today. |
| `structured_output_fenced` | Informational — the OpenAI provider layer strips markdown fences unconditionally; this flag documents whether a particular surface needs it. |
| `supports_vision`          | Informational — there are no vision-only fixtures yet, so this flag is forward-looking. |

Current defaults (in `tests/config/live-tests.toml`):

| Family  | Surface          | input | output | cache_ephemeral | server_emits_completed | fenced |
|---------|------------------|:-----:|:------:|:---------------:|:----------------------:|:------:|
| minimax | openai_compat    |  ✓    |   ✓    |       ✗         |          ✗             |   ✓    |
| minimax | anthropic_compat |  ✗    |   ✓    |       ✗         |          ✓             |   ✗    |
| qwen    | openai_compat    |  ✓    |   ✓    |       ✗         |          ✓             |   ✗    |
| qwen    | anthropic_compat |  ✓    |   ✓    |       ✗         |          ✓             |   ✗    |

Notes on the defaults:

* MiniMax-M3 has **automatic (server-side passive) prompt caching**. The
  server caches prefixes ≥ 512 input tokens on its own, without
  requiring `cache_control` markers. As a side effect, the response
  does **not** report `cache_read_input_tokens` /
  `cache_creation_input_tokens` — there is no observable signal from
  the client. To re-enable the prompt-cache test against this surface,
  you would need a model with explicit Anthropic-style
  `cache_control: ephemeral` markers (e.g. MiniMax M2.x).
* MiniMax's Anthropic-compatible surface on China does not populate
  `input_tokens` in `message_start.usage` — only `output_tokens`
  (filled in `message_delta.usage`).
* MiniMax's OpenAI-compatible surface emits `response.completed`
  unreliably on the structured-output path; the provider layer
  compensates by synthesizing a terminal `Completed` when the stream
  ends without one.

## CI gating

`cargo test --workspace` runs in CI without any `KOLYAN_*_API_KEY`. Every
live test is `#[ignore]`'d, so CI sees zero network calls, zero key
usage, and zero cost. CI does **not** run the `--ignored` set.

`cargo test -p kolyan-integration-tests -- --ignored` requires both keys
(or whichever families you want to exercise) to be exported in the
caller's shell.

## Diagnostics

When a test fails, the failure message embeds the full
`family/surface/model/fixture` label so you can correlate with
`tests/config/live-tests.toml` directly. To see raw SSE events for a given
provider, run:

```bash
cargo test -p kolyan-integration-tests openai_dump_first_text -- --ignored --nocapture
```

This dumps every `ModelEvent` that the first matrix entry produces for
the `text` fixture, including `Completed`/`Usage`/`TextDelta` details.

The Step integration test also captures the normalized Step event stream for
the multi-event `tool_call` fixture. It writes the actual JSONL stream to a
temporary file and compares it with the checked-in protocol-specific snapshot
under `tests/expected/step/`. The temporary actual JSONL keeps text,
reasoning, tool-argument, usage, and structured-output payloads for debugging;
comparison ignores dynamic payload values and checks event semantics/order.

Run it with:

```bash
cargo test -p kolyan-integration-tests --test core_step \
  step_stream_snapshot_minimax_tool_call_over_both_protocols --ignored --nocapture
```

To run the same Step stream contract across every configured model and both
protocol surfaces:

```bash
cargo test -p kolyan-integration-tests --test core_step \
  step_stream_snapshot_all_configured_models_over_both_protocols \
  --ignored --nocapture
```

The matrix contract requires the Tool Call lifecycle and terminal event in
order. Optional `Started`, `ReasoningDelta`, `Usage`, and provider metadata
events remain visible in the temporary actual JSONL snapshot; they are not
required because models expose those events differently.

## What happens if a `KOLYAN_*_API_KEY` env var is unset

`cfg.api_key_env` (set per provider in `live-tests.toml`) names the environment
variable read at runtime. Missing keys skip only the corresponding provider
rows; once a key is present, request and aggregation errors fail the test with
the full provider/model/fixture label.

## Editing endpoints or models

Edit `tests/config/live-tests.toml`. The file is embedded into each test
binary at compile time via `include_str!`, so a rebuild is required for
changes to take effect.
