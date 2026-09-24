"""Diagnostic only: send the same fixture through locally installed official SDKs.

Run with uv --with /path/to/openai-python --with /path/to/anthropic-sdk-python.
Credentials come only from the project config's environment-variable names.
Artifacts contain request bodies and responses, never authentication headers.
"""

import argparse
import concurrent.futures
import copy
import json
import os
from pathlib import Path
import tempfile
import tomllib

import anthropic
import httpx
import httpx2
import openai
import jsonschema


def run_case(family, protocol, config, model, fixture, strict_schema, root):
    label = f"{family}-{protocol}-{model}-{'closed' if strict_schema else 'original'}"
    directory = root / label
    directory.mkdir()

    def capture_request(request):
        (directory / "request.json").write_bytes(request.content)
        (directory / "request-meta.json").write_text(json.dumps({
            "method": request.method, "url": str(request.url),
        }, indent=2))

    def capture_response(response):
        response.read()
        (directory / "response.bin").write_bytes(response.content)
        (directory / "http.json").write_text(json.dumps({
            "status": response.status_code,
            "headers": {key: response.headers.get(key) for key in
                        ("content-type", "content-encoding", "x-request-id")},
        }, indent=2))

    schema = copy.deepcopy(fixture["output_format"]["schema"])
    if strict_schema:
        schema["additionalProperties"] = False
    transport = httpx if protocol == "openai" else httpx2
    with transport.Client(timeout=180, event_hooks={
        "request": [capture_request], "response": [capture_response],
    }) as http:
        try:
            shared = dict(api_key=os.environ[config["api_key_env"]],
                          base_url=config["base_url"] + ("/v1" if protocol == "openai" else ""), http_client=http,
                          max_retries=0)
            if protocol == "openai":
                client = openai.OpenAI(**shared)
                events = client.responses.create(
                    model=model, input=[{"role": message["role"], "content": [
                        {"type": "input_text", "text": block["text"]}
                        for block in message["content"]]} for message in fixture["messages"]],
                    instructions=fixture["system"],
                    max_output_tokens=fixture["max_output_tokens"],
                    text={"format": {"type": "json_schema", "name": "city_info",
                                     "strict": True, "schema": schema}}, stream=True)
            else:
                client = anthropic.Anthropic(**shared)
                events = client.messages.create(
                    model=model, messages=fixture["messages"], system=fixture["system"],
                    max_tokens=fixture["max_output_tokens"],
                    output_config={"format": {"type": "json_schema", "schema": schema}},
                    stream=True)
            text = ""
            with (directory / "events.jsonl").open("w") as output:
                for event in events:
                    output.write(event.model_dump_json() + "\n")
                    if protocol == "openai" and event.type == "response.output_text.delta":
                        text += event.delta
                    elif protocol == "anthropic" and event.type == "content_block_delta" and event.delta.type == "text_delta":
                        text += event.delta.text
            result = {"label": label, "transport": "completed"}
            (directory / "output.txt").write_text(text)
            try:
                jsonschema.validate(json.loads(text), schema)
                result["schema_valid"] = True
            except (ValueError, jsonschema.ValidationError) as error:
                result["schema_valid"] = False
                result["validation_error"] = str(error).splitlines()[0]
        except Exception as error:
            # SDK exception strings can include arbitrary response bodies; artifacts are local.
            result = {"label": label, "error_type": type(error).__name__, "error": str(error)}
    (directory / "result.json").write_text(json.dumps(result, indent=2))
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--all-models", action="store_true")
    args = parser.parse_args()
    tests = Path(__file__).resolve().parents[1]
    config = tomllib.loads((tests / "config/live-tests.toml").read_text())["provider"]
    fixture = json.loads((tests / "fixtures/structured_output.json").read_text())["request"]
    root = Path(tempfile.mkdtemp(prefix="kolyan-sdk-reference-"))
    (root / "sdk-versions.json").write_text(json.dumps({
        "openai": openai.__version__, "anthropic": anthropic.__version__,
        "max_retries": 0,
    }, indent=2))
    print(root, flush=True)
    jobs = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        for family, providers in config.items():
            for protocol in ("openai", "anthropic"):
                cfg = providers[protocol + "_compat"]
                models = ([row["model"] for row in cfg["model_matrix"]]
                          if args.all_models else [cfg["model"]])
                for model in models:
                    for closed in (False, True):
                        jobs.append(pool.submit(run_case, family, protocol, cfg, model,
                                                fixture, closed, root))
        results = [job.result() for job in jobs]
    (root / "summary.json").write_text(json.dumps(results, indent=2))
    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
