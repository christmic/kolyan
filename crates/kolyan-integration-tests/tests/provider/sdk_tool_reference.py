"""Replay a captured wire request through the local official OpenAI SDK.

Deployment config supplies only endpoint/model and an environment-variable name.
The captured request must come from the process test's loopback HTTP recorder.
Independent diagnostic attempts are recorded separately; no hidden SDK retries.
"""

import argparse
import copy
import importlib.metadata
import json
import os
from pathlib import Path
import tempfile

import httpx2
import openai


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--server-config", type=Path, required=True)
    parser.add_argument("--request-file", type=Path, required=True)
    parser.add_argument("--attempts", type=int, default=1)
    args = parser.parse_args()
    config = json.loads(args.server_config.read_text())
    wire = json.loads(args.request_file.read_text())
    wire["model"] = config["request"]["model"]["model"]
    wire["max_output_tokens"] = config["request"]["max_output_tokens"]
    root = Path(tempfile.mkdtemp(prefix="kolyan-sdk-tool-"))
    print(root, flush=True)
    (root / "source.json").write_text(json.dumps({
        "openai": openai.__version__,
        "installed_source": json.loads(importlib.metadata.distribution("openai").read_text("direct_url.json")),
        "request_file": str(args.request_file), "server_config": str(args.server_config),
        "max_retries": 0,
    }, indent=2))
    for attempt in range(args.attempts):
        directory = root / str(attempt)
        directory.mkdir()

        def record_request(request):
            (directory / "request.json").write_bytes(request.content)

        def record_response(response):
            (directory / "http.json").write_text(json.dumps({
                "status": response.status_code,
                "headers": {name: response.headers.get(name) for name in
                            ("content-type", "content-encoding", "x-request-id")},
            }, indent=2))
            original = response.stream

            class RecordingStream(httpx2.SyncByteStream):
                def __iter__(self):
                    with (directory / "response.bin").open("wb") as output:
                        for chunk in original:
                            output.write(chunk)
                            output.flush()
                            yield chunk

                def close(self):
                    original.close()

            response.stream = RecordingStream()

        with openai.DefaultHttpxClient(timeout=180, event_hooks={
            "request": [record_request], "response": [record_response],
        }) as http:
            client = openai.OpenAI(api_key=os.environ[config["api_key_env"]],
                                   base_url=config["base_url"].rstrip("/") + "/v1",
                                   max_retries=0, http_client=http)
            with (directory / "events.jsonl").open("w") as output:
                for event in client.responses.create(**copy.deepcopy(wire)):
                    output.write(event.model_dump_json() + "\n")
                    output.flush()
                    if event.type == "response.output_item.done" and event.item.type == "function_call":
                        print(json.dumps({"attempt": attempt, "type": event.type,
                                          "name": event.item.name, "arguments": event.item.arguments}), flush=True)


if __name__ == "__main__":
    main()
