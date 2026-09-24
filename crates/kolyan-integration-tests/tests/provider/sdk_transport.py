"""Official SDK/Rust protocol differential over identical local HTTP faults.

No API keys or external provider calls. Dataset is the sole scenario source.
"""

import gzip
import http.server
import json
import os
from pathlib import Path
import subprocess
import tempfile
import threading
import time
import zlib

import anthropic
import openai


def outcome_error(error):
    if isinstance(error, UnicodeDecodeError):
        return "utf8"
    if isinstance(error, json.JSONDecodeError):
        return "json"
    status = getattr(error, "status_code", None)
    if status and status != 200:
        return f"http:{status}"
    if isinstance(error, (openai.APIConnectionError, anthropic.APIConnectionError)) or type(error).__name__ in ("ReadTimeout", "RemoteProtocolError", "DecodingError", "ReadError"):
        return "transport"
    if isinstance(error, (openai.APIError, anthropic.APIError)):
        return "api"
    raise error


def main():
    tests = Path(__file__).resolve().parents[1]
    dataset = json.loads((tests / "fixtures/sdk_transport.json").read_text())
    scenarios = {}
    for protocol in ("openai", "anthropic"):
        for case in dataset["cases"]:
            for encoding in dataset["encodings"]:
                for transfer in dataset["transfers"]:
                    label = f"{protocol}-{case['name']}-{encoding}-{transfer}"
                    body = case[protocol].encode()
                    if case.get("invalid_utf8"):
                        body += b"\xff\n\n"
                    if encoding == "gzip":
                        body = gzip.compress(body)
                    elif encoding == "deflate":
                        body = zlib.compress(body)
                    scenarios[label] = (protocol, case, encoding, transfer, body)

    class Handler(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *args):
            pass

        def do_POST(self):
            self.rfile.read(int(self.headers.get("content-length", "0")))
            _, case, encoding, transfer, body = scenarios[self.path.split("/")[1]]
            try:
                time.sleep(case.get("delay", 0))
                self.send_response(case.get("status", 200))
                self.send_header("Content-Type", "text/event-stream")
                if encoding != "identity":
                    self.send_header("Content-Encoding", encoding)
                self.send_header("Connection", "close")
                if transfer == "chunked":
                    self.send_header("Transfer-Encoding", "chunked")
                else:
                    self.send_header("Content-Length", str(len(body) + (7 if case.get("disconnect") else 0)))
                self.end_headers()
                # Single-byte HTTP chunks deliberately split UTF-8, SSE and compressed data.
                if transfer == "chunked":
                    for byte in body:
                        self.wfile.write(b"1\r\n" + bytes([byte]) + b"\r\n")
                    if not case.get("disconnect"):
                        self.wfile.write(b"0\r\n\r\n")
                else:
                    self.wfile.write(body)
                self.wfile.flush()
            except (BrokenPipeError, ConnectionResetError):
                pass
            self.close_connection = True

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    directory = Path(tempfile.mkdtemp(prefix="kolyan-sdk-transport-"))
    print(directory, flush=True)
    report = []
    try:
        for label, (protocol, _, _, _, _) in scenarios.items():
            base = f"http://127.0.0.1:{server.server_port}/{label}"
            result = {"events": [], "error": None}
            cls = openai.OpenAI if protocol == "openai" else anthropic.Anthropic
            with cls(api_key="fixture", base_url=base + ("/v1" if protocol == "openai" else ""), max_retries=0, timeout=0.1) as client:
                try:
                    if protocol == "openai":
                        stream = client.responses.create(model="fixture", input="hello", stream=True)
                    else:
                        stream = client.messages.create(model="fixture", max_tokens=128, messages=[{"role":"user","content":"hello"}], stream=True)
                    with stream:
                        for event in stream:
                            value = event.model_dump(exclude_unset=True)
                            result["events"].append({"type": value.get("type"), "delta": value.get("delta")})
                except Exception as error:
                    result["error"] = outcome_error(error)
            report.append({"label":label, "protocol":protocol, "base_url":base, "expected":result})
        reference = directory / "reference.json"
        reference.write_text(json.dumps(report, indent=2, ensure_ascii=False))
        env = dict(os.environ, KOLYAN_SDK_TRANSPORT_REPORT=str(reference))
        subprocess.run(["cargo", "test", "-p", "kolyan-integration-tests", "--test", "sdk_transport", "--offline", "--", "--ignored", "--nocapture"], env=env, check=True)
    finally:
        server.shutdown()
        server.server_close()


if __name__ == "__main__":
    main()
