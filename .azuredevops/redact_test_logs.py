import argparse
import json
import re
from pathlib import Path


URL_QUERY = re.compile(r"""(https?://[^\s"<>?]+)\?[^\s"<>]*""", re.IGNORECASE)
MAX_JSON_RECORD_BYTES = 60 * 1024


def redact(value):
    if isinstance(value, str):
        return URL_QUERY.sub(r"\1?[REDACTED]", value)
    if isinstance(value, dict):
        return {key: redact(item) for key, item in value.items()}
    if isinstance(value, list):
        return [redact(item) for item in value]
    return value


def sanitize_log(source, destination):
    with source.open(encoding="utf-8") as raw, destination.open(
        "w", encoding="utf-8"
    ) as sanitized:
        pending = {}

        def emit(event):
            event = redact(event)
            line = json.dumps(event)
            if len(line.encode("utf-8")) <= MAX_JSON_RECORD_BYTES:
                sanitized.write(line + "\n")
                return
            if not isinstance(event, dict) or not isinstance(event.get("Output"), str):
                raise ValueError("Test event exceeds the JSON record size limit")
            metadata = json.dumps({**event, "Output": ""})
            # One character can expand to 12 ASCII bytes when JSON-escaped.
            chunk_size = (MAX_JSON_RECORD_BYTES - len(metadata.encode("utf-8"))) // 12
            if chunk_size < 1:
                raise ValueError("Test event metadata exceeds the JSON record size limit")
            for offset in range(0, len(event["Output"]), chunk_size):
                chunk = event["Output"][offset : offset + chunk_size]
                sanitized.write(json.dumps({**event, "Output": chunk}) + "\n")

        def flush(key):
            if key in pending:
                emit(pending.pop(key))

        for line in raw:
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                sanitized.write(redact(line))
            else:
                if isinstance(event, dict):
                    key = (event.get("Package"), event.get("Test"))
                    if isinstance(event.get("Output"), str):
                        if key not in pending:
                            pending[key] = {**event, "Output": ""}
                        pending[key]["Output"] += event["Output"]
                        if pending[key]["Output"].endswith("\n"):
                            flush(key)
                        continue
                    if event.get("Action") in ("pass", "fail", "skip"):
                        for pending_key in list(pending):
                            if pending_key == key or (
                                not event.get("Test") and pending_key[0] == key[0]
                            ):
                                flush(pending_key)
                emit(event)
        for key in list(pending):
            flush(key)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("source", type=Path)
    parser.add_argument("destination", type=Path)
    args = parser.parse_args()
    sanitize_log(args.source, args.destination)
