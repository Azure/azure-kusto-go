import argparse
import json
import re
from pathlib import Path


URL_QUERY = re.compile(r"""(https?://[^\s"'<>?]+)\?[^\s"'<>]*""", re.IGNORECASE)


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

        def flush(key):
            if key in pending:
                sanitized.write(json.dumps(redact(pending.pop(key))) + "\n")

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
                sanitized.write(json.dumps(redact(event)) + "\n")
        for key in list(pending):
            flush(key)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("source", type=Path)
    parser.add_argument("destination", type=Path)
    args = parser.parse_args()
    sanitize_log(args.source, args.destination)
