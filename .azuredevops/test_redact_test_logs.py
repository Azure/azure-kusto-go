import json
import tempfile
import unittest
from pathlib import Path

from redact_test_logs import MAX_JSON_RECORD_BYTES, sanitize_log


def output(text, package="p", test="TestBlob"):
    return {"Action": "output", "Package": package, "Test": test, "Output": text}


class RedactionTests(unittest.TestCase):
    def sanitize(self, events, trailing=""):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "raw.log"
            target = Path(directory) / "sanitized.log"
            raw = "".join(
                json.dumps(event).replace("&", r"\u0026") + "\n" for event in events
            ) + trailing
            source.write_text(raw, encoding="utf-8")
            sanitize_log(source, target)
            self.assertEqual(source.read_text(encoding="utf-8"), raw)
            result = target.read_text(encoding="utf-8")
            self.assertNotIn("SYNTHETIC_TOKEN", result)
            return result

    def test_every_url_split_boundary(self):
        url = "https://account.blob.core.windows.net/c/f?sv=1&sig=SYNTHETIC_TOKEN%2B&sp=r"
        prefix = "x" * 2048 + ' Head "'
        for split in range(1, len(url)):
            with self.subTest(split=split):
                result = self.sanitize(
                    [output(prefix + url[:split]), output(url[split:] + '": failed\n')]
                )
                self.assertEqual(
                    json.loads(result)["Output"],
                    prefix + url.split("?")[0] + '?[REDACTED]": failed\n',
                )

    def test_three_fragments(self):
        result = self.sanitize(
            [
                output("https:/"),
                output("/account.blob.core.windows.net/f?sig=SYN"),
                output("THETIC_TOKEN&sp=r\n"),
            ]
        )
        self.assertEqual(
            json.loads(result)["Output"],
            "https://account.blob.core.windows.net/f?[REDACTED]\n",
        )

    def test_interleaved_packages_and_tests(self):
        streams = [("p1", "A", "a"), ("p1", "B", "b"), ("p2", "A", "c")]
        events = [
            output(f"https://{host}/", package, test)
            for package, test, host in streams
        ]
        events += [
            output("f?sig=SYNTHETIC_TOKEN\n", package, test)
            for package, test, _ in reversed(streams)
        ]
        result = self.sanitize(events)
        actual = {
            (event["Package"], event["Test"]): event["Output"]
            for event in map(json.loads, result.splitlines())
        }
        self.assertEqual(
            actual,
            {
                (package, test): f"https://{host}/f?[REDACTED]\n"
                for package, test, host in streams
            },
        )

    def test_terminal_actions_flush_without_newline(self):
        for action in ("pass", "fail", "skip"):
            with self.subTest(action=action):
                terminal = {
                    "Action": action,
                    "Package": "p",
                    "Test": "TestBlob",
                    "Elapsed": 1.2,
                }
                result = self.sanitize(
                    [output("https://a/f?sig="), output("SYNTHETIC_TOKEN"), terminal]
                )
                events = list(map(json.loads, result.splitlines()))
                self.assertEqual(events[0]["Output"], "https://a/f?[REDACTED]")
                self.assertEqual(events[1], terminal)

    def test_package_terminal_flushes_test_output(self):
        terminal = {"Action": "fail", "Package": "p"}
        result = self.sanitize(
            [output("https://a/f?sig=SYNTHETIC_TOKEN"), terminal]
        )
        events = list(map(json.loads, result.splitlines()))
        self.assertEqual(events[0]["Output"], "https://a/f?[REDACTED]")
        self.assertEqual(events[1], terminal)

    def test_eof_flushes_without_newline(self):
        result = self.sanitize(
            [output("https://a/f?sig=SYN"), output("THETIC_TOKEN")]
        )
        self.assertEqual(json.loads(result)["Output"], "https://a/f?[REDACTED]")

    def test_pause_does_not_flush_partial_url(self):
        result = self.sanitize(
            [
                output("https://a/"),
                {"Action": "pause", "Package": "p", "Test": "TestBlob"},
                {"Action": "cont", "Package": "p", "Test": "TestBlob"},
                output("f?sig=SYNTHETIC_TOKEN\n"),
            ]
        )
        self.assertEqual(
            json.loads(result.splitlines()[-1])["Output"],
            "https://a/f?[REDACTED]\n",
        )

    def test_plain_errors_and_unsigned_output(self):
        event = output("ordinary https://a/f\n")
        lines = self.sanitize(
            [event], 'Head "https://a/f?sig=SYNTHETIC_TOKEN": failed\n'
        ).splitlines()
        self.assertEqual(json.loads(lines[0]), event)
        self.assertEqual(lines[1], 'Head "https://a/f?[REDACTED]": failed')

    def test_apostrophes_in_url_paths_and_queries(self):
        for url in (
            "https://a.blob.core.windows.net/c/report's.json?sv=1&sig=SYNTHETIC_TOKEN",
            "https://a.blob.core.windows.net/c/f?name=report's&sig=SYNTHETIC_TOKEN",
        ):
            for split in range(1, len(url)):
                with self.subTest(url=url, split=split):
                    result = self.sanitize(
                        [output('Head "' + url[:split]), output(url[split:] + '": failed\n')]
                    )
                    self.assertEqual(
                        json.loads(result)["Output"],
                        'Head "' + url.split("?")[0] + '?[REDACTED]": failed\n',
                    )

    def test_long_output_is_redacted_before_chunking(self):
        fragments = ["x" * 2048 for _ in range(40)]
        fragments += ["https://a.blob.core.windows.net/c/report's.json?sig=SYN"]
        fragments += ["THETIC_TOKEN&sp=r\n"]
        result = self.sanitize([output(fragment) for fragment in fragments])
        lines = result.splitlines()
        self.assertGreater(len(lines), 1)
        for line in lines:
            self.assertLessEqual(len(line.encode("utf-8")), MAX_JSON_RECORD_BYTES)
        events = list(map(json.loads, lines))
        self.assertEqual(
            "".join(event["Output"] for event in events),
            "x" * (40 * 2048)
            + "https://a.blob.core.windows.net/c/report's.json?[REDACTED]\n",
        )
        for event in events:
            self.assertEqual(event["Action"], "output")
            self.assertEqual(event["Package"], "p")
            self.assertEqual(event["Test"], "TestBlob")

    def test_chunk_size_accounts_for_json_escaping_and_metadata(self):
        text = ('"\\\t' + chr(0) + chr(0x1F600)) * 6000 + "\n"
        event = output(text, test='"' * 2000)
        event["Time"] = "2026-11-12T15:00:00Z"
        lines = self.sanitize([event]).splitlines()
        self.assertGreater(len(lines), 1)
        events = list(map(json.loads, lines))
        self.assertEqual("".join(item["Output"] for item in events), text)
        for line, item in zip(lines, events):
            self.assertLessEqual(len(line.encode("utf-8")), MAX_JSON_RECORD_BYTES)
            self.assertEqual(item["Test"], event["Test"])
            self.assertEqual(item["Time"], event["Time"])

    def test_oversized_metadata_fails_without_publishing_a_record(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "raw.log"
            target = Path(directory) / "sanitized.log"
            event = output("hello\n", test="x" * MAX_JSON_RECORD_BYTES)
            source.write_text(json.dumps(event) + "\n", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "metadata exceeds"):
                sanitize_log(source, target)
            self.assertEqual(target.read_text(encoding="utf-8"), "")

    def test_missing_source_fails_without_creating_output(self):
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "sanitized.log"
            with self.assertRaises(FileNotFoundError):
                sanitize_log(Path(directory) / "missing.log", target)
            self.assertFalse(target.exists())


if __name__ == "__main__":
    unittest.main()
