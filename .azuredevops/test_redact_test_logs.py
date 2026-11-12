import json
import tempfile
import unittest
from pathlib import Path

from redact_test_logs import sanitize_log


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

    def test_missing_source_fails_without_creating_output(self):
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "sanitized.log"
            with self.assertRaises(FileNotFoundError):
                sanitize_log(Path(directory) / "missing.log", target)
            self.assertFalse(target.exists())


if __name__ == "__main__":
    unittest.main()
