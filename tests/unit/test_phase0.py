# AI-generated tests (Codex): read-only configuration, storage gates and credential handling.
import unittest
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
from unittest.mock import patch
from uuid import uuid4

from papersearch.config import disk_preflight, mysql_url, reserve_gib, settings


class BaselineTests(unittest.TestCase):
    def test_example_loads_with_process_overrides(self):
        with patch.dict("os.environ", {"CORE_FREE_GIB": "7"}, clear=True):
            config = settings(Path(".env.example"))
        self.assertEqual(reserve_gib(config, False), 7)
        self.assertEqual(reserve_gib(config, True), 100)
        self.assertFalse(config["MYSQL_PASSWORD"])

    def test_reserve_must_be_positive(self):
        for value in ["0", "-1", "", "100GB"]:
            with self.subTest(value=value), self.assertRaises(ValueError):
                reserve_gib({"SCALE_FREE_GIB": value}, True)

    def test_preflight_requires_docker_disk_location(self):
        with self.assertRaisesRegex(ValueError, "DOCKER_DISK_DIR"):
            disk_preflight({}, False)

    def test_preflight_checks_both_locations_without_creating_artifacts(self):
        path = Path("artifacts") / uuid4().hex
        config = {"ARTIFACT_DIR": str(path), "DOCKER_DISK_DIR": "."}
        with patch("papersearch.config.shutil.disk_usage") as usage, redirect_stdout(StringIO()):
            usage.return_value.free = 5 * 2**30
            self.assertTrue(disk_preflight(config, False))
            self.assertFalse(disk_preflight(config, True))
            self.assertEqual(usage.call_count, 4)
        self.assertFalse(path.exists())

    def test_password_with_punctuation_stays_out_of_url_display(self):
        url = mysql_url({"MYSQL_PASSWORD": "example:@/%"})
        self.assertEqual(url.password, "example:@/%")
        self.assertNotIn("example", str(url))
        with self.assertRaisesRegex(ValueError, "MYSQL_PASSWORD"):
            mysql_url({})
