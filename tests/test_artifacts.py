import tempfile
import unittest
from pathlib import Path

from artifacts import ArtifactError, ArtifactIntegrityError, ArtifactStore


class ArtifactStoreTests(unittest.TestCase):
    def test_content_is_deduplicated_and_verified_by_digest(self):
        with tempfile.TemporaryDirectory() as directory:
            store = ArtifactStore(Path(directory))
            first = store.put_text("private prompt", kind="model_input")
            second = store.put_text("private prompt", kind="model_input")

            self.assertEqual(first, second)
            self.assertEqual(b"private prompt", store.get_bytes(first))
            self.assertEqual(64, len(first["sha256"]))
            self.assertNotIn("private prompt", str(first))

    def test_tampering_is_detected(self):
        with tempfile.TemporaryDirectory() as directory:
            store = ArtifactStore(Path(directory))
            reference = store.put_json({"answer": 42}, kind="model_output")
            store.path_for(reference).write_bytes(b"tampered")

            with self.assertRaises(ArtifactIntegrityError):
                store.get_bytes(reference)

    def test_configured_size_limit_is_enforced(self):
        with tempfile.TemporaryDirectory() as directory:
            store = ArtifactStore(Path(directory), max_bytes=4)
            store.put_bytes(b"1234", media_type="application/octet-stream", kind="data")
            with self.assertRaisesRegex(ArtifactError, "configured"):
                store.put_bytes(
                    b"12345",
                    media_type="application/octet-stream",
                    kind="data",
                )


if __name__ == "__main__":
    unittest.main()
