"""Local immutable content-addressed storage for large or sensitive artifacts."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from pathlib import Path
from typing import Any, Mapping

from limits import MAX_ARTIFACT_BYTES


class ArtifactError(ValueError):
    """Base class for invalid or unreadable artifact data."""


class ArtifactIntegrityError(ArtifactError):
    """Stored bytes no longer match their immutable reference."""


def validate_artifact_ref(value: object) -> dict[str, object]:
    if not isinstance(value, Mapping) or set(value) != {
        "sha256",
        "size_bytes",
        "media_type",
        "kind",
    }:
        raise ArtifactError("artifact reference has an invalid shape")
    digest = value.get("sha256")
    if (
        not isinstance(digest, str)
        or len(digest) != 64
        or any(character not in "0123456789abcdef" for character in digest)
    ):
        raise ArtifactError("artifact sha256 must be a lowercase SHA-256 digest")
    size_bytes = value.get("size_bytes")
    if (
        not isinstance(size_bytes, int)
        or isinstance(size_bytes, bool)
        or size_bytes < 0
        or size_bytes > MAX_ARTIFACT_BYTES
    ):
        raise ArtifactError(
            f"artifact size_bytes must be between 0 and {MAX_ARTIFACT_BYTES}"
        )
    for field in ("media_type", "kind"):
        item = value.get(field)
        if (
            not isinstance(item, str)
            or not item.strip()
            or item != item.strip()
            or len(item) > 128
        ):
            raise ArtifactError(f"artifact {field} must be a non-empty string")
    return dict(value)


class ArtifactStore:
    """Store bytes by SHA-256 using atomic, same-directory replacement.

    The store never deletes content automatically. A digest may be referenced
    by many immutable events, so retention must only remove blobs proven to be
    unreferenced by the event log.
    """

    def __init__(self, root: str | Path, *, max_bytes: int = MAX_ARTIFACT_BYTES):
        if (
            not isinstance(max_bytes, int)
            or isinstance(max_bytes, bool)
            or max_bytes < 1
            or max_bytes > MAX_ARTIFACT_BYTES
        ):
            raise ValueError(
                f"max_bytes must be between 1 and {MAX_ARTIFACT_BYTES}"
            )
        self.root = Path(root).expanduser().resolve()
        self.max_bytes = max_bytes
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)

    def put_bytes(
        self,
        content: bytes,
        *,
        media_type: str,
        kind: str,
    ) -> dict[str, object]:
        if not isinstance(content, bytes):
            raise TypeError("artifact content must be bytes")
        if len(content) > self.max_bytes:
            raise ArtifactError(
                f"artifact exceeds the configured {self.max_bytes}-byte limit"
            )
        reference = validate_artifact_ref(
            {
                "sha256": hashlib.sha256(content).hexdigest(),
                "size_bytes": len(content),
                "media_type": media_type,
                "kind": kind,
            }
        )
        path = self.path_for(reference)
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        if path.exists():
            self._verify_path(path, reference)
            return reference

        descriptor, temporary_name = tempfile.mkstemp(
            prefix=f".{reference['sha256']}.",
            dir=path.parent,
        )
        try:
            os.fchmod(descriptor, 0o600)
            with os.fdopen(descriptor, "wb") as temporary:
                temporary.write(content)
                temporary.flush()
                os.fsync(temporary.fileno())
            os.replace(temporary_name, path)
        finally:
            try:
                os.unlink(temporary_name)
            except FileNotFoundError:
                pass
        self._verify_path(path, reference)
        return reference

    def put_text(
        self,
        content: str,
        *,
        media_type: str = "text/plain; charset=utf-8",
        kind: str,
    ) -> dict[str, object]:
        if not isinstance(content, str):
            raise TypeError("artifact content must be a string")
        return self.put_bytes(
            content.encode("utf-8"),
            media_type=media_type,
            kind=kind,
        )

    def put_json(self, content: Any, *, kind: str) -> dict[str, object]:
        try:
            encoded = json.dumps(
                content,
                allow_nan=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        except (TypeError, ValueError) as exc:
            raise ArtifactError("artifact must contain JSON-compatible values") from exc
        return self.put_bytes(
            encoded,
            media_type="application/json",
            kind=kind,
        )

    def get_bytes(self, reference: Mapping[str, object]) -> bytes:
        normalized = validate_artifact_ref(reference)
        path = self.path_for(normalized)
        self._verify_path(path, normalized)
        return path.read_bytes()

    def path_for(self, reference: Mapping[str, object]) -> Path:
        normalized = validate_artifact_ref(reference)
        digest = str(normalized["sha256"])
        path = self.root / "sha256" / digest[:2] / digest
        if path.parent.is_symlink() or path.is_symlink():
            raise ArtifactIntegrityError("artifact path must not contain a symlink")
        return path

    @staticmethod
    def _verify_path(path: Path, reference: Mapping[str, object]) -> None:
        try:
            stat = path.stat()
        except FileNotFoundError as exc:
            raise ArtifactIntegrityError("referenced artifact is missing") from exc
        expected_size = int(reference["size_bytes"])
        if stat.st_size != expected_size:
            raise ArtifactIntegrityError("artifact size does not match its reference")
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        if digest != reference["sha256"]:
            raise ArtifactIntegrityError("artifact digest does not match its reference")
