"""Crash-safe immutable model publication and online reload without restarts."""

from __future__ import annotations

from contextlib import contextmanager
import hashlib
import json
import logging
import os
from pathlib import Path
import shutil
import tempfile
import threading

import joblib
import numpy as np


LOGGER = logging.getLogger(__name__)


def storage_root(storage_dir=None):
    return Path(storage_dir or os.environ.get("STORAGE_DIR", "/opt/project/storage"))


def sha256_file(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def fsync_directory(path):
    """Persist a rename/link on Linux; Windows does not support directory fsync."""
    if os.name != "nt":
        descriptor = os.open(str(path), os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)


@contextmanager
def file_lock(path):
    """Serialize writers across processes with flock (Windows test fallback)."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+b") as lock:
        if os.name == "nt":
            import msvcrt
            lock.seek(0, os.SEEK_END)
            if lock.tell() == 0:
                lock.write(b"0")
                lock.flush()
            lock.seek(0)
            msvcrt.locking(lock.fileno(), msvcrt.LK_LOCK, 1)
        else:
            import fcntl
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            if os.name == "nt":
                lock.seek(0)
                msvcrt.locking(lock.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(lock.fileno(), fcntl.LOCK_UN)


def atomic_json(path, value):
    """Write and fsync JSON before atomic replacement; a crash keeps old or new."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as output:
            json.dump(value, output, indent=2, sort_keys=True, allow_nan=False)
            output.write("\n")
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, path)
        fsync_directory(path.parent)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def atomic_joblib(path, value):
    """Save a task artifact completely before making its path visible."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(descriptor, "wb") as output:
            joblib.dump(value, output, compress=3)
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, path)
        fsync_directory(path.parent)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def read_json(path):
    with Path(path).open(encoding="utf-8") as source:
        return json.load(source)


def manifest_identity(manifest):
    if manifest is None:
        return None
    return (manifest["version"], manifest["sha256"], manifest["window_end"])


class ModelUnavailable(RuntimeError):
    pass


class StaleBaseline(RuntimeError):
    pass


class ModelRegistry:
    def __init__(self, storage_dir=None):
        self.storage = storage_root(storage_dir)
        self.directory = self.storage / "models"
        self.directory.mkdir(parents=True, exist_ok=True)
        self.manifest_path = self.directory / "current.json"

    def current(self):
        """Read one atomic deployment pointer, or None before bootstrap."""
        return read_json(self.manifest_path) if self.manifest_path.exists() else None

    def artifact_path(self, manifest):
        name = manifest["artifact"]
        if not isinstance(name, str) or Path(name).name != name or not name.endswith(".joblib"):
            raise ValueError("Model artifact must be a .joblib filename within the registry")
        return self.directory / name

    def load(self, manifest):
        """Verify the exact immutable artifact before deserializing trusted local bytes."""
        path = self.artifact_path(manifest)
        if sha256_file(path) != manifest["sha256"]:
            raise ValueError(f"SHA256 mismatch for {path.name}")
        bundle = joblib.load(path)
        if bundle.get("version") != manifest["version"] or not hasattr(bundle.get("pipeline"), "predict_proba"):
            raise ValueError("Model bundle does not match manifest")
        return bundle

    def publish(self, candidate_path, manifest, expected_baseline):
        """Save/fsync an immutable artifact, then atomically commit model AND month.

        Publication uses a process lock and a baseline compare-and-swap. Retrying
        after any crash reuses the same bytes/name; a stale run cannot overwrite a
        newer deployment. A failed pointer write leaves the old month committed.
        """
        with file_lock(self.directory / ".publish.lock"):
            current = self.current()
            if manifest_identity(current) == manifest_identity(manifest):
                self.load(current)
                return current
            if manifest_identity(current) != manifest_identity(expected_baseline):
                raise StaleBaseline("Another run published after this run pinned its baseline")
            if sha256_file(candidate_path) != manifest["sha256"]:
                raise ValueError("Candidate artifact changed after evaluation")
            destination = self.artifact_path(manifest)
            if destination.exists():
                if sha256_file(destination) != manifest["sha256"]:
                    raise ValueError("Immutable model version already exists with different bytes")
            else:
                descriptor, temporary = tempfile.mkstemp(prefix=".model.", dir=self.directory)
                try:
                    with os.fdopen(descriptor, "wb") as output, Path(candidate_path).open("rb") as source:
                        shutil.copyfileobj(source, output)
                        output.flush()
                        os.fsync(output.fileno())
                    # Hard-link with no replacement prevents accidental version mutation.
                    os.link(temporary, destination)
                    fsync_directory(self.directory)
                finally:
                    if os.path.exists(temporary):
                        os.unlink(temporary)
            self.load(manifest)
            atomic_json(self.manifest_path, manifest)
            return manifest


def fraud_probabilities(pipeline, frame):
    classes = np.asarray(pipeline.classes_)
    positive = np.flatnonzero(classes == 1)
    if len(positive) != 1:
        raise ValueError("A model must have a positive class labelled 1")
    scores = np.asarray(pipeline.predict_proba(frame)[:, positive[0]], dtype=float)
    if scores.shape != (len(frame),) or not np.isfinite(scores).all() or ((scores < 0) | (scores > 1)).any():
        raise ValueError("Model returned invalid fraud probabilities")
    return scores


class HotReloadingModel:
    """Reload a valid newly published model at the next prediction boundary.

    Bad/missing artifacts or malformed pointers leave the last good in-memory
    model active. The model reference and its version are read under one lock,
    so every returned score carries the version that actually produced it.
    """

    def __init__(self, storage_dir=None):
        self.registry = ModelRegistry(storage_dir)
        self._lock = threading.RLock()
        self._pipeline = None
        self._manifest = None
        self.last_error = None

    @property
    def version(self):
        return self._manifest["version"] if self._manifest else None

    def refresh(self):
        with self._lock:
            try:
                manifest = self.registry.current()
                if manifest is None:
                    raise ModelUnavailable("No published model; run the bootstrap pipeline first")
                if manifest_identity(manifest) != manifest_identity(self._manifest):
                    bundle = self.registry.load(manifest)
                    self._pipeline = bundle["pipeline"]
                    self._manifest = manifest
                    LOGGER.info("Activated model version=%s", manifest["version"])
                self.last_error = None
            except Exception as error:
                message = f"{type(error).__name__}: {error}"
                if message != self.last_error:
                    LOGGER.warning("Model reload failed; keeping last valid model: %s", message)
                self.last_error = message
                if self._pipeline is None:
                    raise ModelUnavailable(message) from error
            return self.version

    def predict(self, frame):
        with self._lock:
            self.refresh()
            return fraud_probabilities(self._pipeline, frame), self.version
