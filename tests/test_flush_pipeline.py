"""Tests for the bounded write -> fsync pipeline used by POST /api/restores."""

from __future__ import annotations

import os
import threading
import time

import pytest

from app import restore as restore_mod
from app.restore import (
    InvalidConfiguration,
    _PendingBudget,
    FlushLimits,
    restore_directory,
)


def _pairs(root: str) -> list[tuple[str, int, int]]:
    found = []
    for dirpath, _dirs, files in os.walk(root):
        for name in files:
            path = os.path.join(dirpath, name)
            stat = os.stat(path)
            found.append((os.path.relpath(path, root), stat.st_ino, stat.st_size))
    return sorted(found)


def _assert_no_leftovers(out) -> None:
    staging = os.path.join(str(out), ".staging")
    if os.path.isdir(staging):
        assert os.listdir(staging) == []
    if os.path.isdir(out):
        assert [p for p in os.listdir(out) if not p.startswith(".")] == []


class _Probe:
    """Thread-safe instrumentation for writes, fsyncs and budget accounting."""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.write_delay = 0.0
        self.fsync_delay = 0.0
        self.fail_write_at = None  # 1-based write ordinal
        self.fail_fsync_at = None  # 1-based fsync ordinal
        self.writes: list[str] = []
        self.reserved_sizes: list[int] = []
        self.write_started_during_fsync = 0
        self.max_active_fsyncs = 0
        self.completed_fsyncs = 0
        self.writes_at_fsync_done: list[int] = []
        self.published_during_fsync: list[str] = []
        self.output_root: str | None = None
        self.current_write: str | None = None
        self.chunks_for_current: list[int] = []
        self.chunks_by_write: dict[str, list[int]] = {}
        self.peak_files = 0
        self.peak_bytes = 0
        self._active_fsyncs = 0
        self._write_count = 0
        self._fsync_count = 0

    # -- hooks -----------------------------------------------------------
    def install(self, monkeypatch: pytest.MonkeyPatch) -> None:
        orig_copy = restore_mod._copy_payload
        orig_fsync = restore_mod._fsync_fd
        orig_write_all = restore_mod._write_all
        orig_reserve = _PendingBudget.reserve
        orig_done = _PendingBudget.done

        def copy(reader, dest, member, stats):
            with self.lock:
                self._write_count += 1
                ordinal = self._write_count
                self.writes.append(member.name)
                self.current_write = member.name
                self.chunks_for_current = []
                if self._active_fsyncs:
                    self.write_started_during_fsync += 1
            try:
                if self.write_delay:
                    time.sleep(self.write_delay)
                if self.fail_write_at == ordinal:
                    raise OSError("simulated write failure")
                return orig_copy(reader, dest, member, stats)
            finally:
                with self.lock:
                    if self.current_write is not None:
                        self.chunks_by_write.setdefault(self.current_write, []).extend(
                            self.chunks_for_current
                        )
                    self.current_write = None

        def fsync(fd):
            with self.lock:
                self._fsync_count += 1
                ordinal = self._fsync_count
                self._active_fsyncs += 1
                self.max_active_fsyncs = max(self.max_active_fsyncs, self._active_fsyncs)
                if self.output_root is not None and os.path.isdir(self.output_root):
                    self.published_during_fsync.extend(
                        entry for entry in os.listdir(self.output_root)
                        if not entry.startswith(".")
                    )
            try:
                if self.fsync_delay:
                    time.sleep(self.fsync_delay)
                if self.fail_fsync_at == ordinal:
                    raise OSError("simulated fsync failure")
                return orig_fsync(fd)
            finally:
                with self.lock:
                    self._active_fsyncs -= 1
                    self.completed_fsyncs += 1
                    self.writes_at_fsync_done.append(len(self.writes))

        def write_all(fd, chunk):
            with self.lock:
                if self.current_write is not None:
                    self.chunks_for_current.append(len(chunk))
            return orig_write_all(fd, chunk)

        def reserve(budget, size):
            result = orig_reserve(budget, size)
            with self.lock:
                self.reserved_sizes.append(size)
                self.peak_files = max(self.peak_files, budget._files)
                self.peak_bytes = max(self.peak_bytes, budget._bytes)
            return result

        def done(budget, size):
            result = orig_done(budget, size)
            with self.lock:
                self.peak_files = max(self.peak_files, budget._files)
                self.peak_bytes = max(self.peak_bytes, budget._bytes)
            return result

        monkeypatch.setattr(restore_mod, "_copy_payload", copy)
        monkeypatch.setattr(restore_mod, "_fsync_fd", fsync)
        monkeypatch.setattr(restore_mod, "_write_all", write_all)
        monkeypatch.setattr(_PendingBudget, "reserve", reserve)
        monkeypatch.setattr(_PendingBudget, "done", done)


@pytest.fixture
def pipeline(tmp_path, monkeypatch):
    probe = _Probe()
    probe.install(monkeypatch)
    out = tmp_path / "out"
    probe.output_root = str(out)
    return probe, out


def _build_many(tmp_path, archive_builder, count=6, size=2000):
    members = [("dir", "d"), ("dir", "d/sub")]
    for i in range(count):
        payload = bytes([65 + i]) * size
        members.append(("file", f"d/sub/f{i:03d}.bin", payload))
    return archive_builder("many.tar.gz", members)


# ---------------------------------------------------------------------------
# Admission control primitives
# ---------------------------------------------------------------------------


def test_budget_charges_full_size_immediately_and_until_done():
    budget = _PendingBudget(FlushLimits(max_files=4, max_bytes=10_000))
    budget.reserve(4_000)
    # Full declared size is charged before any byte is written.
    assert budget._bytes == 4_000
    assert budget._files == 1
    budget.done(4_000)
    assert budget._bytes == 0
    assert budget._files == 0


def test_budget_blocks_when_byte_limit_would_be_exceeded():
    budget = _PendingBudget(FlushLimits(max_files=10, max_bytes=10_000))
    budget.reserve(6_000)
    start = threading.Event()

    def second():
        start.set()
        budget.reserve(6_000)
        budget.done(6_000)

    thread = threading.Thread(target=second)
    thread.start()
    start.wait()
    time.sleep(0.1)
    assert thread.is_alive(), "second reservation must wait for the byte budget"
    budget.done(6_000)
    thread.join(timeout=5)
    assert not thread.is_alive()


def test_budget_blocks_when_file_limit_reached():
    budget = _PendingBudget(FlushLimits(max_files=1, max_bytes=10**9))
    budget.reserve(10)
    done = threading.Event()

    def second():
        budget.reserve(10)
        done.set()
        budget.done(10)

    thread = threading.Thread(target=second)
    thread.start()
    time.sleep(0.1)
    assert not done.is_set()
    budget.done(10)
    assert done.wait(5)
    thread.join(timeout=5)


def test_oversized_file_gets_exclusive_access():
    budget = _PendingBudget(FlushLimits(max_files=4, max_bytes=1_000))
    budget.reserve(5_000)  # larger than the whole budget
    assert budget._exclusive
    allowed = threading.Event()

    def other():
        budget.reserve(10)
        allowed.set()
        budget.done(10)

    thread = threading.Thread(target=other)
    thread.start()
    time.sleep(0.1)
    assert not allowed.is_set(), "other writes must wait for the oversized file"
    budget.done(5_000)
    assert allowed.wait(5)
    thread.join(timeout=5)


# ---------------------------------------------------------------------------
# Pipeline behaviour during a restore
# ---------------------------------------------------------------------------


def test_write_overlaps_with_previous_fsync(pipeline, tmp_path, archive_builder):
    probe, out = pipeline
    probe.fsync_delay = 0.15
    probe.write_delay = 0.02
    archive = _build_many(tmp_path, archive_builder, count=5)

    result = restore_directory(
        archive,
        "d/sub",
        str(out),
        fsync=True,
        max_pending_flush_files=5,
        max_pending_flush_bytes=10**9,
    )

    assert result.files == 5
    assert probe.write_started_during_fsync > 0, "next file must be written while fsync runs"
    assert probe.max_active_fsyncs >= 2
    assert probe.published_during_fsync == []
    assert len(_pairs(result.restored_path)) == 5


def test_file_limit_one_serialises_write_behind_fsync(pipeline, tmp_path, archive_builder):
    probe, out = pipeline
    probe.fsync_delay = 0.1
    archive = _build_many(tmp_path, archive_builder, count=3)

    restore_directory(
        archive,
        "d/sub",
        str(out),
        fsync=True,
        max_pending_flush_files=1,
        max_pending_flush_bytes=10**9,
    )

    assert probe.peak_files == 1
    assert probe.max_active_fsyncs == 1
    assert probe.write_started_during_fsync == 0


def test_byte_limit_bounds_pending_declared_bytes(pipeline, tmp_path, archive_builder):
    probe, out = pipeline
    probe.fsync_delay = 0.1
    size = 4_000
    archive = _build_many(tmp_path, archive_builder, count=5, size=size)

    restore_directory(
        archive,
        "d/sub",
        str(out),
        fsync=True,
        max_pending_flush_files=10,
        max_pending_flush_bytes=9_000,  # two 4000-byte files fit, three do not
    )

    assert probe.peak_bytes <= 9_000
    assert probe.peak_bytes == 8_000
    assert probe.peak_files <= 2


def test_hardlinks_to_one_source_are_charged_once(pipeline, sample_archive):
    probe, out = pipeline
    probe.fsync_delay = 0.01
    result = restore_directory(
        sample_archive,
        "data/2024",
        str(out),
        fsync=True,
        max_pending_flush_files=16,
        max_pending_flush_bytes=10**9,
    )

    # One reservation per archive source: 3 selected regular files plus the two
    # external originals fetched for the cross-directory links.  The five
    # selected names that resolve to shared/blob.bin are not charged again.
    assert sorted(probe.reserved_sizes) == [10, 30, 3_328, 4_096, 6_656]
    assert len(probe.reserved_sizes) == 5
    assert result.files == 3
    assert result.hardlinks == 7
    assert result.bytes_written == 14_120


def test_external_fetch_obeys_the_same_limits(pipeline, tmp_path, archive_builder):
    probe, out = pipeline
    probe.fsync_delay = 0.05
    archive = archive_builder(
        "external.tar.gz",
        [
            ("dir", "d"),
            ("dir", "d/sub"),
            ("file", "d/sub/a.bin", b"A" * 1_500),
            ("file", "ext/one.bin", b"1" * 1_500),
            ("link", "d/sub/l1", "ext/one.bin"),
            ("link", "d/sub/l2", "ext/one.bin"),
            ("file", "ext/two.bin", b"2" * 1_500),
            ("link", "d/sub/l3", "ext/two.bin"),
        ],
    )

    restore_directory(
        archive,
        "d/sub",
        str(out),
        fsync=True,
        max_pending_flush_files=2,
        max_pending_flush_bytes=3_000,
    )

    # 3 payload writes total (a.bin + two external sources); l1/l2/l3 add none.
    assert sorted(probe.reserved_sizes) == [1_500, 1_500, 1_500]
    assert probe.peak_bytes <= 3_000
    assert probe.peak_files <= 2


def test_oversized_file_is_restored_in_chunks_and_holds_the_pipeline(
    pipeline, tmp_path, archive_builder, monkeypatch
):
    probe, out = pipeline
    monkeypatch.setattr(restore_mod, "CHUNK_SIZE", 512)
    probe.fsync_delay = 0.1
    big = b"B" * 5_000
    archive = archive_builder(
        "big.tar.gz",
        [
            ("dir", "d"),
            ("dir", "d/sub"),
            ("file", "d/sub/big.bin", big),
            ("file", "d/sub/small1.bin", b"1" * 10),
            ("file", "d/sub/small2.bin", b"2" * 10),
        ],
    )

    result = restore_directory(
        archive,
        "d/sub",
        str(out),
        fsync=True,
        max_pending_flush_files=4,
        max_pending_flush_bytes=1_000,  # smaller than big.bin
    )

    with open(os.path.join(result.restored_path, "big.bin"), "rb") as fh:
        assert fh.read() == big
    # The oversized file was streamed in several chunks, never rejected.
    assert probe.writes[0] == "d/sub/big.bin"
    assert len(probe.chunks_by_write["d/sub/big.bin"]) > 1
    # It owned the pipeline: the small files only started after its fsync.
    small_ordinals = [i for i, name in enumerate(probe.writes) if "small" in name]
    assert small_ordinals
    big_fsync_done_at = probe.writes_at_fsync_done[0]
    assert big_fsync_done_at < min(small_ordinals) + 1
    # While the oversized file was pending it was the only reservation; the
    # peak of two comes from the later small files sharing the budget.
    assert probe.reserved_sizes[0] == 5_000
    assert probe.peak_bytes <= 5_000 + 1_000


def test_fsync_failure_stops_new_writes_and_cleans_up(pipeline, tmp_path, archive_builder):
    probe, out = pipeline
    probe.fail_fsync_at = 1
    archive = _build_many(tmp_path, archive_builder, count=8)

    with pytest.raises(OSError, match="simulated fsync failure"):
        restore_directory(
            archive,
            "d/sub",
            str(out),
            fsync=True,
            max_pending_flush_files=1,
            max_pending_flush_bytes=10**9,
        )

    # Only the in-flight file was written; nothing new started afterwards.
    assert len(probe.writes) == 1
    _assert_no_leftovers(out)


def test_fsync_failure_drains_inflight_then_cleans_up(pipeline, tmp_path, archive_builder):
    probe, out = pipeline
    probe.fsync_delay = 0.05
    probe.fail_fsync_at = 1
    archive = _build_many(tmp_path, archive_builder, count=8)

    failures = []

    def attempt():
        try:
            restore_directory(
                archive,
                "d/sub",
                str(out),
                fsync=True,
                max_pending_flush_files=3,
                max_pending_flush_bytes=10**9,
            )
        except OSError as exc:
            failures.append(exc)

    thread = threading.Thread(target=attempt)
    thread.start()
    thread.join(timeout=20)
    assert not thread.is_alive()
    assert failures

    # At most the files admitted before the failure were written.
    assert len(probe.writes) <= 3
    _assert_no_leftovers(out)


def test_write_failure_stops_the_restore_and_cleans_up(pipeline, tmp_path, archive_builder):
    probe, out = pipeline
    probe.fail_write_at = 2
    archive = _build_many(tmp_path, archive_builder, count=5)

    with pytest.raises(OSError, match="simulated write failure"):
        restore_directory(
            archive,
            "d/sub",
            str(out),
            fsync=True,
            max_pending_flush_files=4,
            max_pending_flush_bytes=10**9,
        )

    assert len(probe.writes) == 2
    _assert_no_leftovers(out)


def test_other_restores_still_work_after_a_failure(pipeline, tmp_path, archive_builder):
    probe, out = pipeline
    probe.fail_fsync_at = 1
    archive = _build_many(tmp_path, archive_builder, count=3, size=100)

    with pytest.raises(OSError):
        restore_directory(archive, "d/sub", str(out), fsync=True)
    probe.fail_fsync_at = None

    result = restore_directory(archive, "d/sub", str(out), fsync=True)
    assert os.path.isdir(result.restored_path)
    assert len(_pairs(result.restored_path)) == 3
    staging = os.path.join(str(out), ".staging")
    if os.path.isdir(staging):
        assert os.listdir(staging) == []
    published = [p for p in os.listdir(out) if not p.startswith(".")]
    assert published == [os.path.basename(os.path.dirname(result.restored_path))]


def test_fsync_off_does_not_use_the_pipeline(pipeline, tmp_path, archive_builder):
    probe, out = pipeline
    archive = _build_many(tmp_path, archive_builder, count=3, size=100)

    result = restore_directory(
        archive,
        "d/sub",
        str(out),
        fsync=False,
        max_pending_flush_files=1,
        max_pending_flush_bytes=1,
    )

    assert probe.reserved_sizes == []
    assert probe._fsync_count == 0
    assert len(_pairs(result.restored_path)) == 3


# ---------------------------------------------------------------------------
# Configuration parsing
# ---------------------------------------------------------------------------


def test_limits_come_from_environment(monkeypatch):
    monkeypatch.setenv("RESTORE_MAX_PENDING_FLUSH_FILES", "3")
    monkeypatch.setenv("RESTORE_MAX_PENDING_FLUSH_BYTES", "12345")
    limits = restore_mod.resolve_flush_limits()
    assert (limits.max_files, limits.max_bytes) == (3, 12_345)


def test_explicit_limits_override_environment(monkeypatch):
    monkeypatch.setenv("RESTORE_MAX_PENDING_FLUSH_FILES", "3")
    limits = restore_mod.resolve_flush_limits(7, 999)
    assert (limits.max_files, limits.max_bytes) == (7, 999)


@pytest.mark.parametrize("value", ["0", "-1", "abc", "1.5"])
def test_invalid_environment_limit_is_rejected(monkeypatch, value):
    monkeypatch.setenv("RESTORE_MAX_PENDING_FLUSH_BYTES", value)
    with pytest.raises(InvalidConfiguration):
        restore_mod.resolve_flush_limits()


@pytest.mark.parametrize("kwargs", [
    {"max_pending_flush_files": 0},
    {"max_pending_flush_files": -2},
    {"max_pending_flush_bytes": 0},
])
def test_invalid_explicit_limits_are_rejected(tmp_path, sample_archive, kwargs):
    with pytest.raises(InvalidConfiguration):
        restore_directory(
            sample_archive, "data/2024", str(tmp_path / "out"), fsync=True, **kwargs
        )


def test_invalid_env_limit_ignored_when_fsync_off(monkeypatch, tmp_path, sample_archive):
    monkeypatch.setenv("RESTORE_MAX_PENDING_FLUSH_FILES", "nonsense")
    result = restore_directory(
        sample_archive, "data/2024", str(tmp_path / "out"), fsync=False
    )
    assert result.files == 3
