"""Restore one subdirectory out of a read-only ``tar.gz`` archive.

Guarantees
----------
* The archive is read sequentially in streaming mode (``r|gz``); no seeking is
  required, which keeps it usable on read-only mounts and pipes.
* Only the selected subtree is materialised.  Bytes of members that are not
  needed are read in chunks and discarded, never written.
* The total number of *extracted* payload bytes is at most twice the payload of
  one complete decompression.  There is exactly one full scan (1x); a second
  scan happens only when a retained hard link points at a regular file outside
  the selection, and it stops as soon as those targets have been read (<= 1x).
* A hard link whose target lies outside the selection, appears later in the
  archive, or is itself a hard link (chained) is resolved against the archive
  root.  All retained entries that resolve to the same regular-file member are
  hard-linked together, so they share one inode.
* Missing / cyclic / non-regular link chains that a retained entry depends on
  raise :class:`LinkError`.
* Restoration is staged in a hidden directory; the final directory is only
  published with one atomic ``rename`` after every byte and every hard link is
  in place.  Any failure removes everything produced by that attempt.
* With ``fsync`` enabled, each payload is handed to a bounded asynchronous
  fsync pipeline.  A file reserves one slot and its *full declared size* from
  the moment writing starts until its fsync succeeds, so the next member can be
  decompressed and written while earlier files are still being flushed.  One
  file larger than the byte budget may still be restored: it takes the whole
  pipeline for its own write and flush and blocks other members meanwhile.
  Links that resolve to the same original share one payload write, so they are
  charged once.  The published rename happens only after every pending fsync
  has succeeded.  After a write or fsync failure no new write is started, the
  in-flight operations are allowed to finish, and only then is staging removed.
"""

from __future__ import annotations

import logging
import os
import shutil
import tarfile
import threading
import uuid
import zlib
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import asdict, dataclass, field
from typing import IO, Optional

logger = logging.getLogger(__name__)

CHUNK_SIZE = 1024 * 1024  # 1 MiB

# Defaults for the per-restore fsync pipeline.  They bound how many files may
# be written but not yet flushed, and how many payload bytes those files
# account for (each file is charged its full declared size).
DEFAULT_MAX_PENDING_FLUSH_FILES = 16
DEFAULT_MAX_PENDING_FLUSH_BYTES = 64 * 1024 * 1024
ENV_MAX_PENDING_FLUSH_FILES = "RESTORE_MAX_PENDING_FLUSH_FILES"
ENV_MAX_PENDING_FLUSH_BYTES = "RESTORE_MAX_PENDING_FLUSH_BYTES"

DIR = "dir"
FILE = "file"
HARDLINK = "hardlink"
SYMLINK = "symlink"
OTHER = "other"


class RestoreError(Exception):
    """Base class for all restore failures."""


class InvalidSelection(RestoreError):
    """The requested subdirectory path is not a safe relative path."""


class PathNotFound(RestoreError):
    """The requested subdirectory is not present in the archive."""


class NotADirectory(RestoreError):
    """The requested path exists but is not a directory."""


class LinkError(RestoreError):
    """A retained hard link is missing, cyclic or points at a non-file."""


class MalformedArchive(RestoreError):
    """The archive contains conflicting or otherwise unusable members."""


class InvalidArchive(RestoreError):
    """The archive cannot be read as a gzip-compressed tar stream."""


class InvalidConfiguration(RestoreError):
    """A restore option (usually an environment setting) is unusable."""


@dataclass
class Member:
    name: str
    kind: str
    size: int = 0
    mode: int = 0o644
    mtime: float = 0.0
    linkname: Optional[str] = None


@dataclass
class RestoreResult:
    requested_path: str
    restored_path: str = ""
    directories: int = 1  # the restored root itself
    files: int = 0
    hardlinks: int = 0
    bytes_written: int = 0
    bytes_extracted: int = 0
    archive_payload_bytes: int = 0
    skipped_unsafe: int = 0
    skipped_unsupported: int = 0

    def as_dict(self) -> dict:
        return asdict(self)


@dataclass(frozen=True)
class FlushLimits:
    """Per-restore bounds for the asynchronous fsync pipeline."""

    max_files: int
    max_bytes: int


def _parse_limit(value: object, default: int, label: str) -> int:
    if value is None:
        value = default
    if isinstance(value, bool) or not isinstance(value, int):
        raise InvalidConfiguration(f"{label} must be an integer, got {value!r}")
    if value < 1:
        raise InvalidConfiguration(f"{label} must be >= 1, got {value!r}")
    return value


def _env_limit(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    try:
        value = int(raw.strip())
    except ValueError as exc:
        raise InvalidConfiguration(
            f"{name} must be an integer, got {raw!r}"
        ) from exc
    return _parse_limit(value, default, name)


def resolve_flush_limits(
    max_files: Optional[int] = None,
    max_bytes: Optional[int] = None,
) -> FlushLimits:
    """Resolve explicit values, then environment defaults."""

    if max_files is None:
        max_files = _env_limit(ENV_MAX_PENDING_FLUSH_FILES, DEFAULT_MAX_PENDING_FLUSH_FILES)
    else:
        max_files = _parse_limit(max_files, DEFAULT_MAX_PENDING_FLUSH_FILES, "max_pending_flush_files")
    if max_bytes is None:
        max_bytes = _env_limit(ENV_MAX_PENDING_FLUSH_BYTES, DEFAULT_MAX_PENDING_FLUSH_BYTES)
    else:
        max_bytes = _parse_limit(max_bytes, DEFAULT_MAX_PENDING_FLUSH_BYTES, "max_pending_flush_bytes")
    return FlushLimits(max_files=max_files, max_bytes=max_bytes)


def normalize_member_name(raw: str) -> str:
    """Normalise an archive member path, rejecting anything that escapes it."""

    value = raw.replace("\\", "/")
    if value.startswith("/"):
        raise MalformedArchive(f"absolute member path rejected: {raw!r}")
    parts = []
    for part in value.split("/"):
        if part in ("", "."):
            continue
        if part == "..":
            raise MalformedArchive(f"member path escapes archive root: {raw!r}")
        parts.append(part)
    return "/".join(parts)


def normalize_selection(raw: str) -> str:
    """Normalise the user-supplied subdirectory path."""

    if not isinstance(raw, str) or not raw.strip():
        raise InvalidSelection("path must be a non-empty relative directory path")
    value = raw.strip().replace("\\", "/")
    if value.startswith("/"):
        raise InvalidSelection("path must be relative to the archive root")
    parts = []
    for part in value.split("/"):
        if part in ("", "."):
            continue
        if part == "..":
            raise InvalidSelection("path must not contain '..'")
        parts.append(part)
    if not parts:
        raise InvalidSelection("path must select a subdirectory, not the archive root")
    return "/".join(parts)


def _within(path: str, root: str) -> bool:
    return bool(root) and (path == root or path.startswith(root + "/"))


def _rel(path: str, root: str) -> str:
    return path[len(root) + 1 :]


def _member_kind(info: tarfile.TarInfo) -> str:
    if info.isdir():
        return DIR
    if info.isreg():
        return FILE
    if info.islnk():
        return HARDLINK
    if info.issym():
        return SYMLINK
    return OTHER


def _open_stream(path: str) -> tarfile.TarFile:
    try:
        return tarfile.open(path, mode="r|gz")
    except (tarfile.TarError, OSError) as exc:
        raise InvalidArchive(f"cannot open archive {path!r}: {exc}") from exc


def _drain(reader: Optional[IO[bytes]], stats: RestoreResult) -> None:
    if reader is None:
        return
    while True:
        chunk = reader.read(CHUNK_SIZE)
        if not chunk:
            return
        stats.bytes_extracted += len(chunk)


def _fsync_fd(fd: int) -> None:
    """fsync a payload descriptor (indirection keeps failure tests simple)."""

    os.fsync(fd)


def _fsync_dir(path: str) -> None:
    try:
        fd = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


def _file_mode(mode: int) -> int:
    # Keep the archived permissions but always let the owner read/write, so the
    # restored data is usable and can be cleaned up on failure.
    return (mode & 0o777) | 0o600


def _dir_mode(mode: int) -> int:
    # Owner needs rwx to traverse the tree; without the execute bit a directory
    # cannot be descended into (and even cleanup would fail).
    return (mode & 0o777) | 0o700


def _write_all(fd: int, chunk: bytes) -> None:
    view = memoryview(chunk)
    while view:
        written = os.write(fd, view)
        if written <= 0:  # pragma: no cover - defensive
            raise OSError("short write to restored file")
        view = view[written:]


def _safe_close(fd: Optional[int]) -> None:
    if fd is None:
        return
    try:
        os.close(fd)
    except OSError:  # pragma: no cover - defensive
        pass


def _copy_payload(
    reader: IO[bytes],
    dest: str,
    member: Member,
    stats: RestoreResult,
) -> int:
    """Stream one member to ``dest``; return the still-open file descriptor."""

    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    fd = os.open(dest, flags, 0o600)
    written = 0
    try:
        while True:
            chunk = reader.read(CHUNK_SIZE)
            if not chunk:
                break
            stats.bytes_extracted += len(chunk)
            _write_all(fd, chunk)
            written += len(chunk)
        os.fchmod(fd, _file_mode(member.mode))
        if member.mtime > 0:
            try:
                os.utime(fd, (member.mtime, member.mtime))
            except OSError:
                pass
    except BaseException:
        _safe_close(fd)
        try:
            os.unlink(dest)
        except OSError:
            pass
        raise
    stats.bytes_written += written
    return fd


def _write_payload(
    reader: IO[bytes],
    dest: str,
    member: Member,
    stats: RestoreResult,
    fsync: bool,
) -> None:
    """Synchronous write (and optional fsync) of a single member."""

    fd = _copy_payload(reader, dest, member, stats)
    try:
        if fsync:
            _fsync_fd(fd)
    finally:
        _safe_close(fd)


class _PendingBudget:
    """Admission control for files that are written but not yet flushed.

    A reservation accounts for the member's full declared size even while only
    part of it has been read, and is released only when its fsync has finished
    (successfully or not).  A file bigger than the byte budget may start once
    the pipeline is idle; it then owns the pipeline until its flush completes.
    """

    def __init__(self, limits: FlushLimits) -> None:
        self.max_files = limits.max_files
        self.max_bytes = limits.max_bytes
        self._cond = threading.Condition()
        self._files = 0
        self._bytes = 0
        self._active = 0
        self._exclusive = False
        self._error: Optional[BaseException] = None

    def reserve(self, size: int) -> None:
        if size < 0:  # pragma: no cover - archive sizes are non-negative
            raise ValueError("size must not be negative")
        with self._cond:
            while True:
                if self._error is not None:
                    raise self._error
                if self._exclusive:
                    self._cond.wait()
                    continue
                if size > self.max_bytes:
                    if self._active == 0:
                        self._exclusive = True
                        self._take(size)
                        return
                elif self._files < self.max_files and self._bytes + size <= self.max_bytes:
                    self._take(size)
                    return
                self._cond.wait()

    def _take(self, size: int) -> None:
        self._files += 1
        self._bytes += size
        self._active += 1

    def done(self, size: int) -> None:
        with self._cond:
            self._files -= 1
            self._bytes -= size
            self._active -= 1
            self._exclusive = False
            self._cond.notify_all()

    def fail(self, exc: BaseException) -> None:
        with self._cond:
            if self._error is None:
                self._error = exc
            self._cond.notify_all()


class _DirectSink:
    """``fsync`` disabled: write straight through, as before."""

    def __init__(self, stats: RestoreResult) -> None:
        self._stats = stats

    def write(self, reader: IO[bytes], dest: str, member: Member) -> None:
        _write_payload(reader, dest, member, self._stats, False)

    def finalize(self) -> None:
        return

    def close(self) -> None:
        return


class _PipelineSink:
    """Write payloads on the caller thread, fsync them on a bounded pool."""

    def __init__(self, stats: RestoreResult, limits: FlushLimits) -> None:
        self._stats = stats
        self._budget = _PendingBudget(limits)
        self._pool = ThreadPoolExecutor(
            max_workers=min(4, limits.max_files),
            thread_name_prefix="restore-fsync",
        )
        self._futures: list[Future] = []
        self._closed = False

    def write(self, reader: IO[bytes], dest: str, member: Member) -> None:
        size = member.size
        # Reserve before the first byte is written: from now until fsync the
        # file is charged at its full size, and this call blocks while the
        # pending-file / pending-byte budget is exhausted.
        self._budget.reserve(size)
        try:
            fd = _copy_payload(reader, dest, member, self._stats)
        except BaseException:
            self._budget.done(size)
            raise
        self._submit(fd, size)

    def _submit(self, fd: int, size: int) -> None:
        def flush() -> None:
            try:
                _fsync_fd(fd)
            except BaseException as exc:
                self._budget.fail(exc)
                raise
            finally:
                _safe_close(fd)
                self._budget.done(size)

        try:
            self._futures.append(self._pool.submit(flush))
        except RuntimeError as exc:  # pragma: no cover - shutdown race
            _safe_close(fd)
            self._budget.done(size)
            raise RestoreError(f"cannot schedule fsync: {exc}") from exc

    def finalize(self) -> None:
        """Wait for every flush and re-raise the first failure."""

        self._shutdown()
        first: Optional[BaseException] = None
        for future in self._futures:
            exc = future.exception()
            if exc is not None and first is None:
                first = exc
        if first is not None:
            raise first

    def close(self) -> None:
        self._shutdown()

    def _shutdown(self) -> None:
        if self._closed:
            return
        self._closed = True
        # Running tasks always release their budget reservation in ``finally``;
        # waiting here is what lets the caller clean up staging without racing
        # an in-flight write or fsync.
        self._pool.shutdown(wait=True)


def _make_sink(fsync: bool, limits: FlushLimits, stats: RestoreResult) -> "_DirectSink | _PipelineSink":
    if fsync:
        return _PipelineSink(stats, limits)
    return _DirectSink(stats)


class _OutputTree:
    """Tracks the staging tree and defends against path/type conflicts."""

    def __init__(self, root: str) -> None:
        self.root = os.path.abspath(root)
        self.dir_paths: set[str] = {""}
        self.file_paths: set[str] = set()
        self.dir_meta: dict[str, tuple[int, float]] = {}

    def dest(self, rel: str) -> str:
        if not rel:
            return self.root
        if rel.startswith("/") or ".." in rel.split("/"):
            raise MalformedArchive(f"refusing path outside restore root: {rel!r}")
        return os.path.join(self.root, *rel.split("/"))

    def ensure_dir(self, rel: str, mode: Optional[int] = None, mtime: float = 0.0) -> None:
        if rel in self.file_paths:
            raise MalformedArchive(f"path {rel!r} is both a file and a directory")
        if rel and rel not in self.dir_paths:
            # Implicit parents (archives do not always list every directory).
            self.ensure_parents(rel)
        if rel not in self.dir_paths:
            try:
                os.mkdir(self.dest(rel), 0o700)
            except FileExistsError:
                path = self.dest(rel)
                if os.path.islink(path) or not os.path.isdir(path):
                    raise MalformedArchive(f"path {rel!r} conflicts with an existing entry")
            self.dir_paths.add(rel)
        if mode is not None:
            self.dir_meta[rel] = (mode, mtime)

    def ensure_parents(self, rel: str) -> None:
        if not rel:
            return
        parts = rel.split("/")[:-1]
        current = ""
        for part in parts:
            current = part if not current else f"{current}/{part}"
            self.ensure_dir(current)

    def register_file(self, rel: str) -> None:
        if not rel:
            raise MalformedArchive("refusing to write the restore root as a file")
        if rel in self.dir_paths or rel in self.file_paths:
            raise MalformedArchive(f"duplicate or conflicting member {rel!r}")
        self.file_paths.add(rel)

    def finalize_dirs(self) -> None:
        for rel in sorted(self.dir_paths, key=lambda p: p.count("/"), reverse=True):
            path = self.dest(rel)
            mode, mtime = self.dir_meta.get(rel, (0o755, 0.0))
            try:
                os.chmod(path, _dir_mode(mode))
                if mtime > 0:
                    os.utime(path, (mtime, mtime))
            except OSError as exc:  # pragma: no cover - unusual filesystem
                logger.warning("could not finalise directory %s: %s", path, exc)

    def sync_dirs(self) -> None:
        """fsync every staging directory after all payloads are durable."""

        for rel in sorted(self.dir_paths, key=lambda p: p.count("/"), reverse=True):
            _fsync_dir(self.dest(rel))


def _resolve_hardlink(
    name: str,
    members: dict[str, Member],
    cache: dict[str, str],
) -> str:
    """Return the archive path of the regular file that owns ``name``'s data."""

    chain: list[str] = []
    seen: set[str] = set()
    current = name
    while True:
        if current in cache:
            source = cache[current]
            for step in chain:
                cache[step] = source
            return source
        if current in seen:
            start = chain.index(current)
            cycle = " -> ".join(chain[start:] + [current])
            raise LinkError(f"cyclic hard link chain required by {name!r}: {cycle}")
        seen.add(current)
        chain.append(current)

        member = members.get(current)
        if member is None:
            raise LinkError(
                f"hard link target {current!r} is missing from the archive"
                f" (required by {' -> '.join(chain)})"
            )
        if member.kind == FILE:
            for step in chain:
                cache[step] = current
            return current
        if member.kind != HARDLINK:
            raise LinkError(
                f"hard link target {current!r} is a {member.kind}, not a regular file"
                f" (required by {' -> '.join(chain)})"
            )
        if not member.linkname:
            raise LinkError(f"hard link {current!r} has an empty target")
        current = member.linkname


@dataclass
class _Plan:
    groups: dict[str, list[str]] = field(default_factory=dict)  # source -> selected names
    canonical: dict[str, str] = field(default_factory=dict)  # source -> selected name
    external: dict[str, str] = field(default_factory=dict)  # source -> canonical dest rel

    def destination_rel(self, source: str, selection: str) -> str:
        """Path (relative to the restored root) holding ``source``'s data."""

        if source in self.external:
            return self.external[source]
        return _rel(source, selection)


def _populate(
    archive_path: str,
    selection: str,
    output_root: str,
    stats: RestoreResult,
    sink: "_DirectSink | _PipelineSink",
) -> _OutputTree:
    tree = _OutputTree(output_root)
    members: dict[str, Member] = {}
    selected_files: set[str] = set()
    selected_links: list[str] = []
    plan = _Plan()
    selection_member: Optional[Member] = None
    has_descendants = False

    stream = _open_stream(archive_path)
    with stream:
        for info in stream:
            try:
                name = normalize_member_name(info.name)
            except MalformedArchive as exc:
                logger.warning("skipping unsafe member: %s", exc)
                stats.skipped_unsafe += 1
                if info.isreg():
                    stats.archive_payload_bytes += info.size
                    _drain(stream.extractfile(info), stats)
                continue
            if not name:
                if info.isreg():
                    stats.archive_payload_bytes += info.size
                    _drain(stream.extractfile(info), stats)
                continue
            kind = _member_kind(info)
            if name in members:
                existing = members[name]
                # Duplicate directory entries occur in real archives (e.g. an
                # explicit "dir/" plus an implicit one); treat them as one.
                if kind == DIR and existing.kind == DIR:
                    continue
                raise MalformedArchive(f"duplicate archive member {name!r}")

            linkname = None
            invalid_link = None
            if kind == HARDLINK:
                try:
                    linkname = normalize_member_name(info.linkname or "")
                    if not linkname:
                        invalid_link = "empty target"
                except MalformedArchive as exc:
                    invalid_link = str(exc)
                if invalid_link and _within(name, selection):
                    # A link we must reproduce cannot be trusted; fail loudly.
                    raise LinkError(f"hard link {name!r}: {invalid_link}")
            member = Member(
                name=name,
                kind=kind,
                size=info.size if kind == FILE else 0,
                mode=info.mode,
                mtime=info.mtime,
                linkname=linkname,
            )
            members[name] = member

            if kind == FILE:
                stats.archive_payload_bytes += info.size

            inside = _within(name, selection)
            if name == selection:
                selection_member = member
            if name.startswith(selection + "/"):
                has_descendants = True

            if kind == FILE:
                if inside and name != selection:
                    rel = _rel(name, selection)
                    tree.ensure_parents(rel)
                    tree.register_file(rel)
                    reader = stream.extractfile(info)
                    if reader is None:
                        raise MalformedArchive(f"cannot read payload of {name!r}")
                    sink.write(reader, tree.dest(rel), member)
                    selected_files.add(name)
                    stats.files += 1
                else:
                    _drain(stream.extractfile(info), stats)
            elif kind == DIR:
                if inside and name != selection:
                    rel = _rel(name, selection)
                    tree.ensure_dir(rel, mode=member.mode, mtime=member.mtime)
            elif kind == HARDLINK:
                if inside and name != selection:
                    rel = _rel(name, selection)
                    tree.ensure_parents(rel)
                    tree.register_file(rel)
                    selected_links.append(name)
                    stats.hardlinks += 1
            elif inside and name != selection:
                # Symlinks, devices, fifos: out of scope, never materialised, so
                # they cannot be used to escape the staging tree.
                stats.skipped_unsupported += 1

    if selection_member is not None and selection_member.kind != DIR:
        raise NotADirectory(
            f"{selection!r} is a {selection_member.kind}, not a directory"
        )
    if selection_member is None and not has_descendants:
        raise PathNotFound(f"directory {selection!r} not found in the archive")

    link_cache: dict[str, str] = {}
    for name in selected_links:
        source = _resolve_hardlink(name, members, link_cache)
        plan.groups.setdefault(source, []).append(name)
    for name in selected_files:
        plan.groups.setdefault(name, []).append(name)

    for source, entries in plan.groups.items():
        if source in selected_files:
            # The owning regular file is itself part of the selection: it is
            # already on disk, so every link can point at it.
            plan.canonical[source] = source
        else:
            # Only links to an outside file were selected. Materialise the
            # payload under one of the selected names; the rest link to it.
            canonical = sorted(entries)[0]
            plan.canonical[source] = canonical
            plan.external[source] = _rel(canonical, selection)

    if plan.external:
        _fetch_external(archive_path, members, plan, tree, stats, sink)

    # Create the hard links only after every payload exists, so forward and
    # chained references are all resolvable.  No new payload writes happen
    # here; the source may still be flushing.
    for source, entries in plan.groups.items():
        canonical_path = tree.dest(plan.destination_rel(source, selection))
        for entry in entries:
            if entry == plan.canonical[source]:
                continue
            rel = _rel(entry, selection)
            try:
                os.link(canonical_path, tree.dest(rel))
            except OSError as exc:
                raise RestoreError(f"cannot create hard link {rel!r}: {exc}") from exc

    if selection_member is not None:
        tree.dir_meta[""] = (selection_member.mode, selection_member.mtime)
    tree.finalize_dirs()
    stats.directories = len(tree.dir_paths)

    if stats.bytes_extracted > 2 * stats.archive_payload_bytes:
        raise RestoreError(
            "internal error: extracted byte budget exceeded "
            f"({stats.bytes_extracted} > 2 x {stats.archive_payload_bytes})"
        )
    return tree


def _fetch_external(
    archive_path: str,
    members: dict[str, Member],
    plan: _Plan,
    tree: _OutputTree,
    stats: RestoreResult,
    sink: "_DirectSink | _PipelineSink",
) -> None:
    """Second pass: copy link targets that live outside the selection.

    These writes go through the same sink, so they obey the same pending-file
    and pending-byte limits.  Each archive source is fetched once no matter how
    many retained hard links point at it.
    """

    remaining = set(plan.external)
    if not remaining:
        return
    stream = _open_stream(archive_path)
    with stream:
        for info in stream:
            if not info.isreg():
                continue
            try:
                name = normalize_member_name(info.name)
            except MalformedArchive:
                _drain(stream.extractfile(info), stats)
                continue
            if name in remaining:
                canonical_rel = plan.external[name]
                tree.ensure_parents(canonical_rel)
                reader = stream.extractfile(info)
                if reader is None:
                    raise MalformedArchive(f"cannot read payload of {name!r}")
                sink.write(reader, tree.dest(canonical_rel), members[name])
                remaining.remove(name)
                if not remaining:
                    break
            else:
                _drain(stream.extractfile(info), stats)
    if remaining:
        raise LinkError(
            "hard link target(s) could not be located during the data pass: "
            + ", ".join(sorted(remaining))
        )


def _cleanup(paths: list[str]) -> None:
    for path in paths:
        shutil.rmtree(path, ignore_errors=True)


def _publish(
    token_dir: str,
    restore_root: str,
    restore_id: str,
    basename: str,
    fsync: bool,
) -> str:
    # ``token_dir`` contains exactly the restored directory (basename). Renaming
    # the token directory publishes the whole result in one atomic step, so
    # nothing partial is ever visible under ``restore_root``.
    final_parent = os.path.join(restore_root, restore_id)
    os.rename(token_dir, final_parent)
    final_path = os.path.join(final_parent, basename)
    if fsync:
        _fsync_dir(final_parent)
        _fsync_dir(restore_root)
    return final_path


def restore_directory(
    archive_path: str,
    selection: str,
    restore_root: str,
    *,
    fsync: Optional[bool] = None,
    max_pending_flush_files: Optional[int] = None,
    max_pending_flush_bytes: Optional[int] = None,
) -> RestoreResult:
    """Restore ``selection`` from ``archive_path`` and publish it atomically.

    ``max_pending_flush_files`` and ``max_pending_flush_bytes`` bound the
    asynchronous fsync pipeline for this attempt; when omitted they come from
    ``RESTORE_MAX_PENDING_FLUSH_FILES`` / ``RESTORE_MAX_PENDING_FLUSH_BYTES``
    and then from built-in defaults.  They are ignored when ``fsync`` is off.
    """

    selection = normalize_selection(selection)
    if fsync is None:
        fsync = os.environ.get("RESTORE_FSYNC", "1").lower() not in ("0", "false", "no")
    if fsync:
        limits = resolve_flush_limits(max_pending_flush_files, max_pending_flush_bytes)
    else:
        # Flushing is off, so the pipeline bounds are irrelevant and a stale
        # environment setting must not break restores.
        limits = FlushLimits(
            max_files=DEFAULT_MAX_PENDING_FLUSH_FILES,
            max_bytes=DEFAULT_MAX_PENDING_FLUSH_BYTES,
        )
    if not os.path.isfile(archive_path):
        raise PathNotFound(f"archive {archive_path!r} does not exist")

    restore_root = os.path.abspath(restore_root)
    os.makedirs(restore_root, exist_ok=True)
    staging_root = os.path.join(restore_root, ".staging")
    os.makedirs(staging_root, mode=0o700, exist_ok=True)

    token = uuid.uuid4().hex
    restore_id = uuid.uuid4().hex
    basename = selection.rsplit("/", 1)[-1]
    token_dir = os.path.join(staging_root, token)
    output_root = os.path.join(token_dir, basename)
    stats = RestoreResult(requested_path=selection)
    sink = _make_sink(fsync, limits, stats)

    try:
        os.makedirs(token_dir, mode=0o700)
        os.mkdir(output_root, 0o700)
        try:
            tree = _populate(archive_path, selection, output_root, stats, sink)
            # Nothing is published until every payload (and every directory
            # entry) is durable.  finalize() waits for in-flight flushes and
            # re-raises the first write/fsync failure.
            sink.finalize()
            if fsync:
                tree.sync_dirs()
        except (tarfile.TarError, EOFError, zlib.error) as exc:
            raise InvalidArchive(f"corrupt or truncated archive: {exc}") from exc
        stats.restored_path = _publish(token_dir, restore_root, restore_id, basename, fsync)
    except BaseException:
        # Stop starting new writes, let the in-flight ones finish (their budget
        # slots and descriptors are released by then), and only then remove the
        # staging tree.  Other restores are unaffected: all state lives here.
        sink.close()
        _cleanup(
            [
                os.path.join(restore_root, restore_id),
                token_dir,
            ]
        )
        raise
    finally:
        # token_dir may be empty after a successful rename.
        if os.path.isdir(token_dir):
            _cleanup([token_dir])

    return stats
