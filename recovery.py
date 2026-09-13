"""Conservative artifact audit and verified local recovery bundles.

No event or artifact retention deletions. Incomplete output is deliberately left
marked for inspection rather than mistaken for a usable backup or installation.
"""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import sqlite3
import stat
import time
from pathlib import Path

from artifacts import validate_artifact_ref


class RecoveryError(ValueError):
    pass


def _connect(path):
    path = Path(path)
    if path.is_symlink() or not path.is_file():
        raise RecoveryError(f"database must be an existing regular file: {path}")
    conn = sqlite3.connect(path.resolve().as_uri() + '?mode=ro', uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def _hash(path):
    if path.is_symlink() or not stat.S_ISREG(path.stat().st_mode):
        raise RecoveryError(f'not a regular checksum input: {path}')
    digest = hashlib.sha256()
    with path.open('rb') as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


def _references(conn):
    refs = {}
    count = 0
    head = 0
    for row in conn.execute('SELECT id,payload FROM events ORDER BY id'):
        count += 1
        head = row['id']
        try:
            pending = [json.loads(row['payload'])]
            while pending:
                item = pending.pop()
                if isinstance(item, dict):
                    # A digest-looking custom reference cannot silently become
                    # garbage merely because it uses an unfamiliar wrapper.
                    if 'sha256' in item:
                        ref = validate_artifact_ref({key: item[key] for key in
                            ('sha256', 'size_bytes', 'media_type', 'kind')})
                        previous = refs.get(ref['sha256'])
                        if previous and previous['size_bytes'] != ref['size_bytes']:
                            raise ValueError('conflicting sizes for one artifact digest')
                        refs[ref['sha256']] = ref
                    pending.extend(item.values())
                elif isinstance(item, list):
                    pending.extend(item)
        except (ValueError, KeyError, TypeError, RecursionError) as exc:
            raise RecoveryError(f"cannot safely classify artifact references at event {head}: {exc}") from exc
    return refs, count, head


def _artifact(root, digest):
    root = Path(root)
    path = root / 'sha256' / digest[:2] / digest
    if any(p.is_symlink() for p in (root, root / 'sha256', path.parent, path)):
        raise RecoveryError(f"symlink in artifact store: {path}")
    return path


def _verify_file(path, digest, size):
    if not path.is_file() or path.is_symlink() or not stat.S_ISREG(path.stat().st_mode):
        raise RecoveryError(f"missing or non-regular artifact: {path}")
    if path.stat().st_size != size or _hash(path) != digest:
        raise RecoveryError(f"artifact integrity mismatch: {path}")


def _find(ref, roots):
    for root in roots:
        candidate = _artifact(root, ref['sha256'])
        if candidate.exists():
            _verify_file(candidate, ref['sha256'], ref['size_bytes'])
            return candidate
    raise RecoveryError(f"missing artifact {ref['sha256']}; supply all stores with --artifact-root")


def audit(database, roots, *, min_age_days=7):
    if min_age_days < 0:
        raise RecoveryError('minimum age cannot be negative')
    roots = [Path(root) for root in roots]
    conn = _connect(database)
    try:
        conn.execute('BEGIN')
        refs, count, head = _references(conn)
    finally:
        conn.close()
    issues = []
    for ref in refs.values():
        try:
            _find(ref, roots)
        except RecoveryError as exc:
            issues.append(str(exc))
    candidates, unknown = [], []
    cutoff = time.time() - min_age_days * 86400
    for root in roots:
        if root.is_symlink() or not root.is_dir():
            issues.append(f'not a regular artifact directory: {root}')
            continue
        for directory, directories, files in os.walk(root, followlinks=False):
            for name in directories + files:
                path = Path(directory) / name
                if path.is_symlink():
                    issues.append(f'symlink in artifact store: {path}')
            for name in files:
                path = Path(directory) / name
                if path.is_symlink():
                    continue
                valid = len(name) == 64 and all(c in '0123456789abcdef' for c in name)
                if not valid or path != root / 'sha256' / name[:2] / name:
                    unknown.append(str(path))
                elif name not in refs and path.stat().st_mtime <= cutoff:
                    candidates.append({'path': str(path), 'size_bytes': path.stat().st_size,
                                       'sha256': name})
    return {'ok': not issues, 'event_count': count, 'through_id': head,
            'referenced_artifacts': len(refs), 'issues': issues,
            'unreferenced_candidates': candidates, 'unclassified_files': unknown,
            'deletion_performed': False,
            'warning': 'Candidates may be in-flight or referenced by another database; no deletion is authorized.'}


def _sync_dir(path):
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _write(path, content):
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, 'wb') as target:
        target.write(content)
        target.flush()
        os.fsync(target.fileno())


def _begin(destination):
    destination = Path(destination)
    destination.mkdir(mode=0o700, parents=False, exist_ok=False)
    _write(destination / '.incomplete', b'Incomplete recovery operation. Do not use.\n')
    _sync_dir(destination)
    return destination


def _copy(source, destination):
    destination.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    descriptor = os.open(destination, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with source.open('rb') as reader, os.fdopen(descriptor, 'wb') as target:
        shutil.copyfileobj(reader, target, 1024 * 1024)
        target.flush()
        os.fsync(target.fileno())
    _sync_dir(destination.parent)


def _inspect_database(database):
    conn = _connect(database)
    try:
        integrity = [row[0] for row in conn.execute('PRAGMA integrity_check')]
        if integrity != ['ok']:
            raise RecoveryError(f'database integrity check failed: {integrity}')
        identity = conn.execute("SELECT value FROM bus_metadata WHERE name='database_id'").fetchone()
        if identity is None:
            raise RecoveryError('database identity missing; upgrade before backup')
        refs, count, head = _references(conn)
        return refs, {'database_id': identity[0], 'event_count': count, 'through_id': head}
    finally:
        conn.close()


def _finish(destination, manifest):
    for directory, _, _ in os.walk(destination, topdown=False):
        _sync_dir(Path(directory))
    _write(destination / 'manifest.json', (json.dumps(manifest, sort_keys=True, indent=2) + '\n').encode())
    _sync_dir(destination)
    (destination / '.incomplete').unlink()
    _sync_dir(destination)
    _sync_dir(destination.parent)


def backup(database, destination, roots=()):
    destination = _begin(destination)
    source = _connect(database)
    target = sqlite3.connect(destination / 'events.db')
    try:
        source.backup(target, pages=256)
        # A portable bundle must be one authoritative DB file, never a main
        # file whose logical contents can be changed by an unhashed WAL sidecar.
        target.execute('PRAGMA journal_mode=DELETE')
    finally:
        target.close()
        source.close()
    os.chmod(destination / 'events.db', 0o600)
    refs, details = _inspect_database(destination / 'events.db')
    for digest, ref in refs.items():
        copied = _artifact(destination / 'artifacts', digest)
        _copy(_find(ref, roots), copied)
        _verify_file(copied, digest, ref['size_bytes'])
    # Flush the SQLite copy before declaring the bundle complete.
    with (destination / 'events.db').open('rb') as copied:
        os.fsync(copied.fileno())
    manifest = {'format': 1, 'kind': 'agent-bus-backup', **details,
                'database_sha256': _hash(destination / 'events.db'),
                'artifact_count': len(refs), 'created_at': time.time()}
    _finish(destination, manifest)
    return manifest


def verify_bundle(bundle):
    bundle = Path(bundle)
    if bundle.is_symlink() or (bundle / '.incomplete').exists() or (bundle / '.incomplete').is_symlink():
        raise RecoveryError('bundle is incomplete or symlinked')
    manifest_path = bundle / 'manifest.json'
    if manifest_path.is_symlink() or not manifest_path.is_file():
        raise RecoveryError('bundle completion manifest missing')
    manifest = json.loads(manifest_path.read_text())
    if not isinstance(manifest, dict) or manifest.get('format') != 1 or manifest.get('kind') != 'agent-bus-backup':
        raise RecoveryError('unsupported backup manifest')
    database = bundle / 'events.db'
    for suffix in ('-wal', '-shm', '-journal'):
        sidecar = bundle / ('events.db' + suffix)
        if sidecar.exists() or sidecar.is_symlink():
            raise RecoveryError('backup contains an unexpected SQLite sidecar; preserve it for inspection')
    if database.is_symlink() or _hash(database) != manifest.get('database_sha256'):
        raise RecoveryError('backup database checksum mismatch')
    refs, details = _inspect_database(database)
    if any(manifest.get(key) != value for key, value in details.items()) or manifest.get('artifact_count') != len(refs):
        raise RecoveryError('backup manifest disagrees with database')
    for ref in refs.values():
        _find(ref, [bundle / 'artifacts'])
    return manifest, refs


def restore(bundle, destination):
    bundle = Path(bundle)
    manifest, refs = verify_bundle(bundle)
    destination = _begin(destination)
    _copy(bundle / 'events.db', destination / 'events.db')
    if _hash(destination / 'events.db') != manifest['database_sha256']:
        raise RecoveryError('database changed while restoring')
    for digest, ref in refs.items():
        target = _artifact(destination / 'artifacts', digest)
        _copy(_find(ref, [bundle / 'artifacts']), target)
        _verify_file(target, digest, ref['size_bytes'])
    # Cache/index tables are projections, not backup authority.
    import bus
    previous = bus.DB_PATH
    try:
        bus.DB_PATH = destination / 'events.db'
        with bus.db() as conn:
            for table in ('coordination_snapshot', 'task_identity', 'external_origin_claims', 'task_supersession_claims'):
                conn.execute(f'DROP TABLE IF EXISTS {table}')
        bus.init_db()
    finally:
        bus.DB_PATH = previous
    from projection_store import load_projection
    replayed = load_projection(destination / 'events.db', use_snapshot=False)
    result = {'format': 1, 'kind': 'agent-bus-restore', 'database_id': replayed.database_id,
              'through_id': replayed.last_event_id, 'artifact_count': len(refs)}
    _finish(destination, result)
    return result
