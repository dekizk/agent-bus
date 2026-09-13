import io
import json
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import bus
from artifacts import ArtifactStore
from agent_bus_cli import main
from projection_store import load_projection, save_projection
from recovery import RecoveryError, audit, backup, restore, verify_bundle


class RecoveryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.previous = bus.DB_PATH
        self.database = bus.DB_PATH = self.root / 'events.db'
        bus.init_db()
        self.store = ArtifactStore(self.root / 'blobs')
        self.ref = self.store.put_text('must survive recovery', kind='output')
        self.task = bus.append_event('task.created', 'human', {'title': 'recovery fixture'})
        bus.append_event('custom.result', 'test', {'nested': [{'artifact': self.ref}]})
        self.bundle = self.root / 'backup'

    def tearDown(self):
        bus.DB_PATH = self.previous
        self.temp.cleanup()

    def test_audit_protects_nested_refs_and_never_deletes_orphans(self):
        orphan = self.store.put_text('old unattached', kind='input')
        fresh = self.store.put_text('new pending publication', kind='input')
        path = self.store.path_for(orphan)
        os.utime(path, (1, 1))
        report = audit(self.database, [self.store.root])
        self.assertTrue(report['ok'])
        self.assertEqual(1, report['referenced_artifacts'])
        self.assertEqual([str(path)], [r['path'] for r in report['unreferenced_candidates']])
        self.assertFalse(report['deletion_performed'])
        self.assertTrue(path.exists())
        self.assertTrue(self.store.path_for(fresh).exists())

    def test_backup_restore_preserve_events_and_rebuild_disposable_state(self):
        before = bus.fetch_after(0, None)
        expected = load_projection(self.database, use_snapshot=False)
        save_projection(self.database, expected)
        manifest = backup(self.database, self.bundle, [self.store.root])
        self.assertEqual(1, manifest['artifact_count'])
        verify_bundle(self.bundle)
        restored = self.root / 'restored'
        restore(self.bundle, restored)
        replayed = load_projection(restored / 'events.db')
        self.assertEqual(vars(expected.state), vars(replayed.state))
        self.assertIsNone(replayed.snapshot_event_id)
        with sqlite3.connect(restored / 'events.db') as conn:
            self.assertEqual([(1, self.task['id'])], list(conn.execute('SELECT * FROM task_identity')))
            restored_events = list(conn.execute('SELECT * FROM events ORDER BY id'))
        with sqlite3.connect(self.database) as conn:
            self.assertEqual(list(conn.execute('SELECT * FROM events ORDER BY id')), restored_events)
        restored_store = ArtifactStore(restored / 'artifacts')
        self.assertEqual(b'must survive recovery', restored_store.get_bytes(self.ref))
        self.assertEqual(before, bus.fetch_after(0, None))
        for file in (self.bundle / 'events.db', self.bundle / 'manifest.json'):
            self.assertEqual(0o600, file.stat().st_mode & 0o777)

    def test_missing_and_corrupt_artifacts_fail_closed(self):
        self.assertFalse(audit(self.database, [])['ok'])
        with self.assertRaises(RecoveryError):
            backup(self.database, self.bundle, [])
        self.assertTrue((self.bundle / '.incomplete').exists())
        with self.assertRaises(RecoveryError):
            verify_bundle(self.bundle)
        self.store.path_for(self.ref).write_bytes(b'corrupt')
        self.assertFalse(audit(self.database, [self.store.root])['ok'])

    def test_no_existing_destination_is_overwritten(self):
        backup(self.database, self.bundle, [self.store.root])
        with self.assertRaises(FileExistsError):
            backup(self.database, self.bundle, [self.store.root])
        destination = self.root / 'existing'
        destination.mkdir()
        with self.assertRaises(FileExistsError):
            restore(self.bundle, destination)
        self.assertEqual([], list(destination.iterdir()))

    def test_database_tampering_refused_before_restore_destination_created(self):
        backup(self.database, self.bundle, [self.store.root])
        with (self.bundle / 'events.db').open('ab') as target:
            target.write(b'tampered')
        destination = self.root / 'refused'
        with self.assertRaisesRegex(RecoveryError, 'checksum'):
            restore(self.bundle, destination)
        self.assertFalse(destination.exists())

    def test_bundle_is_standalone_and_unhashed_sidecars_are_rejected(self):
        backup(self.database, self.bundle, [self.store.root])
        self.assertFalse((self.bundle / 'events.db-wal').exists())
        verify_bundle(self.bundle)
        self.assertFalse((self.bundle / 'events.db-wal').exists())
        (self.bundle / 'events.db-wal').write_bytes(b'unexpected history')
        with self.assertRaisesRegex(RecoveryError, 'sidecar'):
            verify_bundle(self.bundle)

    def test_artifact_store_rejects_symlinked_hash_directory(self):
        root = self.root / 'unsafe-store'
        root.mkdir()
        (root / 'sha256').symlink_to(self.store.root / 'sha256', target_is_directory=True)
        with self.assertRaises(ValueError):
            ArtifactStore(root).get_bytes(self.ref)

    def test_artifact_tampering_and_symlinks_are_refused(self):
        backup(self.database, self.bundle, [self.store.root])
        path = self.bundle / 'artifacts' / 'sha256' / self.ref['sha256'][:2] / self.ref['sha256']
        path.unlink()
        path.symlink_to(self.store.path_for(self.ref))
        with self.assertRaisesRegex(RecoveryError, 'symlink'):
            verify_bundle(self.bundle)

    def test_bad_history_is_not_misclassified_as_unreferenced(self):
        bus.append_event('custom.ambiguous', 'test', {'sha256': self.ref['sha256']})
        with self.assertRaisesRegex(RecoveryError, 'event 3'):
            audit(self.database, [self.store.root])
        with self.assertRaises(RecoveryError):
            backup(self.database, self.bundle, [self.store.root])

    def test_interrupted_copy_leaves_unusable_marked_bundle(self):
        with patch('recovery._copy', side_effect=OSError('injected disk failure')):
            with self.assertRaises(OSError):
                backup(self.database, self.bundle, [self.store.root])
        self.assertTrue((self.bundle / '.incomplete').exists())
        with self.assertRaises(RecoveryError):
            restore(self.bundle, self.root / 'restore')

    def test_sqlite_backup_excludes_an_uncommitted_writer(self):
        with bus.db() as writer:
            writer.execute('BEGIN IMMEDIATE')
            writer.execute("INSERT INTO events(ts,topic,actor,payload) VALUES (1,'custom.uncommitted','test','{}')")
            manifest = backup(self.database, self.bundle, [self.store.root])
            self.assertEqual(2, manifest['event_count'])
            writer.rollback()
        verify_bundle(self.bundle)

    def test_cli_verification_and_restore_need_no_existing_config(self):
        backup(self.database, self.bundle, [self.store.root])
        for args in (['verify-backup', str(self.bundle)],
                     ['restore', str(self.bundle), str(self.root / 'restored')]):
            output = io.StringIO()
            self.assertEqual(0, main(args + ['--json'], stdout=output))
            self.assertTrue(json.loads(output.getvalue())['ok'])


if __name__ == '__main__':
    unittest.main()
