#!/usr/bin/python3
# -*- coding: utf-8 -*-
'''
Tests für deb/zfsbackup_receiver.py - Aufruf: python3 -m unittest test_zfsbackup_receiver
'''

import importlib.util, os, struct, subprocess, sys, threading, unittest
from unittest import mock

sys.dont_write_bytecode = True
_spec = importlib.util.spec_from_file_location(
    'zfsbackup_receiver', os.path.join(os.path.dirname(os.path.abspath(__file__)), 'deb', 'zfsbackup_receiver.py'))
recv = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(recv)

CONFIG = recv.parse_config('''
# Kommentar
lxc_back  tank/backup tank/backup_nd tank/backup_nb
vsb       tank/vsb
user      tank/data/vm-mail
''')

SNAP = 'zfsnappy_2026-09-18T10-00-00-123456'


class Allowed(unittest.TestCase):
    ''' So ruft zfsbackup.py den Receiver auf '''

    def ok(self, user, *args):
        recv.check_args(list(args), user, CONFIG)

    def test_receive(self):
        self.ok('lxc_back', 'zfs', 'receive', '-vsu', 'tank/backup/vm/vm-100-disk-0')
        self.ok('lxc_back', 'zfs', 'receive', '-vsu', '-o', 'compression=lz4', '-o', 'rdonly=on', 'tank/backup_nb/vm_nb')
        self.ok('lxc_back', 'zfs', 'receive', '-vsu', 'tank/backup')

    def test_hold_release(self):
        self.ok('lxc_back', 'zfs', 'hold', 'keep', 'tank/backup/vm@' + SNAP)
        self.ok('lxc_back', 'zfs', 'release', '-r', 'keep', 'tank/backup_nd/vm_nd@' + SNAP)

    def test_keys(self):
        self.ok('vsb', 'zfs', 'load-key', 'tank/vsb')
        self.ok('vsb', 'zfs', 'unload-key', 'tank/vsb')


class Rejected(unittest.TestCase):

    def bad(self, user, *args):
        with self.assertRaises(recv.NotAllowed):
            recv.check_args(list(args), user, CONFIG)

    # Die vier Umgehungsfälle aus dem Befund vom 2026-09-18
    def test_tab_force(self):
        self.bad('user', 'zfs', 'receive', '-vsu', '\t-F\ttank/data/vm-mail')

    def test_quoted_force(self):
        self.bad('user', 'zfs', 'receive', '-vsu', '"-F"\ttank/data/vm-mail')

    def test_tab_mountpoint(self):
        self.bad('user', 'zfs', 'receive', '-vsu', '\t-o\tmountpoint=/etc/cron.d\ttank/neu')

    def test_load_key_all(self):
        self.bad('vsb', 'zfs', 'load-key', '-a')
        self.bad('vsb', 'zfs', 'unload-key', '-a')
        self.bad('lxc_back', 'zfs', 'hold', '-a', 'tank/backup@' + SNAP)
        self.bad('lxc_back', 'zfs', 'release', 'keep', '-r')

    # weitere Varianten
    def test_whitespace_quotes_backslash(self):
        for ds in ('tank/data/vm-mail\n', 'tank/data/vm-mail ', "'tank/data/vm-mail'",
                   'tank/data/vm\\-mail', 'tank/data/vm-mail\x0b', ''):
            self.bad('user', 'zfs', 'receive', '-vsu', ds)

    def test_extra_options(self):
        self.bad('user', 'zfs', 'receive', '-vsu', '-F', 'tank/data/vm-mail')
        self.bad('user', 'zfs', 'receive', '-vsuF', 'tank/data/vm-mail')
        self.bad('user', 'zfs', 'receive', '-vsu', '-o', 'mountpoint=/etc', 'tank/data/vm-mail')
        self.bad('user', 'zfs', 'receive', '-vsu', 'tank/data/vm-mail', 'tank/data/vm-mail')
        self.bad('user', 'zfs', 'destroy', 'tank/data/vm-mail')

    def test_foreign_dataset(self):
        self.bad('user', 'zfs', 'receive', '-vsu', 'tank/backup/vm')
        self.bad('user', 'zfs', 'receive', '-vsu', 'tank/data/vm-mail2')   # kein Präfix-Treffer ohne '/'
        self.bad('user', 'zfs', 'receive', '-vsu', 'tank/data')
        self.bad('lxc_back', 'zfs', 'hold', 'keep', 'tank/vsb@' + SNAP)
        self.bad('lxc_back', 'zfs', 'receive', '-vsu', 'tank/backup_vb/pc1')

    def test_malformed_names(self):
        self.bad('lxc_back', 'zfs', 'receive', '-vsu', 'tank/backup/')
        self.bad('lxc_back', 'zfs', 'receive', '-vsu', 'tank/backup//x')
        self.bad('lxc_back', 'zfs', 'receive', '-vsu', 'tank/backup@' + SNAP)
        self.bad('lxc_back', 'zfs', 'hold', 'keep', 'tank/backup')

    def test_unknown_user(self):
        self.bad(None, 'zfs', 'receive', '-vsu', 'tank/backup/vm')
        self.bad('vb_back', 'zfs', 'receive', '-vsu', 'tank/backup/vm')


def header(order='<', features=0x420004, hdrtype=1, drr_type=0, magic=0x2F5BACBAC):
    return struct.pack(order + 'IIQQ', drr_type, 0, magic, (features << 2) | hdrtype) + b'\0' * 288


RAW = 1 << 24


class StreamHeader(unittest.TestCase):

    def test_plain(self):
        recv.check_stream_header(header())
        recv.check_stream_header(header('>'))

    def bad(self, h):
        with self.assertRaises(recv.NotAllowed):
            recv.check_stream_header(h)

    def test_raw(self):
        self.bad(header(features=0x420004 | RAW))
        self.bad(header('>', features=RAW))

    def test_compound(self):
        # zfs send -R / -I / -p
        self.bad(header(hdrtype=2))

    def test_no_stream(self):
        self.bad(b'')
        self.bad(header()[:23])
        self.bad(header(magic=0x1234))
        self.bad(header(drr_type=1))
        self.bad(b'\0' * 312)


class Receive(unittest.TestCase):
    ''' Weitergabe von stdin an zfs receive - hier mit Ersatzkommandos '''

    def run_with(self, cmd, data):
        r, w = os.pipe()
        def feed():
            try:
                with os.fdopen(w, 'wb') as f:
                    f.write(data)
            except BrokenPipeError:
                pass
        t = threading.Thread(target=feed)
        t.start()
        try:
            return recv.receive(cmd, infd=r)
        finally:
            os.close(r)     # vor join, sonst hängt feed() bei ungelesenen Daten
            t.join()

    def test_passes_stream_unchanged(self):
        data = header() + os.urandom(3 * recv.COPY_SIZE + 17)
        out = os.path.join(os.path.dirname(os.path.abspath(__file__)), '__pycache__', 'recv_test.out')
        os.makedirs(os.path.dirname(out), exist_ok=True)
        try:
            self.assertEqual(self.run_with(['sh', '-c', 'cat > "$0"', out], data), 0)
            with open(out, 'rb') as f:
                self.assertEqual(f.read(), data)
        finally:
            os.remove(out)

    def test_returncode(self):
        self.assertEqual(self.run_with(['sh', '-c', 'cat >/dev/null; exit 3'], header()), 3)

    def test_unread_rest_is_error(self):
        self.assertNotEqual(self.run_with(['sh', '-c', 'head -c 100 >/dev/null'], header() + b'x' * (4 * recv.COPY_SIZE)), 0)

    def test_raw_rejected_before_start(self):
        with self.assertRaises(recv.NotAllowed):
            self.run_with(['false'], header(features=RAW) + b'x' * 1000)


class FakeZfs:
    ''' Ersatz für zfs get/set: nur lokal gesetzte Werte, Vererbung über die Eltern, Default on '''

    def __init__(self, local):
        self.local = local      # {dataset: {'type': ..., eigenschaft: lokaler Wert}}
        self.sets = []

    def value(self, ds, prop):
        if prop == 'type':
            return self.local[ds]['type']
        if self.local[ds]['type'] != 'filesystem':
            return '-'
        while ds:
            if prop in self.local.get(ds, {}):
                return self.local[ds][prop]
            ds = ds.rpartition('/')[0]
        return 'on'

    def __call__(self, *args):
        ds = args[-1]
        if ds not in self.local:
            return subprocess.CompletedProcess(args, 1, '', f"cannot open '{ds}': dataset does not exist\n")
        if args[0] == 'get':
            out = ''.join(f'{p}\t{self.value(ds, p)}\n' for p in args[-2].split(','))
            return subprocess.CompletedProcess(args, 0, out, '')
        if args[0] == 'set':
            self.sets.append(args[1:])
            for kv in args[1:-1]:
                k, v = kv.split('=')
                self.local[ds][k] = v
            return subprocess.CompletedProcess(args, 0, '', '')
        raise AssertionError(args)


FS = {'type': 'filesystem'}


class SafeProps(unittest.TestCase):
    ''' setuid=off devices=off auf den Config-Einträgen '''

    def run_receive(self, local, target, prefixes, ret=0, new_type='filesystem'):
        fake = FakeZfs(local)
        received = []
        def fake_receive(args):
            received.append(args[-1])
            if ret == 0 and target not in fake.local:
                fake.local[target] = {'type': new_type}
            return ret
        with mock.patch.object(recv, 'zfs', fake), mock.patch.object(recv, 'receive', fake_receive), \
             mock.patch('sys.stderr'):
            r = recv.receive_safe(['zfs', 'receive', '-vsu', target], prefixes)
        return r, fake, received

    def test_sets_on_config_root(self):
        r, fake, received = self.run_receive({'tank': dict(FS), 'tank/backup': dict(FS)},
                                             'tank/backup/vm', ['tank/backup'])
        self.assertEqual((r, received), (0, ['tank/backup/vm']))
        self.assertEqual(fake.sets, [('setuid=off', 'devices=off', 'tank/backup')])
        self.assertEqual(fake.value('tank/backup/vm', 'setuid'), 'off')

    def test_already_safe(self):
        r, fake, _ = self.run_receive({'tank': dict(FS), 'tank/backup': dict(FS, setuid='off', devices='off'),
                                       'tank/backup/vm': dict(FS)}, 'tank/backup/vm', ['tank/backup'])
        self.assertEqual((r, fake.sets), (0, []))

    def test_only_missing_prop(self):
        _, fake, _ = self.run_receive({'tank': dict(FS), 'tank/backup': dict(FS, setuid='off')},
                                      'tank/backup/vm', ['tank/backup'])
        self.assertEqual(fake.sets, [('devices=off', 'tank/backup')])

    def test_most_specific_root(self):
        _, fake, _ = self.run_receive({'tank': dict(FS), 'tank/b': dict(FS), 'tank/b/vm': dict(FS)},
                                      'tank/b/vm/x', ['tank/b', 'tank/b/vm'])
        self.assertEqual(fake.sets, [('setuid=off', 'devices=off', 'tank/b/vm')])

    def test_new_root_set_after_receive(self):
        # Config-Eintrag ist selbst das Ziel und existiert noch nicht - Eltern-Dataset bleibt unberührt
        r, fake, received = self.run_receive({'tank': dict(FS), 'tank/data': dict(FS)},
                                             'tank/data/vm-mail', ['tank/data/vm-mail'])
        self.assertEqual((r, received), (0, ['tank/data/vm-mail']))
        self.assertEqual(fake.sets, [('setuid=off', 'devices=off', 'tank/data/vm-mail')])
        self.assertEqual(fake.value('tank/data', 'setuid'), 'on')

    def test_new_root_failed_receive(self):
        r, fake, _ = self.run_receive({'tank': dict(FS), 'tank/data': dict(FS)},
                                      'tank/data/vm-mail', ['tank/data/vm-mail'], ret=1)
        self.assertEqual((r, fake.sets), (1, []))

    def test_local_override_below_root_rejected(self):
        local = {'tank': dict(FS), 'tank/backup': dict(FS), 'tank/backup/pc1': dict(FS, setuid='on')}
        with self.assertRaises(recv.NotAllowed):
            self.run_receive(local, 'tank/backup/pc1/home', ['tank/backup'])
        with self.assertRaises(recv.NotAllowed):
            self.run_receive(dict(local, **{'tank/backup/pc1/home': dict(FS)}), 'tank/backup/pc1/home', ['tank/backup'])

    def test_rejected_before_receive(self):
        fake = FakeZfs({'tank': dict(FS), 'tank/backup': dict(FS), 'tank/backup/pc1': dict(FS, devices='on')})
        with mock.patch.object(recv, 'zfs', fake), mock.patch.object(recv, 'receive') as rcv, \
             mock.patch('sys.stderr'):
            with self.assertRaises(recv.NotAllowed):
                recv.receive_safe(['zfs', 'receive', '-vsu', 'tank/backup/pc1'], ['tank/backup'])
        rcv.assert_not_called()

    def test_volumes(self):
        # zvol als Ziel und als Config-Eintrag: keine setuid/devices-Eigenschaften
        r, fake, _ = self.run_receive({'tank': dict(FS), 'tank/backup': dict(FS, setuid='off', devices='off'),
                                       'tank/backup/vm-100-disk-0': {'type': 'volume'}},
                                      'tank/backup/vm-100-disk-0', ['tank/backup'])
        self.assertEqual((r, fake.sets), (0, []))
        r, fake, _ = self.run_receive({'tank': dict(FS), 'tank/data': dict(FS)},
                                      'tank/data/disk', ['tank/data/disk'], new_type='volume')
        self.assertEqual((r, fake.sets), (0, []))

    def test_zfs_error(self):
        fake = mock.Mock(return_value=subprocess.CompletedProcess((), 1, '', 'permission denied'))
        with mock.patch.object(recv, 'zfs', fake):
            with self.assertRaises(recv.NotAllowed):
                recv.zfs_props('tank/backup')


class Config(unittest.TestCase):

    def test_invalid(self):
        for text in ('lxc_back', 'lxc_back -tank', 'lxc_back tank/x/'):
            with self.assertRaises(recv.NotAllowed):
                recv.parse_config(text)

    def test_merge_lines(self):
        self.assertEqual(recv.parse_config('a x\na y # z\n'), {'a': ['x', 'y']})

    def test_shipped_example(self):
        # die ausgelieferte Vorlage muss gültig sein und darf niemanden freigeben
        with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'deb', 'zfsbackup_receiver.conf.example'), encoding='utf-8') as f:
            self.assertEqual(recv.parse_config(f.read()), {})

    def test_paths(self):
        self.assertEqual(recv.CONFIGFILE, '/etc/zfsbackup/zfsbackup_receiver.conf')
        self.assertEqual(recv.EXAMPLEFILE, '/etc/zfsbackup/zfsbackup_receiver.conf.example')

    def test_missing_names_example(self):
        with self.assertRaisesRegex(recv.NotAllowed, 'fehlt - Vorlage: /etc/zfsbackup/zfsbackup_receiver.conf.example'):
            recv.read_config('/nicht/vorhanden/zfsbackup_receiver.conf')

    @unittest.skipIf(os.geteuid() == 0, 'als root gehört die Testdatei root')
    def test_not_root_owned(self):
        import tempfile
        with tempfile.TemporaryDirectory() as d:
            p = os.path.join(d, 'zfsbackup_receiver.conf')
            with open(p, 'w') as f:
                f.write('a tank/x\n')
            with self.assertRaisesRegex(recv.NotAllowed, 'muss root gehören'):
                recv.read_config(p)


if __name__ == '__main__':
    unittest.main()
