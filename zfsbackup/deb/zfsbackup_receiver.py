#!/usr/bin/python3
# -*- coding: utf-8 -*-
'''

Created on 01.09.2021

@author: Volker Süß

Agiert als Receiver für den zfs receive befehl - damit nicht zu viele Rechte an zfs gegeben werden, muss das so laufen

Ablauf ->

Jedes Argument einzeln gegen die erlaubten Aufrufe prüfen, Ziel-Dataset gegen die für den
aufrufenden Benutzer (SUDO_USER) in CONFIGFILE freigegebenen Zweige prüfen - und dann argv
unverändert (ohne Shell/shlex) an den subprocess samt stdin übergeben.

Bei zfs receive wird vorher der Kopf des Stroms geprüft: nur einfache Ströme ohne -w/--raw.
Ein roher inkrementeller Strom überträgt ein zfs change-key der Quelle auf das Ziel - der
Sender könnte damit den Schlüssel am Ziel ersetzen. Zusammengesetzte Ströme (-R, -I, -p, ...)
werden ebenfalls abgelehnt, sie können Properties wie mountpoint mitbringen.

Vor zfs receive setzt der Receiver auf dem zugehörigen Config-Eintrag setuid=off devices=off, damit
eingehängte Backups keine setuid-Programme oder Gerätedateien des Senders benutzbar machen. Ist der
Config-Eintrag selbst das neue Ziel, wird das direkt nach dem Empfang gesetzt (receive -u hängt nicht ein).
Gilt am Ziel trotzdem etwas anderes (lokal gesetzt), wird abgelehnt.

CONFIGFILE - je Zeile: <benutzer> <dataset> [<dataset> ...]
    Erlaubt sind die genannten Datasets, alle Kinder und deren Snapshots.
    Nicht aufgeführte Benutzer werden abgelehnt. Die Datei und ihr Verzeichnis müssen
    root gehören und dürfen nur für root schreibbar sein. Vorlage: EXAMPLEFILE

2026.35 - 2026-09-18 Config und Vorlage unter /etc/zfsbackup - postinst verschiebt /etc/zfsbackup_receiver.conf
                     von 2026.34 dorthin - vs.
2026.34 - 2026-09-18 Sicherheitsfix: Umgehung der Musterprüfung per Tab/Anführungszeichen (shlex) beseitigt,
                     Argumente werden einzeln geprüft, Ziele je SUDO_USER beschränkt, Returncode wird weitergereicht.
                     NICHT KOMPATIBEL: ohne /etc/zfsbackup_receiver.conf wird jeder Aufruf abgelehnt,
                     rohe (zfs send -w) und zusammengesetzte Ströme (-R, -I, -p) werden abgelehnt,
                     setuid=off devices=off wird auf den Config-Einträgen gesetzt - vs.
2026.32 - 2026-01-31 Erweiterung um hold release load-key unload-key und alles in den Wrapper eingebettet - vs.
2021.0.1 - 2021-09-02 Soweit einsatzfähig
'''


import os, re, stat, struct, sys, subprocess

APPNAME='zfsbackup_receiver'
VERSION='2026.35 - 2026-09-18'
CONFIGFILE='/etc/zfsbackup/zfsbackup_receiver.conf'
EXAMPLEFILE=CONFIGFILE + '.example'

# Namensbestandteil: kein führendes '-', kein Leerraum, keine Anführungszeichen/Backslashes
_NAME = r'[A-Za-z0-9_][A-Za-z0-9_.:-]*'
DATASET_RE = re.compile(_NAME + r'(?:/' + _NAME + r')*')
SNAPSHOT_RE = re.compile(DATASET_RE.pattern + r'@' + _NAME)
TAG_RE = re.compile(_NAME)

# Kopf eines zfs send-Stroms: drr_type, drr_payloadlen, drr_begin.drr_magic, drr_begin.drr_versioninfo
STREAM_HEADER_LEN = 24
DRR_BEGIN = 0
DMU_BACKUP_MAGIC = 0x2F5BACBAC
DMU_SUBSTREAM = 1
DMU_BACKUP_FEATURE_RAW = 1 << 24
COPY_SIZE = 1 << 20

# gilt für alle Empfangszweige
SAFE_PROPS = {'setuid': 'off', 'devices': 'off'}

# Platzhalter in den erlaubten Aufrufen
DATASET, SNAPSHOT, TAG = object(), object(), object()

ALLOWED_COMMANDS = [
    # zfs receive
    ['zfs', 'receive', '-vsu', DATASET],
    ['zfs', 'receive', '-vsu', '-o', 'compression=lz4', '-o', 'rdonly=on', DATASET],

    # zfs hold / release
    ['zfs', 'hold', TAG, SNAPSHOT],
    ['zfs', 'hold', '-r', TAG, SNAPSHOT],
    ['zfs', 'release', TAG, SNAPSHOT],
    ['zfs', 'release', '-r', TAG, SNAPSHOT],

    # zfs load-key / unload-key <filesystem>
    ['zfs', 'load-key', DATASET],
    ['zfs', 'unload-key', DATASET],
]


class NotAllowed(Exception):
    pass


def match_command(args, template):
    ''' Gibt die Dataset-Namen (ohne @snap) zurück, falls args zum template passt, sonst None '''
    if len(args) != len(template):
        return None
    datasets = []
    for arg, t in zip(args, template):
        if t is DATASET:
            if not DATASET_RE.fullmatch(arg):
                return None
            datasets.append(arg)
        elif t is SNAPSHOT:
            if not SNAPSHOT_RE.fullmatch(arg):
                return None
            datasets.append(arg.split('@', 1)[0])
        elif t is TAG:
            if not TAG_RE.fullmatch(arg):
                return None
        elif arg != t:
            return None
    return datasets


def is_below(dataset, prefix):
    return dataset == prefix or dataset.startswith(prefix + '/')


def check_args(args, user, config):
    ''' Wirft NotAllowed, falls args für user nicht erlaubt sind '''
    for template in ALLOWED_COMMANDS:
        datasets = match_command(args, template)
        if datasets is not None:
            break
    else:
        raise NotAllowed('Kommando nicht erlaubt: ' + repr(args))

    prefixes = config.get(user)
    if not prefixes:
        raise NotAllowed('Benutzer nicht freigegeben: ' + repr(user))
    for ds in datasets:
        if not any(is_below(ds, p) for p in prefixes):
            raise NotAllowed(f'Dataset {ds} ist für {user} nicht freigegeben')


def check_stream_header(header):
    ''' Wirft NotAllowed, falls header nicht der Anfang eines einfachen, nicht-rohen zfs send-Stroms ist '''
    if len(header) < STREAM_HEADER_LEN:
        raise NotAllowed('Kein zfs send-Strom auf stdin')
    for order in '<>':
        drr_type, _, magic, versioninfo = struct.unpack(order + 'IIQQ', header[:STREAM_HEADER_LEN])
        if magic == DMU_BACKUP_MAGIC:
            break
    else:
        raise NotAllowed('Kein zfs send-Strom auf stdin')
    if drr_type != DRR_BEGIN or versioninfo & 0x3 != DMU_SUBSTREAM:
        raise NotAllowed('Nur einfache zfs send-Ströme erlaubt (kein -R, -I, -p)')
    if (versioninfo >> 2) & DMU_BACKUP_FEATURE_RAW:
        raise NotAllowed('Rohe Ströme (zfs send -w) sind nicht erlaubt')


def read_exact(fd, n):
    buf = b''
    while len(buf) < n:
        chunk = os.read(fd, n - len(buf))
        if not chunk:
            break
        buf += chunk
    return buf


def receive(args, infd=0):
    ''' Prüft den Strom auf infd und reicht ihn an zfs receive weiter - Rückgabe: Returncode '''
    header = read_exact(infd, STREAM_HEADER_LEN)
    check_stream_header(header)
    proc = subprocess.Popen(args, stdin=subprocess.PIPE)
    broken = False
    try:
        data = header
        while data:
            proc.stdin.write(data)
            data = os.read(infd, COPY_SIZE)
        proc.stdin.close()
    except BrokenPipeError:
        broken = True
        try:
            proc.stdin.close()
        except BrokenPipeError:
            pass
    ret = proc.wait()
    # zfs receive liest nur den ersten Strom, Reste dahinter ignoriert es. Auffallen kann das nur,
    # wenn der Rest nicht mehr in den Pipe-Puffer passt - reine Diagnose, keine Sicherheitsprüfung.
    if broken and ret == 0:
        print(f'{APPNAME}: zfs receive hat nicht den ganzen Strom gelesen', file=sys.stderr)
        return 1
    return ret


def zfs(*args):
    return subprocess.run(('zfs',) + args, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                          universal_newlines=True, env=dict(os.environ, LC_ALL='C'))


def zfs_props(ds):
    ''' {type, setuid, devices} von ds - None, falls ds nicht existiert '''
    r = zfs('get', '-H', '-p', '-o', 'property,value', ','.join(['type'] + list(SAFE_PROPS)), ds)
    if r.returncode != 0:
        if 'does not exist' in r.stderr:
            return None
        raise NotAllowed(f'zfs get auf {ds} fehlgeschlagen: {r.stderr.strip()}')
    return dict(line.split('\t', 1) for line in r.stdout.splitlines())


def unsafe_props(props):
    ''' Die Eigenschaften, die nicht SAFE_PROPS entsprechen - Volumes haben keine '''
    if props['type'] != 'filesystem':
        return []
    return [k for k, v in SAFE_PROPS.items() if props[k] != v]


def ensure_safe(ds):
    ''' Setzt SAFE_PROPS auf ds, falls es existiert und ein Dateisystem ist '''
    props = zfs_props(ds)
    if props is None:
        return
    bad = [f'{k}={SAFE_PROPS[k]}' for k in unsafe_props(props)]
    if bad:
        r = zfs('set', *bad, ds)
        if r.returncode != 0:
            raise NotAllowed(f'zfs set {" ".join(bad)} {ds} fehlgeschlagen: {r.stderr.strip()}')
        print(f'{APPNAME}: {" ".join(bad)} auf {ds} gesetzt', file=sys.stderr)


def check_safe(target, root):
    ''' Am Ziel müssen SAFE_PROPS gelten - ein neues Ziel erbt vom Eltern-Dataset '''
    ds = target
    props = zfs_props(ds)
    if props is None:
        if target == root:
            return      # wird nach dem Empfang gesetzt
        ds = target.rsplit('/', 1)[0]
        props = zfs_props(ds)
        if props is None:
            return      # ohne Eltern-Dataset scheitert zfs receive ohnehin
    bad = unsafe_props(props)
    if bad:
        raise NotAllowed(f'{",".join(bad)} ist auf {ds} nicht off (lokal gesetzt?) - '
                         f'prüfen mit: zfs get -r {",".join(bad)} {root}')


def receive_safe(args, prefixes):
    ''' zfs receive mit SAFE_PROPS auf dem Config-Eintrag und am Ziel '''
    target = args[-1]
    root = max((p for p in prefixes if is_below(target, p)), key=len)
    ensure_safe(root)
    check_safe(target, root)
    ret = receive(args)
    if ret == 0 and target == root:
        ensure_safe(target)
    return ret


def parse_config(text):
    ''' {benutzer: [dataset, ...]} '''
    config = {}
    for nr, line in enumerate(text.splitlines(), 1):
        line = line.split('#', 1)[0].split()
        if not line:
            continue
        if len(line) < 2:
            raise NotAllowed(f'{CONFIGFILE} Zeile {nr}: Benutzer ohne Dataset')
        for ds in line[1:]:
            if not DATASET_RE.fullmatch(ds):
                raise NotAllowed(f'{CONFIGFILE} Zeile {nr}: ungültiges Dataset {ds!r}')
        config.setdefault(line[0], []).extend(line[1:])
    return config


def root_only(st, path):
    if st.st_uid != 0 or st.st_mode & (stat.S_IWGRP | stat.S_IWOTH):
        raise NotAllowed(f'{path} muss root gehören und darf nur für root schreibbar sein')


def read_config(path=CONFIGFILE):
    try:
        # auch das Verzeichnis - sonst ließe sich die Datei darin austauschen
        root_only(os.stat(os.path.dirname(path)), os.path.dirname(path))
        with open(path, encoding='utf-8') as f:
            root_only(os.fstat(f.fileno()), path)
            return parse_config(f.read())
    except FileNotFoundError:
        raise NotAllowed(f'{path} fehlt - Vorlage: {EXAMPLEFILE}')
    except OSError as e:
        raise NotAllowed(f'{path} nicht lesbar: {e}')


def main():
    assert sys.version_info >= (3, 6)
    args = sys.argv[1:]
    if not args:
        print("Kein Kommando übergeben.", file=sys.stderr)
        sys.exit(1)

    try:
        user = os.environ.get('SUDO_USER')
        config = read_config()
        check_args(args, user, config)
        if args[1] == 'receive':
            sys.exit(receive_safe(args, config[user]))
    except NotAllowed as e:
        print(f'{APPNAME}: {e}', file=sys.stderr)
        sys.exit(1)

    # Für load-key brauchen wir stdin durchgereicht, für hold/release ist das egal.
    ret = subprocess.run(args, stdin=sys.stdin)
    sys.exit(ret.returncode)


if __name__ == '__main__':
    main()
