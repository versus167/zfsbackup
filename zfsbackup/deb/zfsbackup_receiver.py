#!/usr/bin/python3
# -*- coding: utf-8 -*-
'''

Created on 01.09.2021

@author: Volker Süß

Agiert als Receiver für den zfs receive befehl - damit nicht zu viele Rechte an zfs gegeben werden, muss das so laufen

Ablauf scheint klar ->

Argument mit re checken - und dann an den subprocess samt stdin übergeben - fertig

2026.32 - 2026-01-31 Erweiterung um hold release load-key unload-key und alles in den Wrapper eingebettet - vs.
2021.0.1 - 2021-09-02 Soweit einsatzfähig
'''


import re, sys, shlex, subprocess

APPNAME='zfsbackup_receiver'
VERSION='2026.32 - 2026-01-31'

ALLOWED_PATTERNS = [
    # zfs receive (wie bisher)
    r'^zfs receive -vsu [^- ][^ ]*$',
    r'^zfs receive -vsu -o compression=lz4 -o rdonly=on [^- ][^ ]*$',

    # zfs hold / release
    r'^zfs hold( -r)? [^ ]+ [^ ]+$',
    r'^zfs release( -r)? [^ ]+ [^ ]+$',

    # zfs load-key <filesystem>
    r'^zfs load-key [^ ]+$',

    # zfs unload-key <filesystem>
    r'^zfs unload-key [^ ]+$',
]

def is_allowed(cmd: str) -> bool:
    for pattern in ALLOWED_PATTERNS:
        if re.fullmatch(pattern, cmd):
            return True
    return False

def main():
    assert sys.version_info >= (3, 5)
    if len(sys.argv) < 2:
        print("Kein Kommando übergeben.")
        sys.exit(1)

    cmd = ' '.join(sys.argv[1:])

    if not is_allowed(cmd):
        print("Kommando nicht erlaubt:", cmd)
        sys.exit(1)

    args = shlex.split(cmd)
    # Für zfs receive brauchen wir stdin durchgereicht, für hold/release ist das egal.
    subprocess.run(args, stdin=sys.stdin)


if __name__ == '__main__':
    main()
