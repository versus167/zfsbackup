#!/usr/bin/python3
# -*- coding: utf-8 -*-
'''

Created on 01.09.2021

@author: Volker Süß

Agiert als Receiver für den zfs receive befehl - damit nicht zu viele Rechte an zfs gegeben werden, muss das so laufen

Ablauf scheint klar ->

Argument mit re checken - und dann an den subprocess samt stdin übergeben - fertig
'''


import re, sys, shlex, subprocess

APPNAME='zfsbackup_receiver'
VERSION='2021.0.1 - 2021-09-02'

pass

if __name__ == '__main__':
    assert sys.version_info >= (3,5)
    cmd = ' '.join(sys.argv[1:])
    if re.fullmatch(r'^zfs receive -vs [^- ][^ ]*',cmd) == None:
        if re.fullmatch(r'^zfs receive -vs -o compression=lz4 -o rdonly=on [^- ][^ ]*',cmd) == None:
            print("Kommando nicht erlaubt: ",cmd)
            exit(1)
    args = shlex.split(cmd)
    ziel = subprocess.run(args, stdin=sys.stdin)
