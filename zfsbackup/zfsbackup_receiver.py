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
#re.fullmatch(r'^zfs receive -vs [^- ][^ ]*',"zfs receive -vs F-Fsdf/madklö")

pass

if __name__ == '__main__':
    assert sys.version_info >= (3,5)
    cmdto = "zfs receive -vs vs2016/test"
    argsto = shlex.split(cmdto)
    ziel = subprocess.Popen(argsto, stdin=sys.stdin)
