#!/usr/bin/python3
# -*- coding: utf-8 -*-
'''

Created on 01.09.2021

@author: Volker Süß

Agiert als Receiver für den zfs receive befehl - damit nicht zu viele Rechte an zfs gegeben werden, muss das so laufen
'''


import re

re.fullmatch(r'^zfs receive -vs [^- ][^ ]*',"zfs receive -vs F-Fsdf/madklö")

pass

