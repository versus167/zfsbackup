#!/usr/bin/python3
# -*- coding: utf-8 -*-
import subprocess,shlex,time

def zeit():
    return time.strftime("%Y-%m-%d %H:%M:%S")

def subrun(command,quiet=False,checkretcode=True,**kwargs):
    '''
    Führt die übergeben Kommandozeile aus und gibt das Ergebnis
    zurück
    '''
    args = shlex.split(command)
    if quiet == False:
        print(zeit(),' '.join(args))
    ret = subprocess.run(args,**kwargs)
    if checkretcode: ret.check_returncode()
    return ret

cmd = 'ssh -T volker@25.196.147.104 zfs get -H receive_resume_token tank/vsb/virtualbox'
ret = subrun(cmd,stdout=subprocess.PIPE,universal_newlines=True,checkretcode=False)
print(ret.stdout)
ergeb = ret.stdout.split('\t')
try:
    a = len(ergeb[2])
except:
    a = 0
if a > 1:
    token = ergeb[2]

cmdfrom = 'zfs send -vt '+token
cmdto = 'ssh -T volker@25.196.147.104 sudo zfs receive -vs tank/vsb/virtualbox'

argsfrom = shlex.split(cmdfrom)
argsto = shlex.split(cmdto)
ps = subprocess.Popen(argsfrom, stdout=subprocess.PIPE,stderr=subprocess.PIPE,universal_newlines=True)
ziel = subprocess.Popen(argsto,stdin=ps.stdout)
for line in ps.stderr:
    print(line,end='')
if ps.returncode != 0:
    raise CalledProcessError(ps.returncode, ps.args)
ziel.wait()
if ziel.returncode != 0:
    raise CalledProcessError(ziel.returncode, ziel.args)
