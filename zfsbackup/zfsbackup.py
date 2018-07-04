#!/usr/bin/python3
# -*- coding: utf-8 -*-
'''
Created on 28.06.2018

@author: Volker Süß

Sieht aus als wäre das alles gar kein echtes Problem -> Ist es auch nicht.

Das Problem liegt an einer anderen Stelle: zfs send/receive funzt nur als root. Damit fällt die 
Abschirmung zwischen src- und dst-Filesystem weg, Über rsync ist die Sicherung eine oneway-Geschichte,

die über snapshots vom Zugriff des Senderechners ausgeschlossen war. 

Mist

'''

APPNAME='zfsbackup'
VERSION='1 - 2018-06-28'
SNAPPREFIX = 'zfsbackup'
HOLDSNAPS = 5 # 5 Backupsnapshots werden behalten

import subprocess,shlex
import time,os.path,sys

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

def subrunPIPE(command,checkretcode=True,**kwargs):
    '''
    Führt die übergeben Kommandozeile aus und gibt das Ergebnis
    zurück
    '''
    args = shlex.split(command)
    print(zeit(),' '.join(args))
    ret = subprocess.run(command,shell=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
    if checkretcode: ret.check_returncode()
    return ret

class zfs_fs(object):
    '''
    Alles rund um das Filesystem direkt
    '''
    def __init__(self,fs,connection = None):
        '''
        Welches FS und worüber erreichen wir das
        '''
        self.fs = fs
        self.connection = connection
        self.__getsnaplist()
        pass

    def get_lastsnap(self):
        if len(self.snaplist) == 0:
            return 0
        return self.snaplist[-1:][0]
        return self.__lastsnap

    def __getsnaplist(self):
        self.snaplist = []
        ret = subrun('zfs list -H -t snapshot -o name',quiet=True,stdout=subprocess.PIPE,universal_newlines=True)
        ret.check_returncode()
        if ret.stdout == None:
            return
        vgl = self.fs+'@'+SNAPPREFIX+'_'
        l = len(vgl)
        
        for i in ret.stdout.split('\n'):
            snp = i
            if snp[0:l] == vgl:
                self.snaplist.append(int(snp[l:]))
        if len(self.snaplist) == 0:
            return
        self.snaplist.sort()
        
    def takenextsnap(self):
        nr = self.lastsnap+1
        ret = subrun('zfs snapshot '+self.fs+'@'+SNAPPREFIX+'_'+str(nr))
        ret.check_returncode()
        self.snaplist.append(nr)
        pass
    def getoldsnap(self):
        pass
    def deletesnap(self,nr):
        cmd = 'zfs destroy '+self.fs+'@'+SNAPPREFIX+'_'+str(nr)
        subrun(cmd)
        self.snaplist.remove(nr)
    lastsnap = property(get_lastsnap, None, None, None)
    

class zfs_back(object):
    '''
    Hier findet als der reine Backupablauf seinen Platz
    '''


    def __init__(self, srcfs,dstfs):
        '''
        src und dst anlegen 
        '''
        self.src = zfs_fs(srcfs)
        self.dst = zfs_fs(dstfs)
        print('Lastsnap Source: '+str(self.src.lastsnap))
        print('Lastsnap Destination: '+str(self.dst.lastsnap))
        if self.dst.lastsnap == 0:
            # dann voll senden (erst neuen Snapshot src erstellen
            self.src.takenextsnap()
            cmd = 'zfs send '+self.src.fs+'@'+SNAPPREFIX+'_'+str(self.src.lastsnap)+' | zfs receive '+self.dst.fs
            ret = subrunPIPE(cmd)
            # Schlussbehandlung (überzählige snaps löschen)
            self.cleansnaps()
            pass
        
    def cleansnaps(self):
        '''
        Soll von src und dest alle Snapshots löschen - bis auf die neuesten 5
        '''
        l1 = len(self.src.snaplist)
        if l1 > HOLDSNAPS:
            # dann paar löschen
            for i in self.src.snaplist[0:l1-HOLDSNAPS]:
                self.src.deletesnap(i)
if __name__ == '__main__':
    zfs = zfs_back('vs2016/orig','vs2016/back')
        