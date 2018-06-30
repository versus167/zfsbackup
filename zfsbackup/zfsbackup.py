#!/usr/bin/python3
# -*- coding: utf-8 -*-
'''
Created on 28.06.2018

@author: Volker Süß

'''

APPNAME='zfsbackup'
VERSION='1 - 2018-06-28'
SNAPPREFIX = 'zfsbackup'

import subprocess,shlex
import time,os.path,sys

def zeit():
    return time.strftime("%Y-%m-%d %H:%M:%S")
def subrun(command,checkretcode=True,**kwargs):
    '''
    Führt die übergeben Kommandozeile aus und gibt das Ergebnis
    zurück
    '''
    args = shlex.split(command)
    print(zeit(),' '.join(args))
    ret = subprocess.run(args,**kwargs)
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
        self.lastsnap = -1
        pass
    def getlastsnap(self):
        ret = subrun('zfs list -H -t snapshot -o name',stdout=subprocess.PIPE,universal_newlines=True)
        ret.check_returncode()
        if ret.stdout == None:
            self.lastsnap = 0
            return 0
        vgl = self.fs+'@'+SNAPPREFIX+'_'
        l = len(vgl)
        listesnaps = []
        for i in ret.stdout.split('\n'):
            snp = i
            if snp[0:l] == vgl:
                listesnaps.append(int(snp[l:]))
        listesnaps.sort()
        self.lastsnap = listesnaps[-1:][0]
        return self.lastsnap
        pass
    def takenextsnap(self):
        nr = self.getlastsnap()+1
        ret = subrun('zfs snapshot '+self.fs+'@'+SNAPPREFIX+'_'+str(nr))
        ret.check_returncode()
        self.lastsnap = nr
        pass
    def getoldsnap(self):
        pass
    

class zfs_back(object):
    '''
    
    '''


    def __init__(self, params):
        '''
        Constructor
        '''
if __name__ == '__main__':
    a = zfs_fs('tank/orig')
    print(a.getlastsnap())
    a.takenextsnap()