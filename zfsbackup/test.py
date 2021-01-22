#!/usr/bin/python3
# -*- coding: utf-8 -*-

import subprocess,shlex

def subrun(command,checkretcode=True,**kwargs):
    '''
    Führt die übergeben Kommandozeile aus und gibt das Ergebnis
    zurück
    '''
    #log = logging.getLogger(LOGNAME)
    args = shlex.split(command)
    #log.debug(' '.join(args))
    ret = subprocess.run(args,**kwargs)
    if checkretcode: ret.check_returncode()
    return ret

def is_snap_hold(snapshotname):
        ''' Return true, wenn schon auf hold '''
        cmd = 'zfs holds -H '+snapshotname
        ret = subrun(cmd,capture_output=True,text=True)
        erg = ret.stdout.split()
        if len(erg) < 2:
            return False
        if erg[1] == 'keep':
            return True
        return False
        
         
if __name__ == '__main__':
    
    print(is_snap_hold('vs2016/bk/vs@zfsnappy_2021-01-21T20:42:01.44586'))   
