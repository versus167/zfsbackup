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
        erg = ret.stdout.split('\n')
        for i in erg:
            t1 = i.split('\t')
            if len(t1) < 2:
                return False
            if t1[1] == 'test':
                return True
        return False
        
         
if __name__ == '__main__':
    
    print(is_snap_hold('vs2016/archiv/picbase@zfsnappy_2021-08-05T14:17:04.678546'))   
