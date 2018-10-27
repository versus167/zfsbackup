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

-> Man kann aber ja imho auch nur bestimmte Kommandos eines Befehls auf suod machen lassen (ohne alle Rechte zu 
haben). Das wäre vlt. die Möglichkeit, das ganze doch noch zu nutzen. Wenn ich am Ziel weder FS noch snapshots 
löschen kann, dann besteht eigentlich auch kein Risiko? 26/10/18  

'''
from future.backports.test.support import captured_output

APPNAME='zfsbackup'
VERSION='1 - 2018-06-28'
SNAPPREFIX = 'zfsbackup'
HOLDSNAPS = 5 # 5 Backupsnapshots werden behalten

import subprocess,shlex, argparse
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

def subrunPIPE(cmdfrom,cmdto,checkretcode=True,**kwargs):
    '''
    Führt die übergeben Kommandozeile aus und gibt das Ergebnis
    zurück
    '''
    args = shlex.split(cmdfrom)
    print(zeit(),' '.join(args))
    #ret = subprocess.run(command,shell=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
    #print(ret.stdout)
    #if checkretcode: ret.check_returncode()
    argsto = shlex.split(cmdto)
    print(zeit(),'pipe to -> ',' '.join(argsto))
    ps = subprocess.Popen(args, stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    argsto = shlex.split(cmdto)
    output = subprocess.check_output(argsto, stdin=ps.stdout)
    ps.wait()
    stdout,stderr = ps.communicate()
    stdr = stderr.decode('utf-8')
    for i in stdr.split('\n'):
        print(i)
    #print(ps.stderr)
    out = output.decode("utf-8") 
    for i in out.split('\n'):
        print(i)
    #return ret

class zfs_fs(object):
    '''
    Alles rund um das Filesystem direkt
    '''
    def __init__(self,fs,connection = ''):
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
        

    def __getsnaplist(self):
        self.snaplist = []
        ret = subrun(self.connection+' zfs list -H -t snapshot -o name',quiet=True,stdout=subprocess.PIPE,universal_newlines=True)
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
        ret = subrun(self.connection+' zfs snapshot '+self.fs+'@'+SNAPPREFIX+'_'+str(nr))
        ret.check_returncode()
        self.snaplist.append(nr)
        pass
    def getoldsnap(self):
        pass
    def deletesnap(self,nr):
        cmd = self.connection+' zfs destroy '+self.fs+'@'+SNAPPREFIX+'_'+str(nr)
        subrun(cmd)
        self.snaplist.remove(nr)
    lastsnap = property(get_lastsnap, None, None, None)
    

class zfs_back(object):
    '''
    Hier findet als der reine Backupablauf seinen Platz
    '''


    def __init__(self, srcfs,dstfs,destserver=None):
        '''
        src und dst anlegen 
        '''
        if destserver != None:
            sshcmd = 'ssh -T '+destserver+' sudo '
        else:
            sshcmd = ''
        self.src = zfs_fs(srcfs)
        self.dst = zfs_fs(dstfs,sshcmd)
        print('Lastsnap Source: '+str(self.src.lastsnap))
        print('Lastsnap Destination: '+str(self.dst.lastsnap))
        prev = self.src.lastsnap
        if self.dst.lastsnap == 0:
            # dann voll senden (erst neuen Snapshot src erstellen
            self.src.takenextsnap()
            cmdfrom = 'zfs send -vce '+self.src.fs+'@'+SNAPPREFIX+'_'+str(self.src.lastsnap) 
            cmdto = sshcmd+'zfs receive -vs '+self.dst.fs
            ret = subrunPIPE(cmdfrom,cmdto)
            
            pass
        elif self.dst.lastsnap == self.src.lastsnap:
            # bei sind auf gleichem Stand - also neuer Snap + send
            self.src.takenextsnap()
            cmdfrom = 'zfs send -vce -i '+self.src.fs+'@'+SNAPPREFIX+'_'+str(prev)+' '+self.src.fs+'@'+SNAPPREFIX+'_'+str(self.src.lastsnap)
            cmdto =  sshcmd+'zfs receive -vs '+self.dst.fs
            ret = subrunPIPE(cmdfrom,cmdto)
            #print(ret.stdout)
        elif self.src.lastsnap > 0:
            '''
            Okay, es gibt in src und dest snapshots von uns -
            vlt. gibt es ja ein resume_token -> schau mer mal
            '''
            cmd = sshcmd +' zfs get -H receive_resume_token '+self.dst.fs
            ret = subrun(cmd,stdout=subprocess.PIPE,universal_newlines=True)
            print(ret.stdout)
            ergeb = ret.stdout.split('\t')
            if len(ergeb[2]) > 1:
                # dann gibt es ein token mit dem wir den restart versuchen können
                cmdfrom = 'zfs send -vt '+ergeb[2]
                cmdto = sshcmd+' zfs receive -vs '+self.dst.fs
                ret = subrunPIPE(cmdfrom, cmdto)
                pass
            
        # Schlussbehandlung (überzählige snaps löschen)
        self.cleansnaps()
        
    def cleansnaps(self):
        '''
        Soll von src und dest alle Snapshots löschen - bis auf die neuesten
        
        für dest soll das jetzt nicht mehr funzen, damit die Rechte nicht überstrapaziert werden
        '''
        l1 = len(self.src.snaplist)
        if l1 > HOLDSNAPS:
            # dann paar löschen
            for i in self.src.snaplist[0:l1-HOLDSNAPS]:
                self.src.deletesnap(i)
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-f","--from",dest='fromfs',
                      help='Übergabe des ZFS-Filesystems welches gesichert werden soll')
    parser.add_argument("-t","--to",dest='tofs',required=True,
                      help='Übergabe des ZFS-Filesystems auf welches gesichert werden soll')
    parser.add_argument("-s","--sshdest",dest='sshdest',
                      help='Übergabe des per ssh zu erreichenden Destination-Rechners')
    parser.add_argument("-c","--clearsnapsonly",dest='clearsnapsonly',action='store_true',
                      help='Löscht nur die überzähligen Snaps des Zielfilesystems')
    parser.set_defaults(clearsnapsonly=False)
    ns = parser.parse_args(sys.argv[1:])
    if ns.clearsnapsonly:
        
    zfs = zfs_back('vs2016/archiv/postgresql','zfsb/vsb','vsuess@192.168.1.61')
        