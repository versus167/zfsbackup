#!/usr/bin/python3
# -*- coding: utf-8 -*-
'''
Created on 28.06.2018

@author: Volker Süß

Sieht aus als wäre das alles gar kein echtes Problem -> Ist es auch nicht.

Das Problem liegt an einer anderen Stelle: zfs send/receive funzt nur als root. Damit fällt die 
Abschirmung zwischen src- und dst-Filesystem weg, Über rsync ist die Sicherung eine oneway-Geschichte,

die über snapshots vom Zugriff des Senderechners ausgeschlossen war. 

-> Man kann aber ja imho auch nur bestimmte Kommandos eines Befehls auf suod machen lassen (ohne alle Rechte zu 
haben). Das wäre vlt. die Möglichkeit, das ganze doch noch zu nutzen. Wenn ich am Ziel weder FS noch snapshots 
löschen kann, dann besteht eigentlich auch kein Risiko? 26/10/18

Um zfs receive für den Sudoer freizuschalten ist in sudoers.d das file zfs so zu gestalten:

## Allow read-only ZoL commands to be called through sudo
## without a password. Remove the first '#' column to enable.
##
## CAUTION: Any syntax error introduced here will break sudo.
##
## Cmnd alias specification
Cmnd_Alias C_ZFS = \
  /sbin/zfs "", /sbin/zfs help *, \
  /sbin/zfs get, /sbin/zfs get *, \
  /sbin/zfs list, /sbin/zfs list *, \
  /sbin/zpool "", /sbin/zpool help *, \
  /sbin/zpool iostat, /sbin/zpool iostat *, \
  /sbin/zpool list, /sbin/zpool list *, \
  /sbin/zpool status, /sbin/zpool status *, \
  /sbin/zpool upgrade, /sbin/zpool upgrade -v
#
## allow any user to use basic read-only ZFS commands
ALL ALL = (root) NOPASSWD: C_ZFS
zfs (END)

Gleichzeitig sollte auf Source und Dest-System zfsnappy im Einsatz sein, da sonst keine Snapshots gelöscht werden   

'''


APPNAME='zfsbackup'
VERSION='1 - 2018-06-28'
SNAPPREFIX = 'zfsnappy'
HOLDSNAPS = 5 # 5 Backupsnapshots werden behalten

import subprocess,shlex, argparse
import time,os.path,sys, datetime

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
    ps = subprocess.Popen(args, stdout=subprocess.PIPE,stderr=subprocess.PIPE,universal_newlines=True)
    argsto = shlex.split(cmdto)
    ziel = subprocess.Popen(argsto, stdin=ps.stdout,)
    vgl = ''
    cnt = 0
    for line in ps.stderr:
        cnt += 1
        test = line.split(' ')
        if test[-1] == vgl:
            if cnt > 30:
                cnt = 0
                print(line,end='')
        else:
            vgl = test[-1]
            print(line,end='')
    if ps.returncode != 0:
        raise CalledProcessError(ps.returncode, ps.args)
    ziel.wait()
    if ziel.returncode != 0:
        raise CalledProcessError(ziel.returncode, ziel.args)
    #return ret
    
def cleansnaps(fs):
    '''
    Soll vom fs alle Snapshots löschen - bis auf die neuesten
    
    
    '''
    l1 = len(fs.snaplist)
    if l1 > HOLDSNAPS:
        # dann paar löschen
        for i in fs.snaplist[0:l1-HOLDSNAPS]:
            fs.deletesnap(i)

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
            return ''
        return self.snaplist[-1]
        

    def __getsnaplist(self):
        self.snaplist = []
        ret = subrun(self.connection+' zfs list -H -t snapshot -o name',quiet=True,stdout=subprocess.PIPE,universal_newlines=True)
        ret.check_returncode()
        if ret.stdout == None:
            return
        vgl = self.fs+'@'+SNAPPREFIX+'_'
        l = len(vgl)
        
        for snp in ret.stdout.split('\n'):
            if snp[0:l] == vgl:
                self.snaplist.append(snp[l:])
        if len(self.snaplist) == 0:
            return
        self.snaplist.sort()
        
    def takenextsnap(self):
        aktuell = datetime.datetime.now()
        snapname = self.fs+'@'+SNAPPREFIX+'_'+aktuell.isoformat()
        ret = subrun(self.connection+' zfs snapshot '+snapname)
        ret.check_returncode()
        self.snaplist.append(aktuell.isoformat())
        pass
    def get_oldsnap(self):
        return self.snaplist[-2]
        pass
#     def deletesnap(self,nr):
#         cmd = self.connection+' zfs destroy '+self.fs+'@'+SNAPPREFIX+'_'+str(nr)
#         subrun(cmd)
#         self.snaplist.remove(nr)
    getoldsnap = property(get_oldsnap, None, None, None)
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
        print('Lastsnap Source: '+self.src.lastsnap)
        print('Lastsnap Destination: '+self.dst.lastsnap)
        
        if self.dst.lastsnap == '':
            # dann voll senden (erst neuen Snapshot src erstellen) zuvor aber noch token checken
            cmd = sshcmd +' zfs get -H receive_resume_token '+self.dst.fs
            ret = subrun(cmd,stdout=subprocess.PIPE,universal_newlines=True,checkretcode=False)
            print(ret.stdout)
            ergeb = ret.stdout.split('\t')
            try:
                a = len(ergeb[2])
            except:
                a = 0
            if a > 1:
                # dann gibt es ein token mit dem wir den restart versuchen können
                cmdfrom = 'zfs send -cevt '+ergeb[2]
                cmdto = sshcmd+' zfs receive -vs '+self.dst.fs
                ret = subrunPIPE(cmdfrom, cmdto)
            else:
                self.src.takenextsnap()
                cmdfrom = 'zfs send -vce '+self.src.fs+'@'+SNAPPREFIX+'_'+self.src.lastsnap 
                cmdto = sshcmd+'zfs receive -vsF '+self.dst.fs
                ret = subrunPIPE(cmdfrom,cmdto)
            
            pass
        elif self.dst.lastsnap == self.src.lastsnap:
            # bei sind auf gleichem Stand - also neuer Snap + send
            self.src.takenextsnap()
            frs1 = self.src.fs+'@'+SNAPPREFIX+'_'+self.src.getoldsnap
            frs2 = self.src.fs+'@'+SNAPPREFIX+'_'+self.src.lastsnap
            cmdfrom = 'zfs send -vce -i '+frs1+' '+frs2
            cmdto =  sshcmd+'zfs receive -Fvs '+self.dst.fs
            ret = subrunPIPE(cmdfrom,cmdto)
            #print(ret.stdout)
        elif self.src.lastsnap != '':
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
                cmdfrom = 'zfs send -cevt '+ergeb[2]
                cmdto = sshcmd+' zfs receive -vs '+self.dst.fs
                ret = subrunPIPE(cmdfrom, cmdto)
            else:
                # Es gibt also kein Resumetoken - dann der Versuch mit -F
                # Aber erst muss gecheckt werden, welcher snap auf dem dest-system vorhanden ist
                lastmatch = None
                for i in self.src.snaplist:
                    if i in self.dst.snaplist:
                        lastmatch = i
                if lastmatch == None:
                    print('Kein identischer Snap vorhanden - Damit muss das Zielsystem erst von allen Snaps befreit oder gelöscht werden!')
                    return
                frs1 = self.src.fs+'@'+SNAPPREFIX+'_'+lastmatch
                frs2 = self.src.fs+'@'+SNAPPREFIX+'_'+self.src.lastsnap
                cmdfrom = 'zfs send -vce -i '+frs1+' '+frs2
                cmdto =  sshcmd+'zfs receive -Fvs '+self.dst.fs
                ret = subrunPIPE(cmdfrom,cmdto)
            
        # Schlussbehandlung (überzählige snaps löschen)
        # cleansnaps(self.src) - das löschen überlassen wir komplett zfsnappy 
        
   
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-f","--from",dest='fromfs',
                      help='Übergabe des ZFS-Filesystems welches gesichert werden soll')
    parser.add_argument("-t","--to",dest='tofs',required=True,
                      help='Übergabe des ZFS-Filesystems auf welches gesichert werden soll')
    parser.add_argument("-s","--sshdest",dest='sshdest',
                      help='Übergabe des per ssh zu erreichenden Destination-Rechners')
#     parser.add_argument("-c","--clearsnapsonly",dest='clearsnapsonly',action='store_true',
#                       help='Löscht nur die überzähligen Snaps des Zielfilesystems')
    parser.set_defaults(clearsnapsonly=False)
    ns = parser.parse_args(sys.argv[1:])
    zfs = zfs_back(ns.fromfs,ns.tofs,ns.sshdest)
        