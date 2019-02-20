#!/usr/bin/python3
# -*- coding: utf-8 -*-
'''
Created on 28.06.2018

@author: Volker Süß

todo -> mit Prefix arbeiten um zfsbackup für verschiedene Zielsysteme möglich zu machen

3 - 2018-11-02 - Soweit sollte alles drin sein und einsatzfähig. Jetzt Praxistest - vs.

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
  /sbin/zfs receive, /sbin/zfs receive *, \
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

Die beiden aktuellen Snapshots sollten auf hold stehen, damit die nicht gelöscht werden
-> wenn kein Token vorhanden ist, oder die Verwendung nicht klappt -> alle Holds freigeben   

'''


APPNAME='zfsbackup'
VERSION='3 - 2018-11-02'
SNAPPREFIX = 'zfsnappy'


import subprocess,shlex, argparse
import time,sys, datetime

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
    ziel = subprocess.Popen(argsto, stdin=ps.stdout)
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
            
def imrunning(fs):
    
    psfaxu = subrun('ps fax',stdout=subprocess.PIPE,universal_newlines=True)
    pids = []
    for i in psfaxu.stdout.split('\n'):
        #print(i)
        if '/usr/bin/python3' in i and 'zfsbackup.py' in i and fs in i:
            pids.append(i.strip(' ').split(' ')[0])
    if len(pids) > 1:
        print(time.strftime("%Y-%m-%d %H:%M:%S"),'Looft bereits! pids: ',pids)
        return True

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
        self.__getsnaplist() # Snaplist ohne Prefix sammeln
        pass
    
    def get_token(self):
        ''' Schaut ob ein Token im Filesystem gespeichert ist '''
        cmd = self.connection +' zfs get -H receive_resume_token '+self.fs
        ret = subrun(cmd,stdout=subprocess.PIPE,universal_newlines=True,checkretcode=True)
        #print(ret.stdout)
        ergeb = ret.stdout.split('\t')
        try:
            a = len(ergeb[2])
        except:
            a = 0
        if a > 1:
            return ergeb[2]
        else:
            return None
    def get_lastsnap(self):
        if len(self.snaplist) == 0:
            return ''
        return self.snaplist[-1]
        

    def __getsnaplist(self):
        # Snaplist ohne Prefix
        self.snaplist = []
        ret = subrun(self.connection+' zfs list -H -d 1 -t snapshot -o name '+self.fs,quiet=True,stdout=subprocess.PIPE,universal_newlines=True)
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
        return
    def get_holdnsaps(self):
        '''
        Gibt eine Liste mit Holdsnaps zurück
        
        zfs list -H -d 1 -t snapshot -o name vs2016/archiv/virtualbox | xargs zfs holds 
        '''
        cmdfrom = shlex.split(self.connection+ ' zfs list -H -d 1 -t snapshot -o name '+self.fs)
        cmdto = shlex.split(self.connection+' xargs zfs holds -H')
        holdsnaps = []
        pfrom = subprocess.Popen(cmdfrom, stdout=subprocess.PIPE,stderr=subprocess.PIPE,universal_newlines=True)
        pto =   subprocess.Popen(cmdto  , stdin=pfrom.stdout,stdout=subprocess.PIPE,stderr=subprocess.PIPE,universal_newlines=True,encoding='UTF-8')
        
        for line in pto.stdout:
            holdsnaps.append(line.split('\t')[0])
        return holdsnaps
    
    def hold_snap(self,snapshotname):
        ''' Setzt den übergeben Snapshot auf Hold  - kompletter Name wird übergeben'''
        cmd = self.connection+' zfs hold keep '+snapshotname
        subrun(cmd)
    
    def clear_holdsnaps(self,listholdsnaps):
        ''' Löscht die HOLD-Flags außer der übergebenen Snaps'''
        
        for i in self.get_holdnsaps():
            if i in listholdsnaps:
                pass
            else:
                cmd = self.connection+' zfs release -r keep '+i
                subrun(cmd)
    def takenextsnap(self):
        ''' 
        Hier wird ein neuer Snap gesetzt - Wenn erfolgreich, dann alle übrigen Holds löschen und den neuen auf 
        Hold setzen
        '''
        aktuell = datetime.datetime.now()
        snapname = self.fs+'@'+SNAPPREFIX+'_'+aktuell.isoformat()
        ret = subrun(self.connection+' zfs snapshot '+snapname)
        ret.check_returncode()
        self.snaplist.append(aktuell.isoformat())
        return snapname
        
    def get_oldsnap(self):
        return self.snaplist[-2]
        pass

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
        
        # 1. Schritt -> Token checken - falls ja, dann Versuch fortsetzen
        token = self.dst.get_token()
        
        if token != None:
            self.resume_transport(token)
            return
        
        # 2. Schritt -> Wie lautet der neueste identische Snapshot?
        lastmatch = self.get_lastmatch()
        if lastmatch == None:
            # es gibt also keinen identischen Snapshot -> Damit Versuch neuen Snapshot zu senden und FS zu erstellen
            newsnap = self.src.takenextsnap()
            self.src.hold_snap(newsnap)
            
            cmdfrom = 'zfs send -vce '+newsnap
            cmdto = sshcmd+'zfs receive -vsF '+self.dst.fs
            subrunPIPE(cmdfrom,cmdto)
            self.src.clear_holdsnaps((newsnap,))
            return
        
        else:
            # es gibt also einen gemeinsamen Snapshot - neuen Snapshot erstellen und inkrementell senden
            newsnap = self.src.takenextsnap()
            oldsnap = self.src.fs+'@'+SNAPPREFIX+'_'+lastmatch
            self.src.hold_snap(newsnap)
            
            cmdfrom = 'zfs send -vce -i '+oldsnap+' '+newsnap
            cmdto =  sshcmd+'zfs receive -Fvs '+self.dst.fs
            subrunPIPE(cmdfrom,cmdto)
            self.src.clear_holdsnaps((oldsnap,newsnap))
            return

        
    def get_lastmatch(self):
        ''' Sucht den letzten identischen Snapshot '''
        lastmatch = None
        for i in self.src.snaplist:
            if i in self.dst.snaplist:
                lastmatch = i
        return lastmatch
    def resume_transport(self,token):
        # Setzt den Transport fort 
        cmdfrom = 'zfs send -cevt '+token
        cmdto = self.dst.connection+' zfs receive -Fvs '+self.dst.fs
        subrunPIPE(cmdfrom, cmdto)
        
   
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-f","--from",dest='fromfs',
                      help='Übergabe des ZFS-Filesystems welches gesichert werden soll')
    parser.add_argument("-t","--to",dest='tofs',required=True,
                      help='Übergabe des ZFS-Filesystems auf welches gesichert werden soll')
    parser.add_argument("-s","--sshdest",dest='sshdest',
                      help='Übergabe des per ssh zu erreichenden Destination-Rechners')
    ns = parser.parse_args(sys.argv[1:])
    print(time.strftime("%Y-%m-%d %H:%M:%S"),APPNAME, VERSION,' ************************** Start')
    print(time.strftime("%Y-%m-%d %H:%M:%S"),'Aufrufparameter:',' '.join(sys.argv[1:]))
    if imrunning(ns.fromfs):
        print(time.strftime("%Y-%m-%d %H:%M:%S"),APPNAME, VERSION,' ************************** Stop')
        exit()
    zfs = zfs_back(ns.fromfs,ns.tofs,ns.sshdest)
    print(time.strftime("%Y-%m-%d %H:%M:%S"),APPNAME, VERSION,' ************************** Stop')
        