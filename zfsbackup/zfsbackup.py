#!/usr/bin/python3
# -*- coding: utf-8 -*-
'''
Created on 28.06.2018

@author: Volker Süß


2019-05-05 - Hold-auch für Destination eingefügt! - vs.
2019-02-21 - Option prefix ergänzt - vs.
2018-11-02 - Soweit sollte alles drin sein und einsatzfähig. Jetzt Praxistest - vs.


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
  /sbin/zfs hold, /sbin/zfs hold *, \
  /sbin/zfs release, /sbin/zfs release *, \
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
VERSION='5 - 2019-05-05'
#SNAPPREFIX = 'zfsnappy'


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
    def __init__(self,fs,prefix,connection = ''):
        '''
        Welches FS und worüber erreichen wir das
        '''
        self.PREFIX = prefix
        self.fs = fs
        self.connection = connection
        self.updatesnaplist() # Snaplist ohne Prefix sammeln
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
        return self.fs+'@'+self.PREFIX+'_'+self.snaplist[-1]
        

    def updatesnaplist(self):
        # Snaplist ohne Prefix
        self.snaplist = []
        ret = subrun(self.connection+' zfs list -H -d 1 -t snapshot -o name '+self.fs,quiet=False,stdout=subprocess.PIPE,universal_newlines=True)
        ret.check_returncode()
        if ret.stdout == None:
            return
        # Noch die Snaps mit dem falschen Prefix rausnehmen
        vgl = self.fs+'@'+self.PREFIX+'_'
        l = len(vgl)
        
        for snp in ret.stdout.split('\n'):
            if snp[0:l] == vgl:
                self.snaplist.append(snp[l:])
        if len(self.snaplist) == 0:
            return
        self.snaplist.sort()
        print(zeit(),self.fs,self.PREFIX)
        print(self.snaplist[-2:])
        return
    def get_holdsnaps(self):
        '''
        Alternative zur Pipe-Variante
        
        zfs list -H -d 1 -t snapshot -o userrefs,name vs2016/bk/vs -> Liefert zwei Spalten - Zahl name
        if zahl > 0 dann ist es auf hold
         '''
        holdsnaps = []
        cmd = self.connection+' zfs list -H -d 1 -t snapshot -o userrefs,name '+self.fs
        ret = subrun(cmd,stdout=subprocess.PIPE,universal_newlines=True)
        ret.check_returncode()
        if ret.stdout == None:
            return
        # Noch die Snaps mit dem falschen Prefix rausnehmen
        vgl = self.fs+'@'+self.PREFIX+'_'
        l = len(vgl)
        
        for i in ret.stdout.split('\n'):
            if len(i) == 0:
                continue 
            j = i.split('\t')
            if int(j[0]) > 0:
                if j[1][0:l] == vgl:
                    holdsnaps.append(j[1])
        return holdsnaps
        

#     def get_holdnsaps(self):
#         '''
#         Gibt eine Liste mit Holdsnaps zurück
#         
#         zfs list -H -d 1 -t snapshot -o name vs2016/archiv/virtualbox | xargs zfs holds 
#         '''
#         cmdfrom = shlex.split(self.connection+ ' zfs list -H -d 1 -t snapshot -o name '+self.fs)
#         cmdto = shlex.split(self.connection+' xargs zfs holds -H')
#         holdsnaps = []
#         pfrom = subprocess.Popen(cmdfrom, stdout=subprocess.PIPE,stderr=subprocess.PIPE,universal_newlines=True)
#         pto =   subprocess.Popen(cmdto  , stdin=pfrom.stdout,stdout=subprocess.PIPE,stderr=subprocess.PIPE,universal_newlines=True,encoding='UTF-8')
#         vgl = self.fs+'@'+self.PREFIX+'_'
#         l = len(vgl)
#         for line in pto.stdout:
#             snp = line.split('\t')[0]
#             if snp[0:l] == vgl:
#                holdsnaps.append(snp)
#         return holdsnaps
    
    def hold_snap(self,snapshotname):
        ''' Setzt den übergeben Snapshot auf Hold  - kompletter Name wird übergeben'''
        cmd = self.connection+' zfs hold keep '+snapshotname
        subrun(cmd)
    
    def clear_holdsnaps(self,listholdsnaps):
        ''' Löscht die HOLD-Flags außer der übergebenen Snaps'''
        #print(listholdsnaps)
        for i in self.get_holdsnaps():
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
        snapname = self.fs+'@'+self.PREFIX+'_'+aktuell.isoformat()
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
    def __init__(self, srcfs,dstfs,prefix,destserver=None):
        '''
        src und dst anlegen 
        '''
        self.PREFIX = prefix
        if destserver != None:
            sshcmd = 'ssh -T '+destserver+' sudo '
        else:
            sshcmd = ''
        self.src = zfs_fs(srcfs,self.PREFIX)
        self.dst = zfs_fs(dstfs,self.PREFIX,sshcmd)
        print('Lastsnap Source: '+self.src.lastsnap)
        print('Lastsnap Destination: '+self.dst.lastsnap)
        
        # 1. Schritt -> Token checken - falls ja, dann Versuch fortsetzen
        token = self.dst.get_token()
        
        if token != None:
            self.resume_transport(token)
            # Send sollte erfolgreich gewesen sein -> Neuesten snap am Ziel auf Hold setzen
            self.dst_hold_update()
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
            self.dst_hold_update()
            return
        
        else:
            # es gibt also einen gemeinsamen Snapshot - neuen Snapshot erstellen und inkrementell senden
            newsnap = self.src.takenextsnap()
            oldsnap = self.src.fs+'@'+self.PREFIX+'_'+lastmatch
            self.src.hold_snap(newsnap)
            
            cmdfrom = 'zfs send -vce -i '+oldsnap+' '+newsnap
            cmdto =  sshcmd+'zfs receive -Fvs '+self.dst.fs
            subrunPIPE(cmdfrom,cmdto)
            self.src.clear_holdsnaps((oldsnap,newsnap))
            self.dst_hold_update()
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
    def dst_hold_update(self):
        ''' setzt den letzten (aktuellsten) Snap auf Hold und released die anderen '''
        # Dann erstmal eine kurze Pause - vlt. hilft das ZFS Luft zu holen und
        # alle Snaps aufzulisten
        time.sleep(10)
        self.dst.updatesnaplist() # neu aufbauen, da neuer Snap vorhanden
        print('Dieser Snap im dst wird auf Hold gesetzt: ',self.dst.lastsnap)
        self.dst.hold_snap(self.dst.lastsnap)
        self.dst.clear_holdsnaps((self.dst.lastsnap,))
            
   
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-f","--from",dest='fromfs',
                      help='Übergabe des ZFS-Filesystems welches gesichert werden soll')
    parser.add_argument("-t","--to",dest='tofs',required=True,
                      help='Übergabe des ZFS-Filesystems auf welches gesichert werden soll')
    parser.add_argument("-s","--sshdest",dest='sshdest',
                      help='Übergabe des per ssh zu erreichenden Destination-Rechners')
    parser.add_argument('-p','--prefix',dest='prefix',help='Der Prefix für die Bezeichnungen der Snapshots',default='zfsnappy')
    ns = parser.parse_args(sys.argv[1:])
    print(time.strftime("%Y-%m-%d %H:%M:%S"),APPNAME, VERSION,' ************************** Start')
    print(time.strftime("%Y-%m-%d %H:%M:%S"),'Aufrufparameter:',' '.join(sys.argv[1:]))
    if imrunning(ns.fromfs):
        print(time.strftime("%Y-%m-%d %H:%M:%S"),APPNAME, VERSION,' ************************** Stop')
        exit()
    zfs = zfs_back(ns.fromfs,ns.tofs,ns.prefix,ns.sshdest)
    print(time.strftime("%Y-%m-%d %H:%M:%S"),APPNAME, VERSION,' ************************** Stop')
        