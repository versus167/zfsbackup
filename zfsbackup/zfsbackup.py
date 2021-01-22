#!/usr/bin/python3
# -*- coding: utf-8 -*-
'''
PYTHON_ARGCOMPLETE_OK

Created on 28.06.2018

@author: Volker Süß

2021-01-22 - Problem mit holdsnaps - offen
2020-05-23 - argcomplete - vs.
2020-02-16 - logging, encryption - vs.
2019-05-06 - Hold-auch für Destination eingefügt! - vs.
2019-02-21 - Option prefix ergänzt - vs.
2018-11-02 - Soweit sollte alles drin sein und einsatzfähig. Jetzt Praxistest - vs.


Sieht aus als wäre das alles gar kein echtes Problem -> Ist es auch nicht.

Das Problem liegt an einer anderen Stelle: zfs send/receive funzt nur als root. Damit fällt die 
Abschirmung zwischen src- und dst-Filesystem weg, Über rsync ist die Sicherung eine oneway-Geschichte,

die über snapshots vom Zugriff des Senderechners ausgeschlossen war. 

-> Man kann aber ja imho auch nur bestimmte Kommandos eines Befehls auf sudo machen lassen (ohne alle Rechte zu 
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
  /sbin/zpool get, /sbin/zpool get *, \
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
VERSION='2020.8 - 2020-05-23'
LOGNAME = 'ZFSB'
#SNAPPREFIX = 'zfsnappy'


import subprocess,shlex, argparse, argcomplete
import time,sys, datetime
import logging

def zeit():
    return time.strftime("%Y-%m-%d %H:%M:%S")
def subrun(command,checkretcode=True,**kwargs):
    '''
    Führt die übergeben Kommandozeile aus und gibt das Ergebnis
    zurück
    '''
    log = logging.getLogger(LOGNAME)
    args = shlex.split(command)
    log.debug(' '.join(args))
    ret = subprocess.run(args,**kwargs)
    if checkretcode: ret.check_returncode()
    return ret

def subrunPIPE(cmdfrom,cmdto,checkretcode=True,**kwargs):
    '''
    Führt die übergeben Kommandozeile aus und gibt das Ergebnis
    zurück
    '''
    args = shlex.split(cmdfrom)
    log = logging.getLogger(LOGNAME)
    log.debug(' '.join(args))
    #ret = subprocess.run(command,shell=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
    #print(ret.stdout)
    #if checkretcode: ret.check_returncode()
    argsto = shlex.split(cmdto)
    log.debug(f'pipe to -> {" ".join(argsto)}')
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
                log.info(line)
        else:
            vgl = test[-1]
            log.info(line)
            #print(line,end='')
            
def imrunning(fs):
    log = logging.getLogger(LOGNAME)
    psfaxu = subrun('ps fax',stdout=subprocess.PIPE,universal_newlines=True)
    pids = []
    for i in psfaxu.stdout.split('\n'):
        #print(i)
        if '/usr/bin/python3' in i and 'zfsbackup.py' in i and fs in i:
            pids.append(i.strip(' ').split(' ')[0])
    if len(pids) > 1:
        log.info(f'Looft bereits! pids: {pids}')
        return True

class zfs_fs(object):
    '''
    Alles rund um das Filesystem direkt
    '''
    def __init__(self,fs,prefix,connection = ''):
        '''
        Welches FS und worüber erreichen wir das
        '''
        self.logger = logging.getLogger(LOGNAME)
        self.__PREFIX = prefix
        self.__fs = fs
        self.__connection = connection
        
        temp = self.fs.split('/')
        self.pool = temp[0]
        self.dataset = temp[1:]
        self.__check_pool_exist()
        self.__check_pool_has_encryption()
        self.__check_dataset_exists()
        self.__check_encryption_feature()
        if self.dataset_exist == False:
            self.__snaplist = []
        else:
            self.updatesnaplist() # Snaplist ohne Prefix sammeln
        pass

    def get_pool_has_encryption(self):
        return self.__pool_has_encryption


    def get_dataset_exist(self):
        return self.__dataset_exist


    def get_has_encryption(self):
        return self.__has_encryption


    def __check_pool_exist(self):
        ''' Soll feststellen, ob der pool vorhanden und erreichbar ist ''' 
        cmd = self.connection +' zpool list -H '+self.pool
        ret = subrun(cmd,stdout=subprocess.PIPE,universal_newlines=True,checkretcode=True)
        #print(ret.stdout)
        ergeb = ret.stdout.split('\t')
        try:
            a = len(ergeb[2])
        except:
            a = 0
        if a > 1 and 'ONLINE' in ergeb[1:]:
            pass
        else:
            print('Pool ist nicht vorhanden!')
            exit(1)
    def __check_pool_has_encryption(self):
        ''' Kann der Pool überhaupt encryption? '''
        cmd = self.connection +' zpool get -H feature@encryption '+self.pool
        ret = subrun(cmd,stdout=subprocess.PIPE,universal_newlines=True,checkretcode=False)
        if ret.returncode != 0:
            self.__pool_has_encryption = False
        else:
            ergeb = ret.stdout.split('\t')
            try:
                a = len(ergeb[2])
            except:
                a = 0
            if a > 1 and (ergeb[2] == 'active' or ergeb[2] == 'enabled'):
                self.__pool_has_encryption = True
            else:
                self.__pool_has_encryption = False
    def __check_encryption_feature(self):
        ''' Soll feststellen, ob das feature encryption active ist im fs ''' 
        cmd = self.connection +' zpool get -H feature@encryption '+self.pool
        ret = subrun(cmd,stdout=subprocess.PIPE,universal_newlines=True,checkretcode=True)
        #print(ret.stdout)
        ergeb = ret.stdout.split('\t')
        try:
            a = len(ergeb[2])
        except:
            a = 0
        if a > 1 and ergeb[2] == 'active':
            self.__has_encryption = True
        else:
            self.__has_encryption = False
    def __check_dataset_exists(self):
        ''' Soll feststellen, ob das dataset vorhanden ist im fs ''' 
        cmd = self.connection +' zfs list -H -d 1 -o name '+self.fs
        ret = subrun(cmd,stdout=subprocess.PIPE,universal_newlines=True,checkretcode=False)
        if ret.returncode != 0:
            self.__dataset_exist = False
        else:
            self.__dataset_exist = True
    def get_prefix(self):
        return self.__PREFIX


    def get_fs(self):
        return self.__fs


    def get_connection(self):
        return self.__connection


    def get_snaplist(self):
        return self.__snaplist

    
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
        self.__snaplist = []
        ret = subrun(self.connection+' zfs list -H -d 1 -t snapshot -o name '+self.fs,stdout=subprocess.PIPE,universal_newlines=True)
        ret.check_returncode()
        if ret.stdout == None:
            return
        # Noch die Snaps mit dem falschen Prefix rausnehmen
        vgl = self.fs+'@'+self.PREFIX+'_'
        l = len(vgl)
        
        for snp in ret.stdout.split('\n'):
            if snp[0:l] == vgl:
                self.__snaplist.append(snp[l:])
        if len(self.__snaplist) == 0:
            return
        self.__snaplist.sort()
        #print(zeit(),self.fs,self.PREFIX)
        #print(self.snaplist[-2:])
        return
    def __get_holdsnaps(self):
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
        
    
    def hold_snap(self,snapshotname):
        ''' Setzt den übergeben Snapshot auf Hold  - kompletter Name wird übergeben'''
        if self.is_snap_hold(snapshotname):
            self.logger.debug(f'Snapshot ist bereits auf hold: {self.connection} {snapshotname}')
            return
        cmd = self.connection+' zfs hold keep '+snapshotname
        subrun(cmd)
    def is_snap_hold(self,snapshotname):
        ''' Return true, wenn schon auf hold '''
        cmd = self.connection+' zfs holds -H '+snapshotname
        ret = subrun(cmd,capture_output=True,text=True)
        erg = ret.stdout.split()
        if len(erg) < 2:
            return False
        if erg[1] == 'keep':
            return True
        return False
    def clear_holdsnaps(self,listholdsnaps):
        ''' Löscht die HOLD-Flags außer der übergebenen Snaps'''
        #print(listholdsnaps)
        for i in self.__get_holdsnaps():
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
        self.__snaplist.append(aktuell.isoformat())
        return snapname
        
    def get_oldsnap(self):
        return self.__snaplist[-2]
        pass

    getoldsnap = property(get_oldsnap, None, None, None)
    lastsnap = property(get_lastsnap, None, None, None)
    PREFIX = property(get_prefix, None, None, None)
    fs = property(get_fs, None, None, None)
    connection = property(get_connection, None, None, None)
    snaplist = property(get_snaplist, None, None, None)
    has_encryption = property(get_has_encryption, None, None, None)
    dataset_exist = property(get_dataset_exist, None, None, None)
    pool_has_encryption = property(get_pool_has_encryption, None, None, None)
    
    
    

class zfs_back(object):
    '''
    Hier findet als der reine Backupablauf seinen Platz
    '''
    def __init__(self,):
        '''
        src und dst anlegen 
        '''
        parser = argparse.ArgumentParser()
        # Source Filesystem welches gesichert werden soll
        parser.add_argument("-f","--from",dest='fromfs',
            help='Übergabe des ZFS-Filesystems welches gesichert werden soll')
        # Destination-FS
        parser.add_argument("-t","--to",dest='tofs',required=True,
            help='Übergabe des ZFS-Filesystems auf welches gesichert werden soll')
        # Destination per ssh zu erreichen?
        parser.add_argument("-s","--sshdest",dest='sshdest',
            help='Übergabe des per ssh zu erreichenden Destination-Rechners')
        parser.add_argument('-d',dest="debugging",help='Debug-Level-Ausgaben',default=False,action='store_true')
        # Prefix für die snapshots - Default: zfsnappy
        parser.add_argument('-p','--prefix',dest='prefix',help='Der Prefix für die Bezeichnungen der Snapshots',default='zfsnappy')
        argcomplete.autocomplete(parser) # bash completion
        self.args = parser.parse_args()
        self.logger = logging.getLogger(LOGNAME)
        if self.args.debugging:
            self.logger.setLevel(logging.DEBUG)
        else:
            self.logger.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        
        fh = logging.StreamHandler()
        fh.setFormatter(formatter)
        self.logger.addHandler(fh)
        self.logger.info(f'{APPNAME} - {VERSION}  ************************** Start')
        self.logger.debug(self.args)
        if imrunning(self.args.fromfs):
            self.logger.info(f'{APPNAME} - {VERSION}  ************************** Stop')
            exit()
        self.PREFIX = self.args.prefix
        if self.args.sshdest != None:
            sshcmd = 'ssh -T '+self.args.sshdest+' sudo '
        else:
            sshcmd = ''
        self.src = zfs_fs(self.args.fromfs,self.PREFIX)
        self.dst = zfs_fs(self.args.tofs,self.PREFIX,sshcmd)
        
        
        self.logger.debug(f'SRC: {self.src.fs} exist: {self.src.dataset_exist} encryption: {self.src.has_encryption} pool_encryption: {self.src.pool_has_encryption}')
        self.logger.debug(f'DST: {self.dst.fs} exist: {self.dst.dataset_exist} encryption: {self.dst.has_encryption} pool_encryption: {self.dst.pool_has_encryption}')
        
        # 1. Schritt -> Token checken - falls ja, dann Versuch fortsetzen
        if self.dst.dataset_exist:
            token = self.dst.get_token()
        else: 
            token = None
        
        if token != None:
            self.resume_transport(token)
            # Send sollte erfolgreich gewesen sein -> Neuesten snap am Ziel auf Hold setzen
            self.dst_hold_update()
            return
        
        # 2. Schritt -> Wie lautet der neueste identische Snapshot?
        lastmatch = self.get_lastmatch()
        
        if lastmatch == None:
            # es gibt also keinen identischen Snapshot -> Damit Versuch neuen Snapshot zu senden und fs zu senden
            if self.dst.dataset_exist:
                self.logger.error(f'Das Zieldataset existiert bereits und es gbt keinen identischen Snapshot')
                exit(1)
            if self.src.has_encryption and self.dst.pool_has_encryption == False:
                self.logger.error('Das Source-Dataset hat encryption aktiv, aber der Zielpool nicht!')
                exit(1)
            newsnap = self.src.takenextsnap()
            self.src.hold_snap(newsnap)
            if self.src.has_encryption:
                # add w to command
                addcmd = 'w'
            else:
                addcmd = ''
            cmdfrom = f'zfs send -{addcmd}v {newsnap}'
            cmdto = sshcmd+'zfs receive -vs '+self.dst.fs
            subrunPIPE(cmdfrom,cmdto)
            
            self.src.clear_holdsnaps((newsnap,))
            self.dst_hold_update()
            return
        
        else:
            # es gibt also einen gemeinsamen Snapshot - neuen Snapshot erstellen und inkrementell senden
            newsnap = self.src.takenextsnap()
            oldsnap = self.src.fs+'@'+self.PREFIX+'_'+lastmatch
            self.src.hold_snap(newsnap)
            if self.src.has_encryption:
                # add w to command
                addcmd = 'w'
            else:
                addcmd = ''
            cmdfrom = f'zfs send -v{addcmd} -i {oldsnap} {newsnap}'
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
        if self.src.has_encryption:
            # add w to command
            addcmd = 'w'
        else:
            addcmd = ''
        cmdfrom = f'zfs send -{addcmd}vt {token}'
        cmdto = self.dst.connection+' zfs receive -Fvs '+self.dst.fs
        subrunPIPE(cmdfrom, cmdto)
    def dst_hold_update(self):
        ''' setzt den letzten (aktuellsten) Snap auf Hold und released die anderen '''
        # Dann erstmal eine kurze Pause - vlt. hilft das ZFS Luft zu holen und
        # alle Snaps aufzulisten
        time.sleep(30) # die Pause scheint manchmal recht lang nötig zu sein - wir haben ja keinen Zeitdruck
        self.dst.updatesnaplist() # neu aufbauen, da neuer Snap vorhanden
        self.logger.debug(f'Dieser Snap im dst wird auf Hold gesetzt: {self.dst.lastsnap}')
        self.dst.hold_snap(self.dst.lastsnap)
        self.dst.clear_holdsnaps((self.dst.lastsnap,))
            
   
if __name__ == '__main__':
    
    zfs = zfs_back()
    zfs.logger.info(f'{APPNAME} - {VERSION}  ************************** Stop')
        