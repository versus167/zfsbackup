#!/usr/bin/python3
# -*- coding: utf-8 -*-
'''

Created on 28.06.2018

@author: Volker Süß

todo:

- Fehler auswerten:
    Abbrechen, wenn das send nicht angenommen werden kann - konkrete Meldung dazu sammeln
    
    - done-file touchen falls angegeben
    - check done-file ob ausgeführt werden soll - nach range

2022.26.3 2022.07.23 - fix touch_file setzen, wenn Fehler aufgetreten sind - vs.
2022.26 2022.01.24 - --without-root lässt das übergebene (relative) Root-System unbehandelt - vs.
2022.25 2022-01-21 - --touch-file --mindays und --maxdays Versuch die Ausführung verteilter zu gestalten - vs.
2021.24 2021-11-13 - Versuch Abbrüche der Netzverbindung abzufangen...zusätzlich --kill Switch vs.
2021.23 2021-09-23 - imrunning verbessert - vs.
2021.22 2021-09-09 - --raw bzw -w eingefügt - damit entscheidet der Aufruf ob raw gesendet wird oder nicht -vs
2021.21 2021-09-06 - Statt Pause jetzt ziel.wait() im subrunpipe - vs.
2021.20 2021-09-02 - Empfänger wird auf zfsbackup_receiver für receive umgestellt - vs.
2021.19 2021-08-20 - Check encryption für fs berichtigt - vs.
2021.17 2021-08-15 - Anpassung an python 3.5 - vs.
2021.16 2021-08-09 - das Hold-Handling etwas klarer gestaltet - vs.
2021.15 2021-08-09 - neue Optionen nosnapshot und holdtag - und Verwendung utc für neue Snapshots 
                     Initoptions für target -o compression=lzr und -o rdonly=on
                     -r für rekursive Ausführung - vs.
2021-04-10 - Info zum Ziel des Backup in log.info aufgenommen - vs.
2021-01-25 - fix typo, entferne argcomplete - vs.
2021-01-23 - sudo auf Destination etwas feiner abgestimmt - vs.
2021-01-22 - Problem mit holdsnaps - vs.
2020-05-23 - argcomplete - vs.
2020-02-16 - logging, encryption - vs.
2019-05-06 - Hold-auch für Destination eingefügt! - vs.
2019-02-21 - Option prefix ergänzt - vs.
2018-11-02 - Soweit sollte alles drin sein und einsatzfähig. Jetzt Praxistest - vs.

Gleichzeitig sollte auf Source und Dest-System zfsnappy im Einsatz sein, da sonst keine Snapshots gelöscht werden

Die beiden aktuellen Snapshots sollten auf hold stehen, damit die nicht gelöscht werden
-> wenn kein Token vorhanden ist, oder die Verwendung nicht klappt -> alle Holds freigeben   

'''


APPNAME='zfsbackup'
VERSION='2022.26.3 - 2022-07-23'
LOGNAME = 'ZFSB'



import subprocess,shlex, argparse, os, signal
import time,sys, datetime
import logging, random
from pathlib import Path

def zeit():
    return time.strftime("%Y-%m-%d %H:%M:%S")
def subrun(command,checkretcode=True,**kwargs):
    '''
    Führt die übergebene Kommandozeile aus und gibt das Ergebnis
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
    log.info(' '.join(args))
    argsto = shlex.split(cmdto)
    log.info(f'pipe to -> {" ".join(argsto)}')
    ps = subprocess.Popen(args, stdout=subprocess.PIPE,stderr=subprocess.PIPE,universal_newlines=True)
    argsto = shlex.split(cmdto)
    ziel = subprocess.Popen(argsto, stdin=ps.stdout)   
    vgl = ''
    cnt = 0
    output = []
    for line in ps.stderr:
        if "closed by remote host" in line or "send disconnect: Broken Pipe" in line:
            log.error("Abbruch der Verbindung -> Ende Script")
            exit(1)
        cnt += 1
        test = line.split(' ')
        if test[-1] == vgl:
            if cnt > 30:
                cnt = 0
                log.info(line)
                output.append(line)
        else:
            vgl = test[-1]
            log.info(line)
            output.append(line)

    ziel.wait()        
    return output        
def imrunning(kill):
    ''' Falls das Ding bereits läuft und nicht gekillt werden soll, dann return true und exit
    '''
    log = logging.getLogger(LOGNAME)
    ownpid = os.getpid()
    log.debug(f'Mein pid: {ownpid}')
    psfaxu = subrun('ps fax',stdout=subprocess.PIPE,universal_newlines=True)
    vgl = '/usr/bin/python3'+' '+" ".join(sys.argv)
    pids = []
    for i in psfaxu.stdout.split('\n'):
        
        if vgl in i:
            log.debug(i)
            pid = int(i.strip(' ').split(' ')[0])
            if pid == ownpid:
                continue
            pids.append(pid)
    
    if len(pids) > 0 and kill:
        for pid in pids:
            pgid = os.getpgid(pid)
            if pgid == 1:
                os.kill(pid, signal.SIGTERM)
                log.debug(f'kill {pid}')
            else:
                log.debug(f'killpg {pgid}')
                os.killpg(pgid, signal.SIGTERM)
                 
            time.sleep(60)
        return False
    if len(pids) > 0:
        log.info(f'Looft bereits! pids: {pids}')
        return True

class zfs_fs(object):
    '''
    Alles rund um das Filesystem direkt
    '''
    def __init__(self,fs,prefix,connection = '',connectionsudo = '',holdtag= 'keep'):
        '''
        Welches FS und worüber erreichen wir das
        '''
        self.logger = logging.getLogger(LOGNAME)
        self.__PREFIX = prefix
        self.__fs = fs
        self.__connection = connection
        self.__connectionsudo = connectionsudo
        self.logger.debug(f'{self.fs} - {self.connection} - {self.connectionsudo}')
        temp = self.fs.split('/')
        self.pool = temp[0]
        self.dataset = temp[1:]
        self.holdtag = holdtag
        self.__check_pool_exist()
        self.__check_pool_has_encryption()
        self.__check_dataset_exists()
        if self.dataset_exist:
            self.__check_encryption_fs()
            self.updatesnaplist() # Snaplist ohne Prefix sammeln
        else:
            self.__has_encryption = False
            self.__snaplist = []
        
       
            
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
            print(f'Pool {self.pool} ist nicht vorhanden!')
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
    def __check_encryption_fs(self):
        ''' Soll feststellen, ob das feature encryption active ist im fs '''
        if self.pool_has_encryption == False:
            self.__has_encryption = False
            return
        cmd = self.connection +' zfs get -H encryption '+self.fs
        try:
            ret = subrun(cmd,stdout=subprocess.PIPE,universal_newlines=True,checkretcode=True)
        except:
            self.__has_encryption = False
        #print(ret.stdout)
        ergeb = ret.stdout.split('\t')
        if ergeb[2] == 'off':
            self.__has_encryption = False
        else:
            self.__has_encryption = True
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
    
    def get_connectionsudo(self):
        return self.__connectionsudo

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
            return None
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
        cmd = self.connectionsudo+' zfs hold '+self.holdtag+' '+snapshotname # 
        subrun(cmd)
    def is_snap_hold(self,snapshotname):
        ''' Return true, wenn schon auf hold '''
        cmd = self.connection+' zfs holds -H '+snapshotname
        ret = subrun(cmd,stdout=subprocess.PIPE,universal_newlines=True)
        erg = ret.stdout.split('\n')
        for i in erg:
            t1 = i.split('\t')
            if len(t1) < 2:
                return False
            if t1[1] == self.holdtag:
                return True
        return False
    def clear_holdsnaps(self,listholdsnaps):
        ''' Löscht die HOLD-Flags außer der übergebenen Snaps'''
        #print(listholdsnaps)
        for i in self.__get_holdsnaps():
            if i in listholdsnaps:
                pass
            else:
                if self.is_snap_hold(i):
                    cmd = self.connectionsudo+' zfs release -r '+self.holdtag+' '+i
                    subrun(cmd)
    def takenextsnap(self):
        ''' 
        Hier wird ein neuer Snapshot gesetzt - Wenn erfolgreich, dann alle übrigen Holds löschen und den neuen auf 
        Hold setzen
        '''
        aktuell = datetime.datetime.utcnow()
        snapname = self.fs+'@'+self.PREFIX+'_'+aktuell.isoformat()
        ret = subrun(self.connectionsudo+' zfs snapshot '+snapname)
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
    connectionsudo = property(get_connectionsudo, None, None, None)
    snaplist = property(get_snaplist, None, None, None)
    has_encryption = property(get_has_encryption, None, None, None)
    dataset_exist = property(get_dataset_exist, None, None, None)
    pool_has_encryption = property(get_pool_has_encryption, None, None, None)
    
    
    
class zfsbackup(object):
    '''
    Die Vorbereitungen von für den Ablauf der einzelnen Backup-Vorgänge.
    '''
    
    def __init__(self):
        
        self.parameters()
        self.logger.info(f'{APPNAME} - {VERSION}  **************************************** Start')
        self.logger.debug(self.args)
        if imrunning(self.args.kill):
            return
        
        if self.touchfile_handling():
            pass
        else:
            return
        erfolg_all = True
        if self.args.recursion:
            # Dann also mit Rekursion und damit etwas anderes Handling
            self.fslist = []
            if self.collect_fs(self.args.fromfs):
                self.logger.debug(self.fslist)
                if self.args.withoutroot:
                    startlist = 1
                else:
                    startlist= 0
                for fs in self.fslist[startlist:]:
                    # Erzeugen der entsprechenden FS-Paare und Übergabe an zfs_back
                    tofs = self.gettofs(fromroot=self.args.fromfs,fromfs=fs,toroot=self.args.tofs)
                    zfsb = zfs_back(fromfs=fs, tofs=tofs, prefix=self.args.prefix, sshdest=self.args.sshdest \
                             ,holdtag=self.args.holdtag,nosnapshot=self.args.nosnapshot,raw=self.args.raw)
                    erfolg = zfsb.start()
                    if erfolg == False:
                        erfolg_all = False
            else:
                self.logger.info(f'Kein korrektes From-Filesystem übergeben! -> {self.args.fromfs}')
                return
            # return - wollen wir hier eigentlich nicht
        else:
            zfsb = zfs_back(fromfs=self.args.fromfs, tofs=self.args.tofs, prefix=self.args.prefix, sshdest=self.args.sshdest \
                     , holdtag=self.args.holdtag,nosnapshot=self.args.nosnapshot,raw=self.args.raw)#
            erfolg = zfsb.start()
            if erfolg == False:
                erfolg_all = False
        # Wenn wir hier sind, dann war alles ok?!
        if self.args.touch_file and erfolg_all:
            self.logger.debug(f"Now touch the file: {self.args.touch_file} aka {self.touchfile}")
            Path(self.touchfile).touch()
        
        
        
    def gettofs(self,fromroot,fromfs,toroot):
        ''' Ermittelt daraus den korrekten Zielnamen '''
        lenf = len(fromroot)
        fs = fromfs[lenf:]
        if len(fs) > 0:
            tofs = toroot+'/'+fs[1:]
        else:
            tofs = toroot
        return tofs
    
    def touchfile_handling(self):
        ''' Gibt true zurück, wenn das touchfile-handling nichts gegenteiliges aussagt '''
        if self.args.touch_file:
            self.touchfile = os.path.expanduser(self.args.touch_file)
            pass
        else:
            return True # Touchfile spricht nicht gegen Ausführung
        
        
        # Check ob touchfile existiert
        if os.path.exists(self.touchfile):
            # File ist da
            # Jetzt Alter feststellen
            self.logger.debug("Touchfile existiert bereits")
            file_mod_time = datetime.datetime.fromtimestamp(os.path.getmtime(self.touchfile))
            today = datetime.datetime.today()
            age = today - file_mod_time
            daysold = age.days
            self.logger.debug(f'Alter in Tagen: {daysold}')
        else:
            return True # Dann ausführen, da touchfile bisher nicht existiert
        
        if self.args.mindays == -1:
            return True # wenn mindays nicht gesetzt, dann wird immer ausgeführt
        
        if daysold < self.args.mindays:
            # Dann keine Ausführung.
            self.logger.info("Touchfile zu jung")
            return False
        
        if self.args.maxdays == -1:
            # keine Obergrenze, dann Ausführung
            return True
        
        if daysold >= self.args.maxdays:
            self.logger.debug("Touchfile älter als maxdays")
            return True # dann auf jeden Fall ausführen
        
        if random.randrange(daysold,self.args.maxdays) == daysold:
            self.logger.debug("Treffer bei randrange für daysold")
            return True # in der Range und zufällig augewählt
        else:
            self.logger.info("Touchfile in Range aber heute kein Treffer!")
            return False # dann heute noch nicht
        
        
    
    def collect_fs(self,fs):
        ''' Sammelt die FS '''
        arg = shlex.split('zfs list -H -r '+fs)
        liste = subprocess.run(arg,stdout=subprocess.PIPE,universal_newlines=True)
        liste.check_returncode()
        for i in liste.stdout.split('\n')[:-1]:
            temp_fs = i.split('\t')[0]
            self.fslist.append(temp_fs)
        if len(self.fslist) > 0:
            return True
        else:
            return False

    def parameters(self):
        ''' Paramter einlesen und Logger anlegen '''
        parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
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
        parser.add_argument('--holdtag',dest='holdtag',help='Die Bezeichnung des tags für den Hold-Status',default='keep')
        parser.add_argument('-x','--no_snapshot',dest='nosnapshot',help='Verwenden, wenn kein neuer Snapshot erstellt werden soll',default=False,action='store_true')
        parser.add_argument('-r','--recursion',dest='recursion',required='--without-root' in sys.argv,
                            help='Alle Sub-Filesysteme sollen auch übertragen werden',
                            default=False,action='store_true')
        parser.add_argument('--without-root',dest='withoutroot',
                            help="zfsnappy wird nicht auf den root des übergebenen Filesystems angewendet",action="store_true")
        parser.add_argument('-w','--raw',dest='raw',help='Send mit Option --raw für zfs send',default=False,action='store_true')
        parser.add_argument('-k','--kill',dest='kill',help='Andere laufende Instanzen dieses Scripts, die mit den gleichen Aufrufparamtern gestartet wurden, werden gekillt.',default=False,action='store_true')
        parser.add_argument('--touch_file',dest='touch_file',required='--mindays' in sys.argv or '--maxdays' in sys.argv,
                            help='Das File welches einen touch erhält bei erfolgreicher Ausführung.',default=None)
        parser.add_argument('--mindays',dest='mindays',help='Das Touchfile sollte mindestens diese Anzahl Tage alt sein, damit ein Backup gestartet wird',
                            type=int,default=-1)
        parser.add_argument('--maxdays',dest='maxdays',help='Falls randrange(mindays,maxdays) == Alter Touch-File in Tagen, dann backup',
                            type=int,default=-1)
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
        

class zfs_back(object):
    '''
    Hier findet als der reine Backupablauf seinen Platz
    '''
    def __init__(self,fromfs,tofs,prefix,sshdest,holdtag,nosnapshot,raw):
        self.nosnapshot = nosnapshot
        self.raw = raw
        self.holdtag = holdtag
        self.logger = logging.getLogger(LOGNAME)
        self.logger.info(f'Backup von {fromfs} nach {tofs} startet.')
        self.PREFIX = prefix
        if sshdest != None:
            self.sshcmdwithoutsudo = 'ssh -T '+sshdest+' '
            self.sshcmdsudo = self.sshcmdwithoutsudo +'sudo '
        else:
            self.sshcmdsudo = ''
            self.sshcmdwithoutsudo = ''
        self.logger.debug(f'{tofs} - {self.sshcmdwithoutsudo} - {self.sshcmdsudo}')
        self.src = zfs_fs(fromfs,self.PREFIX,holdtag = self.holdtag)
        self.dst = zfs_fs(tofs,self.PREFIX,connectionsudo=self.sshcmdsudo,connection=self.sshcmdwithoutsudo,holdtag=self.holdtag)
        
        
        self.logger.debug(f'SRC: {self.src.fs} exist: {self.src.dataset_exist} encryption: {self.src.has_encryption} pool_encryption: {self.src.pool_has_encryption}')
        self.logger.debug(f'DST: {self.dst.fs} exist: {self.dst.dataset_exist} encryption: {self.dst.has_encryption} pool_encryption: {self.dst.pool_has_encryption}')
    def start(self):
        
        
        
        # 1. Schritt -> Token checken - falls ja, dann Versuch fortsetzen
        if self.dst.dataset_exist:
            token = self.dst.get_token()
        else: 
            token = None
        
        if token != None:
            return self.resume_transport(token)

        
        # 2. Schritt -> Wie lautet der neueste identische Snapshot?
        lastmatch = self.get_lastmatch()
        
        if lastmatch == None:
            # es gibt also keinen identischen Snapshot -> Damit Versuch neuen Snapshot zu senden und fs zu senden
            if self.dst.dataset_exist:
                self.logger.error(f'Das Zieldataset existiert bereits und es gibt keinen identischen Snapshot')
                return False
            if self.src.has_encryption and self.dst.pool_has_encryption == False:
                self.logger.error('Das Source-Dataset hat encryption aktiv, aber der Zielpool nicht!')
                return False
            if self.nosnapshot:
                newsnap = self.src.lastsnap
            else:
                newsnap = self.src.takenextsnap()
            if newsnap == None:
                self.logger.error('Kein Snapshot zum Senden vorhanden!')
                return False
            self.src.hold_snap(newsnap)
            if self.raw:
                # add w to command
                addcmd = '-w'
            else:
                addcmd = ''
            cmdfrom = f'zfs send {addcmd} {newsnap}' # -v mal weggelassen
            cmdto = sshcmdsudo+'zfsbackup_receiver zfs receive -vs -o compression=lz4 -o rdonly=on '+self.dst.fs # neues Filesystem am Ziel erstellen
            subrunPIPE(cmdfrom,cmdto)
            
            self.src.clear_holdsnaps((newsnap,))
            self.dst_hold_update(newsnap)
            return True
        
        else:
            # es gibt also einen gemeinsamen Snapshot - neuen Snapshot erstellen und inkrementell senden
            if self.nosnapshot:
                newsnap = self.src.lastsnap
            else:
                newsnap = self.src.takenextsnap()
            if newsnap == None:
                self.logger.error('Kein Snapshot zum Senden vorhanden!')
                return False
            oldsnap = self.src.fs+'@'+self.PREFIX+'_'+lastmatch
            if oldsnap == newsnap:
                self.logger.info(f"Keine neuen Snapshots zu übertragen. {newsnap} ist bereits am Ziel vorhanden")
                return True
            self.src.hold_snap(newsnap)
            if self.raw:
                # add w to command
                addcmd = '-w'
            else:
                addcmd = ''
            cmdfrom = f'zfs send {addcmd} -i {oldsnap} {newsnap}'
            cmdto =  self.sshcmdsudo+'zfsbackup_receiver zfs receive -vs '+self.dst.fs  # Versuch ohne -F vs. 2021/08/31
            subrunPIPE(cmdfrom,cmdto)
            self.src.clear_holdsnaps((oldsnap,newsnap))
            self.dst_hold_update(newsnap)
            return True
        
    def get_snapname(self,snapshotname):
        a = snapshotname.split('@')
        return a[1]
    
    
        
    def get_lastmatch(self):
        ''' Sucht den letzten identischen Snapshot '''
        lastmatch = None
        for i in self.src.snaplist:
            if i in self.dst.snaplist:
                lastmatch = i
        return lastmatch
    def resume_transport(self,token):
        # Setzt den Transport fort 
        if self.raw:
            # add w to command
            addcmd = 'w'
        else:
            addcmd = ''
        cmdfrom = f'zfs send -{addcmd}vt {token}'
        cmdto = self.dst.connectionsudo+' zfsbackup_receiver zfs receive -vs '+self.dst.fs
        output = subrunPIPE(cmdfrom, cmdto)
        fromsnapshot = None
        for i in output:
            j = i.split()
            self.logger.debug(j)
            if j[0] == "toname" and j[1] == '=':
                fromsnapshot = j[2]
        if fromsnapshot == None:
            self.logger.error('Das Resume scheint nicht gelungen...')
            return False
        self.dst_hold_update(fromsnapshot)
        self.src.clear_holdsnaps((fromsnapshot,)) # alle aus den fromsnapshot releasen
        return True
        
    def dst_hold_update(self,fromsnap):
        ''' setzt den aktuell übertragenen Snap auf Hold und released die anderen '''
        self.dst.updatesnaplist() # neu aufbauen, da neuer Snap vorhanden
        self.logger.debug(f'Dieser Snap im dst wird auf Hold gesetzt: {self.dst.lastsnap}')
        destsnap = self.gettargetname(tofs=self.dst.fs,fromsnap=fromsnap)
        self.dst.hold_snap(destsnap)
        self.dst.clear_holdsnaps((destsnap,))
    def gettargetname(self,tofs,fromsnap):
        
        ''' Ermittelt daraus den korrekten Zielnamen des Snapshots'''
        s1,s2 = fromsnap.split('@')
        tosnap = tofs+'@'+s2 
        return tosnap
                
   
if __name__ == '__main__':
    assert sys.version_info >= (3,5)
    zfs = zfsbackup()
    zfs.logger.info(f'{APPNAME} - {VERSION}  *************************************** Stop')
        