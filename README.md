# zfsbackup
Sichert zfs-snapshots in andere zfs-pools/datasets - über ssh auch auf remote-systeme
```
usage: zfsbackup [-h] [-f FROMFS] -t TOFS [-s SSHDEST] [-d] [-p PREFIX] [--holdtag HOLDTAG] [-x] [-r] [--without-root] [-w] [-k] [--touch_file TOUCH_FILE]
                 [--mindays MINDAYS] [--maxdays MAXDAYS] [--bandwith-limit BANDWITH_LIMIT]

options:
  -h, --help            show this help message and exit
  -f FROMFS, --from FROMFS
                        Übergabe des ZFS-Filesystems welches gesichert werden soll (default: None)
  -t TOFS, --to TOFS    Übergabe des ZFS-Filesystems auf welches gesichert werden soll (default: None)
  -s SSHDEST, --sshdest SSHDEST
                        Übergabe des per ssh zu erreichenden Destination-Rechners (default: None)
  -d                    Debug-Level-Ausgaben (default: False)
  -p PREFIX, --prefix PREFIX
                        Der Prefix für die Bezeichnungen der Snapshots (default: zfsnappy)
  --holdtag HOLDTAG     Die Bezeichnung des tags für den Hold-Status (default: keep)
  -x, --no_snapshot     Verwenden, wenn kein neuer Snapshot erstellt werden soll (default: False)
  -r, --recursion       Alle Sub-Filesysteme sollen auch übertragen werden (default: False)
  --without-root        zfsnappy wird nicht auf den root des übergebenen Filesystems angewendet (default: False)
  -w, --raw             Send mit Option --raw für zfs send (default: False)
  -k, --kill            Andere laufende Instanzen dieses Scripts, die mit den gleichen Aufrufparamtern gestartet wurden, werden gekillt. (default: False)
  --touch_file TOUCH_FILE
                        Das File welches einen touch erhält bei erfolgreicher Ausführung. (default: None)
  --mindays MINDAYS     Das Touchfile sollte mindestens diese Anzahl Tage alt sein, damit ein Backup gestartet wird (default: -1)
  --maxdays MAXDAYS     Falls randrange(mindays,maxdays) == Alter Touch-File in Tagen, dann backup (default: -1)
  --bandwith-limit BANDWITH_LIMIT
                        Limitiert die Bandbreite in Bytes pro Sekunde (Anhänge K,M,G,T sind erlaubt) - Beispiel --bandwith-limit 50M = Limit auf 50
                        MByte/sec (default: None)

```
