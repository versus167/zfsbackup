# zfsbackup
Sichert zfs-snapshots in andere zfs-pools/datasets - über ssh auch auf remote-systeme
```
usage: zfsbackup [-h] [-f FROMFS] -t TOFS [-s SSHDEST] [-d] [-p PREFIX] [--holdtag HOLDTAG] [-x] [-r] [-w] [-k]

optional arguments:
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
  -w, --raw             Send mit Option --raw für zfs send (default: False)
  -k, --kill            Andere laufende Instanzen dieses Scripts, die mit den gleichen Aufrufparamtern gestartet wurden, werden
                        gekillt. (default: False)
```
