# zfsbackup
Sichert zfs-snapshots in andere zfs-pools/datasets - über ssh auch auf remote-systeme
```
usage: zfsbackup [-h] [-f FROMFS] -t TOFS [-s SSHDEST] [-d] [-p PREFIX]
                 [--holdtag HOLDTAG] [-x] [-r] [--without-root] [-w] [-k]
                 [--touch_file TOUCH_FILE] [--mindays MINDAYS]
                 [--maxdays MAXDAYS] [--bandwith-limit BANDWITH_LIMIT]
                 [--target_key_file TARGET_KEY_FILE]
                 [--target_encrypted_root TARGET_ENCRYPTED_ROOT]

options:
  -h, --help            show this help message and exit
  -f FROMFS, --from FROMFS
                        Übergabe des ZFS-Filesystems welches gesichert werden
                        soll (default: None)
  -t TOFS, --to TOFS    Übergabe des ZFS-Filesystems auf welches gesichert
                        werden soll - relativ zum target-encrypted-root falls
                        angegeben, sonst absolut (default: None)
  -s SSHDEST, --sshdest SSHDEST
                        Übergabe des per ssh zu erreichenden Destination-
                        Rechners (default: None)
  -d                    Debug-Level-Ausgaben (default: False)
  -p PREFIX, --prefix PREFIX
                        Der Prefix für die Bezeichnungen der Snapshots
                        (default: zfsnappy)
  --holdtag HOLDTAG     Die Bezeichnung des tags für den Hold-Status (default:
                        keep)
  -x, --no_snapshot     Verwenden, wenn kein neuer Snapshot erstellt werden
                        soll (default: False)
  -r, --recursion       Alle Sub-Filesysteme sollen auch übertragen werden
                        (default: False)
  --without-root        zfsbackup wird nicht auf den root des übergebenen
                        Filesystems angewendet (default: False)
  -w, --raw             Send mit Option --raw für zfs send (nicht mit -s)
                        (default: False)
  -k, --kill            Andere laufende Instanzen dieses Scripts, die mit den
                        gleichen Aufrufparamtern gestartet wurden, werden
                        gekillt. (default: False)
  --touch_file TOUCH_FILE
                        Das File welches einen touch erhält bei erfolgreicher
                        Ausführung. (default: None)
  --mindays MINDAYS     Das Touchfile sollte mindestens diese Anzahl Tage alt
                        sein, damit ein Backup gestartet wird (default: -1)
  --maxdays MAXDAYS     Falls randrange(mindays,maxdays) == Alter Touch-File
                        in Tagen, dann backup (default: -1)
  --bandwith-limit BANDWITH_LIMIT
                        Limitiert die Bandbreite in Bytes pro Sekunde (Anhänge
                        K,M,G,T sind erlaubt) - Beispiel --bandwith-limit 50M
                        = Limit auf 50 MByte/sec (default: None)
  --target_key_file TARGET_KEY_FILE
                        Textdatei mit dem Encryption-Key für das Zieldataset
                        (wird vor dem Backup geladen und danach wieder
                        entladen). (default: None)
  --target_encrypted_root TARGET_ENCRYPTED_ROOT
                        Verschlüsseltes Root-Dataset auf dem Ziel, unter dem
                        das Zieldataset liegt (z.B. tank/backuproot).
                        (default: None)


```

## zfsbackup_receiver (Zielrechner bei `-s`)
Am Ziel ruft zfsbackup per `ssh … sudo zfsbackup_receiver zfs …` nur `receive`, `hold`, `release`, `load-key` und `unload-key` auf.
Seit 2026.34 darf jeder Benutzer (`SUDO_USER`) nur in den Datasets arbeiten, die in `/etc/zfsbackup_receiver.conf` für ihn freigegeben sind (inklusive aller Kinder und Snapshots). Fehlt die Datei oder der Benutzer, lehnt der Receiver jeden Aufruf ab. Die Datei muss root gehören und darf nur für root schreibbar sein.

**Nicht kompatibel mit 2026.33 und älter:** Die Paketinstallation allein reicht am Ziel nicht mehr, die Datei muss angelegt werden. Eine Vorlage liegt unter `/usr/share/doc/zfsbackup/examples/zfsbackup_receiver.conf`:
```
install -o root -g root -m 0644 /usr/share/doc/zfsbackup/examples/zfsbackup_receiver.conf /etc/
```
Beispiel:
```
# /etc/zfsbackup_receiver.conf  -  <benutzer> <dataset> [<dataset> ...]
lxc_back  tank/backup tank/backup_nd tank/backup_nb
vsb       tank/vsb
```
Außerdem nimmt der Receiver seit 2026.34 nur einfache, nicht-rohe Ströme an. Rohe Ströme (`zfs send -w`) lehnt er ab, weil ein roher inkrementeller Strom ein `zfs change-key` der Quelle auf das Ziel überträgt. Der Sender könnte so den Schlüssel am Ziel ersetzen. Zusammengesetzte Ströme (`-R`, `-I`, `-p`) lehnt er ebenfalls ab. Verschlüsselte Datasets sichert man deshalb ohne `-w` in ein verschlüsseltes Ziel mit eigenem Schlüssel:
```
zfsbackup -f zfshome/daten -t daten -s backup@ziel --target_encrypted_root tank/backuproot --target_key_file /root/ziel.key
```

Vor jedem Empfang setzt der Receiver auf dem passenden Config-Eintrag `setuid=off devices=off`. Ist der Eintrag selbst das neue Ziel, setzt er beides direkt nach dem Empfang. So kann ein eingehängtes Backup keine setuid-Programme oder Gerätedateien des Senders benutzbar machen. Einhängen und Dateien zurückholen geht weiterhin, die Bits bleiben in den Dateien erhalten. Weil das für alles unterhalb eines Eintrags gilt, gehören nur reine Backup-Zweige in die Config. Hat ein Admin darunter bewusst `setuid=on` oder `devices=on` gesetzt, lehnt der Receiver den Empfang dorthin ab.

Die mitgelieferte sudoers-Regel gilt für `ALL`. Besser ist es, sie auf eine Gruppe zu beschränken (z.B. `%zfsrecv ALL = (root) NOPASSWD: C_ZFSBACKUP_RECEIVER`).
