# Fallback inventory

Dit bestand bevat een inventaris van fallback-gedrag dat nu in de code staat.
Scope: `lib/` en `tools/backup_verify.dart` (runtime en configgedrag).

## 1) Log writer: pad fallback naar `/var`
- Locatie: `lib/common/log_writer.dart:15`
- Gedrag: als `basePath` leeg is, gebruikt de logger automatisch `/var/VirtBackup/logs/<source>.log`.
- Doel: logging blijft werken als er nog geen expliciet pad is geconfigureerd.
- Effect/risico: kan logs op een onverwachte locatie schrijven (bijv. permissieproblemen of verwarring over waar logs staan).

## 2) Log writer: level fallback naar `console`
- Locatie: `lib/common/log_writer.dart:9`, `lib/common/log_writer.dart:90`
- Gedrag: als er geen level voor een source is gezet, wordt `console` gebruikt.
- Doel: voorspelbare minimale logging zonder extra configuratie.
- Effect/risico: mogelijk meer logging dan gewenst als level niet expliciet gezet is.

## 3) GUI: fallback drivercatalogus als agentdrivers niet geladen kunnen worden
- Locatie: `lib/gui/main_screen.dart:636`, `lib/gui/main_screen.dart:646`
- Gedrag: bij fout of lege response van `/drivers` gebruikt de GUI een lokale fallbacklijst (`filesystem`, `dummy`).
- Doel: GUI bruikbaar houden als agent (tijdelijk) niet beschikbaar is.
- Effect/risico: GUI kan afwijken van agent-capabilities (bijv. drivers ontbreken of verschillen).

## 4) GUI: backup destination selectie valt terug op eerste enabled destination
- Locatie: `lib/gui/main_screen.dart:697`
- Gedrag: als huidige/voorkeurs-destination ongeldig is, kiest GUI `enabled.first`.
- Doel: voorkomen dat backup-tab zonder selectie komt te staan.
- Effect/risico: backup kan onbedoeld naar andere destination gaan als configuratie vervuild is.

## 5) AppSettings: `log_level` fallback naar `console`
- Locatie: `lib/common/settings.dart:149`
- Gedrag: lege of ontbrekende `log_level` wordt `console`.
- Doel: altijd een geldig logniveau.
- Effect/risico: implicit default i.p.v. hard-fail op ontbrekende config.

## 6) AppSettings: backup destination id fallback
- Locatie: `lib/common/settings.dart:151`
- Gedrag: als `backupDestinationId` ontbreekt/leeg is, wordt resolved destination id gebruikt.
- Doel: backup blijft functioneel zonder expliciete selectie.
- Effect/risico: gekozen target kan impliciet zijn.

## 7) AppSettings: `backupDriverId` fallback naar `filesystem`
- Locatie: `lib/common/settings.dart:153`
- Gedrag: als geselecteerde destination geen geldige driverId heeft, wordt `filesystem` gebruikt.
- Doel: altijd een bruikbare driver in memory-settings.
- Effect/risico: maskeert foutieve destination-data.

## 8) AppSettings: defaults voor gdrive/sftp velden
- Locatie: `lib/common/settings.dart:158` t/m `lib/common/settings.dart:168`
- Gedrag:
  - gdrive scope fallback: `https://www.googleapis.com/auth/drive.file`
  - gdrive rootPath fallback: `/`
  - ontbrekende tokens/account -> lege string
  - sftp port fallback via parser (zie item 11)
- Doel: parsing robuust houden bij incomplete config.
- Effect/risico: incomplete configuratie kan "geldig" lijken tot later runtime-fout.

## 9) AppSettings: verplichte `filesystem` destination wordt altijd toegevoegd/geforceerd
- Locatie: `lib/common/settings.dart:249`
- Gedrag: `_ensureFilesystemDestination` voegt destination `id=filesystem` toe of overschrijft die naar vaste vorm (`enabled: true`, `driverId: filesystem`).
- Doel: gegarandeerde lokale cache/basispad-bron voor drivers die die nodig hebben.
- Effect/risico: corrigeert stilzwijgend config i.p.v. invalidatie.

## 10) AppSettings: destination resolving fallback-volgorde
- Locatie: `lib/common/settings.dart:220`
- Gedrag: requested id -> eerste enabled -> anders eerste destination.
- Doel: altijd een selectie kunnen maken.
- Effect/risico: implicit targetkeuze bij inconsistente config.

## 11) AppSettings: numerieke parser defaults
- Locatie: `lib/common/settings.dart:294`, `lib/common/settings.dart:302`
- Gedrag:
  - `hashblocksLimitBufferMb` -> `1024` bij leeg/ongeldig/<=0
  - `sftpPort` -> `22` bij leeg/ongeldig/out-of-range
- Doel: veilige numerieke defaults.
- Effect/risico: verbergt configfouten totdat gedrag afwijkt.

## 12) Agent HTTP: onbekende driver valt terug op `filesystem`
- Locatie: `lib/agent/http_server.dart:1120`, `lib/agent/http_server.dart:1413`, `lib/agent/http_server.dart:1783`, `lib/agent/http_server.dart:869`, `lib/agent/http_server.dart:1554`
- Gedrag: lookup in driverregistry/catalogus gebruikt `registry['filesystem']` als fallback bij missende driver.
- Doel: jobs en catalogus laten doorgaan bij ongeldige driverId.
- Effect/risico: verkeerde backend kan stilzwijgend gekozen worden.

## 13) Agent HTTP: default driver keuze gebruikt legacy `backupDriverId` en daarna `filesystem`
- Locatie: `lib/agent/http_server.dart:671`, `lib/agent/http_server.dart:1411`, `lib/agent/http_server.dart:1781`
- Gedrag: zonder override gebruikt code destination driver; anders `backupDriverId`; als die leeg is -> `filesystem`.
- Doel: backward-compatibele driverselectie.
- Effect/risico: legacy veld beïnvloedt nog runtimekeuze.

## 14) Agent HTTP: backupPath fallback naar `_agentSettings.backupPath`
- Locatie: `lib/agent/http_server.dart:870`, `lib/agent/http_server.dart:1785`
- Gedrag: als resolved destination ontbreekt, valt pad terug op `backupPath` uit settings.
- Doel: operationeel blijven als destinationresolutie niet beschikbaar is.
- Effect/risico: menging destination-model met oud padmodel.

## 15) Agent HTTP: standaard destination fallback
- Locatie: `lib/agent/http_server.dart:1142`, `lib/agent/http_server.dart:1153`
- Gedrag: zonder requested id kiest agent `backupDestinationId`, anders eerste enabled destination.
- Doel: backup/restore zonder expliciete destination mogelijk maken.
- Effect/risico: implicit target bij ontbrekende keuze.

## 16) Agent HTTP: non-filesystem destinations gebruiken filesystem-path als lokale cachebasis
- Locatie: `lib/agent/http_server.dart:1165`, `lib/agent/http_server.dart:1173`
- Gedrag: voor `sftp/gdrive/dummy` retourneert `_backupPathForDestination` de path van destination `filesystem`.
- Doel: centrale lokale cache-locatie voor drivers die lokaal temp/blobpad nodig hebben.
- Effect/risico: harde afhankelijkheid van filesystem destination en diens `params.path`.

## 17) Agent HTTP: destination label fallback in notificaties
- Locatie: `lib/agent/http_server.dart:1969`
- Gedrag: `target` -> anders `driverLabel` -> anders `Unknown destination`.
- Doel: notificaties altijd leesbaar houden.
- Effect/risico: kan echte destination-identiteit verbergen.

## 18) Agent endpoint `/sftp/test`: requestvelden vallen terug op opgeslagen settings
- Locatie: `lib/agent/http_server.dart:434` t/m `lib/agent/http_server.dart:439`
- Gedrag: als body geen host/port/user/pass/basePath bevat, worden waarden uit `_agentSettings` gebruikt.
- Doel: snel testen met deels ingevulde payload.
- Effect/risico: test gebruikt mogelijk andere waarden dan verwacht door caller.

## 19) Backup worker: onbekende driver valt terug op `filesystem`
- Locatie: `lib/agent/backup_worker.dart:60`, `lib/agent/backup_worker.dart:89`
- Gedrag: `driverId` default `filesystem`; onbekende key in factorymap -> filesystem-factory.
- Doel: isolate crasht niet op ongeldige driverId.
- Effect/risico: foutieve driverkeuze blijft verborgen.

## 20) Restore worker: onbekende driver valt terug op `filesystem`
- Locatie: `lib/agent/restore_worker.dart:96`, `lib/agent/restore_worker.dart:123`
- Gedrag: zelfde patroon als backup worker.
- Doel: restore isolate robuust bij ongeldige payload.
- Effect/risico: restore kan met verkeerde driver starten.

## 21) SettingsStore: legacy encrypt/decrypt paden voor oud `backup.*` blok
- Locatie:
  - `lib/agent/settings_store.dart:281` t/m `lib/agent/settings_store.dart:331` (gdrive)
  - `lib/agent/settings_store.dart:333` t/m `lib/agent/settings_store.dart:370` (sftp)
- Gedrag: naast nieuwe `destinations[*].params` verwerkt code nog tokens/passwords onder `backup.gdrive` en `backup.sftp`.
- Doel: backward compatibility voor oudere configbestanden.
- Effect/risico: legacy pad blijft actief ondanks destination-migratie; kan oude data in leven houden.

## 22) Drivers: chain-file lookup fallback naar legacy bestandsnaam
- Locatie:
  - `lib/agent/drv/filesystem_driver.dart:209`
  - `lib/agent/drv/sftp_driver.dart:603`
  - `lib/agent/drv/gdrive_driver.dart:554`
- Gedrag: zoekt eerst nieuwe chainnaam `${timestamp}__${diskId}.chain`, anders legacy `${timestamp}.chain` in diskdir.
- Doel: restore compatibel houden met oudere backup-layouts.
- Effect/risico: legacy artifacts blijven invloed hebben op restorepad.

## 23) Tool `backup_verify`: host fallback
- Locatie: `tools/backup_verify.dart:683`
- Gedrag: `_resolveHost(...)` retourneert expliciete waarde of fallback-host wanneer serverlijst/entry ontbreekt.
- Doel: verificatietool bruikbaar houden met minimale argumenten.
- Effect/risico: test kan tegen fallback-host draaien i.p.v. bedoelde server.

## Opmerking
- De lijst hierboven bevat functionele fallback-mechanismen die runtimegedrag beïnvloeden.
- Daarnaast bevat de code veel parse/default-constructies (`?? ''`, `orElse`, etc.) voor defensieve datamapping; die zijn niet allemaal apart uitgewerkt tenzij ze operationele keuzes beïnvloeden.
