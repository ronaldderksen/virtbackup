import 'dart:async';
import 'dart:isolate';
import 'dart:io';

import 'package:virtbackup/agent/backup.dart';
import 'package:virtbackup/agent/backup_host.dart';
import 'package:virtbackup/agent/drv/backup_storage.dart';
import 'package:virtbackup/agent/drv/dummy_driver.dart';
import 'package:virtbackup/agent/drv/filesystem_driver.dart';
import 'package:virtbackup/agent/drv/gdrive_driver.dart';
import 'package:virtbackup/agent/drv/sftp_driver.dart';
import 'package:virtbackup/common/models.dart';
import 'package:virtbackup/common/settings.dart';

const String _typeReady = 'ready';
const String _typeStart = 'start';
const String _typeCancel = 'cancel';
const String _typeProgress = 'progress';
const String _typeResult = 'result';
const String _typeSettings = 'settings';
const String _typeLog = 'log';
const bool _isDebug = !bool.fromEnvironment('dart.vm.product');

Future<void> _deleteSharedBlobCacheIfNeeded(BackupAgentHost host, AppSettings settings, String driverId) async {
  // The shared blobstore at ${backup.base_path}/VirtBackup/blobs is intended as a restore/download cache for remote drivers.
  // Note: for the filesystem driver this deletes actual backups; this is still allowed here because it is debug-only and explicitly requested.
  final basePath = settings.backupPath.trim();
  if (basePath.isEmpty) {
    host.logInfo('Fresh cleanup: backup.base_path is empty; skipping local blob cache cleanup.');
    return;
  }
  final sep = Platform.pathSeparator;
  final blobsDir = Directory('$basePath${sep}VirtBackup${sep}blobs');
  if (!await blobsDir.exists()) {
    host.logInfo('Fresh cleanup: local blob cache not found: ${blobsDir.path}');
    return;
  }
  host.logInfo('Fresh cleanup: deleting local blob cache: ${blobsDir.path}');
  try {
    await blobsDir.delete(recursive: true);
  } catch (error, stackTrace) {
    host.logError('Fresh cleanup: failed to delete local blob cache.', error, stackTrace);
  }
}

void backupWorkerMain(Map<String, dynamic> init) {
  final mainPort = init['sendPort'] as SendPort;
  final commandPort = ReceivePort();
  mainPort.send({'type': _typeReady, 'sendPort': commandPort.sendPort});

  BackupAgent? agent;
  var running = false;

  Future<void> runBackup(Map<String, dynamic> payload) async {
    if (running) {
      return;
    }
    running = true;
    final jobId = payload['jobId']?.toString() ?? '';
    final driverId = payload['driverId']?.toString() ?? 'filesystem';
    final backupPath = payload['backupPath']?.toString() ?? '';
    final driverParams = Map<String, dynamic>.from(payload['driverParams'] as Map? ?? const {});
    final freshRequested = payload['fresh'] == true;
    final settingsMap = Map<String, dynamic>.from(payload['settings'] as Map? ?? const {});
    final serverMap = Map<String, dynamic>.from(payload['server'] as Map? ?? const {});
    final vmMap = Map<String, dynamic>.from(payload['vm'] as Map? ?? const {});

    final settings = AppSettings.fromMap(settingsMap);
    final server = ServerConfig.fromMap(serverMap);
    final vm = VmEntry.fromMap(vmMap);

    final host = BackupAgentHost(
      onInfo: (message) => mainPort.send({'type': _typeLog, 'level': 'info', 'message': message}),
      onError: (message, error, stackTrace) => mainPort.send({'type': _typeLog, 'level': 'error', 'message': '$message $error'}),
      includeTimestamp: false,
    );

    BackupDriver buildDriver() {
      final factories = <String, BackupDriver Function(Map<String, dynamic>)>{
        'dummy': (params) => DummyBackupDriver(backupPath.trim(), tmpWritesEnabled: settings.dummyDriverTmpWrites, driverParams: params),
        'gdrive': (_) => GdriveBackupDriver(
          settings: settings,
          persistSettings: (updated) async => mainPort.send({'type': _typeSettings, 'settings': updated.toMap()}),
          logInfo: (message) => mainPort.send({'type': _typeLog, 'level': 'info', 'message': message}),
        ),
        'filesystem': (_) => FilesystemBackupDriver(backupPath.trim()),
        'sftp': (_) => SftpBackupDriver(settings: settings, logInfo: (message) => mainPort.send({'type': _typeLog, 'level': 'info', 'message': message})),
      };
      final factory = factories[driverId] ?? factories['filesystem']!;
      return factory(driverParams);
    }

    final driver = buildDriver();
    final dependencies = host.buildDependencies();
    agent = BackupAgent(
      dependencies: dependencies,
      onProgress: (progress) => mainPort.send({'type': _typeProgress, 'jobId': jobId, 'progress': progress.toMap()}),
      onInfo: host.logInfo,
      onError: host.logError,
      hashblocksLimitBufferMb: settings.hashblocksLimitBufferMb,
    );

    try {
      if (freshRequested && _isDebug) {
        host.logInfo('Fresh cleanup requested (debug only).');
        await _deleteSharedBlobCacheIfNeeded(host, settings, driverId);
        await driver.freshCleanup();
      }
      final result = await agent!.runVmBackup(server: server, vm: vm, driver: driver);
      mainPort.send({'type': _typeResult, 'jobId': jobId, 'result': result.toMap()});
    } catch (error, _) {
      mainPort.send({'type': _typeLog, 'level': 'error', 'message': 'Backup worker failed: $error'});
      mainPort.send({'type': _typeResult, 'jobId': jobId, 'result': BackupAgentResult(success: false, message: error.toString()).toMap()});
    }
  }

  commandPort.listen((message) async {
    final payload = Map<String, dynamic>.from(message as Map);
    final type = payload['type']?.toString();
    if (type == _typeStart) {
      await runZonedGuarded(
        () async {
          await runBackup(payload);
        },
        (error, _) {
          mainPort.send({'type': _typeLog, 'level': 'error', 'message': 'Backup worker unhandled error: $error'});
          mainPort.send({'type': _typeResult, 'jobId': payload['jobId']?.toString() ?? '', 'result': BackupAgentResult(success: false, message: error.toString()).toMap()});
        },
      );
      Isolate.exit();
    } else if (type == _typeCancel) {
      agent?.cancel();
    }
  });
}
