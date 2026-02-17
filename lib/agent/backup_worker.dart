import 'dart:async';
import 'dart:isolate';

import 'package:virtbackup/agent/backup.dart';
import 'package:virtbackup/agent/backup_host.dart';
import 'package:virtbackup/agent/drv/backup_storage.dart';
import 'package:virtbackup/agent/drv/dummy_driver.dart';
import 'package:virtbackup/agent/drv/filesystem_driver.dart';
import 'package:virtbackup/agent/drv/gdrive_driver.dart';
import 'package:virtbackup/agent/drv/sftp_driver.dart';
import 'package:virtbackup/common/log_writer.dart';
import 'package:virtbackup/common/models.dart';
import 'package:virtbackup/common/settings.dart';

const String _typeReady = 'ready';
const String _typeStart = 'start';
const String _typeCancel = 'cancel';
const String _typeProgress = 'progress';
const String _typeResult = 'result';
const String _typeSettings = 'settings';
const bool _isDebug = !bool.fromEnvironment('dart.vm.product');

Future<void> _deleteSharedBlobCacheIfNeeded() async {
  LogWriter.logAgentSync(level: 'info', message: 'Fresh cleanup: skipping local filesystem blob cleanup.');
}

int? _parseBlockSizeMBOverride(Object? value) {
  if (value == null) {
    return null;
  }
  final parsed = value is num ? value.toInt() : int.tryParse(value.toString().trim());
  if (parsed == null || (parsed != 1 && parsed != 2 && parsed != 4 && parsed != 8)) {
    throw StateError('Invalid blockSizeMB override. Allowed values: 1, 2, 4, 8.');
  }
  return parsed;
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
    final blockSizeMBOverride = _parseBlockSizeMBOverride(payload['blockSizeMB']);
    final freshRequested = payload['fresh'] == true;
    final settingsMap = Map<String, dynamic>.from(payload['settings'] as Map? ?? const {});
    final storageMap = Map<String, dynamic>.from(payload['storage'] as Map? ?? const {});
    final serverMap = Map<String, dynamic>.from(payload['server'] as Map? ?? const {});
    final vmMap = Map<String, dynamic>.from(payload['vm'] as Map? ?? const {});

    final settings = AppSettings.fromMap(settingsMap);
    final effectiveSettings = blockSizeMBOverride == null ? settings : settings.copyWith(blockSizeMB: blockSizeMBOverride);
    final selectedStorage = storageMap.isEmpty ? null : BackupStorage.fromMap(storageMap);
    final isFilesystemStorage = selectedStorage?.id == AppSettings.filesystemStorageId;
    final uploadConcurrency = isFilesystemStorage ? null : selectedStorage?.uploadConcurrency ?? (throw StateError('uploadConcurrency is required for storage "${selectedStorage?.id ?? ''}".'));
    final server = ServerConfig.fromMap(serverMap);
    final vm = VmEntry.fromMap(vmMap);
    await LogWriter.configureSourcePath(
      source: 'agent',
      path: LogWriter.defaultPathForSource('agent', basePath: effectiveSettings.backupPath.trim()),
    );
    LogWriter.configureSourceLevel(source: 'agent', level: effectiveSettings.logLevel);

    final host = BackupAgentHost();

    BackupDriver buildDriver() {
      final factories = <String, BackupDriver Function(Map<String, dynamic>)>{
        'dummy': (params) => DummyBackupDriver(backupPath.trim(), tmpWritesEnabled: effectiveSettings.dummyDriverTmpWrites, blockSizeMB: effectiveSettings.blockSizeMB, driverParams: params),
        'gdrive': (_) => GdriveBackupDriver(
          settings: effectiveSettings,
          persistSettings: (updated) async => mainPort.send({'type': _typeSettings, 'settings': updated.toMap()}),
          logInfo: (message) => LogWriter.logAgentSync(level: 'info', message: message),
        ),
        'filesystem': (_) => FilesystemBackupDriver(backupPath.trim(), blockSizeMB: effectiveSettings.blockSizeMB),
        'sftp': (_) => SftpBackupDriver(settings: effectiveSettings, poolSessions: uploadConcurrency),
      };
      final factory = factories[driverId] ?? factories['filesystem']!;
      return factory(driverParams);
    }

    final driver = buildDriver();
    final dependencies = host.buildDependencies();
    agent = BackupAgent(
      dependencies: dependencies,
      onProgress: (progress) => mainPort.send({'type': _typeProgress, 'jobId': jobId, 'progress': progress.toMap()}),
      blockSizeBytes: effectiveSettings.blockSizeMB * 1024 * 1024,
      onInfo: (message) => LogWriter.logAgentSync(level: 'info', message: message),
      onError: (message, error, stackTrace) {
        LogWriter.logAgentSync(level: 'info', message: '$message $error');
        LogWriter.logAgentSync(level: 'info', message: stackTrace.toString());
      },
      hashblocksLimitBufferMb: effectiveSettings.hashblocksLimitBufferMb,
      writerConcurrencyOverride: uploadConcurrency,
    );

    try {
      if (freshRequested && _isDebug) {
        // Prepare local debug log first so fresh-cleanup logs are not lost by a later ensureReady() call.
        await driver.ensureReady();
        LogWriter.logAgentSync(level: 'info', message: 'Fresh cleanup requested (debug only).');
        await _deleteSharedBlobCacheIfNeeded();
        await driver.freshCleanup();
      }
      final result = await agent!.runVmBackup(server: server, vm: vm, driver: driver);
      mainPort.send({'type': _typeResult, 'jobId': jobId, 'result': result.toMap()});
    } catch (error, _) {
      LogWriter.logAgentSync(level: 'info', message: 'Backup worker failed: $error');
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
          mainPort.send({'type': _typeResult, 'jobId': payload['jobId']?.toString() ?? '', 'result': BackupAgentResult(success: false, message: error.toString()).toMap()});
        },
      );
      Isolate.exit();
    } else if (type == _typeCancel) {
      agent?.cancel();
    }
  });
}
