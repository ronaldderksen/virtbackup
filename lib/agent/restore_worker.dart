import 'dart:async';
import 'dart:collection';
import 'dart:convert';
import 'dart:io';
import 'dart:isolate';
import 'dart:typed_data';
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
const String _typeStatus = 'status';
const String _typeResult = 'result';
const String _typeSettings = 'settings';
const String _typeContext = 'context';

void restoreWorkerMain(Map<String, dynamic> init) {
  final mainPort = init['sendPort'] as SendPort;
  final commandPort = ReceivePort();
  mainPort.send({'type': _typeReady, 'sendPort': commandPort.sendPort});

  var canceled = false;

  void sendStatus(AgentJobStatus status) {
    mainPort.send({'type': _typeStatus, 'jobId': status.id, 'status': status.toMap()});
  }

  void sendResult(AgentJobStatus status) {
    mainPort.send({'type': _typeResult, 'jobId': status.id, 'status': status.toMap()});
  }

  void ensureNotCanceled() {
    if (canceled) {
      throw const _Canceled();
    }
  }

  Future<void> defineOnly(String jobId, BackupAgentHost host, ServerConfig server, File xmlFile, String xmlContent, String timestamp, String vmName) async {
    final remoteXmlPath = '/var/tmp/virtbackup/restore-${_sanitizeFileName(timestamp)}-${_sanitizeFileName(vmName)}.xml';
    await host.runSshCommand(server, 'mkdir -p "/var/tmp/virtbackup"');
    final xmlTempFile = File('${xmlFile.path}.restore_tmp');
    await xmlTempFile.writeAsString(xmlContent);
    try {
      sendStatus(
        AgentJobStatus(
          id: jobId,
          type: AgentJobType.restore,
          state: AgentJobState.running,
          message: 'Uploading domain XML...',
          totalUnits: 0,
          completedUnits: 0,
          bytesTransferred: 0,
          speedBytesPerSec: 0,
          physicalBytesTransferred: 0,
          physicalSpeedBytesPerSec: 0,
          totalBytes: 0,
          sanityBytesTransferred: 0,
          sanitySpeedBytesPerSec: 0,
        ),
      );
      await host.uploadLocalFile(server, xmlTempFile.path, remoteXmlPath);
    } finally {
      if (await xmlTempFile.exists()) {
        await xmlTempFile.delete();
      }
    }
    sendStatus(
      AgentJobStatus(
        id: jobId,
        type: AgentJobType.restore,
        state: AgentJobState.running,
        message: 'Defining VM...',
        totalUnits: 0,
        completedUnits: 0,
        bytesTransferred: 0,
        speedBytesPerSec: 0,
        physicalBytesTransferred: 0,
        physicalSpeedBytesPerSec: 0,
        totalBytes: 0,
        sanityBytesTransferred: 0,
        sanitySpeedBytesPerSec: 0,
      ),
    );
    await host.runSshCommand(server, 'virsh define "$remoteXmlPath"');
  }

  Future<void> runRestore(Map<String, dynamic> payload) async {
    final jobId = payload['jobId']?.toString() ?? '';
    final driverId = payload['driverId']?.toString() ?? 'filesystem';
    final backupPath = payload['backupPath']?.toString() ?? '';
    final decision = payload['decision']?.toString() ?? 'overwrite';
    final xmlPath = payload['xmlPath']?.toString() ?? '';
    final settingsMap = Map<String, dynamic>.from(payload['settings'] as Map? ?? const {});
    final destinationMap = Map<String, dynamic>.from(payload['destination'] as Map? ?? const {});
    final serverMap = Map<String, dynamic>.from(payload['server'] as Map? ?? const {});

    final settings = AppSettings.fromMap(settingsMap);
    final selectedDestination = destinationMap.isEmpty ? null : BackupDestination.fromMap(destinationMap);
    BackupDestination? settingsDestination;
    final selectedDestinationId = selectedDestination?.id;
    if (selectedDestinationId != null && selectedDestinationId.isNotEmpty) {
      for (final destination in settings.destinations) {
        if (destination.id == selectedDestinationId) {
          settingsDestination = destination;
          break;
        }
      }
    }
    final isFilesystemDestination = selectedDestination?.id == AppSettings.filesystemDestinationId;
    final useStoredBlobs = !isFilesystemDestination && selectedDestination?.useBlobs == true;
    final storeDownloadedBlobs = !isFilesystemDestination && selectedDestination?.storeBlobs == true;
    final downloadConcurrency = settingsDestination?.downloadConcurrency ?? selectedDestination?.downloadConcurrency ?? 8;
    final server = ServerConfig.fromMap(serverMap);
    await LogWriter.configureSourcePath(
      source: 'agent',
      path: LogWriter.defaultPathForSource('agent', basePath: settings.backupPath.trim()),
    );
    LogWriter.configureSourceLevel(source: 'agent', level: settings.logLevel);

    final host = BackupAgentHost();

    BackupDriver buildDriver() {
      final factories = <String, BackupDriver Function()>{
        'dummy': () => DummyBackupDriver(backupPath.trim(), tmpWritesEnabled: settings.dummyDriverTmpWrites),
        'gdrive': () => GdriveBackupDriver(
          settings: settings,
          persistSettings: (updated) async => mainPort.send({'type': _typeSettings, 'settings': updated.toMap()}),
          logInfo: (message) => LogWriter.logAgentSync(level: 'info', message: message),
        ),
        'filesystem': () => FilesystemBackupDriver(backupPath.trim()),
        'sftp': () => SftpBackupDriver(settings: settings, poolSessions: downloadConcurrency),
      };
      final factory = factories[driverId] ?? factories['filesystem']!;
      return factory();
    }

    final driver = buildDriver();
    BackupDriver? localBlobDriver;
    if (useStoredBlobs || storeDownloadedBlobs) {
      final filesystemPath = _resolveFilesystemDestinationPath(settings);
      if (filesystemPath.isEmpty) {
        throw 'restore blob cache failed: filesystem destination path is empty';
      }
      localBlobDriver = FilesystemBackupDriver(filesystemPath);
    }
    final xmlFile = File(xmlPath);
    if (!await xmlFile.exists()) {
      sendResult(
        AgentJobStatus(
          id: jobId,
          type: AgentJobType.restore,
          state: AgentJobState.failure,
          message: 'XML not found: $xmlPath',
          totalUnits: 0,
          completedUnits: 0,
          bytesTransferred: 0,
          speedBytesPerSec: 0,
          physicalBytesTransferred: 0,
          physicalSpeedBytesPerSec: 0,
          totalBytes: 0,
          sanityBytesTransferred: 0,
          sanitySpeedBytesPerSec: 0,
        ),
      );
      return;
    }

    try {
      await host.beginLargeTransferSession(server);
      final xmlContent = await xmlFile.readAsString();
      final vmName = _extractVmNameFromXml(xmlContent);
      if (vmName.isNotEmpty) {
        mainPort.send({'type': _typeContext, 'jobId': jobId, 'source': xmlPath, 'target': '${server.name}:$vmName'});
      }
      final diskSourcePaths = _extractDiskSourcePathsFromXml(xmlContent);
      if (diskSourcePaths.isEmpty) {
        throw 'No disk sources found in XML.';
      }
      final timestamp = _extractTimestampFromFileName(_baseName(xmlFile.path));
      final location = driver.restoreLocationFromXml(xmlFile);
      if (location == null) {
        throw 'Cannot resolve restore location for $xmlPath';
      }
      if (driver is GdriveBackupDriver) {
        await driver.ensureXmlOnDrive(xmlFile);
      }

      final localDir = location.vmDir;
      final remoteDiskTargets = <_RestoreDiskTarget>[];
      final chainRebases = <_ChainRebase>[];

      for (final sourcePath in diskSourcePaths) {
        ensureNotCanceled();
        final diskBaseName = sourcePath.split(RegExp(r'[\\/]')).last.trim();
        if (diskBaseName.isEmpty) {
          continue;
        }
        final diskDir = await driver.findDiskDirForTimestamp(localDir, timestamp, diskBaseName);
        File? chainFile = driver.findChainFileForTimestamp(localDir, diskDir, timestamp, diskBaseName);
        if (chainFile == null && driver is GdriveBackupDriver) {
          chainFile = await driver.ensureChainFile(localDir, timestamp, diskBaseName);
        }
        final chainEntries = chainFile == null ? <_ChainEntry>[] : await _readChainEntries(chainFile);
        if (chainEntries.isEmpty) {
          final manifest = await driver.findManifestForTimestamp(localDir, timestamp, diskBaseName);
          if (manifest == null) {
            final diskId = driver.sanitizeFileName(diskBaseName);
            final expectedPath = driver.manifestFile(location.serverId, location.vmName, diskId, timestamp, inProgress: false).path;
            throw 'Manifest not found for $diskBaseName (expected $expectedPath)';
          }
          final manifestBlocks = await _readHashesFromManifest(manifest);
          if (manifestBlocks.blocks.isEmpty) {
            throw 'No blocks found in manifest for $diskBaseName';
          }
          remoteDiskTargets.add(
            _RestoreDiskTarget(
              manifest: manifest,
              diskBaseName: diskBaseName,
              remotePath: sourcePath,
              blocks: manifestBlocks.blocks,
              blockSize: manifestBlocks.blockSize,
              fileSize: manifestBlocks.fileSize,
            ),
          );
        } else {
          if (chainEntries.length > 1) {
            for (var i = chainEntries.length - 1; i > 0; i -= 1) {
              chainRebases.add(_ChainRebase(overlayPath: chainEntries[i - 1].path, backingPath: chainEntries[i].path));
            }
          }
          for (final chainEntry in chainEntries.reversed) {
            final manifest = await driver.findManifestForChainEntry(localDir, timestamp, chainEntry.diskId, chainEntry.path, (manifest) => _readManifestField(manifest, 'source_path'));
            if (manifest == null) {
              final diskId = driver.sanitizeFileName(chainEntry.diskId);
              final expectedPath = driver.manifestFile(location.serverId, location.vmName, diskId, timestamp, inProgress: false).path;
              throw 'Manifest not found for ${chainEntry.diskId} (expected $expectedPath)';
            }
            final manifestBlocks = await _readHashesFromManifest(manifest);
            if (manifestBlocks.blocks.isEmpty) {
              throw 'No blocks found in manifest for ${chainEntry.diskId}';
            }
            remoteDiskTargets.add(
              _RestoreDiskTarget(
                manifest: manifest,
                diskBaseName: chainEntry.diskId,
                remotePath: chainEntry.path,
                blocks: manifestBlocks.blocks,
                blockSize: manifestBlocks.blockSize,
                fileSize: manifestBlocks.fileSize,
              ),
            );
          }
        }
      }

      if (decision == 'overwrite') {
        ensureNotCanceled();
        await host.runSshCommand(server, 'virsh destroy "$vmName" || true');
        await host.runSshCommand(server, 'virsh undefine "$vmName" --nvram || true');
      }

      if (decision == 'define') {
        await defineOnly(jobId, host, server, xmlFile, xmlContent, timestamp, vmName);
        sendResult(
          AgentJobStatus(
            id: jobId,
            type: AgentJobType.restore,
            state: AgentJobState.success,
            message: 'XML redefined',
            totalUnits: 0,
            completedUnits: 0,
            bytesTransferred: 0,
            speedBytesPerSec: 0,
            physicalBytesTransferred: 0,
            physicalSpeedBytesPerSec: 0,
            totalBytes: 0,
            sanityBytesTransferred: 0,
            sanitySpeedBytesPerSec: 0,
          ),
        );
        return;
      }

      var totalBytes = 0;
      for (final target in remoteDiskTargets) {
        ensureNotCanceled();
        if (target.fileSize != null && target.fileSize! > 0) {
          totalBytes += target.fileSize!;
        } else {
          totalBytes += target.blocks.length * target.blockSize;
        }
      }
      sendStatus(
        AgentJobStatus(
          id: jobId,
          type: AgentJobType.restore,
          state: AgentJobState.running,
          message: 'Preparing restore...',
          totalUnits: totalBytes,
          completedUnits: 0,
          bytesTransferred: 0,
          speedBytesPerSec: 0,
          physicalBytesTransferred: 0,
          physicalSpeedBytesPerSec: 0,
          totalBytes: totalBytes,
          sanityBytesTransferred: 0,
          sanitySpeedBytesPerSec: 0,
        ),
      );

      var bytesTransferred = 0;
      final speedTicker = _SpeedTicker();

      for (var i = 0; i < remoteDiskTargets.length; i += 1) {
        ensureNotCanceled();
        final target = remoteDiskTargets[i];
        final remotePath = target.remotePath;
        sendStatus(
          AgentJobStatus(
            id: jobId,
            type: AgentJobType.restore,
            state: AgentJobState.running,
            message: 'Uploading disk ${i + 1} of ${remoteDiskTargets.length}...',
            totalUnits: totalBytes,
            completedUnits: 0,
            bytesTransferred: bytesTransferred,
            speedBytesPerSec: 0,
            physicalBytesTransferred: 0,
            physicalSpeedBytesPerSec: 0,
            totalBytes: totalBytes,
            sanityBytesTransferred: 0,
            sanitySpeedBytesPerSec: 0,
          ),
        );
        final parts = remotePath.split('/');
        final remoteDir = parts.length > 1 ? parts.sublist(0, parts.length - 1).join('/') : '';
        if (remoteDir.isNotEmpty) {
          await host.runSshCommand(server, 'mkdir -p "$remoteDir"');
        }
        await host.runSshCommand(server, 'rm -f "$remotePath"');
        final blobStream = _blobStream(
          driver,
          target.blocks,
          target.blockSize,
          target.fileSize,
          () => canceled,
          localBlobDriver: localBlobDriver,
          useStoredBlobs: useStoredBlobs,
          storeDownloadedBlobs: storeDownloadedBlobs,
          maxConcurrentDownloads: downloadConcurrency,
        );
        await host.uploadRemoteStream(
          server,
          remotePath,
          blobStream,
          onBytes: (bytes) {
            bytesTransferred += bytes;
            final speed = speedTicker.tick(bytes);
            sendStatus(
              AgentJobStatus(
                id: jobId,
                type: AgentJobType.restore,
                state: AgentJobState.running,
                message: 'Uploading disk ${i + 1} of ${remoteDiskTargets.length}...',
                totalUnits: totalBytes,
                completedUnits: 0,
                bytesTransferred: bytesTransferred,
                speedBytesPerSec: speed,
                physicalBytesTransferred: 0,
                physicalSpeedBytesPerSec: 0,
                totalBytes: totalBytes,
                sanityBytesTransferred: 0,
                sanitySpeedBytesPerSec: 0,
              ),
            );
          },
        );
        if (target.fileSize != null && target.fileSize! > 0) {
          try {
            final remoteSize = await host.runSshCommand(server, 'stat -c %s "$remotePath"');
            final remoteValue = int.tryParse(remoteSize.stdout.trim());
            if (remoteValue == target.fileSize) {
              LogWriter.logAgentSync(level: 'info', message: 'restore: ${target.diskBaseName} size=${target.fileSize} remote_size=$remoteValue');
            } else {
              LogWriter.logAgentSync(level: 'info', message: 'restore: ${target.diskBaseName} size=${target.fileSize} remote_size=${remoteSize.stdout.trim()}');
              throw 'restore size mismatch for ${target.diskBaseName}: expected ${target.fileSize}, got ${remoteSize.stdout.trim()}';
            }
          } catch (error, stackTrace) {
            LogWriter.logAgentSync(level: 'info', message: 'restore: size check failed for ${target.diskBaseName}: $error\\n$stackTrace');
            throw 'restore size check failed for ${target.diskBaseName}: $error';
          }
        }
      }

      if (chainRebases.isNotEmpty) {
        sendStatus(
          AgentJobStatus(
            id: jobId,
            type: AgentJobType.restore,
            state: AgentJobState.running,
            message: 'Rebasing restored overlays...',
            totalUnits: totalBytes,
            completedUnits: 0,
            bytesTransferred: bytesTransferred,
            speedBytesPerSec: 0,
            physicalBytesTransferred: 0,
            physicalSpeedBytesPerSec: 0,
            totalBytes: totalBytes,
            sanityBytesTransferred: 0,
            sanitySpeedBytesPerSec: 0,
          ),
        );
        for (final rebase in chainRebases) {
          ensureNotCanceled();
          await host.runSshCommand(server, 'qemu-img rebase -u -b "${rebase.backingPath}" "${rebase.overlayPath}"');
        }
      }

      await defineOnly(jobId, host, server, xmlFile, xmlContent, timestamp, vmName);
      sendResult(
        AgentJobStatus(
          id: jobId,
          type: AgentJobType.restore,
          state: AgentJobState.success,
          message: 'Restore completed',
          totalUnits: totalBytes,
          completedUnits: 0,
          bytesTransferred: bytesTransferred,
          speedBytesPerSec: 0,
          physicalBytesTransferred: 0,
          physicalSpeedBytesPerSec: 0,
          totalBytes: totalBytes,
          sanityBytesTransferred: 0,
          sanitySpeedBytesPerSec: 0,
        ),
      );
    } catch (error, stackTrace) {
      final isCanceled = error is _Canceled;
      if (!isCanceled) {
        LogWriter.logAgentSync(level: 'info', message: 'Restore failed: $error\n$stackTrace');
      }
      sendResult(
        AgentJobStatus(
          id: jobId,
          type: AgentJobType.restore,
          state: isCanceled ? AgentJobState.canceled : AgentJobState.failure,
          message: isCanceled ? 'Canceled' : error.toString(),
          totalUnits: 0,
          completedUnits: 0,
          bytesTransferred: 0,
          speedBytesPerSec: 0,
          physicalBytesTransferred: 0,
          physicalSpeedBytesPerSec: 0,
          totalBytes: 0,
          sanityBytesTransferred: 0,
          sanitySpeedBytesPerSec: 0,
        ),
      );
    } finally {
      await host.endLargeTransferSession(server);
    }
  }

  commandPort.listen((message) async {
    final payload = Map<String, dynamic>.from(message as Map);
    final type = payload['type']?.toString();
    if (type == _typeStart) {
      await runRestore(payload);
      Isolate.exit();
    } else if (type == _typeCancel) {
      canceled = true;
    }
  });
}

class _Canceled implements Exception {
  const _Canceled();
}

String _extractVmNameFromXml(String xmlContent) {
  final match = RegExp(r'<name>([^<]+)</name>').firstMatch(xmlContent);
  return match?.group(1)?.trim() ?? '';
}

List<String> _extractDiskSourcePathsFromXml(String xmlContent) {
  final matches = RegExp(r"<source[^>]+file='([^']+)'").allMatches(xmlContent);
  return matches.map((match) => match.group(1)?.trim() ?? '').where((value) => value.isNotEmpty).toList();
}

String _extractTimestampFromFileName(String name) {
  final parts = name.split('__');
  if (parts.isEmpty) {
    return '';
  }
  return parts.first.trim();
}

String _baseName(String path) {
  final parts = path.split(RegExp(r'[\\/]')).where((part) => part.isNotEmpty).toList();
  return parts.isEmpty ? path : parts.last;
}

String _sanitizeFileName(String name) {
  return name.trim().replaceAll(RegExp(r'[\\/:*?"<>|]'), '_');
}

Future<List<_ChainEntry>> _readChainEntries(File chainFile) async {
  final text = await chainFile.readAsString();
  final entries = <_ChainEntry>[];
  for (final line in text.split('\n')) {
    final trimmed = line.trim();
    if (trimmed.isEmpty || !trimmed.contains('->')) {
      continue;
    }
    final parts = trimmed.split('->').map((part) => part.trim()).toList();
    if (parts.length < 2) {
      continue;
    }
    entries.add(_ChainEntry(diskId: parts[0], path: parts[1]));
  }
  return entries;
}

Future<String?> _readManifestField(File manifest, String field) async {
  if (!await manifest.exists()) {
    return null;
  }
  final lines = await _readManifestLines(manifest);
  for (final line in lines) {
    final trimmed = line.trim();
    if (trimmed.startsWith('$field:')) {
      return trimmed.substring(field.length + 1).trim();
    }
    if (trimmed == 'blocks:') {
      break;
    }
  }
  return null;
}

Future<List<String>> _readManifestLines(File manifest) async {
  if (manifest.path.endsWith('.gz')) {
    final bytes = await manifest.readAsBytes();
    final decoded = gzip.decode(bytes);
    final content = utf8.decode(decoded);
    return const LineSplitter().convert(content);
  }
  return manifest.readAsLines();
}

Future<_ManifestBlocks> _readHashesFromManifest(File manifest) async {
  if (!await manifest.exists()) {
    throw 'Manifest not found: ${manifest.path}';
  }
  final lines = await _readManifestLines(manifest);
  var blockSize = 0;
  int? fileSize;
  var inBlocks = false;
  final blocks = <_BlockRef>[];
  for (final raw in lines) {
    final line = raw.trim();
    if (line.isEmpty) {
      continue;
    }
    if (!inBlocks) {
      if (line.startsWith('block_size:')) {
        blockSize = int.tryParse(line.substring(11).trim()) ?? 0;
      } else if (line.startsWith('file_size:')) {
        fileSize = int.tryParse(line.substring(10).trim());
      } else if (line == 'blocks:') {
        inBlocks = true;
      }
      continue;
    }
    if (line.endsWith('-> ZERO')) {
      final parts = line.split('->');
      final left = parts.first.trim();
      final rangeParts = left.split('-').map((value) => value.trim()).where((value) => value.isNotEmpty).toList();
      final start = int.tryParse(rangeParts.first);
      final end = rangeParts.length > 1 ? int.tryParse(rangeParts.last) : start;
      if (start == null || end == null) {
        continue;
      }
      for (var i = start; i <= end; i += 1) {
        blocks.add(_BlockRef.zero());
      }
      continue;
    }
    final parts = line.split('->');
    if (parts.length < 2) {
      continue;
    }
    final hash = parts.last.trim();
    if (hash.isNotEmpty) {
      blocks.add(_BlockRef.hash(hash));
    }
  }
  return _ManifestBlocks(blockSize: blockSize, blocks: blocks, fileSize: fileSize);
}

int _minInt(int a, int b) => a < b ? a : b;

int _blockLengthForIndex(int index, int totalSize, int blockSize) {
  final start = index * blockSize;
  final end = _minInt(totalSize, start + blockSize);
  return end - start;
}

Stream<List<int>> _blobStream(
  BackupDriver driver,
  List<_BlockRef> blocks,
  int blockSize,
  int? totalSize,
  bool Function()? isCanceled, {
  BackupDriver? localBlobDriver,
  required bool useStoredBlobs,
  required bool storeDownloadedBlobs,
  required int maxConcurrentDownloads,
}) async* {
  if (maxConcurrentDownloads <= 0) {
    throw StateError('restore downloadConcurrency must be greater than 0.');
  }
  final localBlobCache = _BlobReadCache(maxBytes: 512 * 1024 * 1024);
  final debug = _RestorePipelineDebug(mode: driver is RemoteBlobDriver ? 'remote' : 'local');
  var totalEmitted = 0;
  final remote = driver is RemoteBlobDriver ? driver as RemoteBlobDriver : null;
  if (remote != null) {
    final maxConcurrent = maxConcurrentDownloads;
    var nextIndex = 0;
    var nextEmit = 0;
    final inFlight = <int, Future<_BlockData>>{};

    Future<_BlockData> startFetch(int index) async {
      final fetchStartedAt = DateTime.now();
      if (isCanceled?.call() == true) {
        throw const _Canceled();
      }
      final block = blocks[index];
      final expectedLength = totalSize == null ? blockSize : _blockLengthForIndex(index, totalSize, blockSize);
      if (block.zeroRun) {
        debug.markFetchDone(index: index, source: 'zero', expectedLength: expectedLength, fetchStartedAt: fetchStartedAt);
        return _BlockData(index, expectedLength > 0 ? Uint8List(expectedLength) : const <int>[]);
      }
      final hash = block.hash;
      if (hash == null) {
        debug.markFetchDone(index: index, source: 'empty', expectedLength: expectedLength, fetchStartedAt: fetchStartedAt);
        return const _BlockData.empty();
      }
      if (useStoredBlobs && localBlobDriver != null) {
        final localBytes = await _readLocalBlob(localBlobDriver, hash, expectedLength, index, cache: localBlobCache);
        if (localBytes != null) {
          debug.markFetchDone(index: index, source: 'local-cache', expectedLength: expectedLength, fetchStartedAt: fetchStartedAt);
          return _BlockData(index, localBytes);
        }
      }
      final builder = BytesBuilder(copy: false);
      final stream = remote.openBlobStream(hash, length: expectedLength);
      await for (final chunk in stream) {
        if (isCanceled?.call() == true) {
          throw const _Canceled();
        }
        builder.add(chunk);
      }
      final bytes = builder.takeBytes();
      if (expectedLength > 0 && bytes.isEmpty) {
        throw 'restore missing blob hash=$hash index=$index';
      }
      if (bytes.length != expectedLength) {
        throw 'restore blob size mismatch hash=$hash index=$index expected=$expectedLength got=${bytes.length}';
      }
      if (storeDownloadedBlobs && localBlobDriver != null) {
        await _storeLocalBlob(localBlobDriver, hash, bytes, index);
      }
      debug.markFetchDone(index: index, source: 'remote', expectedLength: expectedLength, fetchStartedAt: fetchStartedAt);
      return _BlockData(index, bytes);
    }

    void schedule() {
      while (inFlight.length < maxConcurrent && nextIndex < blocks.length) {
        final index = nextIndex;
        inFlight[index] = startFetch(index).then((data) {
          debug.markReady(index);
          return data;
        });
        nextIndex += 1;
      }
    }

    schedule();
    while (nextEmit < blocks.length) {
      schedule();
      debug.maybeLog(nextIndex: nextIndex, nextEmit: nextEmit, inFlight: inFlight.length);
      final future = inFlight[nextEmit];
      if (future == null) {
        await Future<void>.delayed(const Duration(milliseconds: 1));
        continue;
      }
      final data = await future;
      inFlight.remove(nextEmit);
      debug.markEmitted(index: data.index, bytes: data.bytes.length);
      if (data.bytes.isNotEmpty) {
        yield data.bytes;
        totalEmitted += data.bytes.length;
      }
      nextEmit += 1;
    }
    debug.maybeLog(nextIndex: nextIndex, nextEmit: nextEmit, inFlight: inFlight.length, force: true);
  } else {
    final maxConcurrent = maxConcurrentDownloads;
    var nextIndex = 0;
    var nextEmit = 0;
    final inFlight = <int, Future<_BlockData>>{};

    Future<_BlockData> startFetchLocal(int index) async {
      final fetchStartedAt = DateTime.now();
      if (isCanceled?.call() == true) {
        throw const _Canceled();
      }
      final block = blocks[index];
      final expectedLength = totalSize == null ? blockSize : _blockLengthForIndex(index, totalSize, blockSize);
      if (block.zeroRun) {
        debug.markFetchDone(index: index, source: 'zero', expectedLength: expectedLength, fetchStartedAt: fetchStartedAt);
        if (expectedLength > 0) {
          return _BlockData(index, Uint8List(expectedLength));
        }
        return const _BlockData.empty();
      }
      final hash = block.hash;
      if (hash == null) {
        debug.markFetchDone(index: index, source: 'empty', expectedLength: expectedLength, fetchStartedAt: fetchStartedAt);
        return const _BlockData.empty();
      }
      final localBytes = await _readLocalBlob(driver, hash, expectedLength, index, cache: localBlobCache);
      if (localBytes == null) {
        final blobFile = driver.blobFile(hash);
        throw 'restore missing local blob hash=$hash index=$index path=${blobFile.path}';
      }
      debug.markFetchDone(index: index, source: 'local', expectedLength: expectedLength, fetchStartedAt: fetchStartedAt);
      return _BlockData(index, localBytes);
    }

    void schedule() {
      while (inFlight.length < maxConcurrent && nextIndex < blocks.length) {
        final index = nextIndex;
        inFlight[index] = startFetchLocal(index).then((data) {
          debug.markReady(index);
          return data;
        });
        nextIndex += 1;
      }
    }

    schedule();
    while (nextEmit < blocks.length) {
      schedule();
      debug.maybeLog(nextIndex: nextIndex, nextEmit: nextEmit, inFlight: inFlight.length);
      final future = inFlight[nextEmit];
      if (future == null) {
        await Future<void>.delayed(const Duration(milliseconds: 1));
        continue;
      }
      final data = await future;
      inFlight.remove(nextEmit);
      debug.markEmitted(index: data.index, bytes: data.bytes.length);
      if (data.bytes.isNotEmpty) {
        yield data.bytes;
        totalEmitted += data.bytes.length;
      }
      nextEmit += 1;
    }
    debug.maybeLog(nextIndex: nextIndex, nextEmit: nextEmit, inFlight: inFlight.length, force: true);
  }
  if (totalSize != null && totalEmitted < totalSize) {
    yield Uint8List(totalSize - totalEmitted);
  }
}

Future<List<int>?> _readLocalBlob(BackupDriver localBlobDriver, String hash, int expectedLength, int index, {_BlobReadCache? cache}) async {
  final blobFile = localBlobDriver.blobFile(hash);
  final cached = cache?.get(hash);
  if (cached != null) {
    if (cached.length != expectedLength) {
      throw 'restore local blob size mismatch hash=$hash index=$index expected=$expectedLength got=${cached.length} path=${blobFile.path}';
    }
    return cached;
  }
  Uint8List bytes;
  try {
    bytes = await blobFile.readAsBytes();
  } on FileSystemException {
    return null;
  }
  if (bytes.length != expectedLength) {
    throw 'restore local blob size mismatch hash=$hash index=$index expected=$expectedLength got=${bytes.length} path=${blobFile.path}';
  }
  cache?.put(hash, bytes);
  return bytes;
}

Future<void> _storeLocalBlob(BackupDriver localBlobDriver, String hash, List<int> bytes, int index) async {
  if (bytes.isEmpty) {
    return;
  }
  final blobFile = localBlobDriver.blobFile(hash);
  if (await blobFile.exists()) {
    return;
  }
  try {
    await localBlobDriver.ensureBlobDir(hash);
    await localBlobDriver.writeBlob(hash, bytes);
  } catch (error) {
    throw 'restore local blob store failed hash=$hash index=$index path=${blobFile.path}: $error';
  }
}

String _resolveFilesystemDestinationPath(AppSettings settings) {
  for (final destination in settings.destinations) {
    if (destination.id != AppSettings.filesystemDestinationId) {
      continue;
    }
    return destination.params['path']?.toString().trim() ?? '';
  }
  return '';
}

class _BlockData {
  const _BlockData(this.index, this.bytes);

  const _BlockData.empty() : index = -1, bytes = const <int>[];

  final int index;
  final List<int> bytes;
}

class _BlobReadCache {
  _BlobReadCache({required this.maxBytes});

  final int maxBytes;
  final Map<String, Uint8List> _entries = {};
  final ListQueue<String> _order = ListQueue<String>();
  int _currentBytes = 0;

  Uint8List? get(String hash) {
    final bytes = _entries[hash];
    if (bytes == null) {
      return null;
    }
    _order.remove(hash);
    _order.addLast(hash);
    return bytes;
  }

  void put(String hash, Uint8List bytes) {
    if (bytes.length > maxBytes) {
      _entries.clear();
      _order.clear();
      _currentBytes = 0;
      return;
    }
    final existing = _entries.remove(hash);
    if (existing != null) {
      _currentBytes -= existing.length;
      _order.remove(hash);
    }
    _entries[hash] = bytes;
    _order.addLast(hash);
    _currentBytes += bytes.length;
    while (_currentBytes > maxBytes && _order.isNotEmpty) {
      final oldest = _order.removeFirst();
      final removed = _entries.remove(oldest);
      if (removed != null) {
        _currentBytes -= removed.length;
      }
    }
  }
}

class _RestoreDiskTarget {
  const _RestoreDiskTarget({required this.manifest, required this.diskBaseName, required this.remotePath, required this.blocks, required this.blockSize, required this.fileSize});

  final File manifest;
  final String diskBaseName;
  final String remotePath;
  final List<_BlockRef> blocks;
  final int blockSize;
  final int? fileSize;
}

class _ChainEntry {
  const _ChainEntry({required this.diskId, required this.path});

  final String diskId;
  final String path;
}

class _ChainRebase {
  const _ChainRebase({required this.overlayPath, required this.backingPath});

  final String overlayPath;
  final String backingPath;
}

class _ManifestBlocks {
  const _ManifestBlocks({required this.blockSize, required this.blocks, required this.fileSize});

  final int blockSize;
  final List<_BlockRef> blocks;
  final int? fileSize;
}

class _BlockRef {
  const _BlockRef._(this.hash, this.zeroRun);

  final String? hash;
  final bool zeroRun;

  factory _BlockRef.hash(String hash) => _BlockRef._(hash, false);
  factory _BlockRef.zero() => const _BlockRef._(null, true);
}

class _SpeedTicker {
  DateTime? _lastTick;
  int _bytesSince = 0;
  double _smoothed = 0;

  double tick(int bytes) {
    _bytesSince += bytes;
    final now = DateTime.now();
    final last = _lastTick;
    if (last == null) {
      _lastTick = now;
      return 0;
    }
    final elapsedMs = now.difference(last).inMilliseconds;
    if (elapsedMs < 1000) {
      return _smoothed;
    }
    final instant = _bytesSince / (elapsedMs / 1000);
    _bytesSince = 0;
    _lastTick = now;
    _smoothed = _smooth(_smoothed, instant);
    return _smoothed;
  }
}

double _smooth(double value, double next) {
  if (value <= 0) {
    return next;
  }
  return (value * 0.8) + (next * 0.2);
}

class _RestorePipelineDebug {
  _RestorePipelineDebug({required this.mode});

  final String mode;
  DateTime _lastLogAt = DateTime.now();
  final Map<int, DateTime> _readyAt = <int, DateTime>{};
  int _fetchCount = 0;
  int _fetchMsTotal = 0;
  int _fetchMsMax = 0;
  int _emitCount = 0;
  int _emitBytes = 0;
  int _holCount = 0;
  int _holMsTotal = 0;
  int _holMsMax = 0;
  final Map<String, int> _sourceCounts = <String, int>{};

  void markFetchDone({required int index, required String source, required int expectedLength, required DateTime fetchStartedAt}) {
    final fetchMs = DateTime.now().difference(fetchStartedAt).inMilliseconds;
    _fetchCount += 1;
    _fetchMsTotal += fetchMs;
    if (fetchMs > _fetchMsMax) {
      _fetchMsMax = fetchMs;
    }
    _sourceCounts[source] = (_sourceCounts[source] ?? 0) + 1;
  }

  void markReady(int index) {
    _readyAt[index] = DateTime.now();
  }

  void markEmitted({required int index, required int bytes}) {
    _emitCount += 1;
    _emitBytes += bytes > 0 ? bytes : 0;
    final readyAt = _readyAt.remove(index);
    if (readyAt != null) {
      final waitMs = DateTime.now().difference(readyAt).inMilliseconds;
      _holCount += 1;
      _holMsTotal += waitMs;
      if (waitMs > _holMsMax) {
        _holMsMax = waitMs;
      }
    }
  }

  void maybeLog({required int nextIndex, required int nextEmit, required int inFlight, bool force = false}) {
    final now = DateTime.now();
    final elapsedMs = now.difference(_lastLogAt).inMilliseconds;
    if (!force && elapsedMs < 1000) {
      return;
    }
    final sec = elapsedMs <= 0 ? 1.0 : (elapsedMs / 1000.0);
    final emitMbPerSec = (_emitBytes / sec) / (1024 * 1024);
    final fetchAvgMs = _fetchCount == 0 ? 0 : (_fetchMsTotal / _fetchCount).round();
    final holAvgMs = _holCount == 0 ? 0 : (_holMsTotal / _holCount).round();
    final sourceSummary = _sourceCounts.entries.map((entry) => '${entry.key}:${entry.value}').join(',');
    LogWriter.logAgentSync(
      level: 'trace',
      message:
          'restore pipeline debug: mode=$mode nextEmit=$nextEmit nextIndex=$nextIndex inFlight=$inFlight '
          'ready=${_readyAt.length} emitted=$_emitCount emittedMBps=${emitMbPerSec.toStringAsFixed(1)} '
          'fetchCount=$_fetchCount fetchAvgMs=$fetchAvgMs fetchMaxMs=$_fetchMsMax '
          'holCount=$_holCount holAvgMs=$holAvgMs holMaxMs=$_holMsMax sources=$sourceSummary',
    );
    _lastLogAt = now;
    _fetchCount = 0;
    _fetchMsTotal = 0;
    _fetchMsMax = 0;
    _emitCount = 0;
    _emitBytes = 0;
    _holCount = 0;
    _holMsTotal = 0;
    _holMsMax = 0;
    _sourceCounts.clear();
  }
}
