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
    final fullCheckOnly = decision == 'full_check';
    final xmlPath = payload['xmlPath']?.toString() ?? '';
    final settingsMap = Map<String, dynamic>.from(payload['settings'] as Map? ?? const {});
    final storageMap = Map<String, dynamic>.from(payload['storage'] as Map? ?? const {});
    final serverMap = Map<String, dynamic>.from(payload['server'] as Map? ?? const {});

    final settings = AppSettings.fromMap(settingsMap);
    final selectedStorage = storageMap.isEmpty ? null : BackupStorage.fromMap(storageMap);
    BackupStorage? settingsStorage;
    final selectedStorageId = selectedStorage?.id;
    if (selectedStorageId != null && selectedStorageId.isNotEmpty) {
      for (final storage in settings.storage) {
        if (storage.id == selectedStorageId) {
          settingsStorage = storage;
          break;
        }
      }
    }
    final isFilesystemStorage = selectedStorage?.id == AppSettings.filesystemStorageId;
    final useStoredBlobs = !isFilesystemStorage && selectedStorage?.useBlobs == true;
    final storeDownloadedBlobs = !isFilesystemStorage && selectedStorage?.storeBlobs == true;
    final downloadConcurrency = settingsStorage?.downloadConcurrency ?? selectedStorage?.downloadConcurrency ?? 8;
    final server = ServerConfig.fromMap(serverMap);
    await LogWriter.configureSourcePath(
      source: 'agent',
      path: LogWriter.defaultPathForSource('agent', basePath: settings.backupPath.trim()),
    );
    LogWriter.configureSourceLevel(source: 'agent', level: settings.logLevel);

    final host = BackupAgentHost();

    BackupDriver buildDriverForSettings(AppSettings driverSettings) {
      final factories = <String, BackupDriver Function()>{
        'dummy': () => DummyBackupDriver(backupPath.trim(), tmpWritesEnabled: driverSettings.dummyDriverTmpWrites, blockSizeMB: driverSettings.blockSizeMB),
        'gdrive': () => GdriveBackupDriver(
          settings: driverSettings,
          persistSettings: (updated) async => mainPort.send({'type': _typeSettings, 'settings': updated.toMap()}),
          logInfo: (message) => LogWriter.logAgentSync(level: 'info', message: message),
        ),
        'filesystem': () => FilesystemBackupDriver(backupPath.trim(), blockSizeMB: driverSettings.blockSizeMB),
        'sftp': () => SftpBackupDriver(settings: driverSettings),
      };
      final factory = factories[driverId] ?? factories['filesystem']!;
      return factory();
    }

    final metadataDriver = buildDriverForSettings(settings.copyWith(blockSizeMB: 1));
    final blobDriversByBlockSizeMB = <int, BackupDriver>{};
    final localBlobDriversByBlockSizeMB = <int, BackupDriver>{};
    final filesystemPath = (useStoredBlobs || storeDownloadedBlobs) ? _resolveFilesystemStoragePath(settings) : '';
    if ((useStoredBlobs || storeDownloadedBlobs) && filesystemPath.isEmpty) {
      throw 'restore blob cache failed: filesystem storage path is empty';
    }
    final xmlFile = File(xmlPath);
    var largeTransferSessionStarted = false;

    try {
      if (!fullCheckOnly) {
        await host.beginLargeTransferSession(server);
        largeTransferSessionStarted = true;
      }
      final vmName = _extractVmNameFromXmlPath(xmlPath);
      if (vmName.isNotEmpty) {
        mainPort.send({'type': _typeContext, 'jobId': jobId, 'source': xmlPath, 'target': '${server.name}:$vmName'});
      }
      final timestamp = _extractTimestampFromFileName(_baseName(xmlFile.path));
      final localDir = _vmDirFromXmlPath(xmlPath);
      final manifests = await _listManifestFilesForTimestamp(localDir, timestamp);
      if (manifests.isEmpty) {
        throw 'No manifests found for $timestamp';
      }
      final manifestDataByPath = <String, List<_ManifestData>>{};
      for (final manifest in manifests) {
        final data = await _readManifestDataList(manifest);
        manifestDataByPath[manifest.path] = data;
      }
      String? xmlContent;
      for (final dataList in manifestDataByPath.values) {
        for (final data in dataList) {
          if (xmlContent == null) {
            xmlContent = data.domainXml;
            continue;
          }
          if (xmlContent != data.domainXml) {
            throw 'Manifest metadata mismatch: embedded domain_xml differs within timestamp $timestamp';
          }
        }
      }
      if (xmlContent == null || xmlContent.isEmpty) {
        throw 'Manifest metadata invalid: embedded domain_xml missing for timestamp $timestamp';
      }

      final remoteDiskTargets = <_RestoreDiskTarget>[];
      final seenRemotePaths = <String>{};
      for (final manifest in manifests) {
        ensureNotCanceled();
        for (final manifestData in manifestDataByPath[manifest.path]!) {
          if (manifestData.blocks.isEmpty) {
            throw 'No blocks found in manifest ${manifest.path}';
          }
          final sourcePath = manifestData.sourcePath.trim();
          if (sourcePath.isEmpty) {
            throw 'Manifest metadata invalid: source_path missing in ${manifest.path}';
          }
          if (!seenRemotePaths.add(sourcePath)) {
            continue;
          }
          final diskBaseName = manifestData.diskId.trim().isEmpty ? sourcePath.split(RegExp(r'[\\\\/]')).last.trim() : manifestData.diskId.trim();
          remoteDiskTargets.add(
            _RestoreDiskTarget(
              manifest: manifest,
              diskBaseName: diskBaseName,
              remotePath: sourcePath,
              blocks: manifestData.blocks,
              blockSize: manifestData.blockSize,
              blockSizeMB: manifestData.blockSizeMB,
              fileSize: manifestData.fileSize,
            ),
          );
        }
      }
      final allManifestData = <_ManifestData>[];
      for (final list in manifestDataByPath.values) {
        allManifestData.addAll(list);
      }
      final chainRebases = _collectChainRebases(allManifestData, remoteDiskTargets.map((item) => item.remotePath).toSet());

      if (!fullCheckOnly && decision == 'overwrite') {
        ensureNotCanceled();
        await host.runSshCommand(server, 'virsh destroy "$vmName" || true');
        await host.runSshCommand(server, 'virsh undefine "$vmName" --nvram || true');
      }

      if (!fullCheckOnly && decision == 'define') {
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
      var totalCheckBlocks = 0;
      if (fullCheckOnly) {
        for (final target in remoteDiskTargets) {
          for (final block in target.blocks) {
            final hash = block.hash;
            if (block.zeroRun || hash == null || hash.isEmpty) {
              continue;
            }
            totalCheckBlocks += 1;
          }
        }
      }
      sendStatus(
        AgentJobStatus(
          id: jobId,
          type: AgentJobType.restore,
          state: AgentJobState.running,
          message: fullCheckOnly ? 'Sanity check...' : 'Preparing restore...',
          totalUnits: fullCheckOnly ? totalCheckBlocks : totalBytes,
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
      var checkedBlocks = 0;
      var mismatches = 0;
      var lastCheckProgressUpdate = DateTime.now();

      for (var i = 0; i < remoteDiskTargets.length; i += 1) {
        ensureNotCanceled();
        final target = remoteDiskTargets[i];
        final remotePath = target.remotePath;
        sendStatus(
          AgentJobStatus(
            id: jobId,
            type: AgentJobType.restore,
            state: AgentJobState.running,
            message: fullCheckOnly ? 'Sanity check: ${target.diskBaseName}' : 'Uploading disk ${i + 1} of ${remoteDiskTargets.length}...',
            totalUnits: fullCheckOnly ? totalCheckBlocks : totalBytes,
            completedUnits: fullCheckOnly ? checkedBlocks : 0,
            bytesTransferred: bytesTransferred,
            speedBytesPerSec: 0,
            physicalBytesTransferred: 0,
            physicalSpeedBytesPerSec: 0,
            totalBytes: totalBytes,
            sanityBytesTransferred: 0,
            sanitySpeedBytesPerSec: 0,
          ),
        );
        final blobStream = _blobStream(
          blobDriversByBlockSizeMB.putIfAbsent(target.blockSizeMB, () {
            final driverSettings = settings.copyWith(blockSizeMB: target.blockSizeMB);
            return buildDriverForSettings(driverSettings);
          }),
          target.blocks,
          target.blockSize,
          target.fileSize,
          () => canceled,
          localBlobDriver: !(useStoredBlobs || storeDownloadedBlobs)
              ? null
              : localBlobDriversByBlockSizeMB.putIfAbsent(target.blockSizeMB, () => FilesystemBackupDriver(filesystemPath, blockSizeMB: target.blockSizeMB)),
          useStoredBlobs: useStoredBlobs,
          storeDownloadedBlobs: storeDownloadedBlobs,
          maxConcurrentDownloads: downloadConcurrency,
        );
        if (fullCheckOnly) {
          final streamIterator = StreamIterator<List<int>>(blobStream);
          try {
            var blockIndex = 0;
            for (final block in target.blocks) {
              ensureNotCanceled();
              final expectedLength = target.fileSize == null ? target.blockSize : _blockLengthForIndex(blockIndex, target.fileSize!, target.blockSize);
              if (expectedLength <= 0) {
                blockIndex += 1;
                continue;
              }
              if (!await streamIterator.moveNext()) {
                throw 'Sanity check stream ended early for ${target.diskBaseName} at block index=$blockIndex';
              }
              final bytes = streamIterator.current;
              if (bytes.length != expectedLength) {
                throw 'Sanity check stream length mismatch for ${target.diskBaseName} at block index=$blockIndex: expected=$expectedLength got=${bytes.length}';
              }
              bytesTransferred += bytes.length;
              final speed = speedTicker.tick(bytes.length);
              final hash = block.hash;
              if (!block.zeroRun && hash != null && hash.isNotEmpty) {
                checkedBlocks += 1;
                final hashInput = Uint8List.fromList(bytes);
                final actual = host.sha256Hex(hashInput);
                if (actual != hash) {
                  mismatches += 1;
                  LogWriter.logAgentSync(level: 'info', message: 'Sanity check hash mismatch disk=${target.diskBaseName} index=$blockIndex expected=$hash got=$actual');
                }
              }
              final now = DateTime.now();
              if (now.difference(lastCheckProgressUpdate).inMilliseconds >= 500) {
                lastCheckProgressUpdate = now;
                sendStatus(
                  AgentJobStatus(
                    id: jobId,
                    type: AgentJobType.restore,
                    state: AgentJobState.running,
                    message: 'Sanity check: ${target.diskBaseName}',
                    totalUnits: totalCheckBlocks,
                    completedUnits: checkedBlocks,
                    bytesTransferred: bytesTransferred,
                    speedBytesPerSec: speed,
                    physicalBytesTransferred: 0,
                    physicalSpeedBytesPerSec: 0,
                    totalBytes: totalBytes,
                    sanityBytesTransferred: 0,
                    sanitySpeedBytesPerSec: 0,
                  ),
                );
              }
              blockIndex += 1;
            }
            if (await streamIterator.moveNext()) {
              throw 'Sanity check stream produced extra bytes for ${target.diskBaseName}';
            }
          } finally {
            await streamIterator.cancel();
          }
          continue;
        }

        final parts = remotePath.split('/');
        final remoteDir = parts.length > 1 ? parts.sublist(0, parts.length - 1).join('/') : '';
        if (remoteDir.isNotEmpty) {
          await host.runSshCommand(server, 'mkdir -p "$remoteDir"');
        }
        await host.runSshCommand(server, 'rm -f "$remotePath"');
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
            LogWriter.logAgentSync(level: 'error', message: 'restore: size check failed for ${target.diskBaseName}: $error\\n$stackTrace');
            throw 'restore size check failed for ${target.diskBaseName}: $error';
          }
        }
      }

      if (fullCheckOnly) {
        final resultMessage = mismatches == 0 ? 'Sanity check OK ($checkedBlocks blocks checked)' : 'Sanity check: $mismatches mismatch(es) out of $checkedBlocks blocks';
        sendResult(
          AgentJobStatus(
            id: jobId,
            type: AgentJobType.restore,
            state: AgentJobState.success,
            message: resultMessage,
            totalUnits: totalCheckBlocks,
            completedUnits: checkedBlocks,
            bytesTransferred: bytesTransferred,
            speedBytesPerSec: 0,
            physicalBytesTransferred: 0,
            physicalSpeedBytesPerSec: 0,
            totalBytes: totalBytes,
            sanityBytesTransferred: 0,
            sanitySpeedBytesPerSec: 0,
          ),
        );
        return;
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
        LogWriter.logAgentSync(level: 'error', message: 'Restore failed: $error\n$stackTrace');
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
      for (final blobDriver in blobDriversByBlockSizeMB.values) {
        try {
          await blobDriver.closeConnections();
        } catch (_) {}
      }
      for (final localBlobDriver in localBlobDriversByBlockSizeMB.values) {
        try {
          await localBlobDriver.closeConnections();
        } catch (_) {}
      }
      try {
        await metadataDriver.closeConnections();
      } catch (_) {}
      if (largeTransferSessionStarted) {
        await host.endLargeTransferSession(server);
      }
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

Directory _vmDirFromXmlPath(String xmlPath) {
  return File(xmlPath).parent;
}

Future<List<File>> _listManifestFilesForTimestamp(Directory vmDir, String timestamp) async {
  final manifests = <File>[];
  if (!await vmDir.exists()) {
    return manifests;
  }
  await for (final entity in vmDir.list(recursive: true, followLinks: false)) {
    if (entity is! File) {
      continue;
    }
    final name = _baseName(entity.path).trim();
    final isManifest = name.endsWith('.manifest') || name.endsWith('.manifest.gz');
    if (!isManifest) {
      continue;
    }
    if (!_manifestMatchesTimestamp(name, timestamp)) {
      continue;
    }
    manifests.add(entity);
  }
  manifests.sort((a, b) => a.path.compareTo(b.path));
  return manifests;
}

bool _manifestMatchesTimestamp(String fileName, String timestamp) {
  var value = fileName.trim();
  if (value.endsWith('.manifest.gz')) {
    value = value.substring(0, value.length - '.manifest.gz'.length).trim();
  } else if (value.endsWith('.manifest')) {
    value = value.substring(0, value.length - '.manifest'.length).trim();
  } else {
    return false;
  }
  if (value == timestamp) {
    return true;
  }
  return value.startsWith('${timestamp}__');
}

class _Canceled implements Exception {
  const _Canceled();
}

String _extractVmNameFromXmlPath(String xmlPath) {
  final path = xmlPath.replaceAll('\\', '/');
  final parts = path.split('/').where((part) => part.isNotEmpty).toList();
  if (parts.length < 2) {
    return '';
  }
  return parts[parts.length - 2];
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

Future<String> _readManifestContent(File manifest) async {
  if (!await manifest.exists()) {
    throw 'Manifest not found: ${manifest.path}';
  }
  if (manifest.path.endsWith('.gz')) {
    final bytes = await manifest.readAsBytes();
    final decoded = gzip.decode(bytes);
    return utf8.decode(decoded);
  }
  return manifest.readAsString();
}

Future<List<_ManifestData>> _readManifestDataList(File manifest) async {
  final content = await _readManifestContent(manifest);
  if (!_hasValidManifestEof(content)) {
    throw 'manifest incomplete';
  }
  final lines = const LineSplitter().convert(content);
  var blockSize = 0;
  int? manifestVersion;
  int? fileSize;
  String? sourcePath;
  String? diskId;
  var sawChain = false;
  var sawDomainXml = false;
  final domainXmlBuffer = StringBuffer();
  var inDomainXml = false;
  var inChain = false;
  _ChainEntry? pendingChainEntry;
  final chain = <_ChainEntry>[];
  var inBlocks = false;
  var sawAnyDisk = false;
  final blocks = <_BlockRef>[];
  final results = <_ManifestData>[];

  void finalizeCurrentDisk() {
    if (!sawAnyDisk) {
      return;
    }
    if (pendingChainEntry != null) {
      if (pendingChainEntry!.order < 0 || pendingChainEntry!.diskId.trim().isEmpty || pendingChainEntry!.path.trim().isEmpty) {
        throw 'Manifest metadata invalid: malformed chain entry in ${manifest.path}';
      }
      chain.add(pendingChainEntry!);
      pendingChainEntry = null;
    }
    final encodedXml = domainXmlBuffer.toString().trim();
    if (encodedXml.isEmpty) {
      throw 'Manifest metadata invalid: domain_xml_b64_gz empty in ${manifest.path}';
    }
    final domainXml = _decodeDomainXml(encodedXml, manifest.path);
    final blockSizeMB = _blockSizeMbFromManifestBytes(blockSize, manifest.path);
    results.add(
      _ManifestData(
        sourcePath: sourcePath?.trim() ?? '',
        diskId: diskId?.trim() ?? '',
        blockSize: blockSize,
        blockSizeMB: blockSizeMB,
        blocks: List<_BlockRef>.from(blocks),
        fileSize: fileSize,
        domainXml: domainXml,
        chain: List<_ChainEntry>.from(chain),
      ),
    );
    fileSize = null;
    sourcePath = null;
    diskId = null;
    inChain = false;
    inBlocks = false;
    blocks.clear();
    chain.clear();
  }

  for (final rawLine in lines) {
    if (inDomainXml && !rawLine.startsWith('  ')) {
      inDomainXml = false;
    }
    if (inDomainXml) {
      domainXmlBuffer.write(rawLine.substring(2).trim());
      continue;
    }
    final line = rawLine.trim();
    if (line.isEmpty || line == 'EOF') {
      continue;
    }
    final isTopLevel = !rawLine.startsWith(' ');
    if (isTopLevel) {
      inChain = false;
    }
    if (isTopLevel && line.startsWith('disk_id:')) {
      if (sawAnyDisk) {
        finalizeCurrentDisk();
      }
      sawAnyDisk = true;
      diskId = line.substring('disk_id:'.length).trim();
      continue;
    }

    if (!inBlocks) {
      if (inChain) {
        if (line.startsWith('- order:')) {
          if (pendingChainEntry != null) {
            final current = pendingChainEntry!;
            if (current.order < 0 || current.diskId.trim().isEmpty || current.path.trim().isEmpty) {
              throw 'Manifest metadata invalid: malformed chain entry in ${manifest.path}';
            }
            chain.add(current);
          }
          final order = int.tryParse(line.substring('- order:'.length).trim()) ?? -1;
          pendingChainEntry = _ChainEntry(order: order, diskId: '', path: '');
          continue;
        }
        if (line.startsWith('disk_id:')) {
          final current = pendingChainEntry;
          if (current == null) {
            throw 'Manifest metadata invalid: chain disk_id without order in ${manifest.path}';
          }
          pendingChainEntry = _ChainEntry(order: current.order, diskId: line.substring('disk_id:'.length).trim(), path: current.path);
          continue;
        }
        if (line.startsWith('path:')) {
          final current = pendingChainEntry;
          if (current == null) {
            throw 'Manifest metadata invalid: chain path without order in ${manifest.path}';
          }
          pendingChainEntry = _ChainEntry(order: current.order, diskId: current.diskId, path: line.substring('path:'.length).trim());
          continue;
        }
      }
      if (line.startsWith('block_size:')) {
        blockSize = int.tryParse(line.substring(11).trim()) ?? 0;
      } else if (line.startsWith('version:')) {
        manifestVersion = int.tryParse(line.substring('version:'.length).trim());
      } else if (line.startsWith('file_size:')) {
        fileSize = int.tryParse(line.substring(10).trim());
      } else if (line.startsWith('source_path:')) {
        sourcePath = line.substring('source_path:'.length).trim();
      } else if (line.startsWith('domain_xml_b64_gz:')) {
        sawDomainXml = true;
        inDomainXml = true;
      } else if (line == 'chain:') {
        sawChain = true;
        inChain = true;
      } else if (inChain && line == '[]') {
        inChain = false;
      } else if (line.startsWith('blocks:')) {
        if (pendingChainEntry != null) {
          final current = pendingChainEntry!;
          if (current.order < 0 || current.diskId.trim().isEmpty || current.path.trim().isEmpty) {
            throw 'Manifest metadata invalid: malformed chain entry in ${manifest.path}';
          }
          chain.add(current);
          pendingChainEntry = null;
        }
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
  if (pendingChainEntry != null) {
    final current = pendingChainEntry!;
    if (current.order < 0 || current.diskId.trim().isEmpty || current.path.trim().isEmpty) {
      throw 'Manifest metadata invalid: malformed chain entry in ${manifest.path}';
    }
    chain.add(current);
  }
  if (!sawDomainXml) {
    throw 'Manifest metadata invalid: domain_xml_b64_gz missing in ${manifest.path}';
  }
  if (!sawChain) {
    throw 'Manifest metadata invalid: chain missing in ${manifest.path}';
  }
  if (manifestVersion != 1) {
    throw 'Manifest version invalid in ${manifest.path}: expected version=1';
  }
  if (!sawAnyDisk) {
    throw 'Manifest metadata invalid: disk sections missing in ${manifest.path}';
  }
  finalizeCurrentDisk();
  return results;
}

bool _hasValidManifestEof(String content) {
  if (content == 'EOF\n' || content == 'EOF\n\n') {
    return true;
  }
  return content.endsWith('\nEOF\n') || content.endsWith('\nEOF\n\n');
}

String _decodeDomainXml(String encoded, String manifestPath) {
  try {
    final compressed = base64.decode(encoded);
    final bytes = gzip.decode(compressed);
    final xml = utf8.decode(bytes);
    if (xml.trim().isEmpty) {
      throw StateError('empty xml');
    }
    return xml;
  } catch (_) {
    throw 'Manifest metadata invalid: cannot decode domain_xml_b64_gz in $manifestPath';
  }
}

List<_ChainRebase> _collectChainRebases(List<_ManifestData> manifestData, Set<String> restoredPaths) {
  final entriesByOrder = <int, _ChainEntry>{};
  for (final data in manifestData) {
    for (final entry in data.chain) {
      final existing = entriesByOrder[entry.order];
      if (existing == null) {
        entriesByOrder[entry.order] = entry;
        continue;
      }
      if (existing.path != entry.path || existing.diskId != entry.diskId) {
        throw 'Manifest metadata mismatch: chain order conflict at order=${entry.order}';
      }
    }
  }
  if (entriesByOrder.isEmpty) {
    return const <_ChainRebase>[];
  }
  final orders = entriesByOrder.keys.toList()..sort();
  if (orders.first != 0) {
    throw 'Manifest metadata invalid: chain order must start at 0';
  }
  for (var i = 1; i < orders.length; i += 1) {
    if (orders[i] != orders[i - 1] + 1) {
      throw 'Manifest metadata invalid: chain order must be contiguous';
    }
  }
  final rebases = <_ChainRebase>[];
  for (var i = orders.length - 1; i > 0; i -= 1) {
    final lower = entriesByOrder[orders[i - 1]]!;
    final upper = entriesByOrder[orders[i]]!;
    if (!restoredPaths.contains(lower.path) || !restoredPaths.contains(upper.path)) {
      continue;
    }
    rebases.add(_ChainRebase(overlayPath: lower.path, backingPath: upper.path));
  }
  return rebases;
}

int _blockSizeMbFromManifestBytes(int blockSizeBytes, String manifestPath) {
  const bytesPerMb = 1024 * 1024;
  if (blockSizeBytes <= 0) {
    throw 'restore invalid manifest block_size=$blockSizeBytes in $manifestPath (must be > 0 bytes)';
  }
  if (blockSizeBytes % bytesPerMb != 0) {
    throw 'restore invalid manifest block_size=$blockSizeBytes in $manifestPath (must be divisible by $bytesPerMb)';
  }
  final blockSizeMB = blockSizeBytes ~/ bytesPerMb;
  if (blockSizeMB != 1 && blockSizeMB != 2 && blockSizeMB != 4 && blockSizeMB != 8) {
    throw 'restore invalid manifest block_size=$blockSizeBytes in $manifestPath (allowed: 1048576, 2097152, 4194304, 8388608)';
  }
  return blockSizeMB;
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
  driver.setReadConcurrencyLimit(maxConcurrentDownloads);
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
        debug.markScheduled(index);
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
      debug.markInFlightDone(nextEmit);
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
        debug.markScheduled(index);
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
      debug.markInFlightDone(nextEmit);
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

String _resolveFilesystemStoragePath(AppSettings settings) {
  for (final storage in settings.storage) {
    if (storage.id != AppSettings.filesystemStorageId) {
      continue;
    }
    return storage.params['path']?.toString().trim() ?? '';
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
  const _RestoreDiskTarget({
    required this.manifest,
    required this.diskBaseName,
    required this.remotePath,
    required this.blocks,
    required this.blockSize,
    required this.blockSizeMB,
    required this.fileSize,
  });

  final File manifest;
  final String diskBaseName;
  final String remotePath;
  final List<_BlockRef> blocks;
  final int blockSize;
  final int blockSizeMB;
  final int? fileSize;
}

class _ChainEntry {
  const _ChainEntry({required this.order, required this.diskId, required this.path});

  final int order;
  final String diskId;
  final String path;
}

class _ChainRebase {
  const _ChainRebase({required this.overlayPath, required this.backingPath});

  final String overlayPath;
  final String backingPath;
}

class _ManifestData {
  const _ManifestData({
    required this.sourcePath,
    required this.diskId,
    required this.blockSize,
    required this.blockSizeMB,
    required this.blocks,
    required this.fileSize,
    required this.domainXml,
    required this.chain,
  });

  final String sourcePath;
  final String diskId;
  final int blockSize;
  final int blockSizeMB;
  final List<_BlockRef> blocks;
  final int? fileSize;
  final String domainXml;
  final List<_ChainEntry> chain;
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
  final Map<int, DateTime> _inFlightStartedAt = <int, DateTime>{};
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

  void markScheduled(int index) {
    _inFlightStartedAt[index] = DateTime.now();
  }

  void markInFlightDone(int index) {
    _inFlightStartedAt.remove(index);
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
    int readyMin = -1;
    int readyMax = -1;
    for (final index in _readyAt.keys) {
      if (readyMin < 0 || index < readyMin) {
        readyMin = index;
      }
      if (readyMax < 0 || index > readyMax) {
        readyMax = index;
      }
    }
    int oldestInFlightIndex = -1;
    int oldestInFlightAgeMs = 0;
    for (final entry in _inFlightStartedAt.entries) {
      final ageMs = now.difference(entry.value).inMilliseconds;
      if (ageMs > oldestInFlightAgeMs) {
        oldestInFlightAgeMs = ageMs;
        oldestInFlightIndex = entry.key;
      }
    }
    final nextEmitStartedAt = _inFlightStartedAt[nextEmit];
    final nextEmitAgeMs = nextEmitStartedAt == null ? -1 : now.difference(nextEmitStartedAt).inMilliseconds;
    final readyAhead = readyMin >= 0 && readyMin >= nextEmit ? (readyMin - nextEmit) : -1;
    LogWriter.logAgentSync(
      level: 'trace',
      message:
          'restore pipeline debug: mode=$mode nextEmit=$nextEmit nextIndex=$nextIndex inFlight=$inFlight '
          'ready=${_readyAt.length} readyMin=$readyMin readyMax=$readyMax readyAhead=$readyAhead '
          'oldestInFlightIndex=$oldestInFlightIndex oldestInFlightAgeMs=$oldestInFlightAgeMs nextEmitAgeMs=$nextEmitAgeMs '
          'emitted=$_emitCount emittedMBps=${emitMbPerSec.toStringAsFixed(1)} '
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
