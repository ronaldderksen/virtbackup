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

bool _isExpectedRestoreFailure(Object error) {
  return error.toString().startsWith('server is missing required tools:');
}

double _averageBytesPerSecond(DateTime startedAt, int bytes) {
  final elapsedSeconds = DateTime.now().difference(startedAt).inMilliseconds / 1000;
  if (elapsedSeconds <= 0 || bytes <= 0) {
    return 0;
  }
  return bytes / elapsedSeconds;
}

void restoreWorkerMain(Map<String, dynamic> init) {
  final mainPort = init['sendPort'] as SendPort;
  final commandPort = ReceivePort();
  mainPort.send({'type': _typeReady, 'sendPort': commandPort.sendPort});

  var canceled = false;
  final activeDrivers = <BackupDriver>{};

  void closeActiveDrivers() {
    for (final driver in activeDrivers) {
      unawaited(driver.closeConnections());
    }
  }

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

  Future<void> defineOnly(String jobId, BackupAgentHost host, ServerConfig server, File xmlFile, String xmlContent, String timestamp, String vmName, {Future<void> Function()? beforeDefine}) async {
    final remoteXmlPath = '/var/tmp/virtbackup/restore-${_sanitizeFileName(timestamp)}-${_sanitizeFileName(vmName)}.xml';
    await host.runSshCommand(server, 'mkdir -p "/var/tmp/virtbackup"');
    final xmlTempFile = File('${xmlFile.path}.restore_tmp');
    final defineXmlContent = _removeLibvirtBackingStores(xmlContent);
    await xmlTempFile.writeAsString(defineXmlContent);
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
    if (beforeDefine != null) {
      sendStatus(
        AgentJobStatus(
          id: jobId,
          type: AgentJobType.restore,
          state: AgentJobState.running,
          message: 'Finalizing disk files...',
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
      await beforeDefine();
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
    final restoreStartedAt = DateTime.now();
    final jobId = payload['jobId']?.toString() ?? '';
    final driverId = payload['driverId']?.toString().trim() ?? '';
    final backupPath = payload['backupPath']?.toString() ?? '';
    final decision = payload['decision']?.toString().trim() ?? '';
    if (driverId.isEmpty) {
      throw 'restore requires driverId';
    }
    if (decision.isEmpty) {
      throw 'restore requires decision';
    }
    if (decision != 'overwrite' && decision != 'define' && decision != 'auto_rename' && decision != 'full_check') {
      throw 'unknown restore decision: $decision';
    }
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
    final isFilesystemStorage = driverId == 'filesystem';
    final useStoredBlobs = !isFilesystemStorage && selectedStorage?.useBlobs == true;
    final storeDownloadedBlobs = !isFilesystemStorage && selectedStorage?.storeBlobs == true;
    final downloadConcurrency = isFilesystemStorage ? 1 : settingsStorage?.downloadConcurrency ?? selectedStorage?.downloadConcurrency;
    if (downloadConcurrency == null) {
      throw 'restore requires downloadConcurrency for storage ${selectedStorage?.id ?? ''}';
    }
    final server = ServerConfig.fromMap(serverMap);
    await LogWriter.configureSourcePath(
      source: 'agent',
      path: LogWriter.defaultPathForSource('agent', basePath: settings.backupPath.trim()),
    );
    LogWriter.configureSourceLevel(source: 'agent', level: settings.logLevel);

    final host = BackupAgentHost();
    if (!fullCheckOnly) {
      final missingTools = await host.missingRequiredRemoteTools(server);
      if (missingTools.isNotEmpty) {
        throw 'server is missing required tools: ${missingTools.join(', ')}';
      }
    }

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
      final factory = factories[driverId];
      if (factory == null) {
        throw 'unknown restore driverId: $driverId';
      }
      return factory();
    }

    BackupDriver trackDriver(BackupDriver driver) {
      activeDrivers.add(driver);
      return driver;
    }

    final metadataDriver = trackDriver(buildDriverForSettings(settings.copyWith(blockSizeMB: 1)));
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
              diskSha256: manifestData.diskSha256,
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
      var finalVmName = vmName;
      var finalXmlContent = xmlContent;
      var finalRemoteDiskTargets = remoteDiskTargets;
      var restorePathMap = const <String, String>{};
      var restoreInProgressPathMap = const <String, String>{};

      if (!fullCheckOnly && decision == 'overwrite') {
        ensureNotCanceled();
        await host.runSshCommand(server, 'virsh destroy "$vmName" || true');
        await host.runSshCommand(server, 'virsh undefine "$vmName" --nvram || true');
      }

      if (!fullCheckOnly && decision == 'auto_rename') {
        final autoRenamePlan = await _buildAutoRenamePlanIfNeeded(host: host, server: server, vmName: vmName, timestamp: timestamp, xmlContent: xmlContent, targets: remoteDiskTargets);
        if (autoRenamePlan != null) {
          finalVmName = autoRenamePlan.vmName;
          finalXmlContent = autoRenamePlan.xmlContent;
          restorePathMap = autoRenamePlan.pathMap;
          finalRemoteDiskTargets = remoteDiskTargets
              .map((target) => target.copyWith(remotePath: restorePathMap[target.remotePath] ?? target.remotePath, diskBaseName: _baseName(restorePathMap[target.remotePath] ?? target.diskBaseName)))
              .toList();
          mainPort.send({'type': _typeContext, 'jobId': jobId, 'source': xmlPath, 'target': '${server.name}:$finalVmName'});
          LogWriter.logAgentSync(level: 'info', message: 'restore: auto rename $vmName -> $finalVmName');
        }
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
      for (final target in finalRemoteDiskTargets) {
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
      var restoreWarnings = 0;
      var lastCheckProgressUpdate = DateTime.now();
      if (!fullCheckOnly) {
        restoreInProgressPathMap = _buildRestoreInProgressPathMap(finalRemoteDiskTargets);
      }

      for (var i = 0; i < finalRemoteDiskTargets.length; i += 1) {
        ensureNotCanceled();
        final target = finalRemoteDiskTargets[i];
        final remotePath = target.remotePath;
        sendStatus(
          AgentJobStatus(
            id: jobId,
            type: AgentJobType.restore,
            state: AgentJobState.running,
            message: fullCheckOnly ? 'Sanity check: ${target.diskBaseName}' : 'Uploading disk ${i + 1} of ${finalRemoteDiskTargets.length}...',
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
            return trackDriver(buildDriverForSettings(driverSettings));
          }),
          target.blocks,
          target.blockSize,
          target.fileSize,
          () => canceled,
          localBlobDriver: !(useStoredBlobs || storeDownloadedBlobs)
              ? null
              : localBlobDriversByBlockSizeMB.putIfAbsent(target.blockSizeMB, () => trackDriver(FilesystemBackupDriver(filesystemPath, blockSizeMB: target.blockSizeMB))),
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
                throw 'Sanity check manifest block beyond file_size for ${target.diskBaseName} at block index=$blockIndex';
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

        final uploadPath = restoreInProgressPathMap[remotePath] ?? remotePath;
        final parts = uploadPath.split('/');
        final remoteDir = parts.length > 1 ? parts.sublist(0, parts.length - 1).join('/') : '';
        if (remoteDir.isNotEmpty) {
          await host.runSshCommand(server, 'mkdir -p ${_shellQuote(remoteDir)}');
        }
        await host.runSshCommand(server, 'rm -f ${_shellQuote(remotePath)}');
        await host.runSshCommand(server, 'rm -f ${_shellQuote(uploadPath)}');
        final uploadedSha256 = await host.uploadRemoteStream(
          server,
          uploadPath,
          blobStream,
          onBytes: (bytes) {
            ensureNotCanceled();
            bytesTransferred += bytes;
            final speed = speedTicker.tick(bytes);
            sendStatus(
              AgentJobStatus(
                id: jobId,
                type: AgentJobType.restore,
                state: AgentJobState.running,
                message: 'Uploading disk ${i + 1} of ${finalRemoteDiskTargets.length}...',
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
          isCanceled: () => canceled,
        );
        if (uploadedSha256 != target.diskSha256) {
          restoreWarnings += 1;
          LogWriter.logAgentSync(level: 'warn', message: 'restore: SHA256 mismatch for ${target.diskBaseName}: expected=${target.diskSha256} uploaded=$uploadedSha256');
        }
        if (target.fileSize != null && target.fileSize! > 0) {
          try {
            final remoteSize = await host.runSshCommand(server, 'stat -c %s ${_shellQuote(uploadPath)}');
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
            averageSpeedBytesPerSec: _averageBytesPerSecond(restoreStartedAt, bytesTransferred),
            physicalBytesTransferred: bytesTransferred,
            physicalSpeedBytesPerSec: 0,
            averagePhysicalSpeedBytesPerSec: _averageBytesPerSecond(restoreStartedAt, bytesTransferred),
            totalBytes: totalBytes,
            physicalTotalBytes: totalBytes,
            sanityBytesTransferred: 0,
            sanitySpeedBytesPerSec: 0,
          ),
        );
        return;
      }

      await defineOnly(
        jobId,
        host,
        server,
        xmlFile,
        finalXmlContent,
        timestamp,
        finalVmName,
        beforeDefine: () async {
          await _finalizeRestoreInProgressDisks(host: host, server: server, targets: finalRemoteDiskTargets, inProgressPathMap: restoreInProgressPathMap);
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
              final overlayPath = restorePathMap[rebase.overlayPath] ?? rebase.overlayPath;
              final backingPath = restorePathMap[rebase.backingPath] ?? rebase.backingPath;
              await host.runSshCommand(server, 'qemu-img rebase -u -b ${_shellQuote(backingPath)} ${_shellQuote(overlayPath)}');
            }
          }
        },
      );
      sendResult(
        AgentJobStatus(
          id: jobId,
          type: AgentJobType.restore,
          state: AgentJobState.success,
          message: restoreWarnings == 0 ? 'Restore completed' : 'Restore completed with $restoreWarnings warning${restoreWarnings == 1 ? '' : 's'}',
          totalUnits: totalBytes,
          completedUnits: 0,
          bytesTransferred: bytesTransferred,
          speedBytesPerSec: 0,
          averageSpeedBytesPerSec: _averageBytesPerSecond(restoreStartedAt, bytesTransferred),
          physicalBytesTransferred: bytesTransferred,
          physicalSpeedBytesPerSec: 0,
          averagePhysicalSpeedBytesPerSec: _averageBytesPerSecond(restoreStartedAt, bytesTransferred),
          totalBytes: totalBytes,
          physicalTotalBytes: totalBytes,
          sanityBytesTransferred: 0,
          sanitySpeedBytesPerSec: 0,
        ),
      );
    } catch (error, stackTrace) {
      final isCanceled = error is _Canceled || canceled;
      if (!isCanceled) {
        LogWriter.logAgentSync(level: 'error', message: 'Restore failed: $error');
        if (!_isExpectedRestoreFailure(error)) {
          LogWriter.logAgentSync(level: 'info', message: stackTrace.toString());
        }
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
      activeDrivers.clear();
      if (largeTransferSessionStarted) {
        await host.endLargeTransferSession(server);
      }
    }
  }

  commandPort.listen((message) async {
    final payload = Map<String, dynamic>.from(message as Map);
    final type = payload['type']?.toString();
    if (type == _typeStart) {
      await runZonedGuarded(
        () async {
          await LogWriter.withJobLogging(payload['jobId']?.toString() ?? '', () async {
            await runRestore(payload);
          });
        },
        (error, _) {
          final jobId = payload['jobId']?.toString() ?? '';
          mainPort.send({
            'type': _typeResult,
            'jobId': jobId,
            'status': AgentJobStatus(
              id: jobId,
              type: AgentJobType.restore,
              state: AgentJobState.failure,
              message: error.toString(),
              totalUnits: 0,
              completedUnits: 0,
              bytesTransferred: 0,
              speedBytesPerSec: 0,
              physicalBytesTransferred: 0,
              physicalSpeedBytesPerSec: 0,
              totalBytes: 0,
              sanityBytesTransferred: 0,
              sanitySpeedBytesPerSec: 0,
            ).toMap(),
          });
        },
      );
      Isolate.exit();
    } else if (type == _typeCancel) {
      canceled = true;
      closeActiveDrivers();
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
  final lines = const LineSplitter().convert(content);
  var blockSize = 0;
  int? manifestVersion;
  int? fileSize;
  String? sourcePath;
  String? diskId;
  String? diskSha256;
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
    final diskSha256Value = diskSha256?.trim() ?? '';
    if (diskSha256Value.isEmpty) {
      throw 'manifest incomplete';
    }
    if (!RegExp(r'^[0-9a-f]{64}$').hasMatch(diskSha256Value)) {
      throw 'Manifest metadata invalid: disk_sha256 invalid in ${manifest.path}';
    }
    results.add(
      _ManifestData(
        sourcePath: sourcePath?.trim() ?? '',
        diskId: diskId?.trim() ?? '',
        diskSha256: diskSha256Value,
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
    diskSha256 = null;
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
    if (line.isEmpty) {
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
    if (line.startsWith('disk_sha256:')) {
      diskSha256 = line.substring('disk_sha256:'.length).trim();
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
  final rebases = <_ChainRebase>[];
  for (final data in manifestData) {
    final entriesByOrder = <int, _ChainEntry>{};
    for (final entry in data.chain) {
      if (entriesByOrder.containsKey(entry.order)) {
        throw 'Manifest metadata mismatch: chain order conflict for disk ${data.diskId} at order=${entry.order}';
      }
      entriesByOrder[entry.order] = entry;
    }
    if (entriesByOrder.isEmpty) {
      continue;
    }
    final orders = entriesByOrder.keys.toList()..sort();
    if (orders.first != 0) {
      throw 'Manifest metadata invalid: chain order must start at 0 for disk ${data.diskId}';
    }
    for (var i = 1; i < orders.length; i += 1) {
      if (orders[i] != orders[i - 1] + 1) {
        throw 'Manifest metadata invalid: chain order must be contiguous for disk ${data.diskId}';
      }
    }
    for (var i = orders.length - 1; i > 0; i -= 1) {
      final lower = entriesByOrder[orders[i - 1]]!;
      final upper = entriesByOrder[orders[i]]!;
      if (!restoredPaths.contains(lower.path) || !restoredPaths.contains(upper.path)) {
        continue;
      }
      rebases.add(_ChainRebase(overlayPath: lower.path, backingPath: upper.path));
    }
  }
  return rebases;
}

Future<_AutoRenamePlan?> _buildAutoRenamePlanIfNeeded({
  required BackupAgentHost host,
  required ServerConfig server,
  required String vmName,
  required String timestamp,
  required String xmlContent,
  required List<_RestoreDiskTarget> targets,
}) async {
  final vmExists = await _remoteVmExists(host, server, vmName);
  var diskPathExists = false;
  for (final target in targets) {
    _validateAutoRenameSourcePath(target.remotePath);
    if (await _remotePathExists(host, server, target.remotePath)) {
      diskPathExists = true;
    }
  }
  if (!vmExists && !diskPathExists) {
    return null;
  }

  final baseSuffix = _autoRenameSuffixFromTimestamp(timestamp);
  String? renamedVmName;
  Map<String, String>? pathMap;
  for (var attempt = 0; ; attempt += 1) {
    final suffix = attempt == 0 ? baseSuffix : '$baseSuffix-$attempt';
    final candidateVmName = _sanitizeFileName('$vmName-$suffix');
    if (candidateVmName.isEmpty) {
      throw 'restore auto rename failed: cannot generate VM name';
    }
    if (await _remoteVmExists(host, server, candidateVmName)) {
      continue;
    }

    final candidatePathMap = <String, String>{};
    final candidatePaths = <String>{};
    var pathConflict = false;
    for (final target in targets) {
      final renamedPath = _autoRenamePath(target.remotePath, suffix);
      if (!candidatePaths.add(renamedPath)) {
        throw 'restore auto rename failed: generated duplicate disk path $renamedPath';
      }
      if (await _remotePathExists(host, server, renamedPath)) {
        pathConflict = true;
        break;
      }
      candidatePathMap[target.remotePath] = renamedPath;
    }
    if (pathConflict) {
      continue;
    }
    renamedVmName = candidateVmName;
    pathMap = candidatePathMap;
    break;
  }

  var updatedXml = _replaceDomainNameForAutoRename(xmlContent, oldName: vmName, newName: renamedVmName);
  updatedXml = _removeDomainUuidForAutoRename(updatedXml);
  updatedXml = _replaceXmlDiskPathsForAutoRename(updatedXml, pathMap);
  return _AutoRenamePlan(vmName: renamedVmName, xmlContent: updatedXml, pathMap: pathMap);
}

Future<bool> _remoteVmExists(BackupAgentHost host, ServerConfig server, String vmName) async {
  final result = await host.runSshCommand(server, 'virsh dominfo ${_shellQuote(vmName)}');
  return (result.exitCode ?? 1) == 0;
}

Future<bool> _remotePathExists(BackupAgentHost host, ServerConfig server, String path) async {
  final result = await host.runSshCommand(server, 'test -e ${_shellQuote(path)}');
  return (result.exitCode ?? 1) == 0;
}

Map<String, String> _buildRestoreInProgressPathMap(List<_RestoreDiskTarget> targets) {
  final map = <String, String>{};
  final inProgressPaths = <String>{};
  for (final target in targets) {
    final finalPath = target.remotePath.trim();
    if (finalPath.isEmpty) {
      throw 'restore in-progress path failed: empty target path for ${target.diskBaseName}';
    }
    final inProgressPath = '$finalPath.inprogress';
    if (inProgressPath == finalPath) {
      throw 'restore in-progress path failed: target path is unchanged for ${target.diskBaseName}';
    }
    if (!inProgressPaths.add(inProgressPath)) {
      throw 'restore in-progress path failed: duplicate temporary disk path $inProgressPath';
    }
    map[finalPath] = inProgressPath;
  }
  return map;
}

Future<void> _finalizeRestoreInProgressDisks({
  required BackupAgentHost host,
  required ServerConfig server,
  required List<_RestoreDiskTarget> targets,
  required Map<String, String> inProgressPathMap,
}) async {
  final finalized = <MapEntry<String, String>>[];
  try {
    for (final target in targets) {
      final finalPath = target.remotePath;
      final inProgressPath = inProgressPathMap[finalPath];
      if (inProgressPath == null || inProgressPath.isEmpty) {
        throw 'restore finalize failed: missing temporary path for ${target.diskBaseName}';
      }
      if (!await _remotePathExists(host, server, inProgressPath)) {
        throw 'restore finalize failed: temporary disk does not exist: $inProgressPath';
      }
      if (await _remotePathExists(host, server, finalPath)) {
        throw 'restore finalize failed: target disk already exists: $finalPath';
      }
    }
    for (final target in targets) {
      final finalPath = target.remotePath;
      final inProgressPath = inProgressPathMap[finalPath]!;
      final result = await host.runSshCommand(server, 'mv -- ${_shellQuote(inProgressPath)} ${_shellQuote(finalPath)}');
      if ((result.exitCode ?? 1) != 0) {
        final detail = result.stderr.trim();
        throw detail.isEmpty ? 'restore finalize failed: cannot rename $inProgressPath to $finalPath' : 'restore finalize failed: cannot rename $inProgressPath to $finalPath: $detail';
      }
      finalized.add(MapEntry(inProgressPath, finalPath));
    }
  } catch (_) {
    for (final move in finalized.reversed) {
      if (await _remotePathExists(host, server, move.value) && !await _remotePathExists(host, server, move.key)) {
        try {
          await host.runSshCommand(server, 'mv -- ${_shellQuote(move.value)} ${_shellQuote(move.key)}');
        } catch (_) {}
      }
    }
    rethrow;
  }
}

void _validateAutoRenameSourcePath(String path) {
  final value = path.trim();
  if (value.isEmpty || !value.startsWith('/')) {
    throw 'restore auto rename failed: disk path is not an absolute file path: $path';
  }
  if (value.endsWith('/')) {
    throw 'restore auto rename failed: disk path is a directory path: $path';
  }
  if (value.contains('://') || value.contains('\n') || value.contains('\r') || value.contains('\u0000')) {
    throw 'restore auto rename failed: unsupported disk path: $path';
  }
}

String _autoRenamePath(String originalPath, String suffix) {
  _validateAutoRenameSourcePath(originalPath);
  final normalized = originalPath.trim();
  final slash = normalized.lastIndexOf('/');
  final dir = slash <= 0 ? '' : normalized.substring(0, slash);
  final name = slash < 0 ? normalized : normalized.substring(slash + 1);
  if (name.isEmpty) {
    throw 'restore auto rename failed: disk path has no file name: $originalPath';
  }
  final dot = name.lastIndexOf('.');
  final renamedName = dot > 0 ? '${name.substring(0, dot)}-$suffix${name.substring(dot)}' : '$name-$suffix';
  return '$dir/$renamedName';
}

String _replaceDomainNameForAutoRename(String xml, {required String oldName, required String newName}) {
  final pattern = RegExp('(<name>\\s*)${RegExp.escape(oldName)}(\\s*</name>)');
  final matches = pattern.allMatches(xml).toList();
  if (matches.length != 1) {
    throw 'restore auto rename failed: expected exactly one domain name entry for $oldName, found ${matches.length}';
  }
  return xml.replaceFirst(pattern, '<name>$newName</name>');
}

String _removeDomainUuidForAutoRename(String xml) {
  final pattern = RegExp(r'\s*<uuid>[^<]+</uuid>');
  final matches = pattern.allMatches(xml).toList();
  if (matches.length > 1) {
    throw 'restore auto rename failed: expected at most one domain uuid entry, found ${matches.length}';
  }
  if (matches.isEmpty) {
    return xml;
  }
  return xml.replaceFirst(pattern, '');
}

String _removeLibvirtBackingStores(String xml) {
  var updated = xml;
  while (true) {
    final range = _findFirstXmlElementRange(updated, 'backingStore');
    if (range == null) {
      return updated;
    }
    updated = updated.replaceRange(range.start, range.end, '');
  }
}

({int start, int end})? _findFirstXmlElementRange(String xml, String elementName) {
  final openPattern = RegExp('<$elementName\\b[^>]*>');
  final closePattern = RegExp('</$elementName\\s*>');
  final firstOpen = openPattern.firstMatch(xml);
  if (firstOpen == null) {
    return null;
  }
  var removeStart = firstOpen.start;
  final previousNewline = xml.lastIndexOf('\n', firstOpen.start);
  if (previousNewline >= 0 && xml.substring(previousNewline + 1, firstOpen.start).trim().isEmpty) {
    removeStart = previousNewline;
  }
  if (xml.substring(firstOpen.start, firstOpen.end).endsWith('/>')) {
    var removeEnd = firstOpen.end;
    if (removeEnd < xml.length && xml.codeUnitAt(removeEnd) == 10) {
      removeEnd += 1;
    }
    return (start: removeStart, end: removeEnd);
  }

  var depth = 1;
  var cursor = firstOpen.end;
  while (depth > 0) {
    final nextOpen = _firstXmlMatchAfter(openPattern, xml, cursor);
    final nextClose = _firstXmlMatchAfter(closePattern, xml, cursor);
    final nextOpenStart = nextOpen?.start ?? -1;
    final nextCloseStart = nextClose?.start ?? -1;
    if (nextCloseStart < 0) {
      throw 'restore failed: invalid domain XML backingStore element.';
    }
    if (nextOpenStart >= 0 && nextOpenStart < nextCloseStart) {
      final openEnd = nextOpen!.end;
      if (!xml.substring(nextOpenStart, openEnd).endsWith('/>')) {
        depth += 1;
      }
      cursor = openEnd;
      continue;
    }
    depth -= 1;
    cursor = nextClose!.end;
  }
  if (cursor < xml.length && xml.codeUnitAt(cursor) == 10) {
    cursor += 1;
  }
  return (start: removeStart, end: cursor);
}

({int start, int end})? _firstXmlMatchAfter(RegExp pattern, String xml, int cursor) {
  final tail = xml.substring(cursor);
  final match = pattern.firstMatch(tail);
  if (match == null) {
    return null;
  }
  return (start: cursor + match.start, end: cursor + match.end);
}

String _replaceXmlDiskPathsForAutoRename(String xml, Map<String, String> pathMap) {
  var updated = xml;
  var replacements = 0;
  for (final entry in pathMap.entries) {
    final oldPath = entry.key;
    final newPath = entry.value;
    final doubleQuotePattern = RegExp("file=\"${RegExp.escape(oldPath)}\"");
    final singleQuotePattern = RegExp("file='${RegExp.escape(oldPath)}'");
    final doubleMatches = doubleQuotePattern.allMatches(updated).length;
    final singleMatches = singleQuotePattern.allMatches(updated).length;
    if (doubleMatches + singleMatches > 1) {
      throw 'restore auto rename failed: disk path appears multiple times in XML: $oldPath';
    }
    if (doubleMatches == 1) {
      updated = updated.replaceFirst(doubleQuotePattern, 'file="$newPath"');
      replacements += 1;
    }
    if (singleMatches == 1) {
      updated = updated.replaceFirst(singleQuotePattern, "file='$newPath'");
      replacements += 1;
    }
  }
  if (replacements == 0) {
    throw 'restore auto rename failed: no file-based disk source paths found in XML';
  }
  return updated;
}

String _autoRenameSuffixFromTimestamp(String timestamp) {
  final match = RegExp(r'^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})').firstMatch(timestamp.trim());
  if (match == null) {
    throw 'restore auto rename failed: cannot derive suffix from timestamp $timestamp';
  }
  return '${match.group(1)}${match.group(2)}${match.group(3)}-${match.group(4)}${match.group(5)}${match.group(6)}';
}

String _shellQuote(String value) {
  return "'${value.replaceAll("'", "'\"'\"'")}'";
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
  var totalEmitted = 0;
  final remote = driver is RemoteBlobDriver ? driver as RemoteBlobDriver : null;
  if (remote != null) {
    final maxConcurrent = maxConcurrentDownloads;
    var nextIndex = 0;
    var nextEmit = 0;
    final inFlight = <int, Future<_BlockData>>{};

    Future<_BlockData> startFetch(int index) async {
      if (isCanceled?.call() == true) {
        throw const _Canceled();
      }
      final block = blocks[index];
      final expectedLength = totalSize == null ? blockSize : _blockLengthForIndex(index, totalSize, blockSize);
      if (expectedLength <= 0) {
        throw 'restore manifest block beyond file_size at index=$index';
      }
      if (block.zeroRun) {
        return _BlockData(index, expectedLength > 0 ? Uint8List(expectedLength) : const <int>[]);
      }
      final hash = block.hash;
      if (hash == null) {
        return const _BlockData.empty();
      }
      if (useStoredBlobs && localBlobDriver != null) {
        final localBytes = await _readLocalBlob(localBlobDriver, hash, expectedLength, index, cache: localBlobCache);
        if (localBytes != null) {
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
      return _BlockData(index, bytes);
    }

    void schedule() {
      while (inFlight.length < maxConcurrent && nextIndex < blocks.length) {
        final index = nextIndex;
        final future = startFetch(index);
        unawaited(future.catchError((_) => const _BlockData.empty()));
        inFlight[index] = future;
        nextIndex += 1;
      }
    }

    try {
      schedule();
      while (nextEmit < blocks.length) {
        schedule();
        final future = inFlight[nextEmit];
        if (future == null) {
          await Future<void>.delayed(const Duration(milliseconds: 1));
          continue;
        }
        final data = await future;
        inFlight.remove(nextEmit);
        if (data.bytes.isNotEmpty) {
          yield data.bytes;
          totalEmitted += data.bytes.length;
        }
        nextEmit += 1;
      }
    } finally {
      for (final future in inFlight.values) {
        try {
          await future;
        } catch (_) {}
      }
    }
  } else {
    final maxConcurrent = maxConcurrentDownloads;
    var nextIndex = 0;
    var nextEmit = 0;
    final inFlight = <int, Future<_BlockData>>{};

    Future<_BlockData> startFetchLocal(int index) async {
      if (isCanceled?.call() == true) {
        throw const _Canceled();
      }
      final block = blocks[index];
      final expectedLength = totalSize == null ? blockSize : _blockLengthForIndex(index, totalSize, blockSize);
      if (expectedLength <= 0) {
        throw 'restore manifest block beyond file_size at index=$index';
      }
      if (block.zeroRun) {
        if (expectedLength > 0) {
          return _BlockData(index, Uint8List(expectedLength));
        }
        return const _BlockData.empty();
      }
      final hash = block.hash;
      if (hash == null) {
        return const _BlockData.empty();
      }
      final localBytes = await _readLocalBlob(driver, hash, expectedLength, index, cache: localBlobCache);
      if (localBytes == null) {
        final blobFile = driver.blobFile(hash);
        throw 'restore missing local blob hash=$hash index=$index path=${blobFile.path}';
      }
      return _BlockData(index, localBytes);
    }

    void schedule() {
      while (inFlight.length < maxConcurrent && nextIndex < blocks.length) {
        final index = nextIndex;
        final future = startFetchLocal(index);
        unawaited(future.catchError((_) => const _BlockData.empty()));
        inFlight[index] = future;
        nextIndex += 1;
      }
    }

    try {
      schedule();
      while (nextEmit < blocks.length) {
        schedule();
        final future = inFlight[nextEmit];
        if (future == null) {
          await Future<void>.delayed(const Duration(milliseconds: 1));
          continue;
        }
        final data = await future;
        inFlight.remove(nextEmit);
        if (data.bytes.isNotEmpty) {
          yield data.bytes;
          totalEmitted += data.bytes.length;
        }
        nextEmit += 1;
      }
    } finally {
      for (final future in inFlight.values) {
        try {
          await future;
        } catch (_) {}
      }
    }
  }
  if (totalSize != null && totalEmitted < totalSize) {
    throw 'restore manifest incomplete: emitted $totalEmitted bytes, expected $totalSize';
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
    required this.diskSha256,
    required this.blockSize,
    required this.blockSizeMB,
    required this.fileSize,
  });

  final File manifest;
  final String diskBaseName;
  final String remotePath;
  final List<_BlockRef> blocks;
  final String diskSha256;
  final int blockSize;
  final int blockSizeMB;
  final int? fileSize;

  _RestoreDiskTarget copyWith({String? diskBaseName, String? remotePath}) {
    return _RestoreDiskTarget(
      manifest: manifest,
      diskBaseName: diskBaseName ?? this.diskBaseName,
      remotePath: remotePath ?? this.remotePath,
      blocks: blocks,
      diskSha256: diskSha256,
      blockSize: blockSize,
      blockSizeMB: blockSizeMB,
      fileSize: fileSize,
    );
  }
}

class _AutoRenamePlan {
  const _AutoRenamePlan({required this.vmName, required this.xmlContent, required this.pathMap});

  final String vmName;
  final String xmlContent;
  final Map<String, String> pathMap;
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
    required this.diskSha256,
    required this.blockSize,
    required this.blockSizeMB,
    required this.blocks,
    required this.fileSize,
    required this.domainXml,
    required this.chain,
  });

  final String sourcePath;
  final String diskId;
  final String diskSha256;
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
