import 'dart:async';
import 'dart:io';

import 'package:flutter_test/flutter_test.dart';
import 'package:virtbackup/agent/backup.dart';
import 'package:virtbackup/common/models.dart';
import 'package:virtbackup/agent/drv/filesystem_driver.dart';
import 'package:virtbackup/agent/workers/blob_cache_worker.dart';
import 'package:virtbackup/common/log_writer.dart';

final _firstHash = 'aa${'1' * 62}';
final _secondHash = 'aa${'2' * 62}';
final _otherShardHash = 'bb${'3' * 62}';

class _ListingDriver extends FilesystemBackupDriver {
  _ListingDriver() : super('/unused', blockSizeMB: 1);
  final calls = <String, int>{};
  final responses = <String, Completer<Set<String>>>{'aa': Completer<Set<String>>(), 'bb': Completer<Set<String>>()};
  final started = <String, Completer<void>>{'aa': Completer<void>(), 'bb': Completer<void>()};

  @override
  BackupDriverCapabilities get capabilities => BackupDriverCapabilities.forDriver('gdrive');

  @override
  Future<Set<String>> listBlobShards() async => {'aa', 'bb'};

  @override
  Future<Set<String>> listBlobNames(String shard) async {
    calls.update(shard, (count) => count + 1, ifAbsent: () => 1);
    if (!started[shard]!.isCompleted) started[shard]!.complete();
    return responses[shard]!.future;
  }
}

class _BoundedListingDriver extends FilesystemBackupDriver {
  _BoundedListingDriver() : super('/unused', blockSizeMB: 1);
  final responses = <String, Completer<Set<String>>>{};
  final firstWave = Completer<void>();
  var active = 0;
  var peak = 0;

  @override
  BackupDriverCapabilities get capabilities => BackupDriverCapabilities.forDriver('gdrive');

  @override
  Future<Set<String>> listBlobShards() async => Set.from(List.generate(12, (i) => i.toRadixString(16).padLeft(2, '0')));

  @override
  Future<Set<String>> listBlobNames(String shard) async {
    active++;
    if (active > peak) peak = active;
    final response = Completer<Set<String>>();
    responses[shard] = response;
    if (responses.length == 8) firstWave.complete();
    try {
      return await response.future;
    } finally {
      active--;
    }
  }
}

class _SerialListingDriver extends _ListingDriver {
  @override
  BackupDriverCapabilities get capabilities => BackupDriverCapabilities.forDriver('filesystem');
}

class _CachedBackupDriver extends FilesystemBackupDriver {
  _CachedBackupDriver(super.root) : super(blockSizeMB: 1);
  var listings = 0;
  var closed = false;

  @override
  Future<Set<String>> listBlobShards() async => Set.from(List.generate(256, (index) => index.toRadixString(16).padLeft(2, '0')));

  @override
  Future<Set<String>> listBlobNames(String shard) async {
    listings++;
    await Future<void>.delayed(const Duration(milliseconds: 10));
    return {_firstHash, _secondHash};
  }

  @override
  Future<void> closeConnections() async {
    closed = true;
  }
}

void main() {
  late Directory logs;
  setUp(() async {
    logs = await Directory.systemTemp.createTemp('virtbackup-cache-test-');
    await LogWriter.configureSourcePath(source: 'agent', path: '${logs.path}/agent.log');
    LogWriter.configureSourceLevel(source: 'agent', level: 'error');
  });
  tearDown(() async => logs.delete(recursive: true));

  BlobDirectoryCache cacheFor(_ListingDriver driver) =>
      BlobDirectoryCache(driver: driver, createShard: (_) async => fail('Unexpected directory creation'), ensureNotCanceled: () {}, ensureMissingShards: false);

  test('a fully deduplicated backup saves its manifest and reaches terminal status', () async {
    final driver = _CachedBackupDriver(logs.path);
    final progress = <BackupAgentProgress>[];
    final dependencies = BackupAgentDependencies(
      runSshCommand: (_, command) async => SshCommandResult(
        stdout: command.startsWith('virsh dumpxml')
            ? '<domain><name>vm</name></domain>'
            : command.startsWith('virsh domstate')
            ? 'shut off'
            : 'image: /images/disk.qcow2',
        stderr: '',
        exitCode: 0,
      ),
      loadVmDiskPaths: (_, vm, {inactive = false}) async => [const MapEntry('vda', '/images/disk.qcow2')],
      downloadRemoteFile: (_, path, file, {onBytes}) async => fail('Unexpected download'),
      streamRemoteFile: (_, path, {required onChunk, onBytes}) async => fail('Unexpected disk stream'),
      streamRemoteRange: (_, path, offset, length, {required onChunk, onBytes}) async => fail('Existing blobs must not be fetched'),
      createVmSnapshot: (_, vm, name) async => fail('Stopped VM must not be snapshotted'),
      commitVmSnapshot: (_, vm, disks) async => fail('Stopped VM must not be committed'),
      cleanupActiveOverlays: (_, vm, active, inactive) async {},
      sanitizeFileName: (name) => name,
      uploadLocalFile: (_, source, target, {onBytes}) async => fail('Unexpected hypervisor upload'),
      ensureHashblocks: (_) async => '/hashblocks',
      streamHashblocks: (_, executable, path, blockSize, {required onLine, onController}) async {
        onController!(HashblocksController((_) {}));
        await onLine('0 -> $_firstHash');
        await onLine('1 -> $_firstHash');
        await onLine('2 -> $_secondHash');
        await onLine('SHA256 ${'c' * 64}');
      },
      getRemoteFileSize: (_, path) async => 3 * 1024 * 1024,
    );
    final agent = BackupAgent(dependencies: dependencies, onProgress: progress.add, blockSizeBytes: 1024 * 1024, requireSimpleDisksForBackup: false);
    try {
      final result = await agent
          .runVmBackup(
            server: ServerConfig.fromMap({'id': 'server', 'type': 'ssh'}),
            vm: VmEntry(id: 'vm', name: 'vm', powerState: VmPowerState.stopped),
            driver: driver,
          )
          .timeout(const Duration(seconds: 3));
      expect(result.success, isTrue);
      expect(driver.listings, 1);
      expect(driver.closed, isTrue);
      expect(progress.last.isRunning, isFalse);
      expect(progress.last.bytesTransferred, 3 * 1024 * 1024);
      final manifests = await Directory('${driver.storage}/manifests').list(recursive: true).where((entity) => entity is File && entity.path.endsWith('.manifest.gz')).toList();
      expect(manifests, hasLength(1));
    } finally {
      agent.dispose();
    }
  });

  test('prefetch and required existence checks share one pending listing per shard', () async {
    final driver = _ListingDriver();
    final cache = cacheFor(driver);
    final prefetch = cache.prefetchHash(_firstHash);
    await driver.started['aa']!.future;
    var checkFinished = false;
    final checks = Future.wait(List.generate(50, (index) => cache.blobExists(index.isEven ? _firstHash : _secondHash))).then((values) {
      checkFinished = true;
      return values;
    });
    await Future<void>.delayed(const Duration(milliseconds: 20));
    expect(driver.calls, {'aa': 1});
    expect(checkFinished, isFalse, reason: 'Required scans must wait rather than treating a pending scan as a missing blob.');
    driver.responses['aa']!.complete({_firstHash, _secondHash});
    await prefetch;
    expect(await checks, everyElement(isTrue));
    expect(await cache.blobExists(_firstHash), isTrue);
    expect(driver.calls, {'aa': 1});
  });

  test('prefetches and required checks share the driver listing limit', () async {
    final driver = _BoundedListingDriver();
    final cache = BlobDirectoryCache(driver: driver, createShard: (_) async => fail('Unexpected directory creation'), ensureNotCanceled: () {}, ensureMissingShards: false);
    final hashes = List.generate(12, (i) => '${i.toRadixString(16).padLeft(2, '0')}${'1' * 62}');
    final prefetches = hashes.take(10).map(cache.prefetchHash).toList();
    final checks = hashes.skip(10).map(cache.blobExists).toList();
    await driver.firstWave.future;
    await Future<void>.delayed(Duration.zero);
    expect(driver.responses, hasLength(8));
    expect(cache.diagnosticState, contains('queuedListings=4'));
    for (final response in driver.responses.values.toList()) {
      response.complete({});
    }
    await Future<void>.delayed(Duration.zero);
    expect(driver.responses, hasLength(12));
    for (final response in driver.responses.values) {
      if (!response.isCompleted) response.complete({});
    }
    await Future.wait(prefetches);
    expect(await Future.wait(checks), everyElement(isFalse));
    expect(driver.peak, 8);
    expect(driver.active, 0);
  });

  test('a failed listing releases its slot for a queued shard', () async {
    final driver = _SerialListingDriver();
    final cache = cacheFor(driver);
    final first = cache.blobExists(_firstHash);
    final failure = expectLater(first, throwsStateError);
    final second = cache.blobExists(_otherShardHash);
    await driver.started['aa']!.future;
    expect(driver.started['bb']!.isCompleted, isFalse);
    driver.responses['aa']!.completeError(StateError('listing failed'));
    await failure;
    await driver.started['bb']!.future;
    driver.responses['bb']!.complete({_otherShardHash});
    expect(await second, isTrue);
  });

  test('different shards can list concurrently without blocking each other', () async {
    final driver = _ListingDriver();
    final cache = cacheFor(driver);
    final first = cache.blobExists(_firstHash);
    final other = cache.blobExists(_otherShardHash);
    await Future.wait(driver.started.values.map((started) => started.future));
    expect(driver.calls, {'aa': 1, 'bb': 1});
    driver.responses['aa']!.complete({_firstHash});
    expect(await first, isTrue);
    driver.responses['bb']!.complete({_otherShardHash});
    expect(await other, isTrue);
  });

  test('listing errors reach every waiter and do not become a cached missing result', () async {
    final driver = _ListingDriver();
    final cache = cacheFor(driver);
    final prefetch = cache.prefetchHash(_firstHash);
    await driver.started['aa']!.future;
    final required = cache.blobExists(_firstHash);
    final expectedPrefetch = expectLater(prefetch, throwsStateError);
    final expectedRequired = expectLater(required, throwsStateError);
    driver.responses['aa']!.completeError(StateError('directory listing failed'));
    await Future.wait([expectedPrefetch, expectedRequired]);
    expect(driver.calls, {'aa': 1});
    driver.responses['aa'] = Completer<Set<String>>()..complete({_firstHash});
    expect(await cache.blobExists(_firstHash), isTrue);
    expect(driver.calls, {'aa': 2});
  });

  test('a listing finishing after a write preserves newly registered hashes', () async {
    final driver = _ListingDriver();
    final cache = cacheFor(driver);
    final pending = cache.prefetchHash(_firstHash);
    await driver.started['aa']!.future;
    cache.markHashKnown(_secondHash);
    driver.responses['aa']!.complete({_firstHash});
    await pending;
    expect(await cache.blobExists(_firstHash), isTrue);
    expect(await cache.blobExists(_secondHash), isTrue);
    expect(driver.calls, {'aa': 1});
  });

  test('normal shutdown drains prefetches without aborting driver connections', () async {
    final started = Completer<void>();
    final request = Completer<void>();
    final worker = BlobCacheWorker(
      initialize: () async {},
      processHash: (_) {
        started.complete();
        return request.future;
      },
    );
    unawaited(worker.run());
    worker.enqueue(_firstHash);
    await started.future;
    final finishing = worker.finish(timeout: const Duration(seconds: 1), abortPendingRequests: () async => fail('Successful drain must not abort connections'));
    request.complete();
    await finishing;
    worker.throwIfError();
  });

  test('shutdown aborts a stuck prefetch and ignores its intentional cancellation error', () async {
    final started = Completer<void>();
    final request = Completer<void>();
    var aborted = false;
    final worker = BlobCacheWorker(
      initialize: () async {},
      processHash: (_) {
        started.complete();
        return request.future;
      },
    );
    final running = worker.run();
    worker.enqueue(_firstHash);
    await started.future;
    await worker.finish(
      timeout: const Duration(milliseconds: 20),
      abortPendingRequests: () async {
        aborted = true;
        request.completeError(StateError('HTTP connection closed'));
      },
    );
    await running;
    expect(aborted, isTrue);
    worker.throwIfError();
  });

  test('shutdown preserves an earlier scan error while aborting another stuck request', () async {
    final stuckStarted = Completer<void>();
    final stuck = Completer<void>();
    final failed = Completer<void>();
    var aborted = false;
    final worker = BlobCacheWorker(
      initialize: () async {},
      processHash: (hash) {
        if (hash == _firstHash) {
          stuckStarted.complete();
          return stuck.future;
        }
        return failed.future;
      },
    );
    final running = worker.run();
    worker.enqueue(_firstHash);
    worker.enqueue(_otherShardHash);
    await stuckStarted.future;
    failed.completeError(StateError('original scan failure'));
    await Future<void>.delayed(Duration.zero);
    await expectLater(
      worker.finish(
        timeout: const Duration(milliseconds: 20),
        abortPendingRequests: () async {
          aborted = true;
          stuck.completeError(StateError('HTTP connection closed'));
        },
      ),
      throwsA(isA<StateError>().having((error) => error.message, 'message', 'original scan failure')),
    );
    await running;
    expect(aborted, isTrue);
  });

  for (final abortTimesOut in [false, true]) {
    test('shutdown preserves the scan error and stack when aborting ${abortTimesOut ? 'times out' : 'fails'}', () async {
      final started = Completer<void>();
      final stuck = Completer<void>();
      final failed = Completer<void>();
      final abort = Completer<void>();
      final scanError = StateError('original scan failure');
      final scanStack = StackTrace.fromString('original scan stack');
      final worker = BlobCacheWorker(
        initialize: () async {},
        processHash: (hash) {
          if (hash == _firstHash) {
            started.complete();
            return stuck.future;
          }
          return failed.future;
        },
      );
      final running = worker.run();
      worker.enqueue(_firstHash);
      worker.enqueue(_otherShardHash);
      await started.future;
      failed.completeError(scanError, scanStack);
      await Future<void>.delayed(Duration.zero);
      try {
        await worker.finish(
          timeout: const Duration(milliseconds: 20),
          abortPendingRequests: () async {
            if (abortTimesOut) return abort.future;
            throw StateError('cannot close connections');
          },
        );
        fail('Shutdown must report the original scan failure.');
      } on StateError catch (error, stack) {
        expect(error, same(scanError));
        expect(stack.toString(), scanStack.toString());
      }
      await running;
      final log = await File('${logs.path}/agent.log').readAsString();
      expect(log, contains('blob-cache request abort failed:'));
      expect(log, contains(abortTimesOut ? 'TimeoutException' : 'cannot close connections'));
      abort.complete();
      stuck.completeError(StateError('late closed request'));
      await Future<void>.delayed(Duration.zero);
    });
  }

  test('a failed request abort makes shutdown fail instead of reporting success', () async {
    final started = Completer<void>();
    final request = Completer<void>();
    final worker = BlobCacheWorker(
      initialize: () async {},
      processHash: (_) {
        started.complete();
        return request.future;
      },
    );
    final running = worker.run();
    worker.enqueue(_firstHash);
    await started.future;
    await expectLater(worker.finish(timeout: const Duration(milliseconds: 20), abortPendingRequests: () async => throw StateError('cannot close connections')), throwsStateError);
    await running;
    request.completeError(StateError('late closed request'));
    await Future<void>.delayed(Duration.zero);
  });
}
