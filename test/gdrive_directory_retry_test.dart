import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:flutter_test/flutter_test.dart';
import 'package:http/http.dart' as http;
import 'package:http/io_client.dart';
import 'package:virtbackup/agent/drv/gdrive_driver.dart';
import 'package:virtbackup/common/models.dart';
import 'package:virtbackup/common/settings.dart';

class _LocalDriveClient extends http.BaseClient {
  _LocalDriveClient(this.server);
  final HttpServer server;
  final http.Client _inner = IOClient();
  var aborted = 0;
  var closed = false;

  @override
  Future<http.StreamedResponse> send(http.BaseRequest request) {
    final abortTrigger = (request as http.Abortable).abortTrigger!;
    unawaited(abortTrigger.then((_) => aborted++));
    final localUri = request.url.replace(scheme: 'http', host: server.address.address, port: server.port);
    final forwarded = http.AbortableRequest(request.method, localUri, abortTrigger: abortTrigger)..headers.addAll(request.headers);
    return _inner.send(forwarded);
  }

  @override
  void close() {
    closed = true;
    _inner.close();
  }
}

void main() {
  for (final stallBody in [false, true]) {
    test('a directory timeout aborts ${stallBody ? 'the response body' : 'the header wait'}, logs the error and succeeds through retry', () async {
      await _exerciseTimeout(stallBody: stallBody, alwaysStall: false, cancel: false);
    });
  }
  test('exhausted directory timeouts fail instead of returning an empty listing', () async {
    await _exerciseTimeout(stallBody: false, alwaysStall: true, cancel: false);
  });
  test('closing connections during a stalled directory request stops without retry', () async {
    await _exerciseTimeout(stallBody: false, alwaysStall: true, cancel: true);
  });
}

Future<void> _exerciseTimeout({required bool stallBody, required bool alwaysStall, required bool cancel}) async {
  final root = await Directory.systemTemp.createTemp('virtbackup-directory-retry-');
  final server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
  final client = _LocalDriveClient(server);
  final stalled = Completer<void>();
  final responses = <HttpResponse>[];
  var attempts = 0;
  var clientCreations = 0;
  final subscription = server.listen((request) async {
    final query = request.uri.queryParameters['q']!;
    final parent = RegExp("'([^']+)' in parents").firstMatch(query)![1]!;
    if (query.startsWith("mimeType!='")) {
      attempts++;
      if (alwaysStall || attempts == 1) {
        responses.add(request.response);
        if (stallBody) {
          request.response.write('{"files":[');
          await request.response.flush();
        }
        if (!stalled.isCompleted) stalled.complete();
        return;
      }
      request.response.write(
        jsonEncode({
          'files': [
            {
              'id': 'blob',
              'name': '05hash',
              'parents': [parent],
            },
          ],
        }),
      );
    } else {
      final names = switch (parent) {
        'root' => ['VirtBackup'],
        'VirtBackup' => ['blobs'],
        'blobs' => ['4'],
        '4' => ['05'],
        _ => throw StateError('Unexpected parent: $parent'),
      };
      request.response.write(
        jsonEncode({
          'files': names
              .map(
                (name) => {
                  'id': name,
                  'name': name,
                  'parents': [parent],
                },
              )
              .toList(),
        }),
      );
    }
    await request.response.close();
  });
  final storage = BackupStorage(
    id: 'drive',
    name: 'Drive',
    driverId: 'gdrive',
    enabled: true,
    params: {'rootPath': '/', 'accessToken': 'test-token', 'expiresAt': '2099-01-01T00:00:00Z'},
    uploadConcurrency: 8,
    downloadConcurrency: 8,
  );
  final driver = GdriveBackupDriver(
    settings: AppSettings.empty().copyWith(backupPath: root.path, blockSizeMB: 4, logLevel: 'debug', storage: [storage], backupStorageId: storage.id),
    persistSettings: (_, _) async => fail('No token refresh expected.'),
    createApiClient: () {
      clientCreations++;
      return client;
    },
    directoryRequestTimeout: const Duration(milliseconds: 300),
    retryBaseDelay: Duration.zero,
  );
  try {
    await driver.ensureReady();
    final listing = driver.listBlobNames('05');
    if (cancel) {
      final failure = expectLater(listing, throwsA(contains('storage connection was closed')));
      await stalled.future;
      await driver.closeConnections();
      await failure;
      expect(attempts, 1);
    } else if (alwaysStall) {
      await expectLater(listing, throwsA(contains('failed after retry')));
      expect(attempts, 6);
      expect(client.aborted, 6);
    } else {
      expect(await listing, {'05hash'});
      expect(attempts, 2);
      expect(client.aborted, 1);
    }
    expect(clientCreations, 1, reason: 'A listing retry must not reset the shared HTTP client.');
    if (!cancel) expect(client.closed, isFalse);
    final log = await File('${root.path}/VirtBackup/logs/agent.log').readAsString();
    if (!cancel) {
      expect(log, contains('level=error message=driver=gdrive gdrive: api request failed action=list requestId='));
      expect(log, contains('TimeoutException'));
      expect(log, contains('target=blobs/4/05'));
      expect(log, contains('request aborted'));
      expect(log, contains('level=warn message=driver=gdrive gdrive: list files retrying after error:'));
      expect(log, contains('retry=1/5 delayMs=0'));
    } else {
      expect(log, isNot(contains('retrying after error')));
    }
  } finally {
    await driver.closeConnections();
    for (final response in responses) {
      try {
        await response.close();
      } catch (_) {
        /* The timed-out client already disconnected. */
      }
    }
    await subscription.cancel();
    await server.close(force: true);
    await root.delete(recursive: true);
  }
}
