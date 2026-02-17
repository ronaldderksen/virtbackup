import 'dart:io';

import 'package:flutter_test/flutter_test.dart';
import 'package:virtbackup/agent/settings_store.dart';
import 'package:virtbackup/common/models.dart';
import 'package:virtbackup/common/settings.dart';

void main() {
  test('save does not mutate in-memory Google Drive tokens', () async {
    final tempDir = await Directory.systemTemp.createTemp('virtbackup_settings_store_test_');
    try {
      final settingsFile = File('${tempDir.path}${Platform.pathSeparator}agent.yaml');
      final store = AppSettingsStore(file: settingsFile);
      final settings = AppSettings(
        backupPath: '/tmp/virtbackup',
        logLevel: 'info',
        destinations: <BackupDestination>[
          BackupDestination(
            id: AppSettings.filesystemDestinationId,
            name: AppSettings.filesystemDestinationName,
            driverId: 'filesystem',
            enabled: true,
            params: <String, dynamic>{'path': '/tmp/virtbackup'},
          ),
          BackupDestination(
            id: 'dest-gdrive',
            name: 'Google Drive',
            driverId: 'gdrive',
            enabled: true,
            params: <String, dynamic>{'refreshToken': 'refresh-token-value', 'accessToken': 'access-token-value'},
          ),
        ],
        backupDestinationId: 'dest-gdrive',
        servers: <ServerConfig>[],
        connectionVerified: true,
        hashblocksLimitBufferMb: 1024,
        blockSizeMB: 1,
        dummyDriverTmpWrites: false,
        ntfymeToken: '',
      );

      await store.save(settings);

      final gdriveParams = settings.destinations[1].params;
      expect(gdriveParams['refreshToken'], 'refresh-token-value');
      expect(gdriveParams['accessToken'], 'access-token-value');
    } finally {
      await tempDir.delete(recursive: true);
    }
  });
}
