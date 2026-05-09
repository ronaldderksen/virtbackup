import 'dart:io';

import 'package:flutter_test/flutter_test.dart';
import 'package:yaml/yaml.dart';
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
        storage: <BackupStorage>[
          BackupStorage(id: AppSettings.filesystemStorageId, name: AppSettings.filesystemStorageName, driverId: 'filesystem', enabled: true, params: <String, dynamic>{'path': '/tmp/virtbackup'}),
          BackupStorage(
            id: 'dest-gdrive',
            name: 'Google Drive',
            driverId: 'gdrive',
            enabled: true,
            params: <String, dynamic>{'refreshToken': 'refresh-token-value', 'accessToken': 'access-token-value'},
          ),
        ],
        backupStorageId: 'dest-gdrive',
        servers: <ServerConfig>[],
        connectionVerified: true,
        blockSizeMB: 1,
        dummyDriverTmpWrites: false,
        maxConcurrentBackupRestoreJobs: 1,
        maxConcurrentJobsPerVm: 1,
        maxConcurrentJobsPerStorage: 1,
        ntfymeToken: '',
        schedules: <ScheduledJob>[],
      );

      await store.save(settings);

      final gdriveParams = settings.storage[1].params;
      expect(gdriveParams['refreshToken'], 'refresh-token-value');
      expect(gdriveParams['accessToken'], 'access-token-value');
    } finally {
      await tempDir.delete(recursive: true);
    }
  });

  test('save preserves schedules for other host groups', () async {
    final tempDir = await Directory.systemTemp.createTemp('virtbackup_settings_store_schedule_groups_test_');
    try {
      final settingsFile = File('${tempDir.path}${Platform.pathSeparator}agent.yaml');
      await settingsFile.writeAsString('''
schedules:
  other-host:
    - id: schedule-other
      name: Other host schedule
      enabled: true
      waitForRunningJobs: false
      type: backup
      frequency: hourly
      time: '00:10'
      weekdays: []
      serverId: server-other
      storageId: filesystem
      vmName: vm-other
      restoreXmlPath: ''
      restoreDecision: ''
''');
      final store = AppSettingsStore(file: settingsFile);
      final settings = AppSettings(
        backupPath: '/tmp/virtbackup',
        logLevel: 'info',
        storage: <BackupStorage>[
          BackupStorage(id: AppSettings.filesystemStorageId, name: AppSettings.filesystemStorageName, driverId: 'filesystem', enabled: true, params: <String, dynamic>{'path': '/tmp/virtbackup'}),
        ],
        backupStorageId: AppSettings.filesystemStorageId,
        servers: <ServerConfig>[],
        connectionVerified: true,
        blockSizeMB: 1,
        dummyDriverTmpWrites: false,
        maxConcurrentBackupRestoreJobs: 1,
        maxConcurrentJobsPerVm: 1,
        maxConcurrentJobsPerStorage: 1,
        ntfymeToken: '',
        schedules: <ScheduledJob>[],
      );

      await store.save(settings);

      final decoded = loadYaml(await settingsFile.readAsString()) as YamlMap;
      final schedules = decoded['schedules'] as YamlMap;
      expect(schedules['other-host'], isA<YamlList>());
      expect((schedules['other-host'] as YamlList).single['id'], 'schedule-other');
    } finally {
      await tempDir.delete(recursive: true);
    }
  });
}
