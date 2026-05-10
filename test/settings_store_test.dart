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
        virtBackupAccount: VirtBackupAccountTokens.empty(),
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
        virtBackupAccount: VirtBackupAccountTokens.empty(),
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

  test('save preserves Virt Backup account tokens for other host groups', () async {
    final tempDir = await Directory.systemTemp.createTemp('virtbackup_settings_store_account_groups_test_');
    try {
      final settingsFile = File('${tempDir.path}${Platform.pathSeparator}agent.yaml');
      await settingsFile.writeAsString('''
virtBackupAccount:
  other-host:
    email: other@example.com
    accountBaseUrl: https://virtbackup.net
    accessToken: ''
    accessTokenExpiresAt: '2026-05-17T00:00:00.000Z'
    refreshToken: ''
    refreshTokenExpiresAt: '2026-06-09T00:00:00.000Z'
    accessTokenEnc: encrypted-access-token
    refreshTokenEnc: encrypted-refresh-token
schedules: {}
''');
      final store = AppSettingsStore(file: settingsFile);
      final settings = AppSettings.empty().copyWith(
        virtBackupAccount: VirtBackupAccountTokens(
          email: 'user@example.com',
          accountBaseUrl: 'https://virtbackup.net',
          accessToken: 'access-token-value',
          accessTokenExpiresAt: DateTime.utc(2026, 5, 17),
          refreshToken: 'refresh-token-value',
          refreshTokenExpiresAt: DateTime.utc(2026, 6, 9),
        ),
      );

      await store.save(settings);

      final decoded = loadYaml(await settingsFile.readAsString()) as YamlMap;
      final account = decoded['virtBackupAccount'] as YamlMap;
      expect(account['other-host'], isA<YamlMap>());
      expect((account['other-host'] as YamlMap)['email'], 'other@example.com');
      expect((account['other-host'] as YamlMap)['accessTokenEnc'], 'encrypted-access-token');
      expect((account['other-host'] as YamlMap)['refreshTokenEnc'], 'encrypted-refresh-token');
    } finally {
      await tempDir.delete(recursive: true);
    }
  });

  test('save encrypts Virt Backup account tokens', () async {
    final tempDir = await Directory.systemTemp.createTemp('virtbackup_settings_store_account_test_');
    try {
      final settingsFile = File('${tempDir.path}${Platform.pathSeparator}agent.yaml');
      final store = AppSettingsStore(file: settingsFile);
      final settings = AppSettings.empty().copyWith(
        virtBackupAccount: VirtBackupAccountTokens(
          email: 'user@example.com',
          accountBaseUrl: 'https://virtbackup.net',
          accessToken: 'access-token-value',
          accessTokenExpiresAt: DateTime.utc(2026, 5, 17),
          refreshToken: 'refresh-token-value',
          refreshTokenExpiresAt: DateTime.utc(2026, 6, 9),
        ),
      );

      await store.save(settings);

      final decoded = loadYaml(await settingsFile.readAsString()) as YamlMap;
      final accountGroups = decoded['virtBackupAccount'] as YamlMap;
      final account = accountGroups[Platform.localHostname] as YamlMap;
      expect(account['accessToken'], '');
      expect(account['refreshToken'], '');
      expect(account['accessTokenEnc'].toString(), isNot(contains('access-token-value')));
      expect(account['refreshTokenEnc'].toString(), isNot(contains('refresh-token-value')));

      final loaded = await store.load();
      expect(loaded.virtBackupAccount.accessToken, 'access-token-value');
      expect(loaded.virtBackupAccount.refreshToken, 'refresh-token-value');
    } finally {
      await tempDir.delete(recursive: true);
    }
  });
}
