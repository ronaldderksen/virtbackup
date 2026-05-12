import 'models.dart';

class AppSettings {
  static const String filesystemStorageId = 'filesystem';
  static const String filesystemStorageName = 'Filesystem';

  AppSettings({
    required this.backupPath,
    required this.logLevel,
    required this.storage,
    required this.backupStorageId,
    required this.servers,
    required this.connectionVerified,
    required this.blockSizeMB,
    required this.requireSimpleDisksForBackup,
    required this.dummyDriverTmpWrites,
    required this.maxConcurrentBackupRestoreJobs,
    required this.maxConcurrentJobsPerVm,
    required this.maxConcurrentJobsPerStorage,
    required this.ntfymeToken,
    required this.virtBackupAccount,
    required this.schedules,
  });

  final String backupPath;
  final String logLevel;
  final List<BackupStorage> storage;
  final String? backupStorageId;
  final List<ServerConfig> servers;
  final bool connectionVerified;
  final int blockSizeMB;
  final bool requireSimpleDisksForBackup;
  final bool dummyDriverTmpWrites;
  final int maxConcurrentBackupRestoreJobs;
  final int maxConcurrentJobsPerVm;
  final int maxConcurrentJobsPerStorage;
  final String ntfymeToken;
  final VirtBackupAccountTokens virtBackupAccount;
  final List<ScheduledJob> schedules;

  AppSettings copyWith({
    String? backupPath,
    String? logLevel,
    List<BackupStorage>? storage,
    String? backupStorageId,
    List<ServerConfig>? servers,
    bool? connectionVerified,
    int? blockSizeMB,
    bool? requireSimpleDisksForBackup,
    bool? dummyDriverTmpWrites,
    int? maxConcurrentBackupRestoreJobs,
    int? maxConcurrentJobsPerVm,
    int? maxConcurrentJobsPerStorage,
    String? ntfymeToken,
    VirtBackupAccountTokens? virtBackupAccount,
    List<ScheduledJob>? schedules,
  }) {
    final resolvedServers = servers ?? this.servers;
    final resolvedStorage = storage ?? this.storage;
    final resolvedSchedules = schedules ?? this.schedules;
    final normalizedSchedules = resolvedSchedules
        .map(
          (schedule) => schedule.copyWith(
            name: _generateScheduleName(schedule: schedule, servers: resolvedServers, storage: resolvedStorage),
          ),
        )
        .toList();

    return AppSettings(
      backupPath: backupPath ?? this.backupPath,
      logLevel: logLevel ?? this.logLevel,
      storage: resolvedStorage,
      backupStorageId: backupStorageId ?? this.backupStorageId,
      servers: resolvedServers,
      connectionVerified: connectionVerified ?? this.connectionVerified,
      blockSizeMB: blockSizeMB ?? this.blockSizeMB,
      requireSimpleDisksForBackup: requireSimpleDisksForBackup ?? this.requireSimpleDisksForBackup,
      dummyDriverTmpWrites: dummyDriverTmpWrites ?? this.dummyDriverTmpWrites,
      maxConcurrentBackupRestoreJobs: maxConcurrentBackupRestoreJobs ?? this.maxConcurrentBackupRestoreJobs,
      maxConcurrentJobsPerVm: maxConcurrentJobsPerVm ?? this.maxConcurrentJobsPerVm,
      maxConcurrentJobsPerStorage: maxConcurrentJobsPerStorage ?? this.maxConcurrentJobsPerStorage,
      ntfymeToken: ntfymeToken ?? this.ntfymeToken,
      virtBackupAccount: virtBackupAccount ?? this.virtBackupAccount,
      schedules: normalizedSchedules,
    );
  }

  Map<String, dynamic> toMap() {
    return {
      'backupPath': backupPath,
      'log_level': logLevel,
      'backupStorageId': backupStorageId,
      'connectionVerified': connectionVerified,
      'blockSizeMB': blockSizeMB,
      'requireSimpleDisksForBackup': requireSimpleDisksForBackup,
      'dummyDriverTmpWrites': dummyDriverTmpWrites,
      'maxConcurrentBackupRestoreJobs': maxConcurrentBackupRestoreJobs,
      'maxConcurrentJobsPerVm': maxConcurrentJobsPerVm,
      'maxConcurrentJobsPerStorage': maxConcurrentJobsPerStorage,
      'ntfymeToken': ntfymeToken,
      'virtBackupAccount': virtBackupAccount.toMap(),
      'servers': servers.map((server) => server.toMap()).toList(),
      'storage': storage.map((storage) => storage.toMap()).toList(),
      'schedules': schedules.map((schedule) => schedule.toMap()).toList(),
    };
  }

  factory AppSettings.fromMap(Map<String, dynamic> json) {
    final storage = _ensureFilesystemStorage(_parseStorage(json));
    final resolvedBackupPath = _filesystemPathFromStorage(storage);
    final backupStorageId = json['backupStorageId']?.toString().trim();
    final selectedBackupStorage = _resolveStorage(storage: storage, requestedId: backupStorageId);

    final serversJson = json['servers'];
    final servers = <ServerConfig>[];
    if (serversJson is List) {
      for (final entry in serversJson) {
        if (entry is Map) {
          servers.add(ServerConfig.fromMap(Map<String, dynamic>.from(entry)));
        }
      }
    }

    final schedulesJson = json['schedules'];
    final schedules = <ScheduledJob>[];
    if (schedulesJson is List) {
      for (final entry in schedulesJson) {
        if (entry is Map) {
          final parsed = ScheduledJob.tryFromMap(Map<String, dynamic>.from(entry));
          if (parsed != null) {
            schedules.add(parsed);
          }
        }
      }
    }
    final normalizedSchedules = schedules
        .map(
          (schedule) => schedule.copyWith(
            name: _generateScheduleName(schedule: schedule, servers: servers, storage: storage),
          ),
        )
        .toList();

    return AppSettings(
      backupPath: resolvedBackupPath,
      logLevel: ((json['log_level'] ?? '').toString().trim().isEmpty ? 'info' : json['log_level'].toString().trim()),
      storage: storage,
      backupStorageId: backupStorageId == null || backupStorageId.isEmpty ? selectedBackupStorage?.id : backupStorageId,
      connectionVerified: json['connectionVerified'] == true,
      blockSizeMB: _parseBlockSizeMB(json['blockSizeMB']),
      requireSimpleDisksForBackup: _parseBool(json['requireSimpleDisksForBackup'], field: 'requireSimpleDisksForBackup', defaultValue: true),
      dummyDriverTmpWrites: json['dummyDriverTmpWrites'] == true,
      maxConcurrentBackupRestoreJobs: _parsePositiveInt(json['maxConcurrentBackupRestoreJobs'], field: 'maxConcurrentBackupRestoreJobs', defaultValue: 1),
      maxConcurrentJobsPerVm: _parsePositiveInt(json['maxConcurrentJobsPerVm'], field: 'maxConcurrentJobsPerVm', defaultValue: 1),
      maxConcurrentJobsPerStorage: _parsePositiveInt(json['maxConcurrentJobsPerStorage'], field: 'maxConcurrentJobsPerStorage', defaultValue: 1),
      ntfymeToken: (json['ntfymeToken'] ?? '').toString(),
      virtBackupAccount: VirtBackupAccountTokens.fromMap(Map<String, dynamic>.from(json['virtBackupAccount'] is Map ? json['virtBackupAccount'] as Map : const <String, dynamic>{})),
      servers: servers,
      schedules: normalizedSchedules,
    );
  }

  factory AppSettings.empty() => AppSettings(
    backupPath: '',
    logLevel: 'info',
    storage: const <BackupStorage>[],
    backupStorageId: null,
    servers: <ServerConfig>[],
    connectionVerified: false,
    blockSizeMB: 1,
    requireSimpleDisksForBackup: true,
    dummyDriverTmpWrites: false,
    maxConcurrentBackupRestoreJobs: 1,
    maxConcurrentJobsPerVm: 1,
    maxConcurrentJobsPerStorage: 1,
    ntfymeToken: '',
    virtBackupAccount: VirtBackupAccountTokens.empty(),
    schedules: <ScheduledJob>[],
  );

  static List<BackupStorage> _parseStorage(Map<String, dynamic> json) {
    final raw = json['storage'];
    final storage = <BackupStorage>[];
    if (raw is List) {
      for (final entry in raw) {
        if (entry is! Map) {
          continue;
        }
        final parsed = BackupStorage.fromMap(Map<String, dynamic>.from(entry));
        if (parsed.id.trim().isEmpty || parsed.driverId.trim().isEmpty) {
          continue;
        }
        storage.add(parsed);
      }
    }
    return storage;
  }

  static BackupStorage? _resolveStorage({required List<BackupStorage> storage, required String? requestedId}) {
    if (storage.isEmpty) {
      return null;
    }
    final requested = requestedId?.trim() ?? '';
    if (requested.isNotEmpty) {
      for (final storage in storage) {
        if (storage.id == requested) {
          return storage;
        }
      }
    }
    for (final storage in storage) {
      if (storage.enabled) {
        return storage;
      }
    }
    return storage.first;
  }

  static List<BackupStorage> _ensureFilesystemStorage(List<BackupStorage> input) {
    final storage = List<BackupStorage>.from(input);
    var filesystemIndex = -1;
    String? existingPath;
    var existingDisableFresh = false;
    for (var index = 0; index < storage.length; index++) {
      final entry = storage[index];
      if (entry.id == filesystemStorageId) {
        filesystemIndex = index;
        existingDisableFresh = entry.disableFresh;
        final value = entry.params['path']?.toString().trim();
        if (value != null && value.isNotEmpty) {
          existingPath = value;
        }
        break;
      }
    }

    final normalizedPath = existingPath ?? '';
    final filesystemStorage = BackupStorage(
      id: filesystemStorageId,
      name: filesystemStorageName,
      driverId: 'filesystem',
      enabled: true,
      disableFresh: existingDisableFresh,
      params: <String, dynamic>{'path': normalizedPath},
    );
    if (filesystemIndex >= 0) {
      storage[filesystemIndex] = filesystemStorage;
    } else {
      storage.insert(0, filesystemStorage);
    }
    return storage;
  }

  static String _filesystemPathFromStorage(List<BackupStorage> storage) {
    for (final storage in storage) {
      if (storage.id != filesystemStorageId) {
        continue;
      }
      final path = storage.params['path']?.toString().trim();
      if (path != null) {
        return path;
      }
      return '';
    }
    return '';
  }

  static int _parseBlockSizeMB(Object? value) {
    if (value == null) {
      return 1;
    }
    final parsed = value is num ? value.toInt() : int.tryParse(value.toString().trim());
    if (parsed == null || (parsed != 1 && parsed != 2 && parsed != 4 && parsed != 8)) {
      throw StateError('Invalid blockSizeMB. Allowed values: 1, 2, 4, 8.');
    }
    return parsed;
  }

  static int _parsePositiveInt(Object? value, {required String field, required int defaultValue}) {
    if (value == null) {
      return defaultValue;
    }
    final parsed = value is num ? value.toInt() : int.tryParse(value.toString().trim());
    if (parsed == null || parsed < 1) {
      throw StateError('Invalid $field. Value must be 1 or higher.');
    }
    return parsed;
  }

  static bool _parseBool(Object? value, {required String field, required bool defaultValue}) {
    if (value == null) {
      return defaultValue;
    }
    if (value is bool) {
      return value;
    }
    throw StateError('Invalid $field. Value must be true or false.');
  }

  static String _generateScheduleName({required ScheduledJob schedule, required List<ServerConfig> servers, required List<BackupStorage> storage}) {
    final typeLabel = schedule.type == ScheduledJobType.backup ? 'Backup' : 'Restore';
    final serverName = _serverNameForId(servers, schedule.serverId);
    final storageName = _storageNameForId(storage, schedule.storageId);
    final vmLabel = schedule.vmName.trim().isEmpty ? 'VM' : schedule.vmName.trim();
    final direction = schedule.type == ScheduledJobType.backup ? 'to' : 'from';
    return '$typeLabel $vmLabel on $serverName $direction $storageName';
  }

  static String _serverNameForId(List<ServerConfig> servers, String id) {
    for (final server in servers) {
      if (server.id == id) {
        return server.name;
      }
    }
    return id;
  }

  static String _storageNameForId(List<BackupStorage> storage, String id) {
    for (final entry in storage) {
      if (entry.id == id) {
        return entry.name;
      }
    }
    return id;
  }

  static DateTime? parseDateTimeOrNull(Object? value) {
    if (value == null) {
      return null;
    }
    if (value is num) {
      final asInt = value.toInt();
      if (asInt <= 0) {
        return null;
      }
      final ms = asInt < 1000000000000 ? asInt * 1000 : asInt;
      return DateTime.fromMillisecondsSinceEpoch(ms, isUtc: true);
    }
    final text = value.toString().trim();
    if (text.isEmpty) {
      return null;
    }
    return DateTime.tryParse(text);
  }
}

class VirtBackupAccountTokens {
  const VirtBackupAccountTokens({
    required this.email,
    required this.accountBaseUrl,
    required this.accessToken,
    required this.accessTokenExpiresAt,
    required this.refreshToken,
    required this.refreshTokenExpiresAt,
  });

  final String email;
  final String accountBaseUrl;
  final String accessToken;
  final DateTime? accessTokenExpiresAt;
  final String refreshToken;
  final DateTime? refreshTokenExpiresAt;

  bool get isConnected => email.trim().isNotEmpty && accessToken.trim().isNotEmpty && refreshToken.trim().isNotEmpty;

  Map<String, dynamic> toMap() {
    return <String, dynamic>{
      'email': email,
      'accountBaseUrl': accountBaseUrl,
      'accessToken': accessToken,
      'accessTokenExpiresAt': accessTokenExpiresAt?.toUtc().toIso8601String(),
      'refreshToken': refreshToken,
      'refreshTokenExpiresAt': refreshTokenExpiresAt?.toUtc().toIso8601String(),
    };
  }

  factory VirtBackupAccountTokens.fromMap(Map<String, dynamic> json) {
    return VirtBackupAccountTokens(
      email: (json['email'] ?? '').toString(),
      accountBaseUrl: (json['accountBaseUrl'] ?? '').toString(),
      accessToken: (json['accessToken'] ?? '').toString(),
      accessTokenExpiresAt: AppSettings.parseDateTimeOrNull(json['accessTokenExpiresAt']),
      refreshToken: (json['refreshToken'] ?? '').toString(),
      refreshTokenExpiresAt: AppSettings.parseDateTimeOrNull(json['refreshTokenExpiresAt']),
    );
  }

  factory VirtBackupAccountTokens.empty() => const VirtBackupAccountTokens(email: '', accountBaseUrl: '', accessToken: '', accessTokenExpiresAt: null, refreshToken: '', refreshTokenExpiresAt: null);
}

enum ScheduledJobType { backup, restore }

enum ScheduleFrequency { every5Minutes, hourly, daily, weekly }

class ScheduledJob {
  static const String latestRestoreXmlPath = '__latest__';

  ScheduledJob({
    required this.id,
    required this.name,
    required this.enabled,
    required this.waitForRunningJobs,
    required this.type,
    required this.frequency,
    required this.time,
    required this.weekdays,
    required this.serverId,
    required this.storageId,
    required this.vmName,
    required this.restoreXmlPath,
    required this.restoreDecision,
  });

  final String id;
  final String name;
  final bool enabled;
  final bool waitForRunningJobs;
  final ScheduledJobType type;
  final ScheduleFrequency frequency;
  final String time;
  final List<int> weekdays;
  final String serverId;
  final String storageId;
  final String vmName;
  final String restoreXmlPath;
  final String restoreDecision;

  Map<String, dynamic> toMap() {
    return {
      'id': id,
      'name': name,
      'enabled': enabled,
      'waitForRunningJobs': waitForRunningJobs,
      'type': type.name,
      'frequency': frequency.name,
      'time': time,
      'weekdays': weekdays,
      'serverId': serverId,
      'storageId': storageId,
      'vmName': vmName,
      'restoreXmlPath': restoreXmlPath,
      'restoreDecision': restoreDecision,
    };
  }

  ScheduledJob copyWith({
    String? id,
    String? name,
    bool? enabled,
    bool? waitForRunningJobs,
    ScheduledJobType? type,
    ScheduleFrequency? frequency,
    String? time,
    List<int>? weekdays,
    String? serverId,
    String? storageId,
    String? vmName,
    String? restoreXmlPath,
    String? restoreDecision,
  }) {
    return ScheduledJob(
      id: id ?? this.id,
      name: name ?? this.name,
      enabled: enabled ?? this.enabled,
      waitForRunningJobs: waitForRunningJobs ?? this.waitForRunningJobs,
      type: type ?? this.type,
      frequency: frequency ?? this.frequency,
      time: time ?? this.time,
      weekdays: weekdays ?? this.weekdays,
      serverId: serverId ?? this.serverId,
      storageId: storageId ?? this.storageId,
      vmName: vmName ?? this.vmName,
      restoreXmlPath: restoreXmlPath ?? this.restoreXmlPath,
      restoreDecision: restoreDecision ?? this.restoreDecision,
    );
  }

  static ScheduledJob? tryFromMap(Map<String, dynamic> json) {
    final id = (json['id'] ?? '').toString().trim();
    final name = (json['name'] ?? '').toString().trim();
    final typeName = (json['type'] ?? '').toString().trim();
    final frequencyName = (json['frequency'] ?? '').toString().trim();
    final type = _parseJobType(typeName);
    final frequency = _parseFrequency(frequencyName);
    final time = (json['time'] ?? '').toString().trim();
    final serverId = (json['serverId'] ?? '').toString().trim();
    final storageId = (json['storageId'] ?? '').toString().trim();
    if (id.isEmpty || type == null || frequency == null || !_isValidTimeForFrequency(time, frequency) || serverId.isEmpty || storageId.isEmpty) {
      return null;
    }
    final weekdays = _parseWeekdays(json['weekdays']);
    if (frequency == ScheduleFrequency.weekly && weekdays.isEmpty) {
      return null;
    }
    final vmName = (json['vmName'] ?? '').toString().trim();
    final restoreXmlPath = (json['restoreXmlPath'] ?? '').toString().trim();
    if (type == ScheduledJobType.backup && vmName.isEmpty) {
      return null;
    }
    if (type == ScheduledJobType.restore && restoreXmlPath.isEmpty) {
      return null;
    }
    if (type == ScheduledJobType.restore && restoreXmlPath == latestRestoreXmlPath && vmName.isEmpty) {
      return null;
    }
    final restoreDecision = (json['restoreDecision'] ?? '').toString().trim();
    if (type == ScheduledJobType.restore && restoreDecision.isEmpty) {
      return null;
    }
    if (type == ScheduledJobType.restore && restoreDecision != 'overwrite' && restoreDecision != 'define' && restoreDecision != 'auto_rename') {
      return null;
    }
    return ScheduledJob(
      id: id,
      name: name.isEmpty ? id : name,
      enabled: json['enabled'] == true,
      waitForRunningJobs: json['waitForRunningJobs'] == true,
      type: type,
      frequency: frequency,
      time: time,
      weekdays: weekdays,
      serverId: serverId,
      storageId: storageId,
      vmName: vmName,
      restoreXmlPath: restoreXmlPath,
      restoreDecision: restoreDecision,
    );
  }

  static bool _isValidTime(String value) {
    final match = RegExp(r'^([01]\d|2[0-3]):([0-5]\d)$').firstMatch(value);
    return match != null;
  }

  static bool _isValidTimeForFrequency(String value, ScheduleFrequency frequency) {
    if (frequency == ScheduleFrequency.every5Minutes) {
      return value == '*/5' || _isValidTime(value);
    }
    return _isValidTime(value);
  }

  static ScheduledJobType? _parseJobType(String value) {
    for (final type in ScheduledJobType.values) {
      if (type.name == value) {
        return type;
      }
    }
    return null;
  }

  static ScheduleFrequency? _parseFrequency(String value) {
    for (final frequency in ScheduleFrequency.values) {
      if (frequency.name == value) {
        return frequency;
      }
    }
    return null;
  }

  static List<int> _parseWeekdays(Object? raw) {
    if (raw is! List) {
      return <int>[];
    }
    final result = <int>[];
    for (final value in raw) {
      final parsed = value is num ? value.toInt() : int.tryParse(value.toString().trim());
      if (parsed == null || parsed < 1 || parsed > 7 || result.contains(parsed)) {
        continue;
      }
      result.add(parsed);
    }
    result.sort();
    return result;
  }
}
