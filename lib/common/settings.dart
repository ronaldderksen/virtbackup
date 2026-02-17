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
    required this.hashblocksLimitBufferMb,
    required this.blockSizeMB,
    required this.dummyDriverTmpWrites,
    required this.ntfymeToken,
  });

  final String backupPath;
  final String logLevel;
  final List<BackupStorage> storage;
  final String? backupStorageId;
  final List<ServerConfig> servers;
  final bool connectionVerified;
  final int hashblocksLimitBufferMb;
  final int blockSizeMB;
  final bool dummyDriverTmpWrites;
  final String ntfymeToken;

  AppSettings copyWith({
    String? backupPath,
    String? logLevel,
    List<BackupStorage>? storage,
    String? backupStorageId,
    List<ServerConfig>? servers,
    bool? connectionVerified,
    int? hashblocksLimitBufferMb,
    int? blockSizeMB,
    bool? dummyDriverTmpWrites,
    String? ntfymeToken,
  }) {
    return AppSettings(
      backupPath: backupPath ?? this.backupPath,
      logLevel: logLevel ?? this.logLevel,
      storage: storage ?? this.storage,
      backupStorageId: backupStorageId ?? this.backupStorageId,
      servers: servers ?? this.servers,
      connectionVerified: connectionVerified ?? this.connectionVerified,
      hashblocksLimitBufferMb: hashblocksLimitBufferMb ?? this.hashblocksLimitBufferMb,
      blockSizeMB: blockSizeMB ?? this.blockSizeMB,
      dummyDriverTmpWrites: dummyDriverTmpWrites ?? this.dummyDriverTmpWrites,
      ntfymeToken: ntfymeToken ?? this.ntfymeToken,
    );
  }

  Map<String, dynamic> toMap() {
    return {
      'backupPath': backupPath,
      'log_level': logLevel,
      'backupStorageId': backupStorageId,
      'connectionVerified': connectionVerified,
      'hashblocksLimitBufferMb': hashblocksLimitBufferMb,
      'blockSizeMB': blockSizeMB,
      'dummyDriverTmpWrites': dummyDriverTmpWrites,
      'ntfymeToken': ntfymeToken,
      'servers': servers.map((server) => server.toMap()).toList(),
      'storage': storage.map((storage) => storage.toMap()).toList(),
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

    return AppSettings(
      backupPath: resolvedBackupPath,
      logLevel: ((json['log_level'] ?? '').toString().trim().isEmpty ? 'info' : json['log_level'].toString().trim()),
      storage: storage,
      backupStorageId: backupStorageId == null || backupStorageId.isEmpty ? selectedBackupStorage?.id : backupStorageId,
      connectionVerified: json['connectionVerified'] == true,
      hashblocksLimitBufferMb: _parseHashblocksLimitBufferMb(json['hashblocksLimitBufferMb']),
      blockSizeMB: _parseBlockSizeMB(json['blockSizeMB']),
      dummyDriverTmpWrites: json['dummyDriverTmpWrites'] == true,
      ntfymeToken: (json['ntfymeToken'] ?? '').toString(),
      servers: servers,
    );
  }

  factory AppSettings.empty() => AppSettings(
    backupPath: '',
    logLevel: 'info',
    storage: const <BackupStorage>[],
    backupStorageId: null,
    servers: <ServerConfig>[],
    connectionVerified: false,
    hashblocksLimitBufferMb: 1024,
    blockSizeMB: 1,
    dummyDriverTmpWrites: false,
    ntfymeToken: '',
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

  static int _parseHashblocksLimitBufferMb(Object? value) {
    final parsed = value is num ? value.toInt() : int.tryParse(value?.toString() ?? '');
    if (parsed == null || parsed <= 0) {
      return 1024;
    }
    return parsed;
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
