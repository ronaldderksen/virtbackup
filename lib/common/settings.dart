import 'models.dart';

class AppSettings {
  static const String filesystemDestinationId = 'filesystem';
  static const String filesystemDestinationName = 'Filesystem';

  AppSettings({
    required this.backupPath,
    required this.logLevel,
    required this.destinations,
    required this.backupDestinationId,
    required this.restoreDestinationId,
    required this.backupDriverId,
    required this.sftpHost,
    required this.sftpPort,
    required this.sftpUsername,
    required this.sftpPassword,
    required this.sftpBasePath,
    required this.servers,
    required this.connectionVerified,
    required this.hashblocksLimitBufferMb,
    required this.blockSizeMB,
    required this.dummyDriverTmpWrites,
    required this.ntfymeToken,
    required this.gdriveScope,
    required this.gdriveRootPath,
    required this.gdriveAccessToken,
    required this.gdriveRefreshToken,
    required this.gdriveAccountEmail,
    required this.gdriveExpiresAt,
    this.selectedServerId,
    this.listenAll = false,
  });

  final String backupPath;
  final String logLevel;
  final List<BackupDestination> destinations;
  final String? backupDestinationId;
  final String? restoreDestinationId;
  final String backupDriverId;
  final String sftpHost;
  final int sftpPort;
  final String sftpUsername;
  final String sftpPassword;
  final String sftpBasePath;
  final List<ServerConfig> servers;
  final bool connectionVerified;
  final int hashblocksLimitBufferMb;
  final int blockSizeMB;
  final bool dummyDriverTmpWrites;
  final String ntfymeToken;
  final String gdriveScope;
  final String gdriveRootPath;
  final String gdriveAccessToken;
  final String gdriveRefreshToken;
  final String gdriveAccountEmail;
  final DateTime? gdriveExpiresAt;
  final String? selectedServerId;
  final bool listenAll;

  AppSettings copyWith({
    String? backupPath,
    String? logLevel,
    List<BackupDestination>? destinations,
    String? backupDestinationId,
    String? restoreDestinationId,
    String? backupDriverId,
    String? sftpHost,
    int? sftpPort,
    String? sftpUsername,
    String? sftpPassword,
    String? sftpBasePath,
    List<ServerConfig>? servers,
    bool? connectionVerified,
    int? hashblocksLimitBufferMb,
    int? blockSizeMB,
    bool? dummyDriverTmpWrites,
    String? ntfymeToken,
    String? gdriveScope,
    String? gdriveRootPath,
    String? gdriveAccessToken,
    String? gdriveRefreshToken,
    String? gdriveAccountEmail,
    DateTime? gdriveExpiresAt,
    String? selectedServerId,
    bool? listenAll,
  }) {
    return AppSettings(
      backupPath: backupPath ?? this.backupPath,
      logLevel: logLevel ?? this.logLevel,
      destinations: destinations ?? this.destinations,
      backupDestinationId: backupDestinationId ?? this.backupDestinationId,
      restoreDestinationId: restoreDestinationId ?? this.restoreDestinationId,
      backupDriverId: backupDriverId ?? this.backupDriverId,
      sftpHost: sftpHost ?? this.sftpHost,
      sftpPort: sftpPort ?? this.sftpPort,
      sftpUsername: sftpUsername ?? this.sftpUsername,
      sftpPassword: sftpPassword ?? this.sftpPassword,
      sftpBasePath: sftpBasePath ?? this.sftpBasePath,
      servers: servers ?? this.servers,
      connectionVerified: connectionVerified ?? this.connectionVerified,
      hashblocksLimitBufferMb: hashblocksLimitBufferMb ?? this.hashblocksLimitBufferMb,
      blockSizeMB: blockSizeMB ?? this.blockSizeMB,
      dummyDriverTmpWrites: dummyDriverTmpWrites ?? this.dummyDriverTmpWrites,
      ntfymeToken: ntfymeToken ?? this.ntfymeToken,
      gdriveScope: gdriveScope ?? this.gdriveScope,
      gdriveRootPath: gdriveRootPath ?? this.gdriveRootPath,
      gdriveAccessToken: gdriveAccessToken ?? this.gdriveAccessToken,
      gdriveRefreshToken: gdriveRefreshToken ?? this.gdriveRefreshToken,
      gdriveAccountEmail: gdriveAccountEmail ?? this.gdriveAccountEmail,
      gdriveExpiresAt: gdriveExpiresAt ?? this.gdriveExpiresAt,
      selectedServerId: selectedServerId ?? this.selectedServerId,
      listenAll: listenAll ?? this.listenAll,
    );
  }

  Map<String, dynamic> toMap() {
    return {
      'backupPath': backupPath,
      'log_level': logLevel,
      'destinations': destinations.map((destination) => destination.toMap()).toList(),
      'backupDestinationId': backupDestinationId,
      'restoreDestinationId': restoreDestinationId,
      'backupDriverId': backupDriverId,
      'sftpHost': sftpHost,
      'sftpPort': sftpPort,
      'sftpUsername': sftpUsername,
      'sftpPassword': sftpPassword,
      'sftpBasePath': sftpBasePath,
      'connectionVerified': connectionVerified,
      'hashblocksLimitBufferMb': hashblocksLimitBufferMb,
      'blockSizeMB': blockSizeMB,
      'dummyDriverTmpWrites': dummyDriverTmpWrites,
      'ntfymeToken': ntfymeToken,
      'gdriveScope': gdriveScope,
      'gdriveRootPath': gdriveRootPath,
      'gdriveAccessToken': gdriveAccessToken,
      'gdriveRefreshToken': gdriveRefreshToken,
      'gdriveAccountEmail': gdriveAccountEmail,
      'gdriveExpiresAt': gdriveExpiresAt?.toUtc().toIso8601String(),
      'selectedServerId': selectedServerId,
      'listenAll': listenAll,
      'servers': servers.map((server) => server.toMap()).toList(),
    };
  }

  factory AppSettings.fromMap(Map<String, dynamic> json) {
    final destinations = _ensureFilesystemDestination(_parseDestinations(json));
    final resolvedBackupPath = _filesystemPathFromDestinations(destinations);
    final backupDestinationId = json['backupDestinationId']?.toString().trim();
    final restoreDestinationId = json['restoreDestinationId']?.toString().trim();
    final selectedBackupDestination = _resolveDestination(destinations: destinations, requestedId: backupDestinationId);
    final selectedSftp = _resolveDestinationByDriver(destinations, 'sftp');
    final selectedGdrive = _resolveDestinationByDriver(destinations, 'gdrive');
    final sftpHost = json['sftpHost']?.toString();
    final sftpPort = json['sftpPort'];
    final sftpUsername = json['sftpUsername']?.toString();
    final sftpPassword = json['sftpPassword']?.toString();
    final sftpBasePath = json['sftpBasePath']?.toString();
    final gdriveScope = json['gdriveScope']?.toString();
    final gdriveRootPath = json['gdriveRootPath']?.toString();
    final gdriveAccessToken = json['gdriveAccessToken']?.toString();
    final gdriveRefreshToken = json['gdriveRefreshToken']?.toString();
    final gdriveAccountEmail = json['gdriveAccountEmail']?.toString();
    final gdriveExpiresAt = _parseDateTime(json['gdriveExpiresAt']);

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
      destinations: destinations,
      backupDestinationId: backupDestinationId == null || backupDestinationId.isEmpty ? selectedBackupDestination?.id : backupDestinationId,
      restoreDestinationId: restoreDestinationId == null || restoreDestinationId.isEmpty ? null : restoreDestinationId,
      backupDriverId: (json['backupDriverId']?.toString().trim().isNotEmpty ?? false)
          ? json['backupDriverId'].toString().trim()
          : (selectedBackupDestination?.driverId.isNotEmpty == true ? selectedBackupDestination!.driverId : 'filesystem'),
      connectionVerified: json['connectionVerified'] == true,
      hashblocksLimitBufferMb: _parseHashblocksLimitBufferMb(json['hashblocksLimitBufferMb']),
      blockSizeMB: _parseBlockSizeMB(json['blockSizeMB']),
      dummyDriverTmpWrites: json['dummyDriverTmpWrites'] == true,
      ntfymeToken: (json['ntfymeToken'] ?? '').toString(),
      gdriveScope: gdriveScope ?? (selectedGdrive?.params['scope'] ?? 'https://www.googleapis.com/auth/drive.file').toString(),
      gdriveRootPath: gdriveRootPath ?? (selectedGdrive?.params['rootPath'] ?? '/').toString(),
      gdriveAccessToken: gdriveAccessToken ?? (selectedGdrive?.params['accessToken'] ?? '').toString(),
      gdriveRefreshToken: gdriveRefreshToken ?? (selectedGdrive?.params['refreshToken'] ?? '').toString(),
      gdriveAccountEmail: gdriveAccountEmail ?? (selectedGdrive?.params['accountEmail'] ?? '').toString(),
      gdriveExpiresAt: gdriveExpiresAt ?? _parseDateTime(selectedGdrive?.params['expiresAt']),
      sftpHost: sftpHost ?? (selectedSftp?.params['host'] ?? '').toString(),
      sftpPort: sftpPort == null ? _parsePort(selectedSftp?.params['port']) : _parsePort(sftpPort),
      sftpUsername: sftpUsername ?? (selectedSftp?.params['username'] ?? '').toString(),
      sftpPassword: sftpPassword ?? (selectedSftp?.params['password'] ?? '').toString(),
      sftpBasePath: sftpBasePath ?? (selectedSftp?.params['basePath'] ?? '').toString(),
      selectedServerId: json['selectedServerId']?.toString(),
      listenAll: json['listenAll'] == true,
      servers: servers,
    );
  }

  factory AppSettings.empty() => AppSettings(
    backupPath: '',
    logLevel: 'info',
    destinations: const <BackupDestination>[],
    backupDestinationId: null,
    restoreDestinationId: null,
    backupDriverId: 'filesystem',
    sftpHost: '',
    sftpPort: 22,
    sftpUsername: '',
    sftpPassword: '',
    sftpBasePath: '',
    servers: [],
    connectionVerified: false,
    hashblocksLimitBufferMb: 1024,
    blockSizeMB: 1,
    dummyDriverTmpWrites: false,
    ntfymeToken: '',
    gdriveScope: 'https://www.googleapis.com/auth/drive.file',
    gdriveRootPath: '/',
    gdriveAccessToken: '',
    gdriveRefreshToken: '',
    gdriveAccountEmail: '',
    gdriveExpiresAt: null,
    selectedServerId: null,
    listenAll: true,
  );

  static List<BackupDestination> _parseDestinations(Map<String, dynamic> json) {
    final raw = json['destinations'];
    final destinations = <BackupDestination>[];
    if (raw is List) {
      for (final entry in raw) {
        if (entry is! Map) {
          continue;
        }
        final parsed = BackupDestination.fromMap(Map<String, dynamic>.from(entry));
        if (parsed.id.trim().isEmpty || parsed.driverId.trim().isEmpty) {
          continue;
        }
        destinations.add(parsed);
      }
    }
    return destinations;
  }

  static BackupDestination? _resolveDestination({required List<BackupDestination> destinations, required String? requestedId}) {
    if (destinations.isEmpty) {
      return null;
    }
    final requested = requestedId?.trim() ?? '';
    if (requested.isNotEmpty) {
      for (final destination in destinations) {
        if (destination.id == requested) {
          return destination;
        }
      }
    }
    for (final destination in destinations) {
      if (destination.enabled) {
        return destination;
      }
    }
    return destinations.first;
  }

  static BackupDestination? _resolveDestinationByDriver(List<BackupDestination> destinations, String driverId) {
    for (final destination in destinations) {
      if (destination.driverId == driverId) {
        return destination;
      }
    }
    return null;
  }

  static List<BackupDestination> _ensureFilesystemDestination(List<BackupDestination> input) {
    final destinations = List<BackupDestination>.from(input);
    var filesystemIndex = -1;
    String? existingPath;
    var existingDisableFresh = false;
    for (var index = 0; index < destinations.length; index++) {
      final entry = destinations[index];
      if (entry.id == filesystemDestinationId) {
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
    final filesystemDestination = BackupDestination(
      id: filesystemDestinationId,
      name: filesystemDestinationName,
      driverId: 'filesystem',
      enabled: true,
      disableFresh: existingDisableFresh,
      params: <String, dynamic>{'path': normalizedPath},
    );
    if (filesystemIndex >= 0) {
      destinations[filesystemIndex] = filesystemDestination;
    } else {
      destinations.insert(0, filesystemDestination);
    }
    return destinations;
  }

  static String _filesystemPathFromDestinations(List<BackupDestination> destinations) {
    for (final destination in destinations) {
      if (destination.id != filesystemDestinationId) {
        continue;
      }
      final path = destination.params['path']?.toString().trim();
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

  static int _parsePort(Object? value) {
    final parsed = value is num ? value.toInt() : int.tryParse(value?.toString().trim() ?? '');
    if (parsed == null || parsed <= 0 || parsed > 65535) {
      return 22;
    }
    return parsed;
  }

  static DateTime? _parseDateTime(Object? value) {
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

  static DateTime? parseDateTimeOrNull(Object? value) {
    return _parseDateTime(value);
  }
}
