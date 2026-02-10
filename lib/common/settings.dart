import 'models.dart';

class AppSettings {
  AppSettings({
    required this.backupPath,
    required this.backupDriverId,
    required this.sftpHost,
    required this.sftpPort,
    required this.sftpUsername,
    required this.sftpPassword,
    required this.sftpBasePath,
    required this.servers,
    required this.connectionVerified,
    required this.hashblocksLimitBufferMb,
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
  final String backupDriverId;
  final String sftpHost;
  final int sftpPort;
  final String sftpUsername;
  final String sftpPassword;
  final String sftpBasePath;
  final List<ServerConfig> servers;
  final bool connectionVerified;
  final int hashblocksLimitBufferMb;
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
    String? backupDriverId,
    String? sftpHost,
    int? sftpPort,
    String? sftpUsername,
    String? sftpPassword,
    String? sftpBasePath,
    List<ServerConfig>? servers,
    bool? connectionVerified,
    int? hashblocksLimitBufferMb,
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
      backupDriverId: backupDriverId ?? this.backupDriverId,
      sftpHost: sftpHost ?? this.sftpHost,
      sftpPort: sftpPort ?? this.sftpPort,
      sftpUsername: sftpUsername ?? this.sftpUsername,
      sftpPassword: sftpPassword ?? this.sftpPassword,
      sftpBasePath: sftpBasePath ?? this.sftpBasePath,
      servers: servers ?? this.servers,
      connectionVerified: connectionVerified ?? this.connectionVerified,
      hashblocksLimitBufferMb: hashblocksLimitBufferMb ?? this.hashblocksLimitBufferMb,
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
      'backup': {
        'driverId': backupDriverId,
        'base_path': backupPath,
        'gdrive': {
          'scope': gdriveScope,
          'rootPath': gdriveRootPath,
          'accessToken': gdriveAccessToken,
          'refreshToken': gdriveRefreshToken,
          'accountEmail': gdriveAccountEmail,
          'expiresAt': _encodeDateTime(gdriveExpiresAt),
        },
        'sftp': {'host': sftpHost, 'port': sftpPort, 'username': sftpUsername, 'password': sftpPassword, 'basePath': sftpBasePath},
      },
      'connectionVerified': connectionVerified,
      'hashblocksLimitBufferMb': hashblocksLimitBufferMb,
      'dummyDriverTmpWrites': dummyDriverTmpWrites,
      'ntfymeToken': ntfymeToken,
      'selectedServerId': selectedServerId,
      'listenAll': listenAll,
      'servers': servers.map((server) => server.toMap()).toList(),
    };
  }

  factory AppSettings.fromMap(Map<String, dynamic> json) {
    final backup = json['backup'] is Map ? Map<String, dynamic>.from(json['backup'] as Map) : const <String, dynamic>{};
    final gdrive = backup['gdrive'] is Map ? Map<String, dynamic>.from(backup['gdrive'] as Map) : const <String, dynamic>{};
    final sftp = backup['sftp'] is Map ? Map<String, dynamic>.from(backup['sftp'] as Map) : const <String, dynamic>{};

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
      backupPath: (backup['base_path'] ?? '').toString(),
      backupDriverId: (backup['driverId'] ?? 'filesystem').toString(),
      connectionVerified: json['connectionVerified'] == true,
      hashblocksLimitBufferMb: _parseHashblocksLimitBufferMb(json['hashblocksLimitBufferMb']),
      dummyDriverTmpWrites: json['dummyDriverTmpWrites'] == true,
      ntfymeToken: (json['ntfymeToken'] ?? '').toString(),
      gdriveScope: (gdrive['scope'] ?? 'https://www.googleapis.com/auth/drive.file').toString(),
      gdriveRootPath: (gdrive['rootPath'] ?? '/').toString(),
      gdriveAccessToken: (gdrive['accessToken'] ?? '').toString(),
      gdriveRefreshToken: (gdrive['refreshToken'] ?? '').toString(),
      gdriveAccountEmail: (gdrive['accountEmail'] ?? '').toString(),
      gdriveExpiresAt: _parseDateTime(gdrive['expiresAt']),
      sftpHost: (sftp['host'] ?? '').toString(),
      sftpPort: _parsePort(sftp['port']),
      sftpUsername: (sftp['username'] ?? '').toString(),
      sftpPassword: (sftp['password'] ?? '').toString(),
      sftpBasePath: (sftp['basePath'] ?? '').toString(),
      selectedServerId: json['selectedServerId']?.toString(),
      listenAll: json['listenAll'] == true,
      servers: servers,
    );
  }

  factory AppSettings.empty() => AppSettings(
    backupPath: '',
    backupDriverId: 'filesystem',
    sftpHost: '',
    sftpPort: 22,
    sftpUsername: '',
    sftpPassword: '',
    sftpBasePath: '',
    servers: [],
    connectionVerified: false,
    hashblocksLimitBufferMb: 1024,
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

  static int _parseHashblocksLimitBufferMb(Object? value) {
    final parsed = value is num ? value.toInt() : int.tryParse(value?.toString() ?? '');
    if (parsed == null || parsed <= 0) {
      return 1024;
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

  static String? _encodeDateTime(DateTime? value) {
    if (value == null) {
      return null;
    }
    return value.toUtc().toIso8601String();
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
}
