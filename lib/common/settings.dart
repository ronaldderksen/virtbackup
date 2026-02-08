import 'models.dart';

class AppSettings {
  AppSettings({
    required this.backupPath,
    required this.backupDriverId,
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
      'backupPath': backupPath,
      'backupDriverId': backupDriverId,
      'connectionVerified': connectionVerified,
      'hashblocksLimitBufferMb': hashblocksLimitBufferMb,
      'dummyDriverTmpWrites': dummyDriverTmpWrites,
      'ntfymeToken': ntfymeToken,
      'gdriveScope': gdriveScope,
      'gdriveRootPath': gdriveRootPath,
      'gdriveAccessToken': gdriveAccessToken,
      'gdriveRefreshToken': gdriveRefreshToken,
      'gdriveAccountEmail': gdriveAccountEmail,
      'gdriveExpiresAt': _encodeDateTime(gdriveExpiresAt),
      'selectedServerId': selectedServerId,
      'listenAll': listenAll,
      'servers': servers.map((server) => server.toMap()).toList(),
    };
  }

  factory AppSettings.fromMap(Map<String, dynamic> json) {
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
      backupPath: (json['backupPath'] ?? '').toString(),
      backupDriverId: (json['backupDriverId'] ?? 'filesystem').toString(),
      connectionVerified: json['connectionVerified'] == true,
      hashblocksLimitBufferMb: _parseHashblocksLimitBufferMb(json['hashblocksLimitBufferMb']),
      dummyDriverTmpWrites: json['dummyDriverTmpWrites'] == true,
      ntfymeToken: (json['ntfymeToken'] ?? '').toString(),
      gdriveScope: (json['gdriveScope'] ?? 'https://www.googleapis.com/auth/drive.file').toString(),
      gdriveRootPath: (json['gdriveRootPath'] ?? '/').toString(),
      gdriveAccessToken: (json['gdriveAccessToken'] ?? '').toString(),
      gdriveRefreshToken: (json['gdriveRefreshToken'] ?? '').toString(),
      gdriveAccountEmail: (json['gdriveAccountEmail'] ?? '').toString(),
      gdriveExpiresAt: _parseDateTime(json['gdriveExpiresAt']),
      selectedServerId: json['selectedServerId']?.toString(),
      listenAll: json['listenAll'] == true,
      servers: servers,
    );
  }

  factory AppSettings.empty() => AppSettings(
    backupPath: '',
    backupDriverId: 'filesystem',
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
