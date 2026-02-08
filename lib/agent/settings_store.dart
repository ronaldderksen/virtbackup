import 'dart:convert';
import 'dart:io';
import 'dart:math';
import 'dart:typed_data';

import 'package:crypto/crypto.dart';
import 'package:pointycastle/export.dart';
import 'package:yaml/yaml.dart';

import 'package:virtbackup/common/settings.dart';

const String settingsFileName = 'agent.yaml';
const String agentTokenFileName = 'agent.token';

class AppSettingsStore {
  AppSettingsStore({File? file}) : _file = file ?? File(settingsFileName);

  final File _file;

  static Future<AppSettingsStore> fromAgentDefaultPath() async {
    final dir = _agentDefaultSettingsDirectory();
    final settingsDir = Directory('${dir.path}${Platform.pathSeparator}virtbackup');
    await settingsDir.create(recursive: true);
    final file = File('${settingsDir.path}${Platform.pathSeparator}$settingsFileName');
    return AppSettingsStore(file: file);
  }

  Future<AppSettings> load() async {
    if (!await _file.exists()) {
      return AppSettings.empty();
    }
    try {
      final token = await loadAgentToken();
      return await _loadFromYamlFile(_file, token: token);
    } catch (_) {
      return AppSettings.empty();
    }
  }

  Future<void> save(AppSettings agentSettings) async {
    final token = await _loadOrCreateToken();
    final data = agentSettings.toMap();
    _encryptPasswordsInMap(data, token);
    _encryptGdriveTokensInMap(data, token);
    final encoded = _ensureTrailingNewline(_toYaml(data));
    await _file.writeAsString(encoded);
    await AppSettingsStore.setFilePermissions(_file, ownerOnly: true);
  }

  Future<DateTime?> lastModified() async {
    if (!await _file.exists()) {
      return null;
    }
    return _file.lastModified();
  }

  File get file => _file;

  File get tokenFile => File('${_file.parent.path}${Platform.pathSeparator}$agentTokenFileName');

  Future<String?> loadAgentToken() async {
    final file = tokenFile;
    if (!await file.exists()) {
      return null;
    }
    final token = (await file.readAsString()).trim();
    return token.isEmpty ? null : token;
  }

  Future<String> generateAndStoreAgentToken() async {
    final token = _generateToken(32);
    final file = tokenFile;
    await file.writeAsString('$token\n');
    await AppSettingsStore.setFilePermissions(file, ownerOnly: true);
    return token;
  }

  static Directory _agentDefaultSettingsDirectory() {
    final env = Platform.environment;
    if (Platform.isWindows) {
      final appData = env['LOCALAPPDATA'];
      if (appData != null && appData.isNotEmpty) {
        return Directory(appData);
      }
      throw StateError('LOCALAPPDATA is not set; cannot determine settings directory on Windows.');
    }
    if (Platform.isMacOS) {
      final home = env['HOME'];
      if (home != null && home.isNotEmpty) {
        return Directory('$home${Platform.pathSeparator}.config');
      }
      throw StateError('HOME is not set; cannot determine settings directory on macOS.');
    }
    final home = env['HOME'];
    if (home != null && home.isNotEmpty) {
      return Directory('$home${Platform.pathSeparator}.config');
    }
    throw StateError('HOME is not set; cannot determine settings directory on Linux.');
  }

  Future<AppSettings> _loadFromYamlFile(File file, {String? token}) async {
    final content = await file.readAsString();
    if (content.trim().isEmpty) {
      return AppSettings.empty();
    }
    final decoded = loadYaml(content);
    if (decoded is! YamlMap) {
      return AppSettings.empty();
    }
    final normalized = _normalizeYaml(decoded);
    _decryptPasswordsInMap(normalized, token);
    _decryptGdriveTokensInMap(normalized, token);
    return AppSettings.fromMap(normalized);
  }

  Map<String, dynamic> _normalizeYaml(YamlMap map) {
    final normalized = _yamlToDart(map);
    if (normalized is! Map) {
      return <String, dynamic>{};
    }
    return Map<String, dynamic>.from(normalized);
  }

  Object? _yamlToDart(Object? value) {
    if (value is YamlMap) {
      final result = <String, dynamic>{};
      for (final entry in value.entries) {
        result[entry.key.toString()] = _yamlToDart(entry.value);
      }
      return result;
    }
    if (value is YamlList) {
      return value.map(_yamlToDart).toList();
    }
    return value;
  }

  String _toYaml(Map<String, dynamic> data) {
    final jsonEncoded = jsonEncode(data);
    final decoded = jsonDecode(jsonEncoded);
    return _encodeYaml(decoded);
  }

  String _encodeYaml(Object? value, {int indent = 0}) {
    final indentStr = '  ' * indent;
    if (value is Map) {
      if (value.isEmpty) {
        return '{}';
      }
      final buffer = StringBuffer();
      for (final entry in value.entries) {
        buffer.write(indentStr);
        buffer.write(entry.key);
        buffer.write(':');
        if (_isScalar(entry.value)) {
          buffer.write(' ');
          buffer.writeln(_encodeScalar(entry.value));
        } else {
          buffer.writeln();
          buffer.writeln(_encodeYaml(entry.value, indent: indent + 1));
        }
      }
      return buffer.toString().trimRight();
    }
    if (value is List) {
      if (value.isEmpty) {
        return '[]';
      }
      final buffer = StringBuffer();
      for (final item in value) {
        buffer.write(indentStr);
        buffer.write('-');
        if (_isScalar(item)) {
          buffer.write(' ');
          buffer.writeln(_encodeScalar(item));
        } else {
          buffer.writeln();
          buffer.writeln(_encodeYaml(item, indent: indent + 1));
        }
      }
      return buffer.toString().trimRight();
    }
    return _encodeScalar(value);
  }

  bool _isScalar(Object? value) {
    return value == null || value is String || value is num || value is bool;
  }

  String _encodeScalar(Object? value) {
    if (value == null) {
      return 'null';
    }
    if (value is String) {
      if (value.isEmpty) {
        return "''";
      }
      final needsQuotes = RegExp("[:\\-\\?\\[\\]\\{\\},&\\*\\#\\!\\|>\\\"%@`']").hasMatch(value) || value.startsWith(' ') || value.endsWith(' ') || value.contains('\n') || value.contains('\t');
      if (!needsQuotes) {
        return value;
      }
      final escaped = value.replaceAll("'", "''");
      return "'$escaped'";
    }
    return value.toString();
  }

  String _ensureTrailingNewline(String content) {
    if (content.isEmpty) {
      return '\n';
    }
    return content.endsWith('\n') ? content : '$content\n';
  }

  static Future<void> setFilePermissions(File file, {required bool ownerOnly}) async {
    try {
      if (Platform.isWindows) {
        await Process.run('attrib', [ownerOnly ? '+R' : '-R', file.path]);
        return;
      }
      await Process.run('chmod', [ownerOnly ? '600' : '644', file.path]);
    } catch (_) {}
  }

  static String _generateToken(int lengthBytes) {
    final random = Random.secure();
    final bytes = Uint8List(lengthBytes);
    for (var i = 0; i < lengthBytes; i++) {
      bytes[i] = random.nextInt(256);
    }
    return base64Url.encode(bytes);
  }

  Future<String> _loadOrCreateToken() async {
    final existing = await loadAgentToken();
    if (existing != null && existing.isNotEmpty) {
      return existing;
    }
    return generateAndStoreAgentToken();
  }

  void _encryptPasswordsInMap(Map<String, dynamic> data, String token) {
    final servers = data['servers'];
    if (servers is! List) {
      return;
    }
    for (final entry in servers) {
      if (entry is! Map) {
        continue;
      }
      final password = entry['sshPassword']?.toString() ?? '';
      if (password.isEmpty) {
        entry.remove('sshPasswordEnc');
        continue;
      }
      entry['sshPasswordEnc'] = _encryptPassword(password, token);
      entry['sshPassword'] = '';
    }
  }

  void _decryptPasswordsInMap(Map<String, dynamic> data, String? token) {
    final servers = data['servers'];
    if (servers is! List) {
      return;
    }
    for (final entry in servers) {
      if (entry is! Map) {
        continue;
      }
      final enc = entry['sshPasswordEnc']?.toString();
      if (enc != null && enc.isNotEmpty) {
        entry['sshPassword'] = (token == null || token.isEmpty) ? '' : _decryptPassword(enc, token);
        continue;
      }
      entry['sshPassword'] = '';
    }
  }

  void _encryptGdriveTokensInMap(Map<String, dynamic> data, String token) {
    final accessToken = data['gdriveAccessToken']?.toString() ?? '';
    if (accessToken.isNotEmpty) {
      data['gdriveAccessTokenEnc'] = _encryptPassword(accessToken, token);
      data['gdriveAccessToken'] = '';
    } else {
      data.remove('gdriveAccessTokenEnc');
    }

    final refreshToken = data['gdriveRefreshToken']?.toString() ?? '';
    if (refreshToken.isNotEmpty) {
      data['gdriveRefreshTokenEnc'] = _encryptPassword(refreshToken, token);
      data['gdriveRefreshToken'] = '';
    } else {
      data.remove('gdriveRefreshTokenEnc');
    }
  }

  void _decryptGdriveTokensInMap(Map<String, dynamic> data, String? token) {
    final accessEnc = data['gdriveAccessTokenEnc']?.toString();
    if (accessEnc != null && accessEnc.isNotEmpty) {
      data['gdriveAccessToken'] = (token == null || token.isEmpty) ? '' : _decryptPassword(accessEnc, token);
    } else if (data['gdriveAccessToken'] == null) {
      data['gdriveAccessToken'] = '';
    }

    final refreshEnc = data['gdriveRefreshTokenEnc']?.toString();
    if (refreshEnc != null && refreshEnc.isNotEmpty) {
      data['gdriveRefreshToken'] = (token == null || token.isEmpty) ? '' : _decryptPassword(refreshEnc, token);
    } else if (data['gdriveRefreshToken'] == null) {
      data['gdriveRefreshToken'] = '';
    }
  }

  String _encryptPassword(String plain, String token) {
    final key = _deriveKey(token);
    final iv = _randomBytes(12);
    final cipher = GCMBlockCipher(AESEngine());
    final params = AEADParameters(KeyParameter(key), 128, iv, Uint8List(0));
    cipher.init(true, params);
    final input = Uint8List.fromList(utf8.encode(plain));
    final output = cipher.process(input);
    return 'v1:${base64Url.encode(iv)}:${base64Url.encode(output)}';
  }

  String _decryptPassword(String encoded, String token) {
    try {
      final parts = encoded.split(':');
      if (parts.length != 3 || parts.first != 'v1') {
        return '';
      }
      final iv = base64Url.decode(parts[1]);
      final payload = base64Url.decode(parts[2]);
      final key = _deriveKey(token);
      final cipher = GCMBlockCipher(AESEngine());
      final params = AEADParameters(KeyParameter(key), 128, Uint8List.fromList(iv), Uint8List(0));
      cipher.init(false, params);
      final output = cipher.process(Uint8List.fromList(payload));
      return utf8.decode(output);
    } catch (_) {
      return '';
    }
  }

  Uint8List _deriveKey(String token) {
    final digest = sha256.convert(utf8.encode(token));
    return Uint8List.fromList(digest.bytes);
  }

  Uint8List _randomBytes(int length) {
    final random = Random.secure();
    final bytes = Uint8List(length);
    for (var i = 0; i < length; i++) {
      bytes[i] = random.nextInt(256);
    }
    return bytes;
  }
}
