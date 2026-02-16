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
    } catch (error) {
      if (error is StateError && error.toString().contains('blockSizeMB')) {
        rethrow;
      }
      return AppSettings.empty();
    }
  }

  Future<void> save(AppSettings agentSettings) async {
    final token = await _loadOrCreateToken();
    final data = agentSettings.toMap();
    _encryptPasswordsInMap(data, token);
    _encryptGdriveTokensInMap(data, token);
    _encryptSftpPasswordInMap(data, token);
    final encoded = _ensureTrailingNewline(_toYaml(data));
    final tempFile = File('${_file.path}.tmp');
    var replaced = false;
    try {
      await tempFile.writeAsString(encoded);
      await AppSettingsStore.setFilePermissions(tempFile, ownerOnly: true);
      await _loadFromYamlFile(tempFile, token: token);
      await tempFile.rename(_file.path);
      replaced = true;
    } finally {
      if (!replaced && await tempFile.exists()) {
        try {
          await tempFile.delete();
        } catch (_) {}
      }
    }
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
    final updatedDestinations = _ensureDestinationDefaults(normalized);
    final updatedBlockSize = _ensureBlockSizeMb(normalized);
    final updated = updatedDestinations || updatedBlockSize;
    if (updated) {
      final encoded = _ensureTrailingNewline(_toYaml(normalized));
      await file.writeAsString(encoded);
      await AppSettingsStore.setFilePermissions(file, ownerOnly: true);
    }
    _decryptPasswordsInMap(normalized, token);
    _decryptGdriveTokensInMap(normalized, token);
    _decryptSftpPasswordInMap(normalized, token);
    return AppSettings.fromMap(normalized);
  }

  bool _ensureDestinationDefaults(Map<String, dynamic> data) {
    final destinations = data['destinations'];
    if (destinations is! List) {
      return false;
    }
    var changed = false;
    for (final destination in destinations) {
      if (destination is! Map) {
        continue;
      }
      final driverId = (destination['driverId'] ?? '').toString().trim();
      if (driverId == 'filesystem') {
        continue;
      }
      if (!destination.containsKey('storeBlobs')) {
        destination['storeBlobs'] = false;
        changed = true;
      }
      if (!destination.containsKey('useBlobs')) {
        destination['useBlobs'] = false;
        changed = true;
      }
      if (!destination.containsKey('uploadConcurrency')) {
        destination['uploadConcurrency'] = 8;
        changed = true;
      }
      if (!destination.containsKey('downloadConcurrency')) {
        destination['downloadConcurrency'] = 8;
        changed = true;
      }
    }
    return changed;
  }

  bool _ensureBlockSizeMb(Map<String, dynamic> data) {
    final value = data['blockSizeMB'];
    if (value == null) {
      data['blockSizeMB'] = 1;
      return true;
    }
    final parsed = value is num ? value.toInt() : int.tryParse(value.toString().trim());
    if (parsed == null || (parsed != 1 && parsed != 2 && parsed != 4 && parsed != 8)) {
      throw StateError('Invalid blockSizeMB. Allowed values: 1, 2, 4, 8.');
    }
    if (value is! int) {
      data['blockSizeMB'] = parsed;
      return true;
    }
    return false;
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
        if (_isScalar(entry.value) || _isInlineCollection(entry.value)) {
          buffer.write(' ');
          buffer.writeln(_encodeYaml(entry.value, indent: indent + 1));
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
        if (_isScalar(item) || _isInlineCollection(item)) {
          buffer.write(' ');
          buffer.writeln(_encodeYaml(item, indent: indent + 1));
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

  bool _isInlineCollection(Object? value) {
    if (value is Map) {
      return value.isEmpty;
    }
    if (value is List) {
      return value.isEmpty;
    }
    return false;
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
    _encryptDestinationGdriveTokens(data, token);
    final backup = data['backup'];
    if (backup is! Map) {
      return;
    }
    final gdrive = backup['gdrive'];
    if (gdrive is! Map) {
      return;
    }
    final accessToken = gdrive['accessToken']?.toString() ?? '';
    if (accessToken.isNotEmpty) {
      gdrive['accessTokenEnc'] = _encryptPassword(accessToken, token);
      gdrive['accessToken'] = '';
    } else {
      gdrive.remove('accessTokenEnc');
    }

    final refreshToken = gdrive['refreshToken']?.toString() ?? '';
    if (refreshToken.isNotEmpty) {
      gdrive['refreshTokenEnc'] = _encryptPassword(refreshToken, token);
      gdrive['refreshToken'] = '';
    } else {
      gdrive.remove('refreshTokenEnc');
    }
  }

  void _decryptGdriveTokensInMap(Map<String, dynamic> data, String? token) {
    _decryptDestinationGdriveTokens(data, token);
    final backup = data['backup'];
    if (backup is! Map) {
      return;
    }
    final gdrive = backup['gdrive'];
    if (gdrive is! Map) {
      return;
    }
    final accessEnc = gdrive['accessTokenEnc']?.toString();
    if (accessEnc != null && accessEnc.isNotEmpty) {
      gdrive['accessToken'] = (token == null || token.isEmpty) ? '' : _decryptPassword(accessEnc, token);
    } else if (gdrive['accessToken'] == null) {
      gdrive['accessToken'] = '';
    }

    final refreshEnc = gdrive['refreshTokenEnc']?.toString();
    if (refreshEnc != null && refreshEnc.isNotEmpty) {
      gdrive['refreshToken'] = (token == null || token.isEmpty) ? '' : _decryptPassword(refreshEnc, token);
    } else if (gdrive['refreshToken'] == null) {
      gdrive['refreshToken'] = '';
    }
  }

  void _encryptSftpPasswordInMap(Map<String, dynamic> data, String token) {
    _encryptDestinationSftpPasswords(data, token);
    final backup = data['backup'];
    if (backup is! Map) {
      return;
    }
    final sftp = backup['sftp'];
    if (sftp is! Map) {
      return;
    }
    final password = sftp['password']?.toString() ?? '';
    if (password.isEmpty) {
      sftp.remove('passwordEnc');
      return;
    }
    sftp['passwordEnc'] = _encryptPassword(password, token);
    sftp['password'] = '';
  }

  void _decryptSftpPasswordInMap(Map<String, dynamic> data, String? token) {
    _decryptDestinationSftpPasswords(data, token);
    final backup = data['backup'];
    if (backup is! Map) {
      return;
    }
    final sftp = backup['sftp'];
    if (sftp is! Map) {
      return;
    }
    final enc = sftp['passwordEnc']?.toString();
    if (enc != null && enc.isNotEmpty) {
      sftp['password'] = (token == null || token.isEmpty) ? '' : _decryptPassword(enc, token);
      return;
    }
    if (sftp['password'] == null) {
      sftp['password'] = '';
    }
  }

  void _encryptDestinationGdriveTokens(Map<String, dynamic> data, String token) {
    final destinations = data['destinations'];
    if (destinations is! List) {
      return;
    }
    for (final entry in destinations) {
      if (entry is! Map) {
        continue;
      }
      if ((entry['driverId'] ?? '').toString().trim() != 'gdrive') {
        continue;
      }
      final params = entry['params'];
      if (params is! Map) {
        continue;
      }
      final accessToken = params['accessToken']?.toString() ?? '';
      if (accessToken.isNotEmpty) {
        params['accessTokenEnc'] = _encryptPassword(accessToken, token);
        params['accessToken'] = '';
      } else {
        params.remove('accessTokenEnc');
      }
      final refreshToken = params['refreshToken']?.toString() ?? '';
      if (refreshToken.isNotEmpty) {
        params['refreshTokenEnc'] = _encryptPassword(refreshToken, token);
        params['refreshToken'] = '';
      } else {
        params.remove('refreshTokenEnc');
      }
    }
  }

  void _decryptDestinationGdriveTokens(Map<String, dynamic> data, String? token) {
    final destinations = data['destinations'];
    if (destinations is! List) {
      return;
    }
    for (final entry in destinations) {
      if (entry is! Map) {
        continue;
      }
      if ((entry['driverId'] ?? '').toString().trim() != 'gdrive') {
        continue;
      }
      final params = entry['params'];
      if (params is! Map) {
        continue;
      }
      final accessEnc = params['accessTokenEnc']?.toString();
      if (accessEnc != null && accessEnc.isNotEmpty) {
        params['accessToken'] = (token == null || token.isEmpty) ? '' : _decryptPassword(accessEnc, token);
      } else if (params['accessToken'] == null) {
        params['accessToken'] = '';
      }
      final refreshEnc = params['refreshTokenEnc']?.toString();
      if (refreshEnc != null && refreshEnc.isNotEmpty) {
        params['refreshToken'] = (token == null || token.isEmpty) ? '' : _decryptPassword(refreshEnc, token);
      } else if (params['refreshToken'] == null) {
        params['refreshToken'] = '';
      }
    }
  }

  void _encryptDestinationSftpPasswords(Map<String, dynamic> data, String token) {
    final destinations = data['destinations'];
    if (destinations is! List) {
      return;
    }
    for (final entry in destinations) {
      if (entry is! Map) {
        continue;
      }
      if ((entry['driverId'] ?? '').toString().trim() != 'sftp') {
        continue;
      }
      final params = entry['params'];
      if (params is! Map) {
        continue;
      }
      final password = params['password']?.toString() ?? '';
      if (password.isEmpty) {
        params.remove('passwordEnc');
      } else {
        params['passwordEnc'] = _encryptPassword(password, token);
        params['password'] = '';
      }
    }
  }

  void _decryptDestinationSftpPasswords(Map<String, dynamic> data, String? token) {
    final destinations = data['destinations'];
    if (destinations is! List) {
      return;
    }
    for (final entry in destinations) {
      if (entry is! Map) {
        continue;
      }
      if ((entry['driverId'] ?? '').toString().trim() != 'sftp') {
        continue;
      }
      final params = entry['params'];
      if (params is! Map) {
        continue;
      }
      final enc = params['passwordEnc']?.toString();
      if (enc != null && enc.isNotEmpty) {
        params['password'] = (token == null || token.isEmpty) ? '' : _decryptPassword(enc, token);
      } else if (params['password'] == null) {
        params['password'] = '';
      }
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
