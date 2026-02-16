import 'dart:convert';
import 'dart:io';
import 'dart:math';
import 'dart:typed_data';

import 'package:http/http.dart' as http;
import 'package:virtbackup/agent/settings_store.dart';
import 'package:virtbackup/common/google_oauth_client.dart';
import 'package:virtbackup/common/models.dart';
import 'package:virtbackup/common/settings.dart';

const int _defaultFileSizeBytes = 16 * 1024 * 1024;
const int _defaultConcurrentUploads = 4;
const String _defaultDestPath = 'tmp/test';

Future<void> main(List<String> args) async {
  final parsed = _parseArgs(args);
  if (parsed.showHelp) {
    _printUsage();
    return;
  }

  final store = await AppSettingsStore.fromAgentDefaultPath();
  final settings = await store.load();
  final destination = _selectedGdriveDestination(settings);
  final oauth = await GoogleOAuthClientLocator().load(settingsDir: store.file.parent, requireSecret: true);
  if (_gdriveRefreshToken(settings).isEmpty && _gdriveAccessToken(settings).isEmpty) {
    stderr.writeln('No Google Drive tokens found in ${store.file.path}');
    exitCode = 1;
    return;
  }

  final client = http.Client();
  try {
    final tokenResult = await _ensureAccessToken(settings, store, client, oauth);
    var accessToken = tokenResult.$1;
    var activeSettings = tokenResult.$2;
    final normalizedDest = parsed.destPath.trim().replaceAll('\\', '/');
    final useDriveRoot = normalizedDest.startsWith('/');
    final driveRoot = await _ensureDriveRoot(destination, client, accessToken);
    final baseRoot = useDriveRoot ? driveRoot : await _ensureFolder(driveRoot, 'VirtBackup', client, accessToken);
    final relativeDest = useDriveRoot ? normalizedDest.substring(1) : normalizedDest;
    final destId = relativeDest.isEmpty ? baseRoot : await _ensureFolderPath(baseRoot, relativeDest, client, accessToken);
    final packsId = await _ensureFolder(destId, 'packs', client, accessToken);
    stdout.writeln('Destination ready: ${parsed.destPath} id=$destId packs=$packsId (baseRoot=$baseRoot)');
    stdout.writeln('Upload mode: resumable (session + PUT)');

    var detailsLogged = false;
    var stop = false;
    var counter = 0;
    final inFlight = <Future<void>>[];
    var totalUploadedBytes = 0;
    var lastLoggedBytes = 0;
    var lastSpeedLogAt = DateTime.now().toUtc();

    void maybeLogSpeed() {
      final now = DateTime.now().toUtc();
      final elapsedSec = now.difference(lastSpeedLogAt).inMilliseconds / 1000.0;
      if (elapsedSec < 30) {
        return;
      }
      final deltaBytes = totalUploadedBytes - lastLoggedBytes;
      final speed = elapsedSec > 0 ? deltaBytes / elapsedSec : 0.0;
      stdout.writeln('Speed (last ${elapsedSec.toStringAsFixed(0)}s): ${_formatSpeed(speed)} (+${_formatBytes(deltaBytes)})');
      lastSpeedLogAt = now;
      lastLoggedBytes = totalUploadedBytes;
    }

    Future<void> startUpload() async {
      if (stop) {
        return;
      }
      counter += 1;
      final stamp = DateTime.now().toUtc().toIso8601String().replaceAll(':', '_');
      final packName = 'vb-pack-$stamp-$counter.pack';
      final packBytes = _randomBytes(parsed.fileSizeBytes);
      try {
        final packId = await _uploadResumable(
          packName,
          packsId,
          packBytes,
          client,
          () async {
            final refreshed = await _refreshAccessToken(activeSettings, store, client, oauth);
            accessToken = refreshed.$1;
            activeSettings = refreshed.$2;
            return accessToken;
          },
          () => accessToken,
          () {
            if (detailsLogged) {
              return false;
            }
            detailsLogged = true;
            return true;
          },
        );
        totalUploadedBytes += packBytes.length;
        maybeLogSpeed();
        stdout.writeln('Upload complete. packId=$packId');
      } catch (error, stackTrace) {
        if (!detailsLogged) {
          stderr.writeln('Upload failed: $error');
          stderr.writeln(stackTrace);
          detailsLogged = true;
        }
        stop = true;
        return;
      }
    }

    while (!stop) {
      while (inFlight.length < parsed.concurrentUploads && !stop) {
        final future = startUpload().catchError((_) {});
        inFlight.add(future);
        future.whenComplete(() {
          inFlight.remove(future);
        });
      }
      if (inFlight.isEmpty) {
        break;
      }
      try {
        await Future.any(inFlight);
      } catch (_) {
        stop = true;
        exitCode = 1;
        break;
      }
    }
    if (inFlight.isNotEmpty) {
      try {
        await Future.wait(inFlight);
      } catch (_) {}
    }
  } finally {
    client.close();
  }
}

void _printUsage() {
  stdout.writeln('Usage: dart run tools/gdrive_upload_test.dart [options]');
  stdout.writeln('Options:');
  stdout.writeln('  --size-mib <int>     File size in MiB (default: ${_defaultFileSizeBytes ~/ (1024 * 1024)})');
  stdout.writeln('  --concurrent <int>   Number of concurrent uploads (default: $_defaultConcurrentUploads)');
  stdout.writeln('  --dest <path>        Drive folder path (prefix with / for Drive root; default: $_defaultDestPath)');
  stdout.writeln('  --help               Show this help');
}

_ParsedArgs _parseArgs(List<String> args) {
  var sizeMiB = _defaultFileSizeBytes ~/ (1024 * 1024);
  var concurrent = _defaultConcurrentUploads;
  var destPath = _defaultDestPath;
  var showHelp = false;

  for (var i = 0; i < args.length; i += 1) {
    final arg = args[i];
    if (arg == '--help' || arg == '-h') {
      showHelp = true;
      break;
    }
    if (arg == '--size-mib' && i + 1 < args.length) {
      final value = int.tryParse(args[++i]);
      if (value != null && value > 0) {
        sizeMiB = value;
      }
      continue;
    }
    if (arg == '--concurrent' && i + 1 < args.length) {
      final value = int.tryParse(args[++i]);
      if (value != null && value > 0) {
        concurrent = value;
      }
      continue;
    }
    if (arg == '--dest' && i + 1 < args.length) {
      destPath = args[++i];
      continue;
    }
  }

  return _ParsedArgs(fileSizeBytes: sizeMiB * 1024 * 1024, concurrentUploads: concurrent, destPath: destPath, showHelp: showHelp);
}

class _ParsedArgs {
  const _ParsedArgs({required this.fileSizeBytes, required this.concurrentUploads, required this.destPath, required this.showHelp});

  final int fileSizeBytes;
  final int concurrentUploads;
  final String destPath;
  final bool showHelp;
}

Uint8List _randomBytes(int length) {
  final rng = Random.secure();
  final data = Uint8List(length);
  for (var i = 0; i < data.length; i += 1) {
    data[i] = rng.nextInt(256);
  }
  return data;
}

Future<(String, AppSettings)> _ensureAccessToken(AppSettings settings, AppSettingsStore store, http.Client client, GoogleOAuthInstalledClient oauth) async {
  final now = DateTime.now().toUtc();
  final token = _gdriveAccessToken(settings);
  final expiresAt = _gdriveExpiresAt(settings);
  if (token.isNotEmpty && expiresAt != null && expiresAt.isAfter(now.add(const Duration(minutes: 1)))) {
    return (token, settings);
  }

  final refresh = _gdriveRefreshToken(settings);
  if (refresh.isEmpty) {
    throw StateError('Google Drive refresh token missing; cannot refresh access token.');
  }

  final response = await client.post(
    Uri.parse('https://oauth2.googleapis.com/token'),
    headers: {'Content-Type': 'application/x-www-form-urlencoded'},
    body: <String, String>{'client_id': oauth.clientId, 'client_secret': oauth.clientSecret, 'refresh_token': refresh, 'grant_type': 'refresh_token'},
  );
  if (response.statusCode >= 300) {
    throw StateError('Token refresh failed: ${response.statusCode} ${response.body}');
  }
  final decoded = jsonDecode(response.body);
  if (decoded is! Map) {
    throw StateError('Token refresh failed: invalid response.');
  }
  final accessToken = decoded['access_token']?.toString() ?? '';
  final expiresIn = decoded['expires_in'];
  if (accessToken.isEmpty) {
    throw StateError('Token refresh failed: access_token missing.');
  }
  final newExpiresAt = _calculateExpiresAt(expiresIn);
  final updated = _withUpdatedGdriveTokens(settings: settings, accessToken: accessToken, expiresAt: newExpiresAt);
  await store.save(updated);
  return (accessToken, updated);
}

Future<(String, AppSettings)> _refreshAccessToken(AppSettings settings, AppSettingsStore store, http.Client client, GoogleOAuthInstalledClient oauth) async {
  final refresh = _gdriveRefreshToken(settings);
  if (refresh.isEmpty) {
    throw StateError('Google Drive refresh token missing; cannot refresh access token.');
  }
  final response = await client.post(
    Uri.parse('https://oauth2.googleapis.com/token'),
    headers: {'Content-Type': 'application/x-www-form-urlencoded'},
    body: <String, String>{'client_id': oauth.clientId, 'client_secret': oauth.clientSecret, 'refresh_token': refresh, 'grant_type': 'refresh_token'},
  );
  if (response.statusCode >= 300) {
    throw StateError('Token refresh failed: ${response.statusCode} ${response.body}');
  }
  final decoded = jsonDecode(response.body);
  if (decoded is! Map) {
    throw StateError('Token refresh failed: invalid response.');
  }
  final accessToken = decoded['access_token']?.toString() ?? '';
  final expiresIn = decoded['expires_in'];
  if (accessToken.isEmpty) {
    throw StateError('Token refresh failed: access_token missing.');
  }
  final newExpiresAt = _calculateExpiresAt(expiresIn);
  final updated = _withUpdatedGdriveTokens(settings: settings, accessToken: accessToken, expiresAt: newExpiresAt);
  await store.save(updated);
  return (accessToken, updated);
}

DateTime? _calculateExpiresAt(Object? expiresIn) {
  if (expiresIn is num) {
    return DateTime.now().toUtc().add(Duration(seconds: expiresIn.toInt()));
  }
  final parsed = int.tryParse(expiresIn?.toString() ?? '');
  if (parsed == null || parsed <= 0) {
    return null;
  }
  return DateTime.now().toUtc().add(Duration(seconds: parsed));
}

Future<String> _ensureFolderPath(String rootId, String rawPath, http.Client client, String token) async {
  final cleaned = rawPath.trim().replaceAll('\\', '/');
  final parts = cleaned.split('/').where((part) => part.trim().isNotEmpty).toList();
  var current = rootId;
  for (final part in parts) {
    current = await _ensureFolder(current, part, client, token);
  }
  return current;
}

Future<String> _ensureDriveRoot(BackupDestination destination, http.Client client, String token) async {
  var current = 'root';
  final rootPath = (destination.params['rootPath'] ?? '').toString().trim();
  if (rootPath.isEmpty) {
    return current;
  }
  return _ensureFolderPath(current, rootPath, client, token);
}

BackupDestination _selectedGdriveDestination(AppSettings settings) {
  final selectedId = settings.backupDestinationId?.trim() ?? '';
  if (selectedId.isEmpty) {
    throw StateError('backupDestinationId is required.');
  }
  for (final destination in settings.destinations) {
    if (destination.id != selectedId) {
      continue;
    }
    if (destination.driverId != 'gdrive') {
      throw StateError('Selected destination "$selectedId" is not a Google Drive destination.');
    }
    return destination;
  }
  throw StateError('Google Drive destination "$selectedId" not found.');
}

Map<String, dynamic> _selectedGdriveParams(AppSettings settings) {
  return _selectedGdriveDestination(settings).params;
}

String _gdriveAccessToken(AppSettings settings) {
  return (_selectedGdriveParams(settings)['accessToken'] ?? '').toString().trim();
}

String _gdriveRefreshToken(AppSettings settings) {
  return (_selectedGdriveParams(settings)['refreshToken'] ?? '').toString().trim();
}

DateTime? _gdriveExpiresAt(AppSettings settings) {
  return AppSettings.parseDateTimeOrNull(_selectedGdriveParams(settings)['expiresAt']);
}

AppSettings _withUpdatedGdriveTokens({required AppSettings settings, required String accessToken, required DateTime? expiresAt}) {
  final destination = _selectedGdriveDestination(settings);
  final params = Map<String, dynamic>.from(destination.params);
  params['accessToken'] = accessToken;
  if (expiresAt == null) {
    params.remove('expiresAt');
  } else {
    params['expiresAt'] = expiresAt.toUtc().toIso8601String();
  }
  final updatedDestinations = settings.destinations.map((entry) {
    if (entry.id != destination.id) {
      return entry;
    }
    return BackupDestination(
      id: entry.id,
      name: entry.name,
      driverId: entry.driverId,
      enabled: entry.enabled,
      params: params,
      disableFresh: entry.disableFresh,
      storeBlobs: entry.storeBlobs,
      useBlobs: entry.useBlobs,
      uploadConcurrency: entry.uploadConcurrency,
      downloadConcurrency: entry.downloadConcurrency,
    );
  }).toList();
  return settings.copyWith(destinations: updatedDestinations);
}

Future<String> _ensureFolder(String parentId, String name, http.Client client, String token) async {
  final existing = await _findChildFolder(parentId, name, client, token);
  if (existing != null) {
    return existing;
  }
  final uri = Uri.parse('https://www.googleapis.com/drive/v3/files');
  final body = jsonEncode({
    'name': name,
    'mimeType': 'application/vnd.google-apps.folder',
    'parents': [parentId],
  });
  final response = await client.post(uri, headers: _authHeaders(token)..['Content-Type'] = 'application/json; charset=UTF-8', body: body);
  if (response.statusCode >= 300) {
    throw StateError('Drive create folder failed: ${response.statusCode} ${response.body}');
  }
  final decoded = jsonDecode(response.body);
  if (decoded is! Map || decoded['id'] == null) {
    throw StateError('Drive create folder failed: invalid response');
  }
  return decoded['id'].toString();
}

Future<String?> _findChildFolder(String parentId, String name, http.Client client, String token) async {
  final escaped = name.replaceAll("'", "\\'");
  final query = "name = '$escaped' and mimeType = 'application/vnd.google-apps.folder' and '$parentId' in parents and trashed = false";
  final uri = Uri.https('www.googleapis.com', '/drive/v3/files', {'q': query, 'fields': 'files(id,name)', 'pageSize': '1'});
  final response = await client.get(uri, headers: _authHeaders(token));
  if (response.statusCode >= 300) {
    throw StateError('Drive list failed: ${response.statusCode} ${response.body}');
  }
  final decoded = jsonDecode(response.body);
  if (decoded is! Map) {
    return null;
  }
  final files = decoded['files'];
  if (files is List && files.isNotEmpty) {
    final first = files.first;
    if (first is Map && first['id'] != null) {
      return first['id'].toString();
    }
  }
  return null;
}

Future<String> _uploadResumable(
  String name,
  String parentId,
  List<int> bytes,
  http.Client client,
  Future<String> Function() refreshToken,
  String Function() currentToken,
  bool Function() shouldLogError,
) async {
  final uri = Uri.parse('https://www.googleapis.com/upload/drive/v3/files?uploadType=resumable');
  final metadata = jsonEncode({
    'name': name,
    'parents': [parentId],
  });
  final metadataHeaders = _authHeaders(currentToken())
    ..['Content-Type'] = 'application/json; charset=UTF-8'
    ..['X-Upload-Content-Type'] = 'application/octet-stream'
    ..['X-Upload-Content-Length'] = bytes.length.toString();

  Future<http.Response> attemptCreateSession(String token) async {
    final headers = Map<String, String>.from(metadataHeaders);
    headers[HttpHeaders.authorizationHeader] = 'Bearer $token';
    return client.post(uri, headers: headers, body: metadata);
  }

  Future<http.Response> attemptUpload(Uri sessionUri, String token) async {
    final headers = _authHeaders(token)
      ..['Content-Type'] = 'application/octet-stream'
      ..['Content-Length'] = bytes.length.toString();
    return client.put(sessionUri, headers: headers, body: bytes);
  }

  Uri parseSessionUri(http.Response response) {
    final location = response.headers['location'];
    if (location == null || location.isEmpty) {
      throw StateError('Drive upload failed: missing upload session location.');
    }
    return Uri.parse(location);
  }

  try {
    var token = currentToken();
    var response = await attemptCreateSession(token);
    if (response.statusCode == 401) {
      token = await refreshToken();
      response = await attemptCreateSession(token);
    }
    if (response.statusCode >= 300) {
      if (shouldLogError()) {
        stderr.writeln('Drive upload session failed status=${response.statusCode} endpoint=$uri');
        stderr.writeln('Request headers: ${_redactHeaders(metadataHeaders)}');
        stderr.writeln('Response headers: ${response.headers}');
        stderr.writeln('Response body: ${response.body}');
      }
      throw StateError('Drive upload session failed: ${response.statusCode}');
    }

    var sessionUri = parseSessionUri(response);
    var uploadResponse = await attemptUpload(sessionUri, token);
    if (uploadResponse.statusCode == 401) {
      token = await refreshToken();
      response = await attemptCreateSession(token);
      if (response.statusCode >= 300) {
        if (shouldLogError()) {
          stderr.writeln('Drive upload session failed status=${response.statusCode} endpoint=$uri');
          stderr.writeln('Request headers: ${_redactHeaders(metadataHeaders)}');
          stderr.writeln('Response headers: ${response.headers}');
          stderr.writeln('Response body: ${response.body}');
        }
        throw StateError('Drive upload session failed: ${response.statusCode}');
      }
      sessionUri = parseSessionUri(response);
      uploadResponse = await attemptUpload(sessionUri, token);
    }
    if (uploadResponse.statusCode >= 300) {
      if (shouldLogError()) {
        stderr.writeln('Drive upload failed status=${uploadResponse.statusCode} session=$sessionUri');
        stderr.writeln('Response headers: ${uploadResponse.headers}');
        stderr.writeln('Response body: ${uploadResponse.body}');
      }
      throw StateError('Drive upload failed: ${uploadResponse.statusCode}');
    }
    final decoded = jsonDecode(uploadResponse.body);
    if (decoded is! Map || decoded['id'] == null) {
      if (shouldLogError()) {
        stderr.writeln('Drive upload invalid response body: ${uploadResponse.body}');
      }
      throw StateError('Drive upload failed: invalid response');
    }
    return decoded['id'].toString();
  } on HandshakeException catch (error, stackTrace) {
    if (shouldLogError()) {
      stderr.writeln('Drive upload handshake failure name=$name parentId=$parentId bytes=${bytes.length} endpoint=$uri osError=${error.osError} error=$error');
      stderr.writeln('Request headers: ${_redactHeaders(metadataHeaders)}');
      stderr.writeln(stackTrace);
    }
    rethrow;
  } catch (error, stackTrace) {
    if (shouldLogError()) {
      stderr.writeln('Drive upload exception name=$name parentId=$parentId bytes=${bytes.length} endpoint=$uri error=$error');
      stderr.writeln('Request headers: ${_redactHeaders(metadataHeaders)}');
      stderr.writeln(stackTrace);
    }
    rethrow;
  }
}

String _formatSpeed(double bytesPerSec) {
  if (bytesPerSec <= 0) {
    return '0 B/s';
  }
  const units = ['B/s', 'KB/s', 'MB/s', 'GB/s'];
  var value = bytesPerSec;
  var index = 0;
  while (value >= 1024 && index < units.length - 1) {
    value /= 1024;
    index += 1;
  }
  return '${value.toStringAsFixed(1)} ${units[index]}';
}

String _formatBytes(int bytes) {
  if (bytes <= 0) {
    return '0 B';
  }
  const units = ['B', 'KB', 'MB', 'GB'];
  var value = bytes.toDouble();
  var index = 0;
  while (value >= 1024 && index < units.length - 1) {
    value /= 1024;
    index += 1;
  }
  return '${value.toStringAsFixed(1)} ${units[index]}';
}

Map<String, String> _authHeaders(String token) {
  return {HttpHeaders.authorizationHeader: 'Bearer $token'};
}

Map<String, String> _redactHeaders(Map<String, String> headers) {
  final copy = Map<String, String>.from(headers);
  if (copy.containsKey(HttpHeaders.authorizationHeader)) {
    copy[HttpHeaders.authorizationHeader] = 'Bearer <redacted>';
  }
  return copy;
}
