import 'dart:convert';
import 'dart:io';

class GoogleOAuthInstalledClient {
  GoogleOAuthInstalledClient({required this.clientId, required this.clientSecret});

  final String clientId;
  final String clientSecret;

  bool get hasSecret => clientSecret.trim().isNotEmpty;

  static GoogleOAuthInstalledClient? fromJsonObject(Object? decoded) {
    if (decoded is! Map) {
      return null;
    }
    final installed = decoded['installed'];
    if (installed is! Map) {
      return null;
    }
    final clientId = installed['client_id']?.toString().trim() ?? '';
    final clientSecret = installed['client_secret']?.toString().trim() ?? '';
    if (clientId.isEmpty) {
      return null;
    }
    return GoogleOAuthInstalledClient(clientId: clientId, clientSecret: clientSecret);
  }

  static GoogleOAuthInstalledClient? fromJsonString(String json) {
    if (json.trim().isEmpty) {
      return null;
    }
    try {
      return fromJsonObject(jsonDecode(json));
    } catch (_) {
      return null;
    }
  }

  static Future<GoogleOAuthInstalledClient?> tryLoadFromFile(File file) async {
    try {
      if (!await file.exists()) {
        return null;
      }
      final content = await file.readAsString();
      return fromJsonString(content);
    } catch (_) {
      return null;
    }
  }
}

class GoogleOAuthClientLocator {
  GoogleOAuthClientLocator({this.overrideFile});

  final File? overrideFile;

  List<File> candidateFiles({Directory? settingsDir}) {
    final candidates = <File>[];
    if (overrideFile != null) {
      candidates.add(overrideFile!);
    }
    if (settingsDir != null) {
      candidates.add(File('${settingsDir.path}${Platform.pathSeparator}google_oauth_client.json'));
      candidates.add(File('${settingsDir.path}${Platform.pathSeparator}google_oauth_client.json'.replaceAll('/', Platform.pathSeparator)));
    }
    candidates.add(File('assets${Platform.pathSeparator}google_oauth_client.json')); // local dev override (gitignored)
    candidates.add(File('assets/google_oauth_client.json')); // local dev override (gitignored)
    candidates.add(File('google_oauth_client.json'));
    return candidates;
  }

  Future<GoogleOAuthInstalledClient> load({Directory? settingsDir, bool requireSecret = true}) async {
    for (final candidate in candidateFiles(settingsDir: settingsDir)) {
      final config = await GoogleOAuthInstalledClient.tryLoadFromFile(candidate);
      if (config == null) {
        continue;
      }
      if (requireSecret && !config.hasSecret) {
        throw 'Google OAuth client_secret is missing in ${candidate.path}.';
      }
      return config;
    }
    final hint = settingsDir == null
        ? 'Expected google_oauth_client.json or assets/google_oauth_client.json (local dev override).'
        : 'Expected ${settingsDir.path}${Platform.pathSeparator}google_oauth_client.json or assets/google_oauth_client.json (local dev override).';
    throw 'Google OAuth client config not found. $hint';
  }
}
