import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:math';

import 'package:http/http.dart' as http;
import 'package:http/io_client.dart';
import 'package:virtbackup/agent/settings_store.dart';

class _Args {
  _Args({
    required this.vmName,
    required this.agentUrl,
    required this.agentToken,
    required this.sourceServerId,
    required this.targetServerId,
    required this.sourceHost,
    required this.targetHost,
    required this.sshUser,
    required this.pollInterval,
    required this.timeout,
    required this.sourceHostExplicit,
    required this.targetHostExplicit,
    required this.skipRestore,
    required this.skipBackup,
    required this.cancelAfterRandom,
    required this.fresh,
    required this.driverParams,
    required this.blockSizeMBOverride,
    this.destinationId,
    this.agentTokenFile,
    this.sshKeyPath,
  });

  final String vmName;
  final Uri agentUrl;
  final String? agentToken;
  final String sourceServerId;
  final String targetServerId;
  final String sourceHost;
  final String targetHost;
  final String sshUser;
  final Duration pollInterval;
  final Duration timeout;
  final bool sourceHostExplicit;
  final bool targetHostExplicit;
  final bool skipRestore;
  final bool skipBackup;
  final bool cancelAfterRandom;
  final bool fresh;
  final Map<String, dynamic> driverParams;
  final int? blockSizeMBOverride;
  final String? destinationId;
  final String? agentTokenFile;
  final String? sshKeyPath;

  _Args copyWith({
    String? vmName,
    Uri? agentUrl,
    String? agentToken,
    String? sourceServerId,
    String? targetServerId,
    String? sourceHost,
    String? targetHost,
    String? sshUser,
    Duration? pollInterval,
    Duration? timeout,
    bool? sourceHostExplicit,
    bool? targetHostExplicit,
    bool? skipRestore,
    bool? skipBackup,
    bool? cancelAfterRandom,
    bool? fresh,
    Map<String, dynamic>? driverParams,
    int? blockSizeMBOverride,
    String? destinationId,
    String? agentTokenFile,
    String? sshKeyPath,
  }) {
    return _Args(
      vmName: vmName ?? this.vmName,
      agentUrl: agentUrl ?? this.agentUrl,
      agentToken: agentToken ?? this.agentToken,
      sourceServerId: sourceServerId ?? this.sourceServerId,
      targetServerId: targetServerId ?? this.targetServerId,
      sourceHost: sourceHost ?? this.sourceHost,
      targetHost: targetHost ?? this.targetHost,
      sshUser: sshUser ?? this.sshUser,
      pollInterval: pollInterval ?? this.pollInterval,
      timeout: timeout ?? this.timeout,
      sourceHostExplicit: sourceHostExplicit ?? this.sourceHostExplicit,
      targetHostExplicit: targetHostExplicit ?? this.targetHostExplicit,
      skipRestore: skipRestore ?? this.skipRestore,
      skipBackup: skipBackup ?? this.skipBackup,
      cancelAfterRandom: cancelAfterRandom ?? this.cancelAfterRandom,
      fresh: fresh ?? this.fresh,
      driverParams: driverParams ?? this.driverParams,
      blockSizeMBOverride: blockSizeMBOverride ?? this.blockSizeMBOverride,
      destinationId: destinationId ?? this.destinationId,
      agentTokenFile: agentTokenFile ?? this.agentTokenFile,
      sshKeyPath: sshKeyPath ?? this.sshKeyPath,
    );
  }
}

class _JobStatus {
  const _JobStatus({
    required this.state,
    required this.message,
    required this.type,
    required this.totalBytes,
    required this.bytesTransferred,
    required this.speedBytesPerSec,
    required this.averageSpeedBytesPerSec,
    required this.physicalBytesTransferred,
    required this.physicalSpeedBytesPerSec,
    required this.averagePhysicalSpeedBytesPerSec,
    required this.etaSeconds,
    required this.physicalRemainingBytes,
    required this.physicalTotalBytes,
    required this.physicalProgressPercent,
  });

  final String state;
  final String message;
  final String type;
  final int totalBytes;
  final int bytesTransferred;
  final double speedBytesPerSec;
  final double averageSpeedBytesPerSec;
  final int physicalBytesTransferred;
  final double physicalSpeedBytesPerSec;
  final double averagePhysicalSpeedBytesPerSec;
  final int? etaSeconds;
  final int physicalRemainingBytes;
  final int physicalTotalBytes;
  final double physicalProgressPercent;
}

void main(List<String> args) async {
  final parsed = _parseArgs(args);
  if (parsed == null) {
    _printUsage();
    exitCode = 64;
    return;
  }

  final resolvedToken = await _resolveAgentToken(parsed);
  final resolvedArgs = parsed.copyWith(agentToken: resolvedToken);

  final client = IOClient(_createHttpClient(resolvedArgs.agentUrl));
  final eventLog = _startAgentEventLog(resolvedArgs);
  String? activeJobId;
  Timer? cancelTimer;
  var cancelSent = false;
  var canceledByUser = false;
  var cancelSignalCount = 0;
  List<StreamSubscription<ProcessSignal>> registerCancelHandler(_Args currentArgs) {
    void onSignal(ProcessSignal signal) {
      cancelSignalCount += 1;
      if (cancelSignalCount >= 2) {
        stderr.writeln('Received $signal again. Exiting now.');
        exit(130);
      }
      if (cancelSent || activeJobId == null) {
        stderr.writeln('Received $signal. No active job to cancel.');
        exitCode = 130;
        return;
      }
      cancelSent = true;
      canceledByUser = true;
      final jobId = activeJobId;
      stderr.writeln('Received $signal. Sending cancel for job $jobId...');
      unawaited(
        _cancelJob(client, currentArgs, jobId).whenComplete(() {
          exitCode = 130;
        }),
      );
    }

    final subs = <StreamSubscription<ProcessSignal>>[];
    subs.add(ProcessSignal.sigint.watch().listen(onSignal));
    subs.add(ProcessSignal.sigterm.watch().listen(onSignal));
    return subs;
  }

  List<StreamSubscription<ProcessSignal>> signalSubs = const [];
  try {
    final config = await _fetchConfig(client, resolvedArgs);
    final backupPath = _extractBackupPath(config, resolvedArgs);
    final resolvedSourceId = _resolveServerId(config, resolvedArgs.sourceServerId);
    final resolvedTargetId = _resolveServerId(config, resolvedArgs.targetServerId);
    final sourceHost = _resolveHost(config, resolvedSourceId, resolvedArgs.sourceHost, resolvedArgs.sourceHostExplicit);
    final targetHost = _resolveHost(config, resolvedTargetId, resolvedArgs.targetHost, resolvedArgs.targetHostExplicit);
    final resolved = resolvedArgs.copyWith(sourceServerId: resolvedSourceId, targetServerId: resolvedTargetId, sourceHost: sourceHost, targetHost: targetHost);
    signalSubs = registerCancelHandler(resolved);
    stdout.writeln('Backup base path: $backupPath');

    if (resolved.fresh) {
      stdout.writeln('Fresh cleanup requested: agent will handle cleanup (debug only).');
    }
    if (resolved.blockSizeMBOverride != null) {
      stdout.writeln('Backup block size override for this run: ${resolved.blockSizeMBOverride} MiB');
    }

    if (resolved.skipBackup) {
      stdout.writeln('Skipping backup due to --no-backup');
    } else {
      final backupJobId = await _startBackup(client, resolved);
      activeJobId = backupJobId;
      stdout.writeln('Started backup job: $backupJobId');
      if (resolved.cancelAfterRandom) {
        final delaySeconds = 60 + Random().nextInt(21);
        stdout.writeln('Auto-cancel enabled: canceling job after ${delaySeconds}s');
        cancelTimer = Timer(Duration(seconds: delaySeconds), () {
          if (cancelSent || activeJobId == null) {
            return;
          }
          cancelSent = true;
          final jobId = activeJobId;
          stderr.writeln('Auto-cancel: sending cancel for job $jobId...');
          unawaited(
            _cancelJob(client, resolved, jobId).whenComplete(() {
              exitCode = 0;
            }),
          );
        });
      }
      final backupOk = await _waitForJob(client, resolved, backupJobId, isCanceled: () => cancelSent);
      if (!backupOk) {
        exitCode = 1;
        return;
      }
      activeJobId = null;
    }

    if (canceledByUser || cancelSent) {
      stdout.writeln('Skipping restore + md5 due to cancel request');
      return;
    }

    if (resolved.skipRestore) {
      stdout.writeln('Skipping restore + md5 due to --no-restore');
      return;
    }

    final xmlPath = await _pickLatestRestoreXml(client, resolved);
    stdout.writeln('Using restore xml: $xmlPath');

    await _restorePrecheck(client, resolved, xmlPath);
    final restoreJobId = await _startRestore(client, resolved, xmlPath);
    activeJobId = restoreJobId;
    stdout.writeln('Started restore job: $restoreJobId');
    final restoreOk = await _waitForJob(client, resolved, restoreJobId);
    if (!restoreOk) {
      exitCode = 1;
      return;
    }
    activeJobId = null;

    stdout.writeln('Collecting md5 on source ${resolved.sourceHost}...');
    final sourceChains = await _collectChains(resolved, resolved.sourceHost, resolved.vmName);
    final sourceMd5 = await _collectMd5(resolved, resolved.sourceHost, sourceChains);

    stdout.writeln('Collecting md5 on target ${resolved.targetHost}...');
    final targetChains = await _collectChains(resolved, resolved.targetHost, resolved.vmName);
    final targetMd5 = await _collectMd5(resolved, resolved.targetHost, targetChains);

    final matches = _printComparison(sourceChains, targetChains, sourceMd5, targetMd5);
    if (!matches) {
      exitCode = 1;
      return;
    }
  } finally {
    cancelTimer?.cancel();
    await eventLog.stop();
    for (final sub in signalSubs) {
      await sub.cancel();
    }
    client.close();
  }
}

class _EventLogHandle {
  _EventLogHandle(this._client, this._subscriptionFuture);

  final HttpClient _client;
  final Future<StreamSubscription<String>> _subscriptionFuture;

  Future<void> stop() async {
    try {
      final sub = await _subscriptionFuture;
      await sub.cancel();
    } catch (_) {}
    _client.close(force: true);
  }
}

_EventLogHandle _startAgentEventLog(_Args args) {
  final client = _createHttpClient(args.agentUrl);
  final completer = Completer<StreamSubscription<String>>();
  () async {
    try {
      final request = await client.getUrl(args.agentUrl.resolve('/events'));
      final token = args.agentToken;
      if (token != null && token.isNotEmpty) {
        request.headers.set(HttpHeaders.authorizationHeader, 'Bearer $token');
      }
      final response = await request.close();
      if (response.statusCode != 200) {
        stderr.writeln('Agent events unavailable: HTTP ${response.statusCode}');
        if (!completer.isCompleted) {
          completer.complete(const Stream<String>.empty().listen((_) {}));
        }
        return;
      }
      String? eventName;
      final dataLines = <String>[];
      final sub = response
          .transform(utf8.decoder)
          .transform(const LineSplitter())
          .listen(
            (line) {
              if (line.isEmpty) {
                if (dataLines.isNotEmpty) {
                  final data = dataLines.join('\n');
                  _handleAgentEvent(eventName, data);
                }
                eventName = null;
                dataLines.clear();
                return;
              }
              if (line.startsWith(':')) {
                return;
              }
              if (line.startsWith('event:')) {
                eventName = line.substring('event:'.length).trim();
                return;
              }
              if (line.startsWith('data:')) {
                dataLines.add(line.substring('data:'.length).trim());
              }
            },
            onError: (Object error) {
              stderr.writeln('Agent events closed: $error');
            },
          );
      if (!completer.isCompleted) {
        completer.complete(sub);
      }
    } catch (error) {
      stderr.writeln('Agent events failed: $error');
      if (!completer.isCompleted) {
        completer.complete(const Stream<String>.empty().listen((_) {}));
      }
    }
  }();
  return _EventLogHandle(client, completer.future);
}

void _handleAgentEvent(String? eventName, String data) {
  if (eventName == 'ready') {
    return;
  }
  try {
    final decoded = jsonDecode(data);
    if (decoded is! Map) {
      return;
    }
    final type = (decoded['type'] ?? eventName ?? '').toString();
    if (type != 'agent.job_failure') {
      return;
    }
    final payload = decoded['payload'];
    if (payload is! Map) {
      stderr.writeln('Agent job failure: $decoded');
      return;
    }
    if (type == 'agent.job_failure') {
      final jobId = (payload['jobId'] ?? '').toString();
      final jobType = (payload['type'] ?? '').toString();
      final message = (payload['message'] ?? '').toString();
      stderr.writeln('Agent job failure: $jobType $jobId $message'.trim());
      return;
    }
  } catch (_) {}
}

_Args? _parseArgs(List<String> args) {
  String? vmName;
  var agentUrl = Uri.parse('https://127.0.0.1:33551');
  var sourceServerId = 'nuc04';
  var targetServerId = 'nuc02';
  var sourceHost = 'nuc04';
  var targetHost = 'nuc02';
  var sshUser = 'root';
  String? sshKeyPath;
  var pollInterval = const Duration(seconds: 30);
  var timeout = const Duration(hours: 2);
  var sourceHostExplicit = false;
  var targetHostExplicit = false;
  var skipRestore = false;
  var skipBackup = false;
  var cancelAfterRandom = false;
  var fresh = false;
  int? blockSizeMBOverride;
  final driverParams = <String, dynamic>{};
  String? destinationId;
  String? agentToken;
  String? agentTokenFile;

  for (var i = 0; i < args.length; i++) {
    final arg = args[i];
    switch (arg) {
      case '--vm':
        vmName = _readValue(args, ++i, arg);
        break;
      case '--agent-url':
        agentUrl = Uri.parse(_readValue(args, ++i, arg));
        break;
      case '--source-server':
        sourceServerId = _readValue(args, ++i, arg);
        break;
      case '--target-server':
        targetServerId = _readValue(args, ++i, arg);
        break;
      case '--source-host':
        sourceHost = _readValue(args, ++i, arg);
        sourceHostExplicit = true;
        break;
      case '--target-host':
        targetHost = _readValue(args, ++i, arg);
        targetHostExplicit = true;
        break;
      case '--ssh-user':
        sshUser = _readValue(args, ++i, arg);
        break;
      case '--ssh-key':
        sshKeyPath = _readValue(args, ++i, arg);
        break;
      case '--poll-interval':
        pollInterval = Duration(seconds: int.parse(_readValue(args, ++i, arg)));
        break;
      case '--timeout-minutes':
        timeout = Duration(minutes: int.parse(_readValue(args, ++i, arg)));
        break;
      case '--no-restore':
        skipRestore = true;
        break;
      case '--no-backup':
        skipBackup = true;
        break;
      case '--cancel':
        cancelAfterRandom = true;
        break;
      case '--fresh':
        fresh = true;
        break;
      case '--dest':
        destinationId = _readValue(args, ++i, arg);
        break;
      case '--driver-param':
        _applyDriverParam(driverParams, _readValue(args, ++i, arg));
        break;
      case '--block-size-mb':
        blockSizeMBOverride = _parseBlockSizeMBOverride(_readValue(args, ++i, arg));
        break;
      case '--token':
        agentToken = _readValue(args, ++i, arg);
        break;
      case '--token-file':
        agentTokenFile = _readValue(args, ++i, arg);
        break;
      case '--help':
      case '-h':
        return null;
      default:
        stderr.writeln('Unknown arg: $arg');
        return null;
    }
  }

  if (vmName == null || vmName.isEmpty) {
    return null;
  }

  return _Args(
    vmName: vmName,
    agentUrl: agentUrl,
    agentToken: agentToken,
    sourceServerId: sourceServerId,
    targetServerId: targetServerId,
    sourceHost: sourceHost,
    targetHost: targetHost,
    sshUser: sshUser,
    sshKeyPath: sshKeyPath,
    pollInterval: pollInterval,
    timeout: timeout,
    sourceHostExplicit: sourceHostExplicit,
    targetHostExplicit: targetHostExplicit,
    skipRestore: skipRestore,
    skipBackup: skipBackup,
    cancelAfterRandom: cancelAfterRandom,
    fresh: fresh,
    blockSizeMBOverride: blockSizeMBOverride,
    driverParams: driverParams,
    destinationId: destinationId,
    agentTokenFile: agentTokenFile,
  );
}

String _readValue(List<String> args, int index, String flag) {
  if (index >= args.length) {
    throw ArgumentError('Missing value for $flag');
  }
  return args[index];
}

void _applyDriverParam(Map<String, dynamic> params, String raw) {
  final separator = raw.indexOf('=');
  if (separator <= 0) {
    throw ArgumentError('Invalid --driver-param (expected key=value): $raw');
  }
  final key = raw.substring(0, separator).trim();
  final valueRaw = raw.substring(separator + 1).trim();
  if (key.isEmpty) {
    throw ArgumentError('Invalid --driver-param key: $raw');
  }
  params[key] = _parseDriverParamValue(valueRaw);
}

dynamic _parseDriverParamValue(String valueRaw) {
  final lower = valueRaw.toLowerCase();
  if (lower == 'true') {
    return true;
  }
  if (lower == 'false') {
    return false;
  }
  final numeric = num.tryParse(valueRaw);
  if (numeric != null) {
    return numeric;
  }
  return valueRaw;
}

int _parseBlockSizeMBOverride(String raw) {
  final parsed = int.tryParse(raw.trim());
  if (parsed == null || (parsed != 1 && parsed != 2 && parsed != 4 && parsed != 8)) {
    throw ArgumentError('Invalid --block-size-mb. Allowed values: 1, 2, 4, 8.');
  }
  return parsed;
}

HttpClient _createHttpClient(Uri baseUri) {
  final client = HttpClient();
  if (baseUri.scheme == 'https') {
    client.badCertificateCallback = (cert, host, port) => true;
  }
  return client;
}

Future<String?> _resolveAgentToken(_Args args) async {
  final direct = args.agentToken?.trim();
  if (direct != null && direct.isNotEmpty) {
    return direct;
  }
  final filePath = args.agentTokenFile?.trim();
  if (filePath != null && filePath.isNotEmpty) {
    return _readTokenFromFile(File(filePath));
  }
  try {
    final store = await AppSettingsStore.fromAgentDefaultPath();
    return await store.loadAgentToken();
  } catch (_) {
    return null;
  }
}

Future<String?> _readTokenFromFile(File file) async {
  if (!await file.exists()) {
    return null;
  }
  final token = (await file.readAsString()).trim();
  return token.isEmpty ? null : token;
}

Map<String, String> _authHeaders(_Args args) {
  final token = args.agentToken;
  if (token == null || token.isEmpty) {
    return {};
  }
  return {HttpHeaders.authorizationHeader: 'Bearer $token'};
}

Future<Map<String, dynamic>> _fetchConfig(http.Client client, _Args args) async {
  final uri = args.agentUrl.replace(path: '/config');
  final response = await client.get(uri, headers: _authHeaders(args));
  if (response.statusCode != 200) {
    throw StateError('Failed to load config: ${response.statusCode} ${response.body}');
  }
  return jsonDecode(response.body) as Map<String, dynamic>;
}

String _extractBackupPath(Map<String, dynamic> payload, _Args args) {
  final requestedDestinationId = args.destinationId?.trim() ?? '';
  if (requestedDestinationId.isNotEmpty) {
    final destination = _findDestinationById(payload, requestedDestinationId);
    if (destination == null) {
      throw StateError('Destination not found: $requestedDestinationId');
    }
    final destinationPath = _pathFromDestination(destination);
    if (destinationPath != null && destinationPath.isNotEmpty) {
      return destinationPath;
    }
    final driverId = destination['driverId']?.toString().trim();
    final name = destination['name']?.toString().trim();
    return 'destination=$requestedDestinationId${name == null || name.isEmpty ? '' : ' ($name)'}${driverId == null || driverId.isEmpty ? '' : ' driver=$driverId'}';
  }

  final selectedDestinationId = payload['backupDestinationId']?.toString().trim() ?? '';
  if (selectedDestinationId.isNotEmpty) {
    final destination = _findDestinationById(payload, selectedDestinationId);
    if (destination != null) {
      final destinationPath = _pathFromDestination(destination);
      if (destinationPath != null && destinationPath.isNotEmpty) {
        return destinationPath;
      }
      final driverId = destination['driverId']?.toString().trim();
      final name = destination['name']?.toString().trim();
      return 'destination=$selectedDestinationId${name == null || name.isEmpty ? '' : ' ($name)'}${driverId == null || driverId.isEmpty ? '' : ' driver=$driverId'}';
    }
  }

  final backup = payload['backup'];
  if (backup is Map) {
    final path = backup['base_path']?.toString().trim();
    if (path != null && path.isNotEmpty) {
      return path;
    }
  }

  throw StateError('Config missing backup path (expected backup.base_path).');
}

Map<String, dynamic>? _findDestinationById(Map<String, dynamic> payload, String destinationId) {
  final destinations = payload['destinations'];
  if (destinations is! List) {
    return null;
  }
  for (final entry in destinations) {
    if (entry is! Map) {
      continue;
    }
    final map = Map<String, dynamic>.from(entry);
    if (map['id']?.toString() == destinationId) {
      return map;
    }
  }
  return null;
}

String? _pathFromDestination(Map<String, dynamic> destination) {
  final paramsRaw = destination['params'];
  if (paramsRaw is! Map) {
    return null;
  }
  final params = Map<String, dynamic>.from(paramsRaw);
  final path = params['path']?.toString().trim();
  if (path == null || path.isEmpty) {
    return null;
  }
  return path;
}

String _resolveServerId(Map<String, dynamic> payload, String value) {
  final servers = payload['servers'];
  if (servers is! List) {
    throw StateError('Config missing servers list');
  }
  for (final entry in servers) {
    if (entry is Map<String, dynamic> && entry['id']?.toString() == value) {
      return value;
    }
  }
  for (final entry in servers) {
    if (entry is! Map<String, dynamic>) {
      continue;
    }
    final name = entry['name']?.toString();
    final host = entry['sshHost']?.toString();
    if (name == value || host == value) {
      final id = entry['id']?.toString();
      if (id != null && id.isNotEmpty) {
        return id;
      }
    }
  }
  final names = servers.whereType<Map<String, dynamic>>().map((entry) => entry['name']?.toString()).whereType<String>().where((name) => name.isNotEmpty).toList();
  throw StateError('Server not found for "$value". Known servers: ${names.join(', ')}');
}

String _resolveHost(Map<String, dynamic> payload, String serverId, String fallback, bool explicit) {
  if (explicit) {
    return fallback;
  }
  final servers = payload['servers'];
  if (servers is! List) {
    return fallback;
  }
  for (final entry in servers) {
    if (entry is Map<String, dynamic> && entry['id']?.toString() == serverId) {
      final host = entry['sshHost']?.toString();
      if (host != null && host.isNotEmpty) {
        return host;
      }
    }
  }
  return fallback;
}

Future<String> _startBackup(http.Client client, _Args args) async {
  final uri = args.agentUrl.replace(path: '/servers/${args.sourceServerId}/backup');
  final payload = <String, dynamic>{'vmName': args.vmName};
  if (args.fresh) {
    payload['fresh'] = true;
  }
  final destinationId = args.destinationId;
  if (destinationId != null && destinationId.trim().isNotEmpty) {
    payload['destinationId'] = destinationId.trim();
  }
  if (args.driverParams.isNotEmpty) {
    payload['driverParams'] = args.driverParams;
  }
  if (args.blockSizeMBOverride != null) {
    payload['blockSizeMB'] = args.blockSizeMBOverride;
  }
  final response = await client.post(uri, headers: {..._authHeaders(args), 'content-type': 'application/json'}, body: jsonEncode(payload));
  if (response.statusCode != 200) {
    throw StateError('Start backup failed: ${response.statusCode} ${response.body}');
  }
  final responsePayload = jsonDecode(response.body) as Map<String, dynamic>;
  final jobId = responsePayload['jobId']?.toString();
  if (jobId == null || jobId.isEmpty) {
    throw StateError('Backup response missing jobId: ${response.body}');
  }
  return jobId;
}

Future<_JobStatus> _fetchJob(http.Client client, _Args args, String jobId) async {
  final uri = args.agentUrl.replace(path: '/jobs/$jobId');
  final response = await client.get(uri, headers: _authHeaders(args));
  if (response.statusCode != 200) {
    throw StateError('Job status failed: ${response.statusCode} ${response.body}');
  }
  final payload = jsonDecode(response.body) as Map<String, dynamic>;
  return _JobStatus(
    state: payload['state']?.toString() ?? 'unknown',
    message: payload['message']?.toString() ?? '',
    type: payload['type']?.toString() ?? 'unknown',
    totalBytes: (payload['totalBytes'] as num?)?.toInt() ?? 0,
    bytesTransferred: (payload['bytesTransferred'] as num?)?.toInt() ?? 0,
    speedBytesPerSec: (payload['speedBytesPerSec'] as num?)?.toDouble() ?? 0,
    averageSpeedBytesPerSec: (payload['averageSpeedBytesPerSec'] as num?)?.toDouble() ?? 0,
    physicalBytesTransferred: (payload['physicalBytesTransferred'] as num?)?.toInt() ?? 0,
    physicalSpeedBytesPerSec: (payload['physicalSpeedBytesPerSec'] as num?)?.toDouble() ?? 0,
    averagePhysicalSpeedBytesPerSec: (payload['averagePhysicalSpeedBytesPerSec'] as num?)?.toDouble() ?? 0,
    etaSeconds: (payload['etaSeconds'] as num?)?.toInt(),
    physicalRemainingBytes: (payload['physicalRemainingBytes'] as num?)?.toInt() ?? 0,
    physicalTotalBytes: (payload['physicalTotalBytes'] as num?)?.toInt() ?? 0,
    physicalProgressPercent: (payload['physicalProgressPercent'] as num?)?.toDouble() ?? 0,
  );
}

Future<bool> _waitForJob(http.Client client, _Args args, String jobId, {bool Function()? isCanceled}) async {
  final deadline = DateTime.now().add(args.timeout);
  String? lastState;
  while (true) {
    if (DateTime.now().isAfter(deadline)) {
      stderr.writeln('Timed out waiting for job $jobId');
      return false;
    }
    final status = await _fetchJob(client, args, jobId);
    if (status.state != lastState) {
      stdout.writeln('${DateTime.now().toIso8601String()} job=$jobId state=${status.state}');
      lastState = status.state;
    }
    final total = status.totalBytes;
    final logical = status.bytesTransferred;
    final isRestore = status.type == 'restore';
    final percent = total > 0 ? (logical / total * 100).clamp(0, 100) : null;
    final progress = percent == null ? _formatBytes(logical) : '${percent.toStringAsFixed(1)}% (${_formatBytes(logical)} / ${_formatBytes(total)})';
    final eta = _formatEta(status.etaSeconds);
    final flush = status.physicalTotalBytes > 0
        ? '${status.physicalProgressPercent.toStringAsFixed(1)}% (${_formatBytes(status.physicalBytesTransferred)} / ${_formatBytes(status.physicalTotalBytes)})'
        : _formatBytes(status.physicalBytesTransferred);
    if (isRestore) {
      stdout.writeln('${DateTime.now().toIso8601String()} job=$jobId progress=$progress uploaded=${_formatSpeed(status.speedBytesPerSec)}');
    } else {
      stdout.writeln(
        '${DateTime.now().toIso8601String()} job=$jobId progress=$progress avg=${_formatSpeed(status.averageSpeedBytesPerSec)} eta=$eta | flush $flush speed=${_formatSpeed(status.physicalSpeedBytesPerSec)} remaining=${_formatBytes(status.physicalRemainingBytes)}',
      );
    }
    if (status.message.isNotEmpty) {
      stdout.writeln('${DateTime.now().toIso8601String()} job=$jobId message=${status.message}');
    }
    if (status.state == 'success') {
      return true;
    }
    if (status.state == 'canceled') {
      return isCanceled?.call() == true;
    }
    if (status.state == 'failure') {
      if (isCanceled?.call() == true) {
        return true;
      }
      return false;
    }
    await Future<void>.delayed(args.pollInterval);
  }
}

String _formatBytes(int bytes) {
  if (bytes <= 0) {
    return '0 B';
  }
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  var value = bytes.toDouble();
  var unitIndex = 0;
  while (value >= 1024 && unitIndex < units.length - 1) {
    value /= 1024;
    unitIndex++;
  }
  return '${value.toStringAsFixed(value >= 10 ? 1 : 2)} ${units[unitIndex]}';
}

String _formatSpeed(double bytesPerSec) {
  if (bytesPerSec <= 0) {
    return '0 B/s';
  }
  return '${_formatBytes(bytesPerSec.round())}/s';
}

String _formatEta(int? seconds) {
  if (seconds == null || seconds <= 0) {
    return 'n/a';
  }
  final minutes = seconds ~/ 60;
  final secs = seconds % 60;
  if (minutes <= 0) {
    return '${secs}s';
  }
  final hours = minutes ~/ 60;
  final mins = minutes % 60;
  if (hours <= 0) {
    return '${minutes}m ${secs}s';
  }
  return '${hours}h ${mins}m';
}

Future<String> _pickLatestRestoreXml(http.Client client, _Args args) async {
  final destinationId = args.destinationId;
  final query = (destinationId != null && destinationId.trim().isNotEmpty) ? {'destinationId': destinationId.trim()} : null;
  final uri = args.agentUrl.replace(path: '/restore/entries', queryParameters: query);
  final response = await client.get(uri, headers: _authHeaders(args));
  if (response.statusCode != 200) {
    throw StateError('Restore entries failed: ${response.statusCode} ${response.body}');
  }
  final entries = jsonDecode(response.body) as List<dynamic>;
  Map<String, dynamic>? best;
  Map<String, dynamic>? bestIncomplete;
  for (final entry in entries) {
    if (entry is! Map<String, dynamic>) {
      continue;
    }
    if (entry['vmName']?.toString() != args.vmName) {
      continue;
    }
    final missing = entry['missingDiskBasenames'];
    final hasMissing = missing is List && missing.isNotEmpty;
    if (hasMissing) {
      if (bestIncomplete == null) {
        bestIncomplete = entry;
        continue;
      }
      final ts = entry['timestamp']?.toString() ?? '';
      final bestTs = bestIncomplete['timestamp']?.toString() ?? '';
      if (ts.compareTo(bestTs) > 0) {
        bestIncomplete = entry;
      }
      continue;
    }
    if (best == null) {
      best = entry;
      continue;
    }
    final ts = entry['timestamp']?.toString() ?? '';
    final bestTs = best['timestamp']?.toString() ?? '';
    if (ts.compareTo(bestTs) > 0) {
      best = entry;
    }
  }
  if (best == null) {
    if (bestIncomplete != null) {
      final timestamp = bestIncomplete['timestamp']?.toString() ?? 'unknown';
      final missing = bestIncomplete['missingDiskBasenames'];
      throw StateError('Latest restore entry for vm ${args.vmName} on selected destination is incomplete (timestamp=$timestamp missingDisks=$missing).');
    }
    throw StateError('No restore entry found for vm ${args.vmName}');
  }
  final xmlPath = best['xmlPath']?.toString();
  if (xmlPath == null || xmlPath.isEmpty) {
    throw StateError('Restore entry missing xmlPath');
  }
  return xmlPath;
}

Future<void> _restorePrecheck(http.Client client, _Args args, String xmlPath) async {
  final uri = args.agentUrl.replace(path: '/servers/${args.targetServerId}/restore/precheck');
  final payload = <String, dynamic>{'xmlPath': xmlPath};
  final destinationId = args.destinationId;
  if (destinationId != null && destinationId.trim().isNotEmpty) {
    payload['destinationId'] = destinationId.trim();
  }
  final response = await client.post(uri, headers: {..._authHeaders(args), 'content-type': 'application/json'}, body: jsonEncode(payload));
  if (response.statusCode != 200) {
    throw StateError('Restore precheck failed: ${response.statusCode} ${response.body}');
  }
}

Future<String> _startRestore(http.Client client, _Args args, String xmlPath) async {
  final uri = args.agentUrl.replace(path: '/servers/${args.targetServerId}/restore/start');
  final payload = {'xmlPath': xmlPath, 'decision': 'overwrite'};
  final destinationId = args.destinationId;
  if (destinationId != null && destinationId.trim().isNotEmpty) {
    payload['destinationId'] = destinationId.trim();
  }
  final response = await client.post(uri, headers: {..._authHeaders(args), 'content-type': 'application/json'}, body: jsonEncode(payload));
  if (response.statusCode != 200) {
    throw StateError('Start restore failed: ${response.statusCode} ${response.body}');
  }
  final responsePayload = jsonDecode(response.body) as Map<String, dynamic>;
  final jobId = responsePayload['jobId']?.toString();
  if (jobId == null || jobId.isEmpty) {
    throw StateError('Restore response missing jobId: ${response.body}');
  }
  return jobId;
}

Future<void> _cancelJob(http.Client client, _Args args, String jobId) async {
  final uri = args.agentUrl.replace(path: '/jobs/$jobId/cancel');
  final response = await client.post(uri, headers: {..._authHeaders(args), 'content-type': 'application/json'}, body: jsonEncode({}));
  if (response.statusCode != 200) {
    stderr.writeln('Cancel job failed: ${response.statusCode} ${response.body}');
  } else {
    stderr.writeln('Cancel requested for job $jobId');
  }
}

Future<List<List<String>>> _collectChains(_Args args, String host, String vmName) async {
  final xml = await _runSsh(args, host, ['virsh', 'dumpxml', vmName]);
  final diskPaths = _extractDiskPaths(xml);
  if (diskPaths.isEmpty) {
    throw StateError('No disk paths found for $vmName on $host');
  }
  final chains = <List<String>>[];
  for (final diskPath in diskPaths) {
    final output = await _runSsh(args, host, ['qemu-img', 'info', '--backing-chain', '--force-share', diskPath]);
    final chain = _parseChain(output);
    if (chain.isEmpty) {
      chains.add([diskPath]);
    } else {
      chains.add(chain);
    }
  }
  return chains;
}

List<String> _extractDiskPaths(String xml) {
  final matches = RegExp("<source[^>]+file=['\\\"]([^'\\\"]+)['\\\"]").allMatches(xml);
  final paths = <String>[];
  for (final match in matches) {
    final value = match.group(1);
    if (value != null && value.isNotEmpty) {
      paths.add(value);
    }
  }
  return paths.toSet().toList();
}

List<String> _parseChain(String output) {
  final chain = <String>[];
  final lines = const LineSplitter().convert(output);
  for (final line in lines) {
    final match = RegExp(r'^image: (.+)$').firstMatch(line.trim());
    if (match != null) {
      chain.add(match.group(1)!.trim());
    }
  }
  return chain;
}

Future<Map<String, String>> _collectMd5(_Args args, String host, List<List<String>> chains) async {
  final md5 = <String, String>{};
  for (final chain in chains) {
    for (final path in chain) {
      if (md5.containsKey(path)) {
        continue;
      }
      final output = await _runSsh(args, host, ['md5sum', path]);
      final parts = output.trim().split(RegExp(r'\s+'));
      if (parts.isNotEmpty && parts.first.isNotEmpty) {
        md5[path] = parts.first;
      }
    }
  }
  return md5;
}

bool _printComparison(List<List<String>> sourceChains, List<List<String>> targetChains, Map<String, String> sourceMd5, Map<String, String> targetMd5) {
  stdout.writeln('MD5 comparison:');
  var allMatch = true;
  final diskCount = sourceChains.length > targetChains.length ? sourceChains.length : targetChains.length;
  for (var diskIndex = 0; diskIndex < diskCount; diskIndex++) {
    final srcChain = diskIndex < sourceChains.length ? sourceChains[diskIndex] : const <String>[];
    final tgtChain = diskIndex < targetChains.length ? targetChains[diskIndex] : const <String>[];
    stdout.writeln('Disk ${diskIndex + 1}:');
    final maxLen = srcChain.length > tgtChain.length ? srcChain.length : tgtChain.length;
    for (var i = 0; i < maxLen; i++) {
      final srcPath = i < srcChain.length ? srcChain[i] : '';
      final tgtPath = i < tgtChain.length ? tgtChain[i] : '';
      final srcHash = srcPath.isEmpty ? '' : (sourceMd5[srcPath] ?? '');
      final tgtHash = tgtPath.isEmpty ? '' : (targetMd5[tgtPath] ?? '');
      final status = (srcHash.isNotEmpty && srcHash == tgtHash) ? 'OK' : 'MISMATCH';
      if (status != 'OK') {
        allMatch = false;
      }
      stdout.writeln('  [$i] $status');
      stdout.writeln('    src: $srcPath $srcHash');
      stdout.writeln('    tgt: $tgtPath $tgtHash');
    }
  }
  return allMatch;
}

Future<String> _runSsh(_Args args, String host, List<String> command) async {
  final sshArgs = <String>[];
  if (args.sshKeyPath != null && args.sshKeyPath!.isNotEmpty) {
    sshArgs.addAll(['-i', args.sshKeyPath!]);
  }
  sshArgs.add('${args.sshUser}@$host');
  sshArgs.addAll(command);
  final result = await Process.run('ssh', sshArgs);
  if (result.exitCode != 0) {
    throw StateError('SSH failed on $host: ${result.exitCode}\n${result.stderr}');
  }
  return result.stdout.toString();
}

void _printUsage() {
  stdout.writeln('Usage: dart run tools/backup_verify.dart --vm <name> [options]');
  stdout.writeln('Options:');
  stdout.writeln('  --agent-url <url>         Default https://127.0.0.1:33551');
  stdout.writeln('  --source-server <id>      Default nuc04');
  stdout.writeln('  --target-server <id>      Default nuc02');
  stdout.writeln('  --source-host <host>      Default nuc04');
  stdout.writeln('  --target-host <host>      Default nuc02');
  stdout.writeln('  --ssh-user <user>         Default root');
  stdout.writeln('  --ssh-key <path>          Optional private key');
  stdout.writeln('  --dest <id>               Use destination id for backup + restore');
  stdout.writeln('  --driver-param <k=v>      Driver parameter (repeatable)');
  stdout.writeln('  --block-size-mb <1|2|4|8> Override backup dedup block size for this run only');
  stdout.writeln('  --token <value>           Agent bearer token (overrides token file)');
  stdout.writeln('  --token-file <path>       Agent token file path');
  stdout.writeln('  --no-restore              Skip restore + md5 compare');
  stdout.writeln('  --no-backup               Skip backup and restore latest backup');
  stdout.writeln('  --cancel                  Auto-cancel backup after 60-80s');
  stdout.writeln('  --fresh                   Request agent cleanup before backup (debug builds only)');
  stdout.writeln('  --poll-interval <sec>     Default 30');
  stdout.writeln('  --timeout-minutes <min>   Default 120');
}
