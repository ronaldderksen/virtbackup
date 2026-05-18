import 'dart:async';
import 'dart:collection';
import 'dart:convert';
import 'dart:io';

class LogWriter {
  static const String _agentSource = 'agent';
  static const String _guiSource = 'gui';
  static const String _infoLevel = 'info';
  static const String _defaultLevel = _infoLevel;
  static const Object _jobIdZoneKey = Object();
  static final Queue<_LogOp> _queue = Queue<_LogOp>();
  static final Map<String, String> _pathsBySource = <String, String>{};
  static final Map<String, String> _levelsBySource = <String, String>{};
  static bool _draining = false;

  static String defaultPathForSource(String source, {String? basePath}) {
    final normalizedSource = _normalizeSource(source);
    final normalizedBase = (basePath ?? '').trim();
    final rootPath = normalizedBase.isEmpty ? '${Platform.pathSeparator}var' : normalizedBase;
    final fileName = switch (normalizedSource) {
      _agentSource => 'agent.log',
      _guiSource => 'gui.log',
      _ => '${_sanitizeFileName(normalizedSource)}.log',
    };
    return '$rootPath${Platform.pathSeparator}VirtBackup${Platform.pathSeparator}logs${Platform.pathSeparator}$fileName';
  }

  static R withJobLogging<R>(String jobId, R Function() body) {
    final normalizedJobId = jobId.trim();
    if (normalizedJobId.isEmpty) {
      return body();
    }
    return runZoned(body, zoneValues: {_jobIdZoneKey: normalizedJobId});
  }

  static Future<void> configureSourcePath({required String source, required String path}) async {
    final normalizedSource = _normalizeSource(source);
    final normalizedPath = path.trim();
    if (normalizedSource.isEmpty || normalizedPath.isEmpty) {
      return;
    }
    _pathsBySource[normalizedSource] = normalizedPath;
  }

  static void configureSourceLevel({required String source, required String level}) {
    final normalizedSource = _normalizeSource(source);
    final normalizedLevel = _normalizeLevel(level, source: normalizedSource);
    if (normalizedSource.isEmpty || normalizedLevel.isEmpty) {
      return;
    }
    _levelsBySource[normalizedSource] = normalizedLevel;
  }

  static Future<void> truncateSource(String source) async {
    final path = _resolvePath(source);
    final allowParentCreate = _allowParentCreate(source);
    final completer = Completer<void>();
    _queue.add(_LogOp(path: path, line: '', truncate: true, rotate: false, allowParentCreate: allowParentCreate, completer: completer));
    _ensureDrain();
    return completer.future;
  }

  static Future<void> rotateSource(String source) async {
    final path = _resolvePath(source);
    final allowParentCreate = _allowParentCreate(source);
    final completer = Completer<void>();
    _queue.add(_LogOp(path: path, line: '', truncate: false, rotate: true, allowParentCreate: allowParentCreate, completer: completer));
    _ensureDrain();
    return completer.future;
  }

  static Future<void> log({required String source, required String level, required String message, String? jobId}) async {
    final trimmedLevel = level.trim();
    final trimmedMessage = message.trimRight();
    if (trimmedLevel.isEmpty || trimmedMessage.isEmpty) {
      return;
    }
    final normalizedLevel = _normalizeLevel(trimmedLevel, source: source);
    if (!_shouldLog(source: source, messageLevel: normalizedLevel)) {
      return;
    }
    final timestamp = _formatTimestamp(DateTime.now());
    _writeConsole(timestamp: timestamp, level: normalizedLevel, message: trimmedMessage);
    final line = '$timestamp level=$normalizedLevel message=${_sanitize(trimmedMessage)}';
    final allowParentCreate = _allowParentCreate(source);
    final completers = <Future<void>>[];
    void enqueue(String path) {
      final completer = Completer<void>();
      completers.add(completer.future);
      _queue.add(_LogOp(path: path, line: line, truncate: false, rotate: false, allowParentCreate: allowParentCreate, completer: completer));
    }

    enqueue(_resolvePath(source));
    final jobPath = _resolveJobPath(source: source, jobId: jobId);
    if (jobPath != null) {
      enqueue(jobPath);
    }
    _ensureDrain();
    await Future.wait(completers);
  }

  static Future<void> logAgent({required String level, required String message, String? jobId}) {
    return log(source: _agentSource, level: level, message: message, jobId: jobId);
  }

  static Future<void> logGui({required String level, required String message}) {
    return log(source: _guiSource, level: level, message: message);
  }

  static void logSync({required String source, required String level, required String message, String? jobId}) {
    final trimmedLevel = level.trim();
    final trimmedMessage = message.trimRight();
    if (trimmedLevel.isEmpty || trimmedMessage.isEmpty) {
      return;
    }
    final normalizedLevel = _normalizeLevel(trimmedLevel, source: source);
    if (!_shouldLog(source: source, messageLevel: normalizedLevel)) {
      return;
    }
    final timestamp = _formatTimestamp(DateTime.now());
    _writeConsole(timestamp: timestamp, level: normalizedLevel, message: trimmedMessage);
    final line = '$timestamp level=$normalizedLevel message=${_sanitize(trimmedMessage)}';
    final allowParentCreate = _allowParentCreate(source);
    final path = _resolvePath(source);
    try {
      _appendSync(path, line, allowParentCreate: allowParentCreate);
    } catch (error) {
      _writeIoWarning(action: 'write log', path: path, error: error);
    }
    final jobPath = _resolveJobPath(source: source, jobId: jobId);
    if (jobPath == null) {
      return;
    }
    try {
      _appendSync(jobPath, line, allowParentCreate: allowParentCreate);
    } catch (error) {
      _writeIoWarning(action: 'write job log', path: jobPath, error: error);
    }
  }

  static void logAgentSync({required String level, required String message, String? jobId}) {
    logSync(source: _agentSource, level: level, message: message, jobId: jobId);
  }

  static void logAgentJsonSync({required String level, required Map<String, Object?> fields, String? jobId}) {
    logJsonSync(source: _agentSource, level: level, fields: fields, jobId: jobId);
  }

  static void logGuiSync({required String level, required String message}) {
    logSync(source: _guiSource, level: level, message: message);
  }

  static void logJsonSync({required String source, required String level, required Map<String, Object?> fields, String? jobId}) {
    final trimmedLevel = level.trim();
    if (trimmedLevel.isEmpty || fields.isEmpty) {
      return;
    }
    final normalizedLevel = _normalizeLevel(trimmedLevel, source: source);
    if (!_shouldLog(source: source, messageLevel: normalizedLevel)) {
      return;
    }
    final timestamp = _formatTimestamp(DateTime.now());
    final message = jsonEncode(fields);
    final line = '$timestamp level=$normalizedLevel message=${_sanitize(message)}';
    _writeConsole(timestamp: timestamp, level: normalizedLevel, message: message);
    final allowParentCreate = _allowParentCreate(source);
    final path = _resolvePath(source);
    try {
      _appendSync(path, line, allowParentCreate: allowParentCreate);
    } catch (error) {
      _writeIoWarning(action: 'write log', path: path, error: error);
    }
    final jobPath = _resolveJobPath(source: source, jobId: jobId);
    if (jobPath == null) {
      return;
    }
    try {
      _appendSync(jobPath, line, allowParentCreate: allowParentCreate);
    } catch (error) {
      _writeIoWarning(action: 'write job log', path: jobPath, error: error);
    }
  }

  static String _resolvePath(String source) {
    final normalizedSource = _normalizeSource(source);
    return _pathsBySource[normalizedSource] ?? defaultPathForSource(normalizedSource);
  }

  static String? _resolveJobPath({required String source, String? jobId}) {
    if (_normalizeSource(source) != _agentSource) {
      return null;
    }
    final normalizedJobId = (jobId ?? Zone.current[_jobIdZoneKey]?.toString() ?? '').trim();
    if (normalizedJobId.isEmpty) {
      return null;
    }
    final sourcePath = _resolvePath(source);
    final separatorIndex = sourcePath.lastIndexOf(Platform.pathSeparator);
    final directory = separatorIndex < 0 ? '.' : sourcePath.substring(0, separatorIndex);
    return '$directory${Platform.pathSeparator}agent-job-${_sanitizeFileName(normalizedJobId)}.log';
  }

  static bool _shouldLog({required String source, required String messageLevel}) {
    final normalizedSource = _normalizeSource(source);
    final configured = _levelsBySource[normalizedSource] ?? _defaultLevel;
    final configuredRank = _levelRank(configured);
    final messageRank = _levelRank(messageLevel);
    if (configuredRank == null || messageRank == null) {
      return true;
    }
    return messageRank <= configuredRank;
  }

  static int? _levelRank(String level) {
    final normalized = _normalizeLevel(level);
    return switch (normalized) {
      'fatal' => 0,
      'error' => 1,
      'warn' => 2,
      'info' => 3,
      'debug' => 4,
      'trace' => 5,
      _ => null,
    };
  }

  static String _normalizeSource(String source) {
    return source.trim().toLowerCase();
  }

  static bool _allowParentCreate(String source) {
    return _normalizeSource(source) != _guiSource;
  }

  static void _writeConsole({required String timestamp, required String level, required String message}) {
    switch (level) {
      case 'fatal':
      case 'error':
      case 'warn':
        stderr.writeln('$timestamp $message');
        return;
      case _infoLevel:
        stdout.writeln('$timestamp $message');
        return;
      default:
        return;
    }
  }

  static String _normalizeLevel(String level, {String? source}) {
    final normalized = level.trim().toLowerCase();
    if (normalized == 'console') {
      _failObsoleteConsoleLevel(level, source: source);
    }
    return switch (normalized) {
      'fatal' => 'fatal',
      'error' => 'error',
      'warn' => 'warn',
      'info' => 'info',
      'debug' => 'debug',
      'trace' => 'trace',
      _ => _failInvalidLogLevel(level, source: source),
    };
  }

  static Never _failObsoleteConsoleLevel(String level, {String? source}) {
    final resolvedSource = _normalizeSource(source ?? _agentSource);
    final timestamp = _formatTimestamp(DateTime.now());
    final errorLine = '$timestamp level=fatal message=${_sanitize('LogWriter fatal: obsolete log level "$level" is not allowed. Use "info" instead.')}';
    final stackLine = '$timestamp level=fatal message=${_sanitize(StackTrace.current.toString())}';
    try {
      final path = _resolvePath(resolvedSource);
      final allowParentCreate = _allowParentCreate(resolvedSource);
      _appendSync(path, errorLine, allowParentCreate: allowParentCreate);
      _appendSync(path, stackLine, allowParentCreate: allowParentCreate);
    } catch (_) {}
    stderr.writeln('LogWriter fatal: obsolete log level "$level" is not allowed. Use "info" instead.');
    stderr.writeln(StackTrace.current.toString());
    exit(1);
  }

  static Never _failInvalidLogLevel(String level, {String? source}) {
    final resolvedSource = _normalizeSource(source ?? _agentSource);
    final timestamp = _formatTimestamp(DateTime.now());
    final errorLine = '$timestamp level=fatal message=${_sanitize('LogWriter fatal: invalid log level "$level". Allowed: fatal,error,warn,info,debug,trace.')}';
    final stackLine = '$timestamp level=fatal message=${_sanitize(StackTrace.current.toString())}';
    try {
      final path = _resolvePath(resolvedSource);
      final allowParentCreate = _allowParentCreate(resolvedSource);
      _appendSync(path, errorLine, allowParentCreate: allowParentCreate);
      _appendSync(path, stackLine, allowParentCreate: allowParentCreate);
    } catch (_) {}
    stderr.writeln('LogWriter fatal: invalid log level "$level". Allowed: fatal,error,warn,info,debug,trace.');
    stderr.writeln(StackTrace.current.toString());
    exit(1);
  }

  static void _appendSync(String path, String line, {required bool allowParentCreate}) {
    final file = File(path);
    if (allowParentCreate) {
      file.parent.createSync(recursive: true);
    } else if (!file.parent.existsSync()) {
      return;
    }
    final raf = file.openSync(mode: FileMode.append);
    try {
      raf.lockSync(FileLock.exclusive);
      final length = raf.lengthSync();
      raf.setPositionSync(length);
      raf.writeStringSync('$line\n');
      raf.flushSync();
    } finally {
      try {
        raf.unlockSync();
      } catch (_) {}
      raf.closeSync();
    }
  }

  static String _formatTimestamp(DateTime value) {
    final year = value.year.toString().padLeft(4, '0');
    final month = value.month.toString().padLeft(2, '0');
    final day = value.day.toString().padLeft(2, '0');
    final hour = value.hour.toString().padLeft(2, '0');
    final minute = value.minute.toString().padLeft(2, '0');
    final second = value.second.toString().padLeft(2, '0');
    final millis = value.millisecond.toString().padLeft(3, '0');
    return '$year-$month-$day'
        'T$hour:$minute:$second.$millis';
  }

  static String _sanitize(String value) {
    return value.replaceAll('\n', r'\n').replaceAll('\r', r'\r');
  }

  static String _sanitizeFileName(String value) {
    final trimmed = value.trim();
    if (trimmed.isEmpty) {
      return 'unknown';
    }
    return trimmed.replaceAll(RegExp(r'[^a-zA-Z0-9._-]'), '_');
  }

  static void _ensureDrain() {
    if (_draining) {
      return;
    }
    _draining = true;
    unawaited(_drainQueue());
  }

  static Future<void> _drainQueue() async {
    while (_queue.isNotEmpty) {
      final op = _queue.removeFirst();
      try {
        if (op.truncate) {
          await _truncateLocked(op.path, allowParentCreate: op.allowParentCreate);
        } else if (op.rotate) {
          await _rotateLocked(op.path, allowParentCreate: op.allowParentCreate);
        } else {
          await _appendLocked(op.path, op.line, allowParentCreate: op.allowParentCreate);
        }
        op.completer.complete();
      } catch (error) {
        _writeIoWarning(action: op.action, path: op.path, error: error);
        op.completer.complete();
      }
    }
    _draining = false;
  }

  static void _writeIoWarning({required String action, required String path, required Object error}) {
    final timestamp = _formatTimestamp(DateTime.now());
    stderr.writeln('$timestamp LogWriter warning: cannot $action at "$path": $error');
  }

  static Future<void> _truncateLocked(String path, {required bool allowParentCreate}) async {
    final file = File(path);
    if (allowParentCreate) {
      await file.parent.create(recursive: true);
    } else if (!await file.parent.exists()) {
      return;
    }
    final raf = await file.open(mode: FileMode.write);
    try {
      await raf.lock(FileLock.exclusive);
      await raf.truncate(0);
      await raf.flush();
    } finally {
      try {
        await raf.unlock();
      } catch (_) {}
      await raf.close();
    }
  }

  static Future<void> _appendLocked(String path, String line, {required bool allowParentCreate}) async {
    final file = File(path);
    if (allowParentCreate) {
      await file.parent.create(recursive: true);
    } else if (!await file.parent.exists()) {
      return;
    }
    final raf = await file.open(mode: FileMode.append);
    try {
      await raf.lock(FileLock.exclusive);
      final length = await raf.length();
      await raf.setPosition(length);
      await raf.writeString('$line\n');
      await raf.flush();
    } finally {
      try {
        await raf.unlock();
      } catch (_) {}
      await raf.close();
    }
  }

  static Future<void> _rotateLocked(String path, {required bool allowParentCreate}) async {
    final file = File(path);
    if (allowParentCreate) {
      await file.parent.create(recursive: true);
    } else if (!await file.parent.exists()) {
      return;
    }
    final rotated = File('$path.1');
    if (await rotated.exists()) {
      await rotated.delete();
    }
    if (await file.exists()) {
      await file.rename(rotated.path);
    }
    await file.writeAsString('');
  }
}

class _LogOp {
  _LogOp({required this.path, required this.line, required this.truncate, required this.rotate, required this.allowParentCreate, required this.completer});

  final String path;
  final String line;
  final bool truncate;
  final bool rotate;
  final bool allowParentCreate;
  final Completer<void> completer;

  String get action {
    if (truncate) {
      return 'truncate log';
    }
    if (rotate) {
      return 'rotate log';
    }
    return 'write log';
  }
}
