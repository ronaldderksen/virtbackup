import 'dart:async';
import 'dart:collection';
import 'dart:io';

class LogWriter {
  static const String _agentSource = 'agent';
  static const String _guiSource = 'gui';
  static const String _consoleLevel = 'console';
  static const String _defaultLevel = _consoleLevel;
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

  static Future<void> configureSourcePath({required String source, required String path}) async {
    final normalizedSource = _normalizeSource(source);
    final normalizedPath = path.trim();
    if (normalizedSource.isEmpty || normalizedPath.isEmpty) {
      return;
    }
    _pathsBySource[normalizedSource] = normalizedPath;
    await Directory(File(normalizedPath).parent.path).create(recursive: true);
  }

  static void configureSourceLevel({required String source, required String level}) {
    final normalizedSource = _normalizeSource(source);
    final normalizedLevel = _normalizeLevel(level);
    if (normalizedSource.isEmpty || normalizedLevel.isEmpty) {
      return;
    }
    _levelsBySource[normalizedSource] = normalizedLevel;
  }

  static Future<void> truncateSource(String source) async {
    final path = _resolvePath(source);
    final completer = Completer<void>();
    _queue.add(_LogOp(path: path, line: '', truncate: true, rotate: false, completer: completer));
    _ensureDrain();
    return completer.future;
  }

  static Future<void> rotateSource(String source) async {
    final path = _resolvePath(source);
    final completer = Completer<void>();
    _queue.add(_LogOp(path: path, line: '', truncate: false, rotate: true, completer: completer));
    _ensureDrain();
    return completer.future;
  }

  static Future<void> log({required String source, required String level, required String message}) async {
    final trimmedLevel = level.trim();
    final trimmedMessage = message.trimRight();
    if (trimmedLevel.isEmpty || trimmedMessage.isEmpty) {
      return;
    }
    final normalizedLevel = _normalizeLevel(trimmedLevel);
    if (!_shouldLog(source: source, messageLevel: normalizedLevel)) {
      return;
    }
    if (normalizedLevel == _consoleLevel) {
      stdout.writeln(trimmedMessage);
    }
    final timestamp = _formatTimestamp(DateTime.now());
    final line = '$timestamp level=$normalizedLevel message=${_sanitize(trimmedMessage)}';
    final completer = Completer<void>();
    _queue.add(_LogOp(path: _resolvePath(source), line: line, truncate: false, rotate: false, completer: completer));
    _ensureDrain();
    return completer.future;
  }

  static String _resolvePath(String source) {
    final normalizedSource = _normalizeSource(source);
    return _pathsBySource[normalizedSource] ?? defaultPathForSource(normalizedSource);
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
      'console' => 0,
      'info' => 1,
      'warn' => 1,
      'warning' => 1,
      'error' => 1,
      'debug' => 2,
      _ => null,
    };
  }

  static String _normalizeSource(String source) {
    return source.trim().toLowerCase();
  }

  static String _normalizeLevel(String level) {
    return level.trim().toLowerCase();
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
          await _truncateLocked(op.path);
        } else if (op.rotate) {
          await _rotateLocked(op.path);
        } else {
          await _appendLocked(op.path, op.line);
        }
        op.completer.complete();
      } catch (error, stackTrace) {
        op.completer.completeError(error, stackTrace);
      }
    }
    _draining = false;
  }

  static Future<void> _truncateLocked(String path) async {
    final file = File(path);
    await file.parent.create(recursive: true);
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

  static Future<void> _appendLocked(String path, String line) async {
    final file = File(path);
    await file.parent.create(recursive: true);
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

  static Future<void> _rotateLocked(String path) async {
    final file = File(path);
    await file.parent.create(recursive: true);
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
  _LogOp({required this.path, required this.line, required this.truncate, required this.rotate, required this.completer});

  final String path;
  final String line;
  final bool truncate;
  final bool rotate;
  final Completer<void> completer;
}
