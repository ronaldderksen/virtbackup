import 'dart:async';
import 'dart:io';

class DebugLogWriter {
  static final Map<String, Future<void>> _tails = <String, Future<void>>{};

  static Future<void> truncate(String path) async {
    final normalized = path.trim();
    if (normalized.isEmpty) {
      return;
    }
    final chained = (_tails[normalized] ?? Future<void>.value()).catchError((_) {}).then((_) => _truncateLocked(normalized));
    _tails[normalized] = chained;
    await chained;
  }

  static Future<void> appendLine(String path, String line) async {
    final normalized = path.trim();
    if (normalized.isEmpty) {
      return;
    }
    final chained = (_tails[normalized] ?? Future<void>.value()).catchError((_) {}).then((_) => _appendLocked(normalized, line));
    _tails[normalized] = chained;
    await chained;
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
}
