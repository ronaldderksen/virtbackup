import 'dart:convert';
import 'dart:ffi';
import 'dart:io';
import 'dart:math';

import 'package:ffi/ffi.dart';

class AgentInstanceLockException implements Exception {
  const AgentInstanceLockException(this.message);

  final String message;

  @override
  String toString() => message;
}

class AgentInstanceLock {
  AgentInstanceLock._(this.file, this._owner, this._onMessage);

  final File file;
  final _LockOwner _owner;
  final void Function(String message) _onMessage;
  bool _released = false;

  static AgentInstanceLock acquire({required File settingsFile, required void Function(String message) onMessage}) {
    final directory = Directory(settingsFile.absolute.parent.resolveSymbolicLinksSync());
    final file = File('${directory.path}${Platform.pathSeparator}agent.lock');
    final random = Random.secure();
    final owner = _LockOwner(hostname: Platform.localHostname, processId: pid, token: base64UrlEncode(List.generate(24, (_) => random.nextInt(256))));
    if (owner.hostname.isEmpty) throw const AgentInstanceLockException('Agent not started: the local hostname is empty.');
    return _withGuard(file, () {
      if (_exists(file)) {
        final previous = _readOwner(file);
        if (previous.hostname != owner.hostname) {
          throw AgentInstanceLockException(
            'Agent not started: lockfile ${file.path} belongs to host "${previous.hostname}" (PID ${previous.processId}). Remote locks are never removed automatically.',
          );
        }
        if (_LockSystem.processExists(previous.processId)) {
          throw AgentInstanceLockException('Agent not started: lockfile ${file.path} belongs to running PID ${previous.processId} on host "${previous.hostname}".');
        }
        if (!_readOwner(file).matches(previous)) {
          throw AgentInstanceLockException('Agent not started: lockfile ${file.path} changed during stale-lock verification.');
        }
        file.deleteSync();
        onMessage('Removed stale lockfile ${file.path}: PID ${previous.processId} is no longer running on host "${previous.hostname}". Continuing agent startup.');
      }
      // Prepare and flush complete metadata before publishing it atomically.
      // link(2) supplies exclusion on NFS without relying on O_EXCL support.
      final temporary = directory.createTempSync('.agent-lock-');
      try {
        final candidate = File('${temporary.path}${Platform.pathSeparator}owner.json');
        candidate.writeAsStringSync(jsonEncode(owner.toMap()), flush: true);
        try {
          _LockSystem.hardLink(candidate.path, file.path);
        } on FileSystemException {
          // An NFS server may have completed LINK despite a lost response.
          // Verify that our unique inode was published before claiming ownership.
          if (!_exists(file) || !FileSystemEntity.identicalSync(candidate.path, file.path)) rethrow;
        }
        onMessage('Acquired agent lockfile ${file.path} for host "${owner.hostname}" (PID ${owner.processId}).');
        return AgentInstanceLock._(file, owner, onMessage);
      } finally {
        temporary.deleteSync(recursive: true);
      }
    });
  }

  void release() {
    if (_released) return;
    _withGuard(file, () {
      if (_exists(file)) {
        if (!_readOwner(file).matches(_owner)) {
          throw AgentInstanceLockException('Cannot remove agent lockfile ${file.path}: ownership changed. The lockfile was preserved.');
        }
        file.deleteSync();
        _onMessage('Removed agent lockfile ${file.path} during shutdown.');
      }
      _released = true;
    });
  }

  static bool _exists(File file) => FileSystemEntity.typeSync(file.path, followLinks: false) != FileSystemEntityType.notFound;

  static _LockOwner _readOwner(File file) {
    try {
      final data = jsonDecode(file.readAsStringSync());
      if (data is! Map ||
          data['version'] != 1 ||
          data['hostname'] is! String ||
          (data['hostname'] as String).isEmpty ||
          data['pid'] is! int ||
          data['token'] is! String ||
          (data['token'] as String).isEmpty) {
        throw const FormatException('Invalid lock metadata.');
      }
      final processId = data['pid'] as int;
      if (processId < 1 || processId > 0x7fffffff) throw const FormatException('Invalid lock PID.');
      return _LockOwner(hostname: data['hostname'] as String, processId: processId, token: data['token'] as String);
    } catch (error) {
      throw AgentInstanceLockException('Agent lockfile ${file.path} cannot be verified and was preserved. Check it manually before restarting. Error: $error');
    }
  }

  static T _withGuard<T>(File lockFile, T Function() action) {
    // This inode must remain in place: unlinking a guard lets contenders lock
    // different inodes while reclaiming the same stale PID file.
    final guard = File('${lockFile.path}.guard').openSync(mode: FileMode.append);
    try {
      try {
        guard.lockSync(FileLock.exclusive);
      } on FileSystemException catch (error) {
        throw AgentInstanceLockException('Cannot check agent lockfile ${lockFile.path}: its startup/shutdown guard could not be locked. Error: $error');
      }
      return action();
    } finally {
      guard.closeSync();
    }
  }
}

class _LockOwner {
  const _LockOwner({required this.hostname, required this.processId, required this.token});

  final String hostname;
  final int processId;
  final String token;

  bool matches(_LockOwner other) => hostname == other.hostname && processId == other.processId && token == other.token;
  Map<String, Object> toMap() => {'version': 1, 'hostname': hostname, 'pid': processId, 'token': token};
}

class _LockSystem {
  static final DynamicLibrary _library = Platform.isWindows ? DynamicLibrary.open('kernel32.dll') : DynamicLibrary.process();

  static void hardLink(String original, String destination) {
    if (Platform.isWindows) {
      final link = _library.lookupFunction<Int32 Function(Pointer<Utf16>, Pointer<Utf16>, Pointer<Void>), int Function(Pointer<Utf16>, Pointer<Utf16>, Pointer<Void>)>('CreateHardLinkW');
      final lastError = _windowsLastError;
      final source = original.toNativeUtf16();
      final target = destination.toNativeUtf16();
      try {
        if (link(target, source, nullptr) == 0) throw FileSystemException('Cannot publish agent lock using CreateHardLinkW', destination, OSError('CreateHardLinkW failed', lastError()));
      } finally {
        calloc.free(source);
        calloc.free(target);
      }
      return;
    }
    final link = _library.lookupFunction<Int32 Function(Pointer<Utf8>, Pointer<Utf8>), int Function(Pointer<Utf8>, Pointer<Utf8>)>('link');
    final errno = _posixErrorPointer();
    final source = original.toNativeUtf8();
    final target = destination.toNativeUtf8();
    try {
      if (link(source, target) != 0) throw FileSystemException('Cannot publish agent lock using link(2)', destination, OSError('link(2) failed', errno.value));
    } finally {
      calloc.free(source);
      calloc.free(target);
    }
  }

  static bool processExists(int processId) {
    if (Platform.isWindows) return _windowsProcessExists(processId);
    final kill = _library.lookupFunction<Int32 Function(Int32, Int32), int Function(int, int)>('kill');
    final errno = _posixErrorPointer();
    if (kill(processId, 0) == 0) return true;
    final error = errno.value;
    if (error == 3) return false; // ESRCH: no such process.
    if (error == 1) return true; // EPERM: exists but is not ours to signal.
    throw AgentInstanceLockException('Cannot verify local PID $processId: kill(pid, 0) failed with errno $error. The lockfile was preserved.');
  }

  static Pointer<Int32> _posixErrorPointer() {
    if (!Platform.isLinux && !Platform.isMacOS) throw UnsupportedError('Agent locking is unsupported on ${Platform.operatingSystem}.');
    return _library.lookupFunction<Pointer<Int32> Function(), Pointer<Int32> Function()>(Platform.isMacOS ? '__error' : '__errno_location')();
  }

  static final _windowsLastError = _library.lookupFunction<Uint32 Function(), int Function()>('GetLastError');

  static bool _windowsProcessExists(int processId) {
    final lastError = _windowsLastError;
    final open = _library.lookupFunction<Pointer<Void> Function(Uint32, Int32, Uint32), Pointer<Void> Function(int, int, int)>('OpenProcess');
    final close = _library.lookupFunction<Int32 Function(Pointer<Void>), int Function(Pointer<Void>)>('CloseHandle');
    final getExitCode = _library.lookupFunction<Int32 Function(Pointer<Void>, Pointer<Uint32>), int Function(Pointer<Void>, Pointer<Uint32>)>('GetExitCodeProcess');
    final handle = open(0x1000, 0, processId); // PROCESS_QUERY_LIMITED_INFORMATION.
    if (handle == nullptr) {
      final error = lastError();
      if (error == 87) return false; // ERROR_INVALID_PARAMETER: PID is absent.
      if (error == 5) return true; // ERROR_ACCESS_DENIED: do not reclaim.
      throw AgentInstanceLockException('Cannot verify local PID $processId: OpenProcess failed with error $error. The lockfile was preserved.');
    }
    final code = calloc<Uint32>();
    try {
      if (getExitCode(handle, code) == 0) throw AgentInstanceLockException('Cannot verify local PID $processId: GetExitCodeProcess failed with error ${lastError()}. The lockfile was preserved.');
      return code.value == 259; // STILL_ACTIVE.
    } finally {
      calloc.free(code);
      close(handle);
    }
  }
}
