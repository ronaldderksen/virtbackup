part of 'backup.dart';

typedef SshCommandRunner = Future<SshCommandResult> Function(ServerConfig server, String command);
typedef DiskPathLoader = Future<List<MapEntry<String, String>>> Function(ServerConfig server, VmEntry vm, {bool inactive});
typedef SftpDownloader = Future<void> Function(ServerConfig server, String remotePath, File file, {void Function(int bytes)? onBytes});
typedef SftpStreamer = Future<void> Function(ServerConfig server, String remotePath, {required Future<void> Function(List<int> chunk) onChunk, void Function(int bytes)? onBytes});
typedef RemoteRangeStreamer =
    Future<void> Function(ServerConfig server, String remotePath, int offset, int length, {required Future<void> Function(List<int> chunk) onChunk, void Function(int bytes)? onBytes});

typedef SnapshotCreator = Future<void> Function(ServerConfig server, VmEntry vm, String snapshotName);
typedef SnapshotCommitter = Future<void> Function(ServerConfig server, VmEntry vm, List<MapEntry<String, String>> disks);
typedef OverlayCleaner = Future<void> Function(ServerConfig server, VmEntry vm, List<MapEntry<String, String>> activeDisks, List<MapEntry<String, String>> inactiveDisks);
typedef SftpUploader = Future<void> Function(ServerConfig server, String localPath, String remotePath, {void Function(int bytes)? onBytes});
typedef HashblocksEnsurer = Future<String> Function(ServerConfig server);
typedef HashblocksStreamer =
    Future<void> Function(
      ServerConfig server,
      String remoteHashblocksPath,
      String remotePath,
      int blockSize, {
      required Future<void> Function(String line) onLine,
      void Function(HashblocksController controller)? onController,
    });
typedef RemoteFileSizer = Future<int> Function(ServerConfig server, String remotePath);
typedef LargeTransferHook = Future<void> Function(ServerConfig server);

typedef FileNameSanitizer = String Function(String value);
typedef LogInfo = void Function(String message);
typedef LogError = void Function(String message, Object error, StackTrace stackTrace);

typedef BackupProgressListener = void Function(BackupAgentProgress progress);

const int backupAgentPort = 33551;

class BackupAgentProgress {
  const BackupAgentProgress({
    required this.isRunning,
    required this.statusMessage,
    required this.completedDisks,
    required this.totalDisks,
    required this.bytesTransferred,
    required this.speedBytesPerSec,
    required this.averageSpeedBytesPerSec,
    required this.physicalBytesTransferred,
    required this.physicalSpeedBytesPerSec,
    required this.averagePhysicalSpeedBytesPerSec,
    required this.totalBytes,
    required this.sanityBytesTransferred,
    required this.sanitySpeedBytesPerSec,
    this.etaSeconds,
    this.physicalRemainingBytes = 0,
    this.physicalTotalBytes = 0,
    this.physicalProgressPercent = 0,
    this.writerQueuedBytes = 0,
    this.writerInFlightBytes = 0,
    this.driverBufferedBytes = 0,
  });

  final bool isRunning;
  final String statusMessage;
  final int completedDisks;
  final int totalDisks;
  final int bytesTransferred;
  final double speedBytesPerSec;
  final double averageSpeedBytesPerSec;
  final int physicalBytesTransferred;
  final double physicalSpeedBytesPerSec;
  final double averagePhysicalSpeedBytesPerSec;
  final int totalBytes;
  final int sanityBytesTransferred;
  final double sanitySpeedBytesPerSec;
  final int? etaSeconds;
  final int physicalRemainingBytes;
  final int physicalTotalBytes;
  final double physicalProgressPercent;
  final int writerQueuedBytes;
  final int writerInFlightBytes;
  final int driverBufferedBytes;

  BackupAgentProgress copyWith({
    bool? isRunning,
    String? statusMessage,
    int? completedDisks,
    int? totalDisks,
    int? bytesTransferred,
    double? speedBytesPerSec,
    double? averageSpeedBytesPerSec,
    int? physicalBytesTransferred,
    double? physicalSpeedBytesPerSec,
    double? averagePhysicalSpeedBytesPerSec,
    int? totalBytes,
    int? sanityBytesTransferred,
    double? sanitySpeedBytesPerSec,
    int? etaSeconds,
    int? physicalRemainingBytes,
    int? physicalTotalBytes,
    double? physicalProgressPercent,
    int? writerQueuedBytes,
    int? writerInFlightBytes,
    int? driverBufferedBytes,
  }) {
    return BackupAgentProgress(
      isRunning: isRunning ?? this.isRunning,
      statusMessage: statusMessage ?? this.statusMessage,
      completedDisks: completedDisks ?? this.completedDisks,
      totalDisks: totalDisks ?? this.totalDisks,
      bytesTransferred: bytesTransferred ?? this.bytesTransferred,
      speedBytesPerSec: speedBytesPerSec ?? this.speedBytesPerSec,
      averageSpeedBytesPerSec: averageSpeedBytesPerSec ?? this.averageSpeedBytesPerSec,
      physicalBytesTransferred: physicalBytesTransferred ?? this.physicalBytesTransferred,
      physicalSpeedBytesPerSec: physicalSpeedBytesPerSec ?? this.physicalSpeedBytesPerSec,
      averagePhysicalSpeedBytesPerSec: averagePhysicalSpeedBytesPerSec ?? this.averagePhysicalSpeedBytesPerSec,
      totalBytes: totalBytes ?? this.totalBytes,
      sanityBytesTransferred: sanityBytesTransferred ?? this.sanityBytesTransferred,
      sanitySpeedBytesPerSec: sanitySpeedBytesPerSec ?? this.sanitySpeedBytesPerSec,
      etaSeconds: etaSeconds ?? this.etaSeconds,
      physicalRemainingBytes: physicalRemainingBytes ?? this.physicalRemainingBytes,
      physicalTotalBytes: physicalTotalBytes ?? this.physicalTotalBytes,
      physicalProgressPercent: physicalProgressPercent ?? this.physicalProgressPercent,
      writerQueuedBytes: writerQueuedBytes ?? this.writerQueuedBytes,
      writerInFlightBytes: writerInFlightBytes ?? this.writerInFlightBytes,
      driverBufferedBytes: driverBufferedBytes ?? this.driverBufferedBytes,
    );
  }

  Map<String, dynamic> toMap() {
    return {
      'isRunning': isRunning,
      'statusMessage': statusMessage,
      'completedDisks': completedDisks,
      'totalDisks': totalDisks,
      'bytesTransferred': bytesTransferred,
      'speedBytesPerSec': speedBytesPerSec,
      'averageSpeedBytesPerSec': averageSpeedBytesPerSec,
      'physicalBytesTransferred': physicalBytesTransferred,
      'physicalSpeedBytesPerSec': physicalSpeedBytesPerSec,
      'averagePhysicalSpeedBytesPerSec': averagePhysicalSpeedBytesPerSec,
      'totalBytes': totalBytes,
      'sanityBytesTransferred': sanityBytesTransferred,
      'sanitySpeedBytesPerSec': sanitySpeedBytesPerSec,
      'etaSeconds': etaSeconds,
      'physicalRemainingBytes': physicalRemainingBytes,
      'physicalTotalBytes': physicalTotalBytes,
      'physicalProgressPercent': physicalProgressPercent,
      'writerQueuedBytes': writerQueuedBytes,
      'writerInFlightBytes': writerInFlightBytes,
      'driverBufferedBytes': driverBufferedBytes,
    };
  }

  factory BackupAgentProgress.fromMap(Map<String, dynamic> json) {
    return BackupAgentProgress(
      isRunning: json['isRunning'] == true,
      statusMessage: (json['statusMessage'] ?? '').toString(),
      completedDisks: (json['completedDisks'] as num?)?.toInt() ?? 0,
      totalDisks: (json['totalDisks'] as num?)?.toInt() ?? 0,
      bytesTransferred: (json['bytesTransferred'] as num?)?.toInt() ?? 0,
      speedBytesPerSec: (json['speedBytesPerSec'] as num?)?.toDouble() ?? 0,
      averageSpeedBytesPerSec: (json['averageSpeedBytesPerSec'] as num?)?.toDouble() ?? 0,
      physicalBytesTransferred: (json['physicalBytesTransferred'] as num?)?.toInt() ?? 0,
      physicalSpeedBytesPerSec: (json['physicalSpeedBytesPerSec'] as num?)?.toDouble() ?? 0,
      averagePhysicalSpeedBytesPerSec: (json['averagePhysicalSpeedBytesPerSec'] as num?)?.toDouble() ?? 0,
      totalBytes: (json['totalBytes'] as num?)?.toInt() ?? 0,
      sanityBytesTransferred: (json['sanityBytesTransferred'] as num?)?.toInt() ?? 0,
      sanitySpeedBytesPerSec: (json['sanitySpeedBytesPerSec'] as num?)?.toDouble() ?? 0,
      etaSeconds: (json['etaSeconds'] as num?)?.toInt(),
      physicalRemainingBytes: (json['physicalRemainingBytes'] as num?)?.toInt() ?? 0,
      physicalTotalBytes: (json['physicalTotalBytes'] as num?)?.toInt() ?? 0,
      physicalProgressPercent: (json['physicalProgressPercent'] as num?)?.toDouble() ?? 0,
      writerQueuedBytes: (json['writerQueuedBytes'] as num?)?.toInt() ?? 0,
      writerInFlightBytes: (json['writerInFlightBytes'] as num?)?.toInt() ?? 0,
      driverBufferedBytes: (json['driverBufferedBytes'] as num?)?.toInt() ?? 0,
    );
  }

  static const BackupAgentProgress idle = BackupAgentProgress(
    isRunning: false,
    statusMessage: '',
    completedDisks: 0,
    totalDisks: 0,
    bytesTransferred: 0,
    speedBytesPerSec: 0,
    averageSpeedBytesPerSec: 0,
    physicalBytesTransferred: 0,
    physicalSpeedBytesPerSec: 0,
    averagePhysicalSpeedBytesPerSec: 0,
    totalBytes: 0,
    sanityBytesTransferred: 0,
    sanitySpeedBytesPerSec: 0,
    etaSeconds: null,
    physicalRemainingBytes: 0,
    physicalTotalBytes: 0,
    physicalProgressPercent: 0,
    writerQueuedBytes: 0,
    writerInFlightBytes: 0,
    driverBufferedBytes: 0,
  );
}

class BackupAgentResult {
  const BackupAgentResult({required this.success, this.message, this.canceled = false});

  final bool success;
  final String? message;
  final bool canceled;

  Map<String, dynamic> toMap() {
    return {'success': success, 'message': message, 'canceled': canceled};
  }

  factory BackupAgentResult.fromMap(Map<String, dynamic> json) {
    return BackupAgentResult(success: json['success'] == true, message: json['message']?.toString(), canceled: json['canceled'] == true);
  }
}

class HashblocksController {
  HashblocksController(this._send);

  final void Function(String command) _send;

  void setLimit(int maxBlockIndex) => _send('LIMIT $maxBlockIndex');
  void stop() => _send('STOP');
}
