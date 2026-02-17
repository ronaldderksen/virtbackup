enum ConnectionType { ssh, api }

enum DriverParamType { number, text, boolean }

class DriverParamDefinition {
  DriverParamDefinition({required this.key, required this.label, required this.type, this.defaultValue, this.min, this.max, this.step, this.unit, this.help});

  final String key;
  final String label;
  final DriverParamType type;
  final dynamic defaultValue;
  final num? min;
  final num? max;
  final num? step;
  final String? unit;
  final String? help;

  Map<String, dynamic> toMap() {
    return {'key': key, 'label': label, 'type': type.name, 'defaultValue': defaultValue, 'min': min, 'max': max, 'step': step, 'unit': unit, 'help': help};
  }

  factory DriverParamDefinition.fromMap(Map<String, dynamic> json) {
    final typeName = (json['type'] ?? '').toString();
    final parsedType = DriverParamType.values.firstWhere((value) => value.name == typeName, orElse: () => DriverParamType.text);
    return DriverParamDefinition(
      key: (json['key'] ?? '').toString(),
      label: (json['label'] ?? '').toString(),
      type: parsedType,
      defaultValue: json['defaultValue'],
      min: json['min'] is num ? json['min'] as num : num.tryParse((json['min'] ?? '').toString()),
      max: json['max'] is num ? json['max'] as num : num.tryParse((json['max'] ?? '').toString()),
      step: json['step'] is num ? json['step'] as num : num.tryParse((json['step'] ?? '').toString()),
      unit: json['unit']?.toString(),
      help: json['help']?.toString(),
    );
  }
}

class BackupDriverCapabilities {
  BackupDriverCapabilities({
    required this.supportsRangeRead,
    required this.supportsBatchDelete,
    required this.supportsMultipartUpload,
    required this.supportsServerSideCopy,
    required this.supportsConditionalWrite,
    required this.supportsVersioning,
    required this.maxConcurrentWrites,
    this.params = const <DriverParamDefinition>[],
  });

  final bool supportsRangeRead;
  final bool supportsBatchDelete;
  final bool supportsMultipartUpload;
  final bool supportsServerSideCopy;
  final bool supportsConditionalWrite;
  final bool supportsVersioning;
  final int maxConcurrentWrites;
  final List<DriverParamDefinition> params;

  Map<String, dynamic> toMap() {
    return {
      'supportsRangeRead': supportsRangeRead,
      'supportsBatchDelete': supportsBatchDelete,
      'supportsMultipartUpload': supportsMultipartUpload,
      'supportsServerSideCopy': supportsServerSideCopy,
      'supportsConditionalWrite': supportsConditionalWrite,
      'supportsVersioning': supportsVersioning,
      'maxConcurrentWrites': maxConcurrentWrites,
      'params': params.map((param) => param.toMap()).toList(),
    };
  }

  factory BackupDriverCapabilities.fromMap(Map<String, dynamic> json) {
    return BackupDriverCapabilities(
      supportsRangeRead: json['supportsRangeRead'] == true,
      supportsBatchDelete: json['supportsBatchDelete'] == true,
      supportsMultipartUpload: json['supportsMultipartUpload'] == true,
      supportsServerSideCopy: json['supportsServerSideCopy'] == true,
      supportsConditionalWrite: json['supportsConditionalWrite'] == true,
      supportsVersioning: json['supportsVersioning'] == true,
      maxConcurrentWrites: (json['maxConcurrentWrites'] as num?)?.toInt() ?? 16,
      params: (json['params'] as List? ?? const <dynamic>[]).whereType<Map>().map((entry) => DriverParamDefinition.fromMap(Map<String, dynamic>.from(entry))).toList(),
    );
  }
}

class BackupDriverInfo {
  BackupDriverInfo({required this.id, required this.label, required this.usesPath, required this.capabilities});

  final String id;
  final String label;
  final bool usesPath;
  final BackupDriverCapabilities capabilities;

  Map<String, dynamic> toMap() {
    return {'id': id, 'label': label, 'usesPath': usesPath, 'capabilities': capabilities.toMap()};
  }

  factory BackupDriverInfo.fromMap(Map<String, dynamic> json) {
    return BackupDriverInfo(
      id: (json['id'] ?? '').toString(),
      label: (json['label'] ?? '').toString(),
      usesPath: json['usesPath'] == true,
      capabilities: BackupDriverCapabilities.fromMap(Map<String, dynamic>.from(json['capabilities'] ?? const <String, dynamic>{})),
    );
  }
}

class BackupStorage {
  BackupStorage({
    required this.id,
    required this.name,
    required this.driverId,
    required this.enabled,
    required this.params,
    this.disableFresh = false,
    this.storeBlobs = false,
    this.useBlobs = false,
    this.uploadConcurrency,
    this.downloadConcurrency,
  });

  final String id;
  final String name;
  final String driverId;
  final bool enabled;
  final bool disableFresh;
  final bool storeBlobs;
  final bool useBlobs;
  final int? uploadConcurrency;
  final int? downloadConcurrency;
  final Map<String, dynamic> params;

  Map<String, dynamic> toMap() {
    final map = <String, dynamic>{'id': id, 'name': name, 'driverId': driverId, 'enabled': enabled, 'disableFresh': disableFresh, 'params': Map<String, dynamic>.from(params)};
    if (driverId.trim() != 'filesystem') {
      map['storeBlobs'] = storeBlobs;
      map['useBlobs'] = useBlobs;
      if (uploadConcurrency != null) {
        map['uploadConcurrency'] = uploadConcurrency;
      }
      if (downloadConcurrency != null) {
        map['downloadConcurrency'] = downloadConcurrency;
      }
    }
    return map;
  }

  factory BackupStorage.fromMap(Map<String, dynamic> json) {
    final paramsRaw = json['params'];
    final params = paramsRaw is Map ? Map<String, dynamic>.from(paramsRaw) : <String, dynamic>{};
    return BackupStorage(
      id: (json['id'] ?? '').toString(),
      name: (json['name'] ?? '').toString(),
      driverId: (json['driverId'] ?? '').toString(),
      enabled: json['enabled'] != false,
      disableFresh: json['disableFresh'] == true,
      storeBlobs: json['storeBlobs'] == true,
      useBlobs: json['useBlobs'] == true,
      uploadConcurrency: _parsePositiveIntOrNull(json['uploadConcurrency'], field: 'uploadConcurrency'),
      downloadConcurrency: _parsePositiveIntOrNull(json['downloadConcurrency'], field: 'downloadConcurrency'),
      params: params,
    );
  }

  static int? _parsePositiveIntOrNull(Object? raw, {required String field}) {
    if (raw == null) {
      return null;
    }
    final parsed = raw is num ? raw.toInt() : int.tryParse(raw.toString().trim());
    if (parsed == null || parsed <= 0) {
      throw FormatException('Invalid $field value: $raw');
    }
    return parsed;
  }
}

class ServerConfig {
  ServerConfig({
    required this.id,
    required this.name,
    required this.connectionType,
    required this.sshHost,
    required this.sshPort,
    required this.sshUser,
    required this.sshPassword,
    required this.apiBaseUrl,
    required this.apiToken,
  });

  final String id;
  final String name;
  final ConnectionType connectionType;
  final String sshHost;
  final String sshPort;
  final String sshUser;
  final String sshPassword;
  final String apiBaseUrl;
  final String apiToken;

  Map<String, dynamic> toMap() {
    return {
      'id': id,
      'name': name,
      'type': connectionType.name,
      'sshHost': sshHost,
      'sshPort': sshPort,
      'sshUser': sshUser,
      'sshPassword': _encodePassword(sshPassword),
      'apiBaseUrl': apiBaseUrl,
      'apiToken': apiToken,
    };
  }

  factory ServerConfig.fromMap(Map<String, dynamic> json) {
    final typeValue = json['type'] as String?;
    final connectionType = ConnectionType.values.firstWhere((type) => type.name == typeValue, orElse: () => ConnectionType.ssh);
    return ServerConfig(
      id: json['id']?.toString() ?? DateTime.now().millisecondsSinceEpoch.toString(),
      name: json['name']?.toString() ?? 'Server',
      connectionType: connectionType,
      sshHost: json['sshHost']?.toString() ?? '',
      sshPort: json['sshPort']?.toString() ?? '22',
      sshUser: json['sshUser']?.toString() ?? '',
      sshPassword: _decodePassword(json['sshPassword']?.toString()),
      apiBaseUrl: json['apiBaseUrl']?.toString() ?? '',
      apiToken: json['apiToken']?.toString() ?? '',
    );
  }

  static String _encodePassword(String value) {
    return value;
  }

  static String _decodePassword(String? value) {
    if (value == null || value.isEmpty) {
      return '';
    }
    return value;
  }
}

enum VmPowerState { running, stopped }

class VmEntry {
  VmEntry({required this.id, required this.name, required this.powerState});

  final String id;
  final String name;
  final VmPowerState powerState;

  Map<String, dynamic> toMap() {
    return {'id': id, 'name': name, 'powerState': powerState.name};
  }

  factory VmEntry.fromMap(Map<String, dynamic> json) {
    final rawState = (json['powerState'] ?? json['state'] ?? json['status'] ?? '').toString();
    final powerState = VmPowerState.values.firstWhere((state) => state.name == rawState, orElse: () => VmPowerState.stopped);
    return VmEntry(id: (json['id'] ?? json['name'] ?? '').toString(), name: (json['name'] ?? json['id'] ?? '').toString(), powerState: powerState);
  }
}

class VmStatus {
  VmStatus({required this.vm, required this.hasOverlay});

  final VmEntry vm;
  final bool hasOverlay;

  Map<String, dynamic> toMap() {
    return {'vm': vm.toMap(), 'hasOverlay': hasOverlay};
  }

  factory VmStatus.fromMap(Map<String, dynamic> json) {
    final vmJson = json['vm'];
    return VmStatus(
      vm: vmJson is Map ? VmEntry.fromMap(Map<String, dynamic>.from(vmJson)) : VmEntry(id: 'unknown', name: 'unknown', powerState: VmPowerState.stopped),
      hasOverlay: json['hasOverlay'] == true,
    );
  }
}

enum VmAction { start, reboot, shutdown, forceReset, forceOff }

class SshCommandResult {
  SshCommandResult({required this.stdout, required this.stderr, required this.exitCode});

  final String stdout;
  final String stderr;
  final int? exitCode;

  Map<String, dynamic> toMap() {
    return {'stdout': stdout, 'stderr': stderr, 'exitCode': exitCode};
  }

  factory SshCommandResult.fromMap(Map<String, dynamic> json) {
    return SshCommandResult(stdout: (json['stdout'] ?? '').toString(), stderr: (json['stderr'] ?? '').toString(), exitCode: (json['exitCode'] as num?)?.toInt());
  }
}

enum AgentJobType { backup, restore, sanity }

enum AgentJobState { running, success, failure, canceled }

class AgentJobStatus {
  AgentJobStatus({
    required this.id,
    required this.type,
    required this.state,
    required this.message,
    required this.totalUnits,
    required this.completedUnits,
    required this.bytesTransferred,
    required this.speedBytesPerSec,
    required this.physicalBytesTransferred,
    required this.physicalSpeedBytesPerSec,
    required this.totalBytes,
    required this.sanityBytesTransferred,
    required this.sanitySpeedBytesPerSec,
    this.averageSpeedBytesPerSec = 0,
    this.averagePhysicalSpeedBytesPerSec = 0,
    this.etaSeconds,
    this.physicalRemainingBytes = 0,
    this.physicalTotalBytes = 0,
    this.physicalProgressPercent = 0,
    this.writerQueuedBytes = 0,
    this.writerInFlightBytes = 0,
    this.driverBufferedBytes = 0,
  });

  final String id;
  final AgentJobType type;
  final AgentJobState state;
  final String message;
  final int totalUnits;
  final int completedUnits;
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

  Map<String, dynamic> toMap() {
    return {
      'id': id,
      'type': type.name,
      'state': state.name,
      'message': message,
      'totalUnits': totalUnits,
      'completedUnits': completedUnits,
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

  factory AgentJobStatus.fromMap(Map<String, dynamic> json) {
    final typeValue = (json['type'] ?? '').toString();
    final stateValue = (json['state'] ?? '').toString();
    return AgentJobStatus(
      id: (json['id'] ?? '').toString(),
      type: AgentJobType.values.firstWhere((value) => value.name == typeValue, orElse: () => AgentJobType.backup),
      state: AgentJobState.values.firstWhere((value) => value.name == stateValue, orElse: () => AgentJobState.failure),
      message: (json['message'] ?? '').toString(),
      totalUnits: (json['totalUnits'] as num?)?.toInt() ?? 0,
      completedUnits: (json['completedUnits'] as num?)?.toInt() ?? 0,
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

  AgentJobStatus copyWith({
    AgentJobState? state,
    String? message,
    int? totalUnits,
    int? completedUnits,
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
    return AgentJobStatus(
      id: id,
      type: type,
      state: state ?? this.state,
      message: message ?? this.message,
      totalUnits: totalUnits ?? this.totalUnits,
      completedUnits: completedUnits ?? this.completedUnits,
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
}

class AgentJobStart {
  AgentJobStart({required this.jobId});

  final String jobId;

  Map<String, dynamic> toMap() {
    return {'jobId': jobId};
  }

  factory AgentJobStart.fromMap(Map<String, dynamic> json) {
    return AgentJobStart(jobId: (json['jobId'] ?? '').toString());
  }
}

class RestorePrecheckResult {
  RestorePrecheckResult({required this.vmExists, required this.canDefineOnly});

  final bool vmExists;
  final bool canDefineOnly;

  Map<String, dynamic> toMap() {
    return {'vmExists': vmExists, 'canDefineOnly': canDefineOnly};
  }

  factory RestorePrecheckResult.fromMap(Map<String, dynamic> json) {
    return RestorePrecheckResult(vmExists: json['vmExists'] == true, canDefineOnly: json['canDefineOnly'] == true);
  }
}

class RestoreEntry {
  RestoreEntry({
    required this.xmlPath,
    required this.vmName,
    required this.timestamp,
    required this.diskBasenames,
    required this.missingDiskBasenames,
    required this.blockSizeMbValues,
    required this.sourceServerId,
    required this.sourceServerName,
  });

  final String xmlPath;
  final String vmName;
  final String timestamp;
  final List<String> diskBasenames;
  final List<String> missingDiskBasenames;
  final List<int> blockSizeMbValues;
  final String sourceServerId;
  final String sourceServerName;

  bool get hasAllDisks => missingDiskBasenames.isEmpty;

  Map<String, dynamic> toMap() {
    return {
      'xmlPath': xmlPath,
      'vmName': vmName,
      'timestamp': timestamp,
      'diskBasenames': diskBasenames,
      'missingDiskBasenames': missingDiskBasenames,
      'blockSizeMbValues': blockSizeMbValues,
      'sourceServerId': sourceServerId,
      'sourceServerName': sourceServerName,
    };
  }

  factory RestoreEntry.fromMap(Map<String, dynamic> json) {
    final diskBasenamesRaw = json['diskBasenames'];
    final missingRaw = json['missingDiskBasenames'];
    final blockSizeMbValuesRaw = json['blockSizeMbValues'];
    return RestoreEntry(
      xmlPath: (json['xmlPath'] ?? '').toString(),
      vmName: (json['vmName'] ?? '').toString(),
      timestamp: (json['timestamp'] ?? '').toString(),
      diskBasenames: diskBasenamesRaw is List ? diskBasenamesRaw.map((item) => item.toString()).toList() : const [],
      missingDiskBasenames: missingRaw is List ? missingRaw.map((item) => item.toString()).toList() : const [],
      blockSizeMbValues: blockSizeMbValuesRaw is List
          ? blockSizeMbValuesRaw.map((item) => item is num ? item.toInt() : int.tryParse(item.toString()) ?? 0).where((item) => item > 0).toList()
          : const [],
      sourceServerId: (json['sourceServerId'] ?? '').toString(),
      sourceServerName: (json['sourceServerName'] ?? '').toString(),
    );
  }
}
