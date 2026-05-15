part of 'main_screen.dart';

extension _BackupServerSetupScheduleSection on _BackupServerSetupScreenState {
  List<Widget> _buildScheduleSection(ColorScheme colorScheme) {
    final allSchedules = List<ScheduledJob>.from(_agentSettings.schedules)..sort(_compareSchedulesForList);
    final filters = _effectiveScheduleQuickFilters(allSchedules);
    final schedules = allSchedules.where((schedule) => _scheduleMatchesQuickFilters(schedule, filters)).toList();
    return [
      Card(
        elevation: 2,
        shadowColor: colorScheme.shadow.withValues(alpha: 0.2),
        shape: RoundedRectangleBorder(borderRadius: BorderRadius.circular(16)),
        child: Padding(
          padding: const EdgeInsets.all(20),
          child: Column(
            crossAxisAlignment: CrossAxisAlignment.start,
            children: [
              Row(
                children: [
                  Expanded(child: Text('Schedules', style: Theme.of(context).textTheme.titleMedium)),
                  FilledButton.icon(
                    onPressed: _canCreateBackupSchedule() ? () => _openScheduleEditor(type: ScheduledJobType.backup) : null,
                    icon: const Icon(Icons.add),
                    label: const Text('Backup'),
                  ),
                  const SizedBox(width: 12),
                  FilledButton.icon(
                    onPressed: _canCreateRestoreSchedule() ? () => _openScheduleEditor(type: ScheduledJobType.restore) : null,
                    icon: const Icon(Icons.add),
                    label: const Text('Restore'),
                  ),
                ],
              ),
              const SizedBox(height: 16),
              _buildScheduleQuickFilters(allSchedules, filters),
              const SizedBox(height: 16),
              if (allSchedules.isEmpty)
                Text('No schedules configured.', style: Theme.of(context).textTheme.bodyMedium)
              else if (schedules.isEmpty)
                Text('No schedules match the selected filters.', style: Theme.of(context).textTheme.bodyMedium)
              else
                ListView.separated(
                  shrinkWrap: true,
                  physics: const NeverScrollableScrollPhysics(),
                  itemCount: schedules.length,
                  separatorBuilder: (_, _) => const Divider(height: 1),
                  itemBuilder: (context, index) {
                    final schedule = schedules[index];
                    final isRunning = _scheduleIsRunning(schedule);
                    return ListTile(
                      contentPadding: EdgeInsets.zero,
                      selected: isRunning,
                      selectedTileColor: colorScheme.primaryContainer.withValues(alpha: 0.45),
                      tileColor: isRunning ? colorScheme.primaryContainer.withValues(alpha: 0.45) : null,
                      leading: Icon(schedule.type == ScheduledJobType.backup ? Icons.backup_outlined : Icons.restore_outlined),
                      title: Row(
                        children: [
                          Expanded(child: Text(schedule.name)),
                          if (isRunning) ...[
                            const SizedBox(width: 8),
                            Chip(
                              visualDensity: VisualDensity.compact,
                              label: const Text('Running'),
                              avatar: SizedBox(width: 14, height: 14, child: CircularProgressIndicator(strokeWidth: 2, color: colorScheme.onPrimaryContainer)),
                            ),
                          ],
                        ],
                      ),
                      subtitle: Text(_scheduleSummary(schedule)),
                      isThreeLine: true,
                      trailing: Row(
                        mainAxisSize: MainAxisSize.min,
                        children: [
                          TextButton.icon(onPressed: () => _runScheduleNow(schedule), icon: const Icon(Icons.play_arrow), label: const Text('Run')),
                          Switch(value: schedule.enabled, onChanged: (value) => _toggleSchedule(schedule, value)),
                          IconButton(
                            tooltip: 'Edit',
                            onPressed: () => _openScheduleEditor(existing: schedule),
                            icon: const Icon(Icons.edit_outlined),
                          ),
                          IconButton(tooltip: 'Delete', onPressed: () => _deleteSchedule(schedule), icon: const Icon(Icons.delete_outline)),
                        ],
                      ),
                    );
                  },
                ),
            ],
          ),
        ),
      ),
    ];
  }

  bool _scheduleIsRunning(ScheduledJob schedule) {
    return _latestAgentJobs.any((job) => job.scheduleId == schedule.id && job.state == AgentJobState.running);
  }

  int _compareSchedulesForList(ScheduledJob a, ScheduledJob b) {
    final typeCompare = _scheduleTypeSortRank(a.type).compareTo(_scheduleTypeSortRank(b.type));
    if (typeCompare != 0) {
      return typeCompare;
    }
    final frequencyCompare = _scheduleFrequencySortRank(a.frequency).compareTo(_scheduleFrequencySortRank(b.frequency));
    if (frequencyCompare != 0) {
      return frequencyCompare;
    }
    final timeCompare = _scheduleTimeSortValue(a).compareTo(_scheduleTimeSortValue(b));
    if (timeCompare != 0) {
      return timeCompare;
    }
    final nameCompare = a.name.toLowerCase().compareTo(b.name.toLowerCase());
    if (nameCompare != 0) {
      return nameCompare;
    }
    return a.id.compareTo(b.id);
  }

  int _scheduleTypeSortRank(ScheduledJobType type) {
    return type == ScheduledJobType.backup ? 0 : 1;
  }

  int _scheduleFrequencySortRank(ScheduleFrequency frequency) {
    return switch (frequency) {
      ScheduleFrequency.every5Minutes => 0,
      ScheduleFrequency.hourly => 1,
      ScheduleFrequency.daily => 2,
      ScheduleFrequency.weekly => 3,
    };
  }

  int _scheduleTimeSortValue(ScheduledJob schedule) {
    final minute = _scheduleMinuteSortValue(schedule.time);
    if (schedule.frequency == ScheduleFrequency.every5Minutes || schedule.frequency == ScheduleFrequency.hourly) {
      return minute;
    }
    final minutesOfDay = _scheduleMinutesOfDaySortValue(schedule.time);
    if (schedule.frequency == ScheduleFrequency.daily) {
      return minutesOfDay;
    }
    final weekday = schedule.weekdays.isEmpty ? 8 : schedule.weekdays.reduce(min);
    return (weekday * 24 * 60) + minutesOfDay;
  }

  int _scheduleMinuteSortValue(String time) {
    if (time == '*/5') {
      return 0;
    }
    final parts = time.split(':');
    final rawMinute = parts.length == 2 ? parts[1] : time;
    final minute = int.tryParse(rawMinute.trim());
    return minute == null ? 60 : minute.clamp(0, 59);
  }

  int _scheduleMinutesOfDaySortValue(String time) {
    final parts = time.split(':');
    if (parts.length != 2) {
      return 24 * 60;
    }
    final hour = int.tryParse(parts[0].trim());
    final minute = int.tryParse(parts[1].trim());
    if (hour == null || minute == null) {
      return 24 * 60;
    }
    return (hour.clamp(0, 23) * 60) + minute.clamp(0, 59);
  }

  Widget _buildScheduleQuickFilters(List<ScheduledJob> schedules, ({String serverId, String type, String serverVmKey, String storageId}) filters) {
    final serverIds = schedules.map((schedule) => schedule.serverId).toSet().toList()..sort((a, b) => _serverNameForId(a).toLowerCase().compareTo(_serverNameForId(b).toLowerCase()));
    final storageIds = schedules.map((schedule) => schedule.storageId).toSet().toList()..sort((a, b) => _storageNameForId(a).toLowerCase().compareTo(_storageNameForId(b).toLowerCase()));
    final serverVmKeys = schedules.map(_scheduleServerVmKey).toSet().toList()..sort((a, b) => _scheduleServerVmLabel(a).toLowerCase().compareTo(_scheduleServerVmLabel(b).toLowerCase()));
    return Column(
      crossAxisAlignment: CrossAxisAlignment.start,
      children: [
        Text('Quick filter', style: Theme.of(context).textTheme.labelLarge),
        const SizedBox(height: 8),
        Wrap(
          spacing: 12,
          runSpacing: 12,
          crossAxisAlignment: WrapCrossAlignment.center,
          children: [
            SizedBox(
              width: 220,
              child: DropdownButtonFormField<String>(
                initialValue: filters.serverId,
                decoration: _scheduleDecoration(context, labelText: 'Server'),
                items: [
                  const DropdownMenuItem(value: '', child: Text('All servers')),
                  ...serverIds.map((id) => DropdownMenuItem(value: id, child: Text(_serverNameForId(id)))),
                ],
                onChanged: (value) => _updateUi(() => _scheduleFilterServerId = value ?? ''),
              ),
            ),
            SizedBox(
              width: 190,
              child: DropdownButtonFormField<String>(
                initialValue: filters.type,
                decoration: _scheduleDecoration(context, labelText: 'Backup/Restore'),
                items: const [
                  DropdownMenuItem(value: '', child: Text('All types')),
                  DropdownMenuItem(value: 'backup', child: Text('Backup')),
                  DropdownMenuItem(value: 'restore', child: Text('Restore')),
                ],
                onChanged: (value) => _updateUi(() => _scheduleFilterType = value ?? ''),
              ),
            ),
            SizedBox(
              width: 260,
              child: DropdownButtonFormField<String>(
                isExpanded: true,
                initialValue: filters.serverVmKey,
                decoration: _scheduleDecoration(context, labelText: 'Server - VM'),
                items: [
                  const DropdownMenuItem(value: '', child: Text('All server - VM')),
                  ...serverVmKeys.map((key) => DropdownMenuItem(value: key, child: Text(_scheduleServerVmLabel(key)))),
                ],
                onChanged: (value) => _updateUi(() => _scheduleFilterServerVmKey = value ?? ''),
              ),
            ),
            SizedBox(
              width: 220,
              child: DropdownButtonFormField<String>(
                initialValue: filters.storageId,
                decoration: _scheduleDecoration(context, labelText: 'Storage'),
                items: [
                  const DropdownMenuItem(value: '', child: Text('All storage')),
                  ...storageIds.map((id) => DropdownMenuItem(value: id, child: Text(_storageNameForId(id)))),
                ],
                onChanged: (value) => _updateUi(() => _scheduleFilterStorageId = value ?? ''),
              ),
            ),
            TextButton.icon(onPressed: _hasScheduleQuickFilter ? () => _updateUi(_clearScheduleQuickFilters) : null, icon: const Icon(Icons.filter_alt_off_outlined), label: const Text('Clear')),
          ],
        ),
      ],
    );
  }

  bool get _hasScheduleQuickFilter {
    return _scheduleFilterServerId.isNotEmpty || _scheduleFilterType.isNotEmpty || _scheduleFilterServerVmKey.isNotEmpty || _scheduleFilterStorageId.isNotEmpty;
  }

  void _clearScheduleQuickFilters() {
    _scheduleFilterServerId = '';
    _scheduleFilterType = '';
    _scheduleFilterServerVmKey = '';
    _scheduleFilterStorageId = '';
  }

  ({String serverId, String type, String serverVmKey, String storageId}) _effectiveScheduleQuickFilters(List<ScheduledJob> schedules) {
    final serverIds = schedules.map((schedule) => schedule.serverId).toSet();
    final storageIds = schedules.map((schedule) => schedule.storageId).toSet();
    final serverVmKeys = schedules.map(_scheduleServerVmKey).toSet();
    return (
      serverId: serverIds.contains(_scheduleFilterServerId) ? _scheduleFilterServerId : '',
      type: _scheduleFilterType == ScheduledJobType.backup.name || _scheduleFilterType == ScheduledJobType.restore.name ? _scheduleFilterType : '',
      serverVmKey: serverVmKeys.contains(_scheduleFilterServerVmKey) ? _scheduleFilterServerVmKey : '',
      storageId: storageIds.contains(_scheduleFilterStorageId) ? _scheduleFilterStorageId : '',
    );
  }

  bool _scheduleMatchesQuickFilters(ScheduledJob schedule, ({String serverId, String type, String serverVmKey, String storageId}) filters) {
    if (filters.serverId.isNotEmpty && schedule.serverId != filters.serverId) {
      return false;
    }
    if (filters.type == ScheduledJobType.backup.name && schedule.type != ScheduledJobType.backup) {
      return false;
    }
    if (filters.type == ScheduledJobType.restore.name && schedule.type != ScheduledJobType.restore) {
      return false;
    }
    if (filters.serverVmKey.isNotEmpty && _scheduleServerVmKey(schedule) != filters.serverVmKey) {
      return false;
    }
    if (filters.storageId.isNotEmpty && schedule.storageId != filters.storageId) {
      return false;
    }
    return true;
  }

  String _scheduleServerVmKey(ScheduledJob schedule) {
    return '${schedule.serverId}\u0000${schedule.backupAllVms || schedule.restoreAllLatestVms ? '__all_vms__' : schedule.vmName}';
  }

  String _scheduleServerVmLabel(String key) {
    final parts = key.split('\u0000');
    if (parts.length != 2) {
      return key;
    }
    final vmLabel = parts[1] == '__all_vms__' ? 'All VMs' : parts[1];
    return '${_serverNameForId(parts[0])} - $vmLabel';
  }

  bool _canCreateBackupSchedule() {
    return _servers.isNotEmpty && _enabledStorages().isNotEmpty;
  }

  bool _canCreateRestoreSchedule() {
    return _servers.isNotEmpty && _enabledStorages().where((storage) => storage.driverId != 'dummy').isNotEmpty;
  }

  String _scheduleSummary(ScheduledJob schedule) {
    final serverName = _serverNameForId(schedule.serverId);
    final storageName = _storageNameForId(schedule.storageId);
    final cadence = switch (schedule.frequency) {
      ScheduleFrequency.every5Minutes => 'Every 5 minutes from minute ${schedule.time == '*/5' ? '00' : schedule.time.substring(3)}',
      ScheduleFrequency.hourly => 'Hourly at minute ${schedule.time.substring(3)}',
      ScheduleFrequency.daily => 'Daily at ${schedule.time}',
      ScheduleFrequency.weekly => '${_formatWeekdays(schedule.weekdays)} at ${schedule.time}',
    };
    if (schedule.type == ScheduledJobType.backup) {
      if (schedule.backupAllVms) {
        return '$cadence • backup all VMs on $serverName to $storageName${_scheduleWaitSummary(schedule)}';
      }
      return '$cadence • backup ${schedule.vmName} on $serverName to $storageName${_scheduleWaitSummary(schedule)}';
    }
    if (schedule.restoreAllLatestVms) {
      return '$cadence • restore latest XML for all VMs to $serverName from $storageName${_scheduleWaitSummary(schedule)}';
    }
    if (schedule.restoreXmlPath == ScheduledJob.latestRestoreXmlPath) {
      return '$cadence • restore latest ${schedule.vmName} XML to $serverName from $storageName${_scheduleWaitSummary(schedule)}';
    }
    return '$cadence • restore ${schedule.restoreXmlPath} to $serverName from $storageName${_scheduleWaitSummary(schedule)}';
  }

  String _scheduleWaitSummary(ScheduledJob schedule) {
    return schedule.waitForRunningJobs ? ' • waits for running jobs' : '';
  }

  String _serverNameForId(String id) {
    for (final server in _servers) {
      if (server.id == id) {
        return server.name;
      }
    }
    return id;
  }

  String _storageNameForId(String id) {
    for (final storage in _agentSettings.storage) {
      if (storage.id == id) {
        return storage.name;
      }
    }
    return id;
  }

  String _formatWeekdays(List<int> weekdays) {
    const labels = <int, String>{1: 'Mon', 2: 'Tue', 3: 'Wed', 4: 'Thu', 5: 'Fri', 6: 'Sat', 7: 'Sun'};
    return weekdays.map((day) => labels[day] ?? day.toString()).join(', ');
  }

  Future<void> _toggleSchedule(ScheduledJob schedule, bool enabled) async {
    await _saveSchedule(schedule.copyWith(enabled: enabled), showSnackBar: false);
  }

  Future<void> _runScheduleNow(ScheduledJob schedule) async {
    try {
      final start = await _agentApiClient.runSchedule(schedule.id);
      if (start.queued) {
        _showSnackBarInfo('Schedule queued until running jobs finish.');
      } else {
        _showSnackBarInfo('Schedule started: ${start.jobId}');
      }
      await _syncRunningJobs();
    } catch (error, stackTrace) {
      _logError('Schedule run failed.', error, stackTrace);
      _showSnackBarError('Schedule run failed: $error');
    }
  }

  Future<void> _saveSchedule(ScheduledJob schedule, {bool showSnackBar = true}) async {
    final previous = _agentSettings;
    final schedules = List<ScheduledJob>.from(_agentSettings.schedules);
    final index = schedules.indexWhere((item) => item.id == schedule.id);
    if (index >= 0) {
      schedules[index] = schedule;
    } else {
      schedules.add(schedule);
    }
    final updated = _agentSettings.copyWith(schedules: schedules);
    _agentSettings = updated;
    try {
      await _pushAgentSettings();
    } catch (error) {
      _agentSettings = previous;
      if (mounted) {
        _updateUi(() {});
        _showSnackBarError('Schedule save failed: $error');
      }
      return;
    }
    if (mounted) {
      _updateUi(() {});
      if (showSnackBar) {
        _showSnackBarInfo('Schedule saved');
      }
    }
  }

  Future<void> _deleteSchedule(ScheduledJob schedule) async {
    final confirmed = await showDialog<bool>(
      context: context,
      builder: (dialogContext) {
        return AlertDialog(
          title: const Text('Delete schedule?'),
          content: Text('Delete "${schedule.name}"?'),
          actions: [
            TextButton(onPressed: () => Navigator.of(dialogContext).pop(false), child: const Text('Cancel')),
            FilledButton(onPressed: () => Navigator.of(dialogContext).pop(true), child: const Text('Delete')),
          ],
        );
      },
    );
    if (confirmed != true) {
      return;
    }
    final previous = _agentSettings;
    final schedules = _agentSettings.schedules.where((item) => item.id != schedule.id).toList();
    _agentSettings = _agentSettings.copyWith(schedules: schedules);
    try {
      await _pushAgentSettings();
    } catch (error) {
      _agentSettings = previous;
      if (mounted) {
        _updateUi(() {});
        _showSnackBarError('Schedule delete failed: $error');
      }
      return;
    }
    if (mounted) {
      _updateUi(() {});
      _showSnackBarInfo('Schedule deleted');
    }
  }

  Future<void> _openScheduleEditor({ScheduledJob? existing, ScheduledJobType? type}) async {
    final scheduleType = existing?.type ?? type;
    if (scheduleType == null) {
      return;
    }
    final result = await _showScheduleDialog(existing: existing, type: scheduleType);
    if (result == null) {
      return;
    }
    await _saveSchedule(result);
  }

  Future<ScheduledJob?> _showScheduleDialog({ScheduledJob? existing, required ScheduledJobType type}) async {
    var enabled = existing?.enabled ?? true;
    var waitForRunningJobs = existing?.waitForRunningJobs ?? true;
    var backupAllVms = existing?.backupAllVms ?? false;
    var restoreAllLatestVms = existing?.restoreAllLatestVms ?? false;
    var frequency = existing?.frequency ?? ScheduleFrequency.daily;
    final timeController = TextEditingController(
      text: frequency == ScheduleFrequency.hourly || frequency == ScheduleFrequency.every5Minutes ? _minuteFromScheduleTime(existing?.time ?? '00:00') : existing?.time ?? '',
    );
    var weekdays = List<int>.from(existing?.weekdays ?? <int>[]);
    var serverId = existing?.serverId ?? (_servers.isNotEmpty ? _servers.first.id : null);
    var storageId = existing?.storageId ?? (_enabledStorages().isNotEmpty ? _enabledStorages().first.id : null);
    var vmName = existing?.vmName ?? '';
    var restoreXmlPath = existing?.restoreXmlPath ?? '';
    var restoreDecision = existing?.restoreDecision ?? 'overwrite';
    String? validationError;
    var invalidFields = <String>{};
    final availableFrequencies = _availableScheduleFrequencies(frequency);
    final initialServer = serverId == null ? null : _serverById(serverId);
    if (initialServer != null && type == ScheduledJobType.backup) {
      await _loadVmInventory(initialServer);
    }
    if (type == ScheduledJobType.restore && storageId != null) {
      await _setSelectedBackupStorage(storageId, refreshRestoreEntries: true);
    }
    if (!mounted) {
      return null;
    }
    try {
      return await showDialog<ScheduledJob>(
        context: context,
        builder: (dialogContext) {
          return StatefulBuilder(
            builder: (context, setDialogState) {
              void refreshValidation() {
                final weeklyDays = List<int>.from(weekdays)..sort();
                final fields = _scheduleInvalidFields(
                  time: timeController.text.trim(),
                  serverId: serverId?.trim() ?? '',
                  storageId: storageId?.trim() ?? '',
                  frequency: frequency,
                  weekdays: weeklyDays,
                  type: type,
                  vmName: vmName.trim(),
                  backupAllVms: backupAllVms,
                  restoreAllLatestVms: restoreAllLatestVms,
                  restoreXmlPath: restoreXmlPath.trim(),
                );
                invalidFields = fields;
                validationError = fields.isEmpty
                    ? null
                    : _scheduleValidationError(
                        time: timeController.text.trim(),
                        serverId: serverId?.trim() ?? '',
                        storageId: storageId?.trim() ?? '',
                        frequency: frequency,
                        weekdays: weeklyDays,
                        type: type,
                        vmName: vmName.trim(),
                        backupAllVms: backupAllVms,
                        restoreAllLatestVms: restoreAllLatestVms,
                        restoreXmlPath: restoreXmlPath.trim(),
                      );
              }

              void updateDialog(void Function() updates) {
                setDialogState(() {
                  updates();
                  if ((frequency == ScheduleFrequency.hourly || frequency == ScheduleFrequency.every5Minutes) && timeController.text.contains(':')) {
                    timeController.text = _minuteFromScheduleTime(timeController.text);
                  }
                  refreshValidation();
                });
              }

              final selectedServer = serverId == null ? null : _serverById(serverId!);
              final vmOptions = selectedServer == null ? <VmEntry>[] : List<VmEntry>.from(_vmCacheByServerId[selectedServer.id] ?? <VmEntry>[]);
              final storageOptions = type == ScheduledJobType.restore ? _enabledStorages().where((storage) => storage.driverId != 'dummy').toList() : _enabledStorages();
              final restoreEntries = storageId == _selectedBackupStorageId ? _restoreEntries : <RestoreEntry>[];
              final restoreVmOptions = restoreEntries.map((entry) => entry.vmName).toSet().toList()..sort();
              final filteredRestoreEntries = vmName.trim().isEmpty || restoreAllLatestVms ? restoreEntries : restoreEntries.where((entry) => entry.vmName == vmName.trim()).toList();
              final selectedRestoreEntry = _restoreEntryByXmlPath(restoreEntries, restoreXmlPath);
              return AlertDialog(
                title: Text(existing == null ? 'Add ${type.name} schedule' : 'Edit schedule'),
                content: SizedBox(
                  width: 620,
                  child: SingleChildScrollView(
                    child: Column(
                      mainAxisSize: MainAxisSize.min,
                      children: [
                        SwitchListTile(contentPadding: EdgeInsets.zero, title: const Text('Enabled'), value: enabled, onChanged: (value) => setDialogState(() => enabled = value)),
                        SwitchListTile(
                          contentPadding: EdgeInsets.zero,
                          title: const Text('Wait for running jobs'),
                          value: waitForRunningJobs,
                          onChanged: (value) => setDialogState(() => waitForRunningJobs = value),
                        ),
                        const SizedBox(height: 12),
                        Row(
                          children: [
                            Expanded(
                              child: DropdownButtonFormField<ScheduleFrequency>(
                                initialValue: frequency,
                                decoration: _scheduleDecoration(context, labelText: 'Frequency'),
                                items: availableFrequencies.map((item) => DropdownMenuItem(value: item, child: Text(_frequencyLabel(item)))).toList(),
                                onChanged: (value) {
                                  if (value != null) {
                                    updateDialog(() {
                                      frequency = value;
                                      if (value == ScheduleFrequency.every5Minutes || value == ScheduleFrequency.hourly) {
                                        timeController.text = _minuteFromScheduleTime(timeController.text);
                                      } else if (!timeController.text.contains(':')) {
                                        timeController.text = '00:${_normalizeMinuteInput(timeController.text)}';
                                      }
                                    });
                                  }
                                },
                              ),
                            ),
                            const SizedBox(width: 12),
                            SizedBox(
                              width: 140,
                              child: TextField(
                                controller: timeController,
                                decoration: _scheduleDecoration(
                                  context,
                                  labelText: frequency == ScheduleFrequency.hourly || frequency == ScheduleFrequency.every5Minutes ? 'Minute' : 'Time',
                                  hintText: frequency == ScheduleFrequency.hourly || frequency == ScheduleFrequency.every5Minutes ? '15' : '23:30',
                                  invalid: invalidFields.contains('time'),
                                ),
                                keyboardType: frequency == ScheduleFrequency.hourly || frequency == ScheduleFrequency.every5Minutes ? TextInputType.number : TextInputType.datetime,
                                textInputAction: TextInputAction.next,
                                onChanged: (_) => updateDialog(() {}),
                              ),
                            ),
                          ],
                        ),
                        if (frequency == ScheduleFrequency.weekly) ...[
                          const SizedBox(height: 12),
                          Align(
                            alignment: Alignment.centerLeft,
                            child: Text('Weekdays', style: Theme.of(context).textTheme.labelLarge),
                          ),
                          const SizedBox(height: 8),
                          Container(
                            width: double.infinity,
                            padding: const EdgeInsets.all(8),
                            decoration: BoxDecoration(
                              border: Border.all(color: invalidFields.contains('weekdays') ? Theme.of(context).colorScheme.error : Colors.transparent),
                              borderRadius: BorderRadius.circular(4),
                            ),
                            child: Wrap(
                              spacing: 8,
                              children: <int>[1, 2, 3, 4, 5, 6, 7].map((day) {
                                return FilterChip(
                                  selected: weekdays.contains(day),
                                  label: Text(_formatWeekdays(<int>[day])),
                                  onSelected: (selected) {
                                    updateDialog(() {
                                      weekdays = List<int>.from(weekdays);
                                      if (selected) {
                                        weekdays.add(day);
                                        weekdays.sort();
                                      } else {
                                        weekdays.remove(day);
                                      }
                                    });
                                  },
                                );
                              }).toList(),
                            ),
                          ),
                        ],
                        const SizedBox(height: 12),
                        DropdownButtonFormField<String>(
                          initialValue: serverId,
                          decoration: _scheduleDecoration(context, labelText: 'Server', invalid: invalidFields.contains('server')),
                          items: _servers.map((server) => DropdownMenuItem(value: server.id, child: Text(server.name))).toList(),
                          onChanged: (value) {
                            updateDialog(() {
                              serverId = value;
                              vmName = '';
                            });
                            final server = value == null ? null : _serverById(value);
                            if (server != null) {
                              unawaited(
                                _loadVmInventory(server).then((_) {
                                  if (mounted) {
                                    updateDialog(() {});
                                  }
                                }),
                              );
                            }
                          },
                        ),
                        const SizedBox(height: 12),
                        DropdownButtonFormField<String>(
                          initialValue: storageId,
                          decoration: _scheduleDecoration(context, labelText: 'Storage', invalid: invalidFields.contains('storage')),
                          items: storageOptions.map((storage) => DropdownMenuItem(value: storage.id, child: Text(storage.name))).toList(),
                          onChanged: (value) {
                            updateDialog(() {
                              storageId = value;
                              restoreXmlPath = '';
                              vmName = '';
                            });
                            if (type == ScheduledJobType.restore && value != null) {
                              unawaited(
                                _setSelectedBackupStorage(value, refreshRestoreEntries: true).then((_) {
                                  if (mounted) {
                                    updateDialog(() {});
                                  }
                                }),
                              );
                            }
                          },
                        ),
                        const SizedBox(height: 12),
                        if (type == ScheduledJobType.backup)
                          Column(
                            children: [
                              SwitchListTile(
                                contentPadding: EdgeInsets.zero,
                                title: const Text('Back up all VMs on this server'),
                                value: backupAllVms,
                                onChanged: (value) => updateDialog(() {
                                  backupAllVms = value;
                                  if (value) {
                                    vmName = '';
                                  }
                                }),
                              ),
                              if (!backupAllVms)
                                DropdownButtonFormField<String>(
                                  initialValue: vmOptions.any((vm) => vm.name == vmName) ? vmName : null,
                                  decoration: _scheduleDecoration(context, labelText: 'VM', invalid: invalidFields.contains('vm')),
                                  items: vmOptions.map((vm) => DropdownMenuItem(value: vm.name, child: Text(vm.name))).toList(),
                                  onChanged: (value) => updateDialog(() => vmName = value ?? ''),
                                ),
                            ],
                          )
                        else ...[
                          SwitchListTile(
                            contentPadding: EdgeInsets.zero,
                            title: const Text('Restore latest XML for all VMs'),
                            value: restoreAllLatestVms,
                            onChanged: (value) => updateDialog(() {
                              restoreAllLatestVms = value;
                              if (value) {
                                vmName = '';
                                restoreXmlPath = ScheduledJob.latestRestoreXmlPath;
                              } else {
                                restoreXmlPath = '';
                              }
                            }),
                          ),
                          if (!restoreAllLatestVms) ...[
                            DropdownButtonFormField<String>(
                              initialValue: restoreVmOptions.contains(vmName) ? vmName : null,
                              decoration: _scheduleDecoration(context, labelText: 'VM', invalid: invalidFields.contains('vm')),
                              items: restoreVmOptions.map((name) => DropdownMenuItem(value: name, child: Text(name))).toList(),
                              onChanged: (value) {
                                updateDialog(() {
                                  vmName = value ?? '';
                                  backupAllVms = false;
                                  restoreXmlPath = '';
                                });
                              },
                            ),
                            const SizedBox(height: 12),
                            DropdownButtonFormField<String>(
                              isExpanded: true,
                              initialValue: restoreXmlPath == ScheduledJob.latestRestoreXmlPath || filteredRestoreEntries.any((entry) => entry.xmlPath == restoreXmlPath) ? restoreXmlPath : null,
                              decoration: _scheduleDecoration(context, labelText: 'Backup XML', invalid: invalidFields.contains('restoreXmlPath')),
                              items: [
                                const DropdownMenuItem(value: ScheduledJob.latestRestoreXmlPath, child: Text('Latest XML for selected VM')),
                                ...filteredRestoreEntries.map((entry) => DropdownMenuItem(value: entry.xmlPath, child: Text('${entry.vmName} • ${entry.timestamp}'))),
                              ],
                              onChanged: (value) => updateDialog(() => restoreXmlPath = value ?? ''),
                            ),
                          ],
                          const SizedBox(height: 12),
                          DropdownButtonFormField<String>(
                            initialValue: restoreDecision,
                            decoration: _scheduleDecoration(context, labelText: 'Existing VM handling'),
                            items: const [
                              DropdownMenuItem(value: 'overwrite', child: Text('Overwrite all')),
                              DropdownMenuItem(value: 'auto_rename', child: Text('Auto rename on conflict')),
                              DropdownMenuItem(value: 'define', child: Text('Define XML only')),
                            ],
                            onChanged: (value) => updateDialog(() => restoreDecision = value ?? 'overwrite'),
                          ),
                          if (selectedRestoreEntry != null) ...[
                            const SizedBox(height: 8),
                            Align(
                              alignment: Alignment.centerLeft,
                              child: Text(selectedRestoreEntry.xmlPath, style: Theme.of(context).textTheme.bodySmall),
                            ),
                          ],
                        ],
                      ],
                    ),
                  ),
                ),
                actions: [
                  SizedBox(
                    width: double.infinity,
                    child: Row(
                      children: [
                        Expanded(
                          child: validationError == null
                              ? const SizedBox.shrink()
                              : Text(validationError!, style: Theme.of(context).textTheme.bodySmall?.copyWith(color: Theme.of(context).colorScheme.error)),
                        ),
                        const SizedBox(width: 12),
                        TextButton(onPressed: () => Navigator.of(dialogContext).pop(), child: const Text('Cancel')),
                        const SizedBox(width: 8),
                        FilledButton(
                          onPressed: () {
                            final rawTime = timeController.text.trim();
                            final time = _normalizeScheduleTimeInput(rawTime, frequency);
                            final currentServerId = serverId?.trim() ?? '';
                            final currentStorageId = storageId?.trim() ?? '';
                            final currentVmName = vmName.trim();
                            final effectiveRestoreAllLatestVms = type == ScheduledJobType.restore && restoreAllLatestVms;
                            final currentRestoreXmlPath = effectiveRestoreAllLatestVms ? ScheduledJob.latestRestoreXmlPath : restoreXmlPath.trim();
                            final effectiveBackupAllVms = type == ScheduledJobType.backup && backupAllVms;
                            final name = _generateScheduleName(
                              type: type,
                              serverId: currentServerId,
                              storageId: currentStorageId,
                              vmName: currentVmName,
                              backupAllVms: effectiveBackupAllVms,
                              restoreAllLatestVms: effectiveRestoreAllLatestVms,
                            );
                            final weeklyDays = List<int>.from(weekdays)..sort();
                            final fields = _scheduleInvalidFields(
                              time: rawTime,
                              serverId: currentServerId,
                              storageId: currentStorageId,
                              frequency: frequency,
                              weekdays: weeklyDays,
                              type: type,
                              vmName: currentVmName,
                              backupAllVms: effectiveBackupAllVms,
                              restoreAllLatestVms: effectiveRestoreAllLatestVms,
                              restoreXmlPath: currentRestoreXmlPath,
                            );
                            final error = _scheduleValidationError(
                              time: rawTime,
                              serverId: currentServerId,
                              storageId: currentStorageId,
                              frequency: frequency,
                              weekdays: weeklyDays,
                              type: type,
                              vmName: currentVmName,
                              backupAllVms: effectiveBackupAllVms,
                              restoreAllLatestVms: effectiveRestoreAllLatestVms,
                              restoreXmlPath: currentRestoreXmlPath,
                            );
                            if (error != null) {
                              setDialogState(() {
                                validationError = error;
                                invalidFields = fields;
                              });
                              return;
                            }
                            Navigator.of(dialogContext).pop(
                              ScheduledJob(
                                id: existing?.id ?? 'schedule_${DateTime.now().microsecondsSinceEpoch}',
                                name: name,
                                enabled: enabled,
                                waitForRunningJobs: waitForRunningJobs,
                                type: type,
                                frequency: frequency,
                                time: time,
                                weekdays: frequency == ScheduleFrequency.weekly ? weeklyDays : <int>[],
                                serverId: currentServerId,
                                storageId: currentStorageId,
                                backupAllVms: effectiveBackupAllVms,
                                restoreAllLatestVms: effectiveRestoreAllLatestVms,
                                vmName: effectiveBackupAllVms || effectiveRestoreAllLatestVms ? '' : currentVmName,
                                restoreXmlPath: type == ScheduledJobType.restore ? currentRestoreXmlPath : '',
                                restoreDecision: type == ScheduledJobType.restore ? restoreDecision : '',
                              ),
                            );
                          },
                          child: const Text('Save'),
                        ),
                      ],
                    ),
                  ),
                ],
              );
            },
          );
        },
      );
    } finally {
      timeController.dispose();
    }
  }

  ServerConfig? _serverById(String id) {
    for (final server in _servers) {
      if (server.id == id) {
        return server;
      }
    }
    return null;
  }

  RestoreEntry? _restoreEntryByXmlPath(List<RestoreEntry> entries, String xmlPath) {
    for (final entry in entries) {
      if (entry.xmlPath == xmlPath) {
        return entry;
      }
    }
    return null;
  }

  InputDecoration _scheduleDecoration(BuildContext context, {required String labelText, String? hintText, bool invalid = false}) {
    final borderSide = invalid ? BorderSide(color: Theme.of(context).colorScheme.error, width: 1.6) : const BorderSide();
    final border = OutlineInputBorder(borderSide: borderSide);
    return InputDecoration(
      labelText: labelText,
      hintText: hintText,
      border: border,
      enabledBorder: border,
      focusedBorder: invalid ? OutlineInputBorder(borderSide: BorderSide(color: Theme.of(context).colorScheme.error, width: 2)) : null,
    );
  }

  String? _scheduleValidationError({
    required String time,
    required String serverId,
    required String storageId,
    required ScheduleFrequency frequency,
    required List<int> weekdays,
    required ScheduledJobType type,
    required String vmName,
    required bool backupAllVms,
    required bool restoreAllLatestVms,
    required String restoreXmlPath,
  }) {
    if (frequency == ScheduleFrequency.hourly || frequency == ScheduleFrequency.every5Minutes) {
      if (!_isValidMinute(time)) {
        return 'Enter a minute from 0 to 59.';
      }
    } else if (!RegExp(r'^([01]\d|2[0-3]):([0-5]\d)$').hasMatch(time)) {
      return 'Enter a valid time in HH:mm format.';
    }
    if (frequency == ScheduleFrequency.weekly && weekdays.isEmpty) {
      return 'Select at least one weekday.';
    }
    if (serverId.isEmpty) {
      return 'Select a server.';
    }
    if (storageId.isEmpty) {
      return 'Select a storage.';
    }
    if (type == ScheduledJobType.backup && !backupAllVms && vmName.isEmpty) {
      return 'Select a VM.';
    }
    if (type == ScheduledJobType.restore && !restoreAllLatestVms && vmName.isEmpty) {
      return 'Select a VM.';
    }
    if (type == ScheduledJobType.restore && !restoreAllLatestVms && restoreXmlPath.isEmpty) {
      return 'Select a backup XML option.';
    }
    return null;
  }

  Set<String> _scheduleInvalidFields({
    required String time,
    required String serverId,
    required String storageId,
    required ScheduleFrequency frequency,
    required List<int> weekdays,
    required ScheduledJobType type,
    required String vmName,
    required bool backupAllVms,
    required bool restoreAllLatestVms,
    required String restoreXmlPath,
  }) {
    final fields = <String>{};
    if (frequency == ScheduleFrequency.hourly || frequency == ScheduleFrequency.every5Minutes) {
      if (!_isValidMinute(time)) {
        fields.add('time');
      }
    } else if (!RegExp(r'^([01]\d|2[0-3]):([0-5]\d)$').hasMatch(time)) {
      fields.add('time');
    }
    if (frequency == ScheduleFrequency.weekly && weekdays.isEmpty) {
      fields.add('weekdays');
    }
    if (serverId.isEmpty) {
      fields.add('server');
    }
    if (storageId.isEmpty) {
      fields.add('storage');
    }
    if (type == ScheduledJobType.backup && !backupAllVms && vmName.isEmpty) {
      fields.add('vm');
    }
    if (type == ScheduledJobType.restore && !restoreAllLatestVms && vmName.isEmpty) {
      fields.add('vm');
    }
    if (type == ScheduledJobType.restore && !restoreAllLatestVms && restoreXmlPath.isEmpty) {
      fields.add('restoreXmlPath');
    }
    return fields;
  }

  String _generateScheduleName({
    required ScheduledJobType type,
    required String serverId,
    required String storageId,
    required String vmName,
    required bool backupAllVms,
    required bool restoreAllLatestVms,
  }) {
    final typeLabel = type == ScheduledJobType.backup ? 'Backup' : 'Restore';
    final serverName = _serverNameForId(serverId);
    final storageName = _storageNameForId(storageId);
    final vmLabel = backupAllVms || restoreAllLatestVms ? 'all VMs' : (vmName.trim().isEmpty ? 'VM' : vmName.trim());
    final direction = type == ScheduledJobType.backup ? 'to' : 'from';
    return '$typeLabel $vmLabel on $serverName $direction $storageName';
  }

  List<ScheduleFrequency> _availableScheduleFrequencies(ScheduleFrequency current) {
    final values = ScheduleFrequency.values.where((frequency) => frequency != ScheduleFrequency.every5Minutes || kDebugMode || current == ScheduleFrequency.every5Minutes).toList();
    return values;
  }

  String _frequencyLabel(ScheduleFrequency frequency) {
    return switch (frequency) {
      ScheduleFrequency.every5Minutes => 'Every 5 minutes',
      ScheduleFrequency.hourly => 'Hourly',
      ScheduleFrequency.daily => 'Daily',
      ScheduleFrequency.weekly => 'Weekly',
    };
  }

  bool _isValidMinute(String value) {
    final parsed = int.tryParse(value.trim());
    return parsed != null && parsed >= 0 && parsed <= 59;
  }

  String _normalizeMinuteInput(String value) {
    final parsed = int.tryParse(value.trim());
    if (parsed == null) {
      return '00';
    }
    return parsed.clamp(0, 59).toString().padLeft(2, '0');
  }

  String _minuteFromScheduleTime(String value) {
    final trimmed = value.trim();
    if (!trimmed.contains(':')) {
      if (trimmed == '*/5') {
        return '0';
      }
      return _isValidMinute(trimmed) ? trimmed : '';
    }
    final parts = trimmed.split(':');
    return parts.length == 2 ? parts[1] : '';
  }

  String _normalizeScheduleTimeInput(String value, ScheduleFrequency frequency) {
    if (frequency == ScheduleFrequency.every5Minutes) {
      return '00:${_normalizeMinuteInput(value)}';
    }
    if (frequency == ScheduleFrequency.hourly) {
      return '00:${_normalizeMinuteInput(value)}';
    }
    return value;
  }
}
