part of 'main_screen.dart';

const double _historyTableMinWidth = 834;
const double _historyDatePickerDialogWidth = 570;
const double _historyTimePickerDialogWidth = 600;
const double _historyPickerDialogHeight = 620;

extension _BackupServerSetupHistorySection on _BackupServerSetupScreenState {
  List<Widget> _buildHistorySection(ColorScheme colorScheme) {
    final allEntries = List<AgentJobHistoryEntry>.from(_jobHistory)..sort(_compareHistoryEntries);
    final filters = _effectiveHistoryQuickFilters(allEntries);
    final entries = allEntries.where((entry) => _historyEntryMatchesQuickFilters(entry, filters)).toList();
    final totalCount = entries.length;
    final successCount = entries.where((entry) => entry.state == AgentJobState.success).length;
    final failureCount = entries.where((entry) => entry.state == AgentJobState.failure).length;
    final canceledCount = entries.where((entry) => entry.state == AgentJobState.canceled).length;
    final unknownCount = entries.where((entry) => entry.state == AgentJobState.unknown).length;
    final summaryParts = <String>['$totalCount total', '$successCount success', '$failureCount failed', '$canceledCount canceled'];
    if (unknownCount > 0) {
      summaryParts.add('$unknownCount unknown');
    }
    return [
      Row(
        children: [
          Expanded(child: Text('Job history', style: Theme.of(context).textTheme.titleMedium)),
          IconButton.filledTonal(
            onPressed: _isLoadingJobHistory ? null : _refreshJobHistory,
            icon: _isLoadingJobHistory ? const SizedBox(width: 18, height: 18, child: CircularProgressIndicator(strokeWidth: 2)) : const Icon(Icons.refresh),
            tooltip: 'Refresh history',
          ),
        ],
      ),
      Text(summaryParts.join(' • '), style: Theme.of(context).textTheme.bodyMedium),
      const SizedBox(height: 16),
      _buildHistoryQuickFilters(allEntries, filters),
      const SizedBox(height: 16),
      if (_isLoadingJobHistory && allEntries.isEmpty)
        const Center(
          child: Padding(padding: EdgeInsets.all(24), child: CircularProgressIndicator()),
        )
      else if (allEntries.isEmpty)
        Card(
          elevation: 2,
          shadowColor: colorScheme.shadow.withValues(alpha: 0.2),
          shape: RoundedRectangleBorder(borderRadius: BorderRadius.circular(8)),
          child: Padding(
            padding: const EdgeInsets.all(20),
            child: Row(
              children: [
                Icon(Icons.history_toggle_off_outlined, color: colorScheme.primary),
                const SizedBox(width: 12),
                Expanded(child: Text('No completed jobs with result log records found.', style: Theme.of(context).textTheme.bodyMedium)),
              ],
            ),
          ),
        )
      else if (entries.isEmpty)
        Text('No jobs match the selected filters.', style: Theme.of(context).textTheme.bodyMedium)
      else
        _buildHistoryEntryList(entries, colorScheme),
    ];
  }

  Widget _buildHistoryQuickFilters(
    List<AgentJobHistoryEntry> entries,
    ({String state, String type, String vmName, String storage, DateTime? from, bool fromValid, DateTime? to, bool toValid}) filters,
  ) {
    final states = entries.map((entry) => entry.state).toSet().toList()..sort((a, b) => _historyStateLabel(a).toLowerCase().compareTo(_historyStateLabel(b).toLowerCase()));
    final types = entries.map((entry) => entry.type).toSet().toList()..sort((a, b) => _historyTypeLabel(a).toLowerCase().compareTo(_historyTypeLabel(b).toLowerCase()));
    final vmNames = entries.map((entry) => entry.vmName.trim()).where((value) => value.isNotEmpty).toSet().toList()..sort((a, b) => a.toLowerCase().compareTo(b.toLowerCase()));
    final storageKeys = entries.map(_historyStorageFilterValue).where((value) => value.isNotEmpty).toSet().toList()..sort((a, b) => a.toLowerCase().compareTo(b.toLowerCase()));
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
              width: 180,
              child: DropdownButtonFormField<String>(
                initialValue: filters.state,
                decoration: _scheduleDecoration(context, labelText: 'Status'),
                items: [
                  const DropdownMenuItem(value: '', child: Text('All status')),
                  ...states.map((state) => DropdownMenuItem(value: state.name, child: Text(_historyStateLabel(state)))),
                ],
                onChanged: (value) => _updateUi(() => _historyFilterState = value ?? ''),
              ),
            ),
            SizedBox(
              width: 180,
              child: DropdownButtonFormField<String>(
                initialValue: filters.type,
                decoration: _scheduleDecoration(context, labelText: 'Type'),
                items: [
                  const DropdownMenuItem(value: '', child: Text('All types')),
                  ...types.map((type) => DropdownMenuItem(value: type.name, child: Text(_historyTypeLabel(type)))),
                ],
                onChanged: (value) => _updateUi(() => _historyFilterType = value ?? ''),
              ),
            ),
            SizedBox(
              width: 220,
              child: DropdownButtonFormField<String>(
                isExpanded: true,
                initialValue: filters.vmName,
                decoration: _scheduleDecoration(context, labelText: 'VM'),
                items: [
                  const DropdownMenuItem(value: '', child: Text('All VMs')),
                  ...vmNames.map((name) => DropdownMenuItem(value: name, child: Text(name))),
                ],
                onChanged: (value) => _updateUi(() => _historyFilterVmName = value ?? ''),
              ),
            ),
            SizedBox(
              width: 220,
              child: DropdownButtonFormField<String>(
                isExpanded: true,
                initialValue: filters.storage,
                decoration: _scheduleDecoration(context, labelText: 'Storage'),
                items: [
                  const DropdownMenuItem(value: '', child: Text('All storage')),
                  ...storageKeys.map((key) => DropdownMenuItem(value: key, child: Text(key))),
                ],
                onChanged: (value) => _updateUi(() => _historyFilterStorage = value ?? ''),
              ),
            ),
          ],
        ),
        const SizedBox(height: 12),
        Wrap(
          spacing: 12,
          runSpacing: 12,
          crossAxisAlignment: WrapCrossAlignment.center,
          children: [
            _buildHistoryDateFilterField(label: 'From', controller: _historyFromController, valid: filters.fromValid),
            _buildHistoryDateFilterField(label: 'To', controller: _historyToController, valid: filters.toValid),
            TextButton.icon(onPressed: _hasHistoryQuickFilter ? () => _updateUi(_clearHistoryQuickFilters) : null, icon: const Icon(Icons.filter_alt_off_outlined), label: const Text('Clear')),
          ],
        ),
      ],
    );
  }

  Widget _buildHistoryDateFilterField({required String label, required TextEditingController controller, required bool valid}) {
    final defaultTime = label == 'To' ? const TimeOfDay(hour: 23, minute: 59) : const TimeOfDay(hour: 0, minute: 0);
    return SizedBox(
      width: 220,
      child: TextField(
        controller: controller,
        decoration: _scheduleDecoration(context, labelText: label).copyWith(
          errorText: valid ? null : 'Use YYYY-MM-DD HH:mm',
          suffixIcon: IconButton(
            icon: const Icon(Icons.event_outlined),
            tooltip: 'Pick $label date and time',
            onPressed: () => _pickHistoryDateTime(controller: controller, defaultTime: defaultTime, defaultDateOffset: label == 'From' ? const Duration(days: 7) : Duration.zero),
          ),
        ),
        keyboardType: TextInputType.datetime,
        onChanged: (_) => _updateUi(() {}),
      ),
    );
  }

  Widget _buildHistoryEntryList(List<AgentJobHistoryEntry> entries, ColorScheme colorScheme) {
    return Card(
      elevation: 2,
      shadowColor: colorScheme.shadow.withValues(alpha: 0.2),
      shape: RoundedRectangleBorder(borderRadius: BorderRadius.circular(16)),
      clipBehavior: Clip.antiAlias,
      child: LayoutBuilder(
        builder: (context, constraints) {
          final tableWidth = max(constraints.maxWidth, _historyTableMinWidth);
          return SingleChildScrollView(
            scrollDirection: Axis.horizontal,
            child: SizedBox(
              width: tableWidth,
              child: Column(
                children: [
                  _buildHistoryHeader(colorScheme),
                  const Divider(height: 1),
                  ListView.separated(
                    shrinkWrap: true,
                    physics: const NeverScrollableScrollPhysics(),
                    itemCount: entries.length,
                    separatorBuilder: (_, _) => const Divider(height: 1),
                    itemBuilder: (context, index) => _buildHistoryEntryItem(entries[index], colorScheme),
                  ),
                ],
              ),
            ),
          );
        },
      ),
    );
  }

  Widget _buildHistoryHeader(ColorScheme colorScheme) {
    final style = Theme.of(context).textTheme.labelMedium?.copyWith(color: colorScheme.onSurfaceVariant);
    return Padding(
      padding: const EdgeInsets.symmetric(horizontal: 12, vertical: 10),
      child: Row(
        children: [
          const SizedBox(width: 28),
          const SizedBox(width: 10),
          SizedBox(width: 84, child: _buildHistorySortHeader('Status', 'state', style, colorScheme)),
          const SizedBox(width: 10),
          SizedBox(width: 68, child: _buildHistorySortHeader('Type', 'type', style, colorScheme)),
          const SizedBox(width: 10),
          SizedBox(width: 124, child: _buildHistorySortHeader('Time', 'timestamp', style, colorScheme)),
          const SizedBox(width: 10),
          SizedBox(width: 140, child: _buildHistorySortHeader('VM', 'vmName', style, colorScheme)),
          const SizedBox(width: 10),
          SizedBox(width: 220, child: _buildHistorySortHeader('Storage', 'storage', style, colorScheme)),
          const Spacer(),
          const SizedBox(width: 36),
          const SizedBox(width: 8),
          const SizedBox(width: 32),
        ],
      ),
    );
  }

  Widget _buildHistorySortHeader(String label, String field, TextStyle? style, ColorScheme colorScheme) {
    final selected = _historySortField == field;
    return InkWell(
      onTap: () => _updateUi(() {
        if (_historySortField == field) {
          _historySortAscending = !_historySortAscending;
        } else {
          _historySortField = field;
          _historySortAscending = field != 'timestamp';
        }
      }),
      borderRadius: BorderRadius.circular(6),
      child: Padding(
        padding: const EdgeInsets.symmetric(horizontal: 2, vertical: 3),
        child: Row(
          mainAxisSize: MainAxisSize.min,
          children: [
            Flexible(
              child: Text(
                label,
                overflow: TextOverflow.ellipsis,
                style: selected ? style?.copyWith(color: colorScheme.primary) : style,
              ),
            ),
            const SizedBox(width: 3),
            Icon(selected ? (_historySortAscending ? Icons.arrow_upward : Icons.arrow_downward) : Icons.unfold_more, size: 14, color: selected ? colorScheme.primary : colorScheme.onSurfaceVariant),
          ],
        ),
      ),
    );
  }

  Widget _buildHistoryEntryItem(AgentJobHistoryEntry entry, ColorScheme colorScheme) {
    final expanded = _expandedHistoryJobIds.contains(entry.jobId);
    return InkWell(
      onTap: () => _updateUi(() {
        if (expanded) {
          _expandedHistoryJobIds.remove(entry.jobId);
        } else {
          _expandedHistoryJobIds.add(entry.jobId);
        }
      }),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          _buildHistoryEntryRow(entry, colorScheme, expanded: expanded),
          if (expanded) _buildHistoryEntryDetails(entry, colorScheme),
        ],
      ),
    );
  }

  Widget _buildHistoryEntryRow(AgentJobHistoryEntry entry, ColorScheme colorScheme, {required bool expanded}) {
    final statusColor = _historyStatusColor(entry.state, colorScheme);
    final statusIcon = _historyStatusIcon(entry.state);
    final vmName = entry.vmName.trim();
    final storage = entry.storage.trim().isNotEmpty ? entry.storage.trim() : entry.storageId.trim();
    return Padding(
      padding: const EdgeInsets.symmetric(horizontal: 12, vertical: 10),
      child: Row(
        crossAxisAlignment: CrossAxisAlignment.center,
        children: [
          Container(
            width: 28,
            height: 28,
            alignment: Alignment.center,
            decoration: BoxDecoration(color: statusColor.$1, borderRadius: BorderRadius.circular(8)),
            child: Icon(statusIcon, size: 18, color: statusColor.$2),
          ),
          const SizedBox(width: 10),
          SizedBox(
            width: 84,
            child: Text(
              _historyStateLabel(entry.state),
              overflow: TextOverflow.ellipsis,
              style: Theme.of(context).textTheme.labelLarge?.copyWith(color: statusColor.$2),
            ),
          ),
          const SizedBox(width: 10),
          SizedBox(
            width: 68,
            child: Text(_historyTypeLabel(entry.type), overflow: TextOverflow.ellipsis, style: Theme.of(context).textTheme.bodyMedium),
          ),
          const SizedBox(width: 10),
          SizedBox(
            width: 124,
            child: Text(_formatHistoryTimestamp(entry.timestamp), overflow: TextOverflow.ellipsis, style: Theme.of(context).textTheme.bodySmall),
          ),
          const SizedBox(width: 10),
          SizedBox(
            width: 140,
            child: Text(
              vmName.isEmpty ? '-' : vmName,
              overflow: TextOverflow.ellipsis,
              style: Theme.of(context).textTheme.bodyMedium?.copyWith(color: vmName.isEmpty ? colorScheme.outline : null),
            ),
          ),
          const SizedBox(width: 10),
          SizedBox(
            width: 220,
            child: Text(
              storage.isEmpty ? '-' : storage,
              overflow: TextOverflow.ellipsis,
              style: Theme.of(context).textTheme.bodyMedium?.copyWith(color: storage.isEmpty ? colorScheme.outline : null),
            ),
          ),
          const Spacer(),
          const SizedBox(width: 8),
          SizedBox(
            width: 36,
            height: 36,
            child: IconButton(
              padding: EdgeInsets.zero,
              tooltip: 'Open job log',
              onPressed: _loadingHistoryLogJobIds.contains(entry.jobId) ? null : () => _openHistoryJobLog(entry),
              icon: _loadingHistoryLogJobIds.contains(entry.jobId) ? const SizedBox(width: 18, height: 18, child: CircularProgressIndicator(strokeWidth: 2)) : const Icon(Icons.download_outlined),
            ),
          ),
          const SizedBox(width: 8),
          Icon(expanded ? Icons.expand_less : Icons.expand_more, color: colorScheme.onSurfaceVariant),
        ],
      ),
    );
  }

  Future<void> _openHistoryJobLog(AgentJobHistoryEntry entry) async {
    if (_loadingHistoryLogJobIds.contains(entry.jobId)) {
      return;
    }
    _updateUi(() => _loadingHistoryLogJobIds.add(entry.jobId));
    try {
      final logFile = await _agentApiClient.fetchJobHistoryLog(entry.jobId);
      if (!mounted) {
        return;
      }
      await _showHistoryJobLogDialog(entry: entry, logFile: logFile);
    } catch (error, stackTrace) {
      _logError('Failed to load job log.', error, stackTrace);
      if (mounted) {
        _showSnackBarError('Unable to load job log: $error');
      }
    } finally {
      if (mounted) {
        _updateUi(() => _loadingHistoryLogJobIds.remove(entry.jobId));
      }
    }
  }

  Future<void> _showHistoryJobLogDialog({required AgentJobHistoryEntry entry, required AgentJobLogFile logFile}) async {
    final title = logFile.fileName.isEmpty ? entry.jobId : logFile.fileName;
    await showDialog<void>(
      context: context,
      builder: (context) {
        return AlertDialog(
          title: Text(title, overflow: TextOverflow.ellipsis),
          content: SizedBox(
            width: min(MediaQuery.sizeOf(context).width * 0.82, 1100),
            height: min(MediaQuery.sizeOf(context).height * 0.72, 720),
            child: _HistoryJobLogViewer(content: logFile.content),
          ),
          actions: [
            TextButton.icon(
              onPressed: () {
                unawaited(Clipboard.setData(ClipboardData(text: logFile.content)));
                _showSnackBar('Job log copied to clipboard.');
              },
              icon: const Icon(Icons.copy_outlined),
              label: const Text('Copy'),
            ),
            TextButton.icon(onPressed: () => _saveHistoryJobLog(logFile), icon: const Icon(Icons.download_outlined), label: const Text('Download')),
            TextButton(onPressed: () => Navigator.of(context).pop(), child: const Text('Close')),
          ],
        );
      },
    );
  }

  Future<void> _saveHistoryJobLog(AgentJobLogFile logFile) async {
    try {
      final fileName = logFile.fileName.trim();
      final location = await getSaveLocation(
        suggestedName: fileName.isEmpty ? 'job-history.log' : fileName,
        acceptedTypeGroups: const [
          XTypeGroup(label: 'Log files', extensions: ['log', 'txt']),
        ],
      );
      if (location == null) {
        return;
      }
      await File(location.path).writeAsString(logFile.content);
      if (mounted) {
        _showSnackBar('Job log saved.');
      }
    } catch (error, stackTrace) {
      _logError('Failed to save job log.', error, stackTrace);
      if (mounted) {
        _showSnackBarError('Unable to save job log: $error');
      }
    }
  }

  Widget _buildHistoryEntryDetails(AgentJobHistoryEntry entry, ColorScheme colorScheme) {
    final fields = _historyJsonFields(entry).where((field) => field.$2.trim().isNotEmpty).toList();
    return Container(
      width: double.infinity,
      padding: const EdgeInsets.fromLTRB(50, 0, 12, 14),
      child: DecoratedBox(
        decoration: BoxDecoration(color: colorScheme.surfaceContainerHighest.withValues(alpha: 0.55), borderRadius: BorderRadius.circular(8)),
        child: Column(
          children: [
            for (var index = 0; index < fields.length; index++) ...[_buildHistoryDetailField(fields[index].$1, fields[index].$2, colorScheme), if (index < fields.length - 1) const Divider(height: 1)],
          ],
        ),
      ),
    );
  }

  Widget _buildHistoryDetailField(String label, String value, ColorScheme colorScheme) {
    return Padding(
      padding: const EdgeInsets.symmetric(horizontal: 12, vertical: 8),
      child: Row(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          SizedBox(
            width: 190,
            child: Text(label, style: Theme.of(context).textTheme.labelMedium?.copyWith(color: colorScheme.onSurfaceVariant)),
          ),
          const SizedBox(width: 12),
          Expanded(child: SelectableText(value.isEmpty ? '-' : value, style: Theme.of(context).textTheme.bodyMedium)),
        ],
      ),
    );
  }

  (Color, Color) _historyStatusColor(AgentJobState state, ColorScheme colorScheme) {
    return switch (state) {
      AgentJobState.success => (colorScheme.primaryContainer, colorScheme.onPrimaryContainer),
      AgentJobState.failure => (colorScheme.errorContainer, colorScheme.onErrorContainer),
      AgentJobState.canceled => (colorScheme.tertiaryContainer, colorScheme.onTertiaryContainer),
      AgentJobState.running => (colorScheme.secondaryContainer, colorScheme.onSecondaryContainer),
      AgentJobState.unknown => (colorScheme.surfaceContainerHighest, colorScheme.onSurfaceVariant),
    };
  }

  IconData _historyStatusIcon(AgentJobState state) {
    return switch (state) {
      AgentJobState.success => Icons.check_circle_outline,
      AgentJobState.failure => Icons.error_outline,
      AgentJobState.canceled => Icons.cancel_outlined,
      AgentJobState.running => Icons.play_circle_outline,
      AgentJobState.unknown => Icons.help_outline,
    };
  }

  String _historyTypeLabel(AgentJobType type) {
    return switch (type) {
      AgentJobType.backup => 'Backup',
      AgentJobType.restore => 'Restore',
      AgentJobType.sanity => 'Check',
      AgentJobType.unknown => 'Unknown',
    };
  }

  String _historyStateLabel(AgentJobState state) {
    return switch (state) {
      AgentJobState.success => 'Success',
      AgentJobState.failure => 'Failed',
      AgentJobState.canceled => 'Canceled',
      AgentJobState.running => 'Running',
      AgentJobState.unknown => 'Unknown',
    };
  }

  List<(String, String)> _historyJsonFields(AgentJobHistoryEntry entry) {
    return entry.fields.entries.map((field) => (field.key, _historyFieldValue(field.value))).toList();
  }

  String _historyFieldValue(Object? value) {
    if (value == null) {
      return '';
    }
    if (value is String) {
      return value;
    }
    if (value is num || value is bool) {
      return value.toString();
    }
    return jsonEncode(value);
  }

  bool get _hasHistoryQuickFilter {
    return _historyFilterState.isNotEmpty ||
        _historyFilterType.isNotEmpty ||
        _historyFilterVmName.isNotEmpty ||
        _historyFilterStorage.isNotEmpty ||
        _historyFromController.text.trim().isNotEmpty ||
        _historyToController.text.trim().isNotEmpty;
  }

  void _clearHistoryQuickFilters() {
    _historyFilterState = '';
    _historyFilterType = '';
    _historyFilterVmName = '';
    _historyFilterStorage = '';
    _historyFromController.clear();
    _historyToController.clear();
  }

  ({String state, String type, String vmName, String storage, DateTime? from, bool fromValid, DateTime? to, bool toValid}) _effectiveHistoryQuickFilters(List<AgentJobHistoryEntry> entries) {
    final states = entries.map((entry) => entry.state.name).toSet();
    final types = entries.map((entry) => entry.type.name).toSet();
    final vmNames = entries.map((entry) => entry.vmName.trim()).where((value) => value.isNotEmpty).toSet();
    final storageKeys = entries.map(_historyStorageFilterValue).where((value) => value.isNotEmpty).toSet();
    final fromText = _historyFromController.text.trim();
    final toText = _historyToController.text.trim();
    final from = _parseHistoryDateTimeFilter(fromText);
    final to = _parseHistoryDateTimeFilter(toText);
    return (
      state: states.contains(_historyFilterState) ? _historyFilterState : '',
      type: types.contains(_historyFilterType) ? _historyFilterType : '',
      vmName: vmNames.contains(_historyFilterVmName) ? _historyFilterVmName : '',
      storage: storageKeys.contains(_historyFilterStorage) ? _historyFilterStorage : '',
      from: from,
      fromValid: fromText.isEmpty || from != null,
      to: to,
      toValid: toText.isEmpty || to != null,
    );
  }

  bool _historyEntryMatchesQuickFilters(AgentJobHistoryEntry entry, ({String state, String type, String vmName, String storage, DateTime? from, bool fromValid, DateTime? to, bool toValid}) filters) {
    if (!filters.fromValid || !filters.toValid) {
      return false;
    }
    if (filters.state.isNotEmpty && entry.state.name != filters.state) {
      return false;
    }
    if (filters.type.isNotEmpty && entry.type.name != filters.type) {
      return false;
    }
    if (filters.vmName.isNotEmpty && entry.vmName.trim() != filters.vmName) {
      return false;
    }
    if (filters.storage.isNotEmpty && _historyStorageFilterValue(entry) != filters.storage) {
      return false;
    }
    final localTimestamp = entry.timestamp.toLocal();
    if (filters.from != null && localTimestamp.isBefore(filters.from!)) {
      return false;
    }
    if (filters.to != null && localTimestamp.isAfter(filters.to!.add(const Duration(minutes: 1)).subtract(const Duration(milliseconds: 1)))) {
      return false;
    }
    return true;
  }

  DateTime? _parseHistoryDateTimeFilter(String value) {
    final match = RegExp(r'^(\d{4})-(\d{2})-(\d{2}) ([01]\d|2[0-3]):([0-5]\d)$').firstMatch(value);
    if (match == null) {
      return null;
    }
    final year = int.parse(match.group(1)!);
    final month = int.parse(match.group(2)!);
    final day = int.parse(match.group(3)!);
    final hour = int.parse(match.group(4)!);
    final minute = int.parse(match.group(5)!);
    final parsed = DateTime(year, month, day, hour, minute);
    if (parsed.year != year || parsed.month != month || parsed.day != day || parsed.hour != hour || parsed.minute != minute) {
      return null;
    }
    return parsed;
  }

  Future<void> _pickHistoryDateTime({required TextEditingController controller, required TimeOfDay defaultTime, required Duration defaultDateOffset}) async {
    final parsed = _parseHistoryDateTimeFilter(controller.text.trim());
    final initial = parsed ?? DateTime.now().subtract(defaultDateOffset);
    final date = await showDatePicker(context: context, initialDate: initial, firstDate: DateTime(2000), lastDate: DateTime(2100), builder: _buildHistoryDatePickerDialog);
    if (date == null || !mounted) {
      return;
    }
    final time = await showTimePicker(context: context, initialTime: parsed == null ? defaultTime : TimeOfDay.fromDateTime(initial), builder: _buildHistoryTimePickerDialog);
    if (time == null || !mounted) {
      return;
    }
    final selected = DateTime(date.year, date.month, date.day, time.hour, time.minute);
    _updateUi(() {
      controller.text = _formatHistoryDateTimeInput(selected);
    });
  }

  Widget _buildHistoryDatePickerDialog(BuildContext context, Widget? child) {
    return _buildHistoryPickerDialog(width: _historyDatePickerDialogWidth, child: child);
  }

  Widget _buildHistoryTimePickerDialog(BuildContext context, Widget? child) {
    return _buildHistoryPickerDialog(width: _historyTimePickerDialogWidth, child: child);
  }

  Widget _buildHistoryPickerDialog({required double width, required Widget? child}) {
    return Center(
      child: SizedBox(
        width: width,
        height: _historyPickerDialogHeight,
        child: Theme(
          data: Theme.of(context).copyWith(
            dialogTheme: DialogThemeData(constraints: BoxConstraints.tightFor(width: width)),
          ),
          child: child ?? const SizedBox.shrink(),
        ),
      ),
    );
  }

  String _formatHistoryDateTimeInput(DateTime timestamp) {
    final local = timestamp.toLocal();
    final month = local.month.toString().padLeft(2, '0');
    final day = local.day.toString().padLeft(2, '0');
    final hour = local.hour.toString().padLeft(2, '0');
    final minute = local.minute.toString().padLeft(2, '0');
    return '${local.year}-$month-$day $hour:$minute';
  }

  String _historyStorageFilterValue(AgentJobHistoryEntry entry) {
    final storage = entry.storage.trim();
    if (storage.isNotEmpty) {
      return storage;
    }
    return entry.storageId.trim();
  }

  int _compareHistoryEntries(AgentJobHistoryEntry a, AgentJobHistoryEntry b) {
    final result = switch (_historySortField) {
      'state' => _historyStateLabel(a.state).toLowerCase().compareTo(_historyStateLabel(b.state).toLowerCase()),
      'type' => _historyTypeLabel(a.type).toLowerCase().compareTo(_historyTypeLabel(b.type).toLowerCase()),
      'vmName' => a.vmName.toLowerCase().compareTo(b.vmName.toLowerCase()),
      'storage' => _historyStorageFilterValue(a).toLowerCase().compareTo(_historyStorageFilterValue(b).toLowerCase()),
      _ => a.timestamp.compareTo(b.timestamp),
    };
    final normalized = result == 0 ? b.timestamp.compareTo(a.timestamp) : result;
    return _historySortAscending ? normalized : -normalized;
  }

  String _formatHistoryTimestamp(DateTime timestamp) {
    final local = timestamp.toLocal();
    final month = local.month.toString().padLeft(2, '0');
    final day = local.day.toString().padLeft(2, '0');
    final hour = local.hour.toString().padLeft(2, '0');
    final minute = local.minute.toString().padLeft(2, '0');
    return '${local.year}-$month-$day $hour:$minute';
  }
}

class _HistoryJobLogViewer extends StatefulWidget {
  const _HistoryJobLogViewer({required this.content});

  final String content;

  @override
  State<_HistoryJobLogViewer> createState() => _HistoryJobLogViewerState();
}

class _HistoryJobLogViewerState extends State<_HistoryJobLogViewer> {
  late final ScrollController _scrollController;
  late final ScrollController _horizontalScrollController;
  late final FocusNode _focusNode;
  late final List<String> _lines;
  late final int _maxLineLength;

  @override
  void initState() {
    super.initState();
    _scrollController = ScrollController();
    _horizontalScrollController = ScrollController();
    _focusNode = FocusNode();
    _lines = widget.content.isEmpty ? const <String>['Log file is empty.'] : const LineSplitter().convert(widget.content);
    _maxLineLength = _lines.fold<int>(0, (length, line) => max(length, line.length));
  }

  @override
  void dispose() {
    _scrollController.dispose();
    _horizontalScrollController.dispose();
    _focusNode.dispose();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    final colorScheme = Theme.of(context).colorScheme;
    final textStyle = Theme.of(context).textTheme.bodySmall?.copyWith(fontFamily: 'monospace');
    final lineNumberStyle = textStyle?.copyWith(color: colorScheme.onSurfaceVariant);
    final lineNumberWidth = max(44.0, (_lines.length.toString().length * 8.0) + 18);
    return DecoratedBox(
      decoration: BoxDecoration(color: colorScheme.surfaceContainerHighest.withValues(alpha: 0.5), borderRadius: BorderRadius.circular(8)),
      child: Focus(
        focusNode: _focusNode,
        autofocus: true,
        onKeyEvent: _handleKeyEvent,
        child: GestureDetector(
          behavior: HitTestBehavior.opaque,
          onTap: _focusNode.requestFocus,
          child: LayoutBuilder(
            builder: (context, constraints) {
              final contentWidth = max(constraints.maxWidth, lineNumberWidth + 48 + (_maxLineLength * 7.5));
              return Scrollbar(
                controller: _scrollController,
                thumbVisibility: true,
                notificationPredicate: (notification) => notification.metrics.axis == Axis.vertical,
                child: Scrollbar(
                  controller: _horizontalScrollController,
                  thumbVisibility: true,
                  notificationPredicate: (notification) => notification.metrics.axis == Axis.horizontal,
                  child: SingleChildScrollView(
                    controller: _horizontalScrollController,
                    scrollDirection: Axis.horizontal,
                    child: SizedBox(
                      width: contentWidth,
                      child: SelectionArea(
                        child: ListView.builder(
                          controller: _scrollController,
                          padding: const EdgeInsets.symmetric(vertical: 8),
                          itemCount: _lines.length,
                          itemBuilder: (context, index) {
                            return Padding(
                              padding: const EdgeInsets.symmetric(horizontal: 12, vertical: 2),
                              child: Row(
                                crossAxisAlignment: CrossAxisAlignment.start,
                                children: [
                                  SizedBox(
                                    width: lineNumberWidth,
                                    child: SelectionContainer.disabled(
                                      child: Text((index + 1).toString(), textAlign: TextAlign.right, style: lineNumberStyle),
                                    ),
                                  ),
                                  const SizedBox(width: 12),
                                  Expanded(child: Text(_lines[index], maxLines: 1, softWrap: false, style: textStyle)),
                                ],
                              ),
                            );
                          },
                        ),
                      ),
                    ),
                  ),
                ),
              );
            },
          ),
        ),
      ),
    );
  }

  KeyEventResult _handleKeyEvent(FocusNode node, KeyEvent event) {
    if (event is! KeyDownEvent && event is! KeyRepeatEvent) {
      return KeyEventResult.ignored;
    }
    switch (event.logicalKey) {
      case LogicalKeyboardKey.arrowUp:
        _scrollBy(_scrollController, -40);
        return KeyEventResult.handled;
      case LogicalKeyboardKey.arrowDown:
        _scrollBy(_scrollController, 40);
        return KeyEventResult.handled;
      case LogicalKeyboardKey.arrowLeft:
        _scrollBy(_horizontalScrollController, -60);
        return KeyEventResult.handled;
      case LogicalKeyboardKey.arrowRight:
        _scrollBy(_horizontalScrollController, 60);
        return KeyEventResult.handled;
      default:
        return KeyEventResult.ignored;
    }
  }

  void _scrollBy(ScrollController controller, double delta) {
    if (!controller.hasClients) {
      return;
    }
    final target = (controller.offset + delta).clamp(controller.position.minScrollExtent, controller.position.maxScrollExtent);
    controller.jumpTo(target);
  }
}
