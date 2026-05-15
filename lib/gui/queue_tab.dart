part of 'main_screen.dart';

extension _BackupServerSetupQueueSection on _BackupServerSetupScreenState {
  List<Widget> _buildQueueSection(ColorScheme colorScheme) {
    final queue = List<ScheduleQueueEntry>.from(_latestScheduleQueue)..sort((a, b) => a.position.compareTo(b.position));
    final runningCount = queue.where((entry) => entry.status == 'running').length;
    final waitingCount = queue.length - runningCount;
    return [
      Row(
        children: [
          Expanded(child: Text('Schedule queue', style: Theme.of(context).textTheme.titleMedium)),
          IconButton.filledTonal(
            onPressed: _isLoadingScheduleQueue ? null : _refreshScheduleQueue,
            icon: _isLoadingScheduleQueue ? const SizedBox(width: 18, height: 18, child: CircularProgressIndicator(strokeWidth: 2)) : const Icon(Icons.refresh),
            tooltip: 'Refresh queue',
          ),
        ],
      ),
      Text('$runningCount running • $waitingCount waiting', style: Theme.of(context).textTheme.bodyMedium),
      const SizedBox(height: 16),
      if (queue.isEmpty)
        Card(
          elevation: 2,
          shadowColor: colorScheme.shadow.withValues(alpha: 0.2),
          shape: RoundedRectangleBorder(borderRadius: BorderRadius.circular(8)),
          child: Padding(
            padding: const EdgeInsets.all(20),
            child: Row(
              children: [
                Icon(Icons.check_circle_outline, color: colorScheme.primary),
                const SizedBox(width: 12),
                Expanded(child: Text('No schedule runs are waiting.', style: Theme.of(context).textTheme.bodyMedium)),
              ],
            ),
          ),
        )
      else
        ...queue.map((entry) => _buildQueueEntryCard(entry, colorScheme)),
    ];
  }

  Widget _buildQueueEntryCard(ScheduleQueueEntry entry, ColorScheme colorScheme) {
    final running = entry.status == 'running';
    final statusContainer = running ? colorScheme.primaryContainer : colorScheme.tertiaryContainer;
    final onStatusContainer = running ? colorScheme.onPrimaryContainer : colorScheme.onTertiaryContainer;
    return Padding(
      padding: const EdgeInsets.only(bottom: 12),
      child: Card(
        elevation: 2,
        shadowColor: colorScheme.shadow.withValues(alpha: 0.2),
        shape: RoundedRectangleBorder(borderRadius: BorderRadius.circular(8)),
        child: Padding(
          padding: const EdgeInsets.all(16),
          child: Column(
            crossAxisAlignment: CrossAxisAlignment.start,
            children: [
              Row(
                children: [
                  Container(
                    width: 34,
                    height: 34,
                    alignment: Alignment.center,
                    decoration: BoxDecoration(color: statusContainer, borderRadius: BorderRadius.circular(8)),
                    child: running
                        ? Icon(Icons.play_arrow, size: 20, color: onStatusContainer)
                        : Text(entry.position.toString(), style: Theme.of(context).textTheme.labelLarge?.copyWith(color: onStatusContainer)),
                  ),
                  const SizedBox(width: 12),
                  Expanded(
                    child: Column(
                      crossAxisAlignment: CrossAxisAlignment.start,
                      children: [
                        Text(entry.scheduleName, style: Theme.of(context).textTheme.titleSmall),
                        const SizedBox(height: 2),
                        Text('${_queueTypeLabel(entry)} • ${entry.manual ? 'Manual start' : 'Scheduled start'}', style: Theme.of(context).textTheme.bodySmall),
                      ],
                    ),
                  ),
                  const SizedBox(width: 12),
                  Container(
                    padding: const EdgeInsets.symmetric(horizontal: 10, vertical: 6),
                    decoration: BoxDecoration(color: statusContainer, borderRadius: BorderRadius.circular(8)),
                    child: Text(running ? 'Running' : 'Waiting', style: Theme.of(context).textTheme.labelMedium?.copyWith(color: onStatusContainer)),
                  ),
                  const SizedBox(width: 8),
                  IconButton.filledTonal(
                    onPressed: running && entry.jobId.trim().isEmpty ? null : () => _queueEntryAction(entry),
                    icon: Icon(running ? Icons.cancel_outlined : Icons.delete_outline),
                    tooltip: running ? 'Cancel running job' : 'Remove from queue',
                  ),
                ],
              ),
              const SizedBox(height: 14),
              Wrap(
                spacing: 10,
                runSpacing: 8,
                children: [
                  _buildQueueChip(Icons.dns_outlined, _queueServerLabel(entry), colorScheme),
                  _buildQueueChip(Icons.storage_outlined, _queueStorageLabel(entry), colorScheme),
                  _buildQueueChip(running ? Icons.play_circle_outline : Icons.schedule_outlined, _formatQueueTime(entry), colorScheme),
                  if (entry.jobId.trim().isNotEmpty) _buildQueueChip(Icons.tag, entry.jobId, colorScheme),
                ],
              ),
            ],
          ),
        ),
      ),
    );
  }

  Widget _buildQueueChip(IconData icon, String label, ColorScheme colorScheme) {
    return Container(
      padding: const EdgeInsets.symmetric(horizontal: 10, vertical: 7),
      decoration: BoxDecoration(color: colorScheme.surfaceContainerHighest, borderRadius: BorderRadius.circular(8)),
      child: Row(
        mainAxisSize: MainAxisSize.min,
        children: [
          Icon(icon, size: 16, color: colorScheme.onSurfaceVariant),
          const SizedBox(width: 6),
          Text(label, style: Theme.of(context).textTheme.bodySmall?.copyWith(color: colorScheme.onSurfaceVariant)),
        ],
      ),
    );
  }

  Future<void> _refreshScheduleQueue() async {
    if (_isLoadingScheduleQueue) {
      return;
    }
    _updateUi(() {
      _isLoadingScheduleQueue = true;
    });
    try {
      await _syncScheduleQueue(updateUi: false);
    } finally {
      if (mounted) {
        _updateUi(() {
          _isLoadingScheduleQueue = false;
        });
      }
    }
  }

  Future<void> _queueEntryAction(ScheduleQueueEntry entry) async {
    try {
      if (entry.status == 'running') {
        final jobId = entry.jobId.trim();
        if (jobId.isEmpty) {
          _showSnackBarError('Running queue entry has no job id.');
          return;
        }
        await _agentApiClient.cancelJob(jobId);
        _showSnackBarInfo('Schedule job cancel requested.');
      } else {
        await _agentApiClient.removeQueuedScheduleRun(entry.scheduleId);
        _showSnackBarInfo('Schedule run removed from queue.');
      }
      await _syncRunningJobs();
      await _syncScheduleQueue();
    } catch (error, stackTrace) {
      _logError('Queue action failed.', error, stackTrace);
      _showSnackBarError('Queue action failed: $error');
    }
  }

  String _queueTypeLabel(ScheduleQueueEntry entry) {
    return switch (entry.type) {
      'backup' => 'Backup',
      'restore' => 'Restore',
      _ => entry.type,
    };
  }

  String _queueServerLabel(ScheduleQueueEntry entry) {
    if (entry.serverName.trim().isNotEmpty) {
      return entry.serverName;
    }
    return entry.serverId;
  }

  String _queueStorageLabel(ScheduleQueueEntry entry) {
    if (entry.storageName.trim().isNotEmpty) {
      return entry.storageName;
    }
    return entry.storageId;
  }

  String _formatQueueTime(ScheduleQueueEntry entry) {
    final prefix = entry.status == 'running' ? 'Started' : 'Queued';
    return _formatQueueTimestamp(prefix, entry.queuedAt);
  }

  String _formatQueueTimestamp(String prefix, DateTime? queuedAt) {
    if (queuedAt == null) {
      return '$prefix time unknown';
    }
    final local = queuedAt.toLocal();
    final month = local.month.toString().padLeft(2, '0');
    final day = local.day.toString().padLeft(2, '0');
    final hour = local.hour.toString().padLeft(2, '0');
    final minute = local.minute.toString().padLeft(2, '0');
    return '$prefix ${local.year}-$month-$day $hour:$minute';
  }
}
