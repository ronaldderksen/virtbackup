import 'package:flutter_test/flutter_test.dart';
import 'package:virtbackup/common/models.dart';
import 'package:virtbackup/common/settings.dart';

BackupStorage storage({String id = 'drive', String name = 'Drive', String root = '/original', String accessToken = 'old', String refreshToken = 'refresh', bool enabled = true, int concurrency = 8}) =>
    BackupStorage(
      id: id,
      name: name,
      driverId: 'gdrive',
      enabled: enabled,
      params: {'rootPath': root, 'accessToken': accessToken, 'refreshToken': refreshToken, 'expiresAt': '2026-10-26T12:00:00Z'},
      uploadConcurrency: concurrency,
      downloadConcurrency: concurrency,
    );

void main() {
  test('refresh preserves current storage properties and unrelated storage edits', () {
    final previous = AppSettings.empty().copyWith(
      storage: [
        storage(),
        storage(id: 'other'),
      ],
      backupStorageId: 'drive',
    );
    final updated = previous.copyWith(
      storage: [
        storage(accessToken: 'new'),
        storage(id: 'other'),
      ],
    );
    final current = previous.copyWith(
      storage: [
        storage(name: 'Renamed', enabled: false, concurrency: 3),
        storage(id: 'other', root: '/changed'),
      ],
    );
    final merged = current.mergeStorageTokenRefresh(previous: previous, updated: updated);
    expect(merged.storage.first.name, 'Renamed');
    expect(merged.storage.first.enabled, isFalse);
    expect(merged.storage.first.uploadConcurrency, 3);
    expect(merged.storage.first.params['accessToken'], 'new');
    expect(merged.storage.last.params['rootPath'], '/changed');
  });

  test('refresh cannot overwrite credentials or paths edited during a job', () {
    final previous = AppSettings.empty().copyWith(storage: [storage()], backupStorageId: 'drive');
    final updated = previous.copyWith(storage: [storage(accessToken: 'obsolete')]);
    for (final currentStorage in [storage(root: '/changed'), storage(refreshToken: 'reconnected'), storage(accessToken: 'newer')]) {
      final current = previous.copyWith(storage: [currentStorage]);
      expect(current.mergeStorageTokenRefresh(previous: previous, updated: updated), same(current));
    }
  });

  test('refresh cannot recreate a deleted storage', () {
    final previous = AppSettings.empty().copyWith(storage: [storage()], backupStorageId: 'drive');
    final current = previous.copyWith(storage: []);
    expect(
      current.mergeStorageTokenRefresh(
        previous: previous,
        updated: previous.copyWith(storage: [storage(accessToken: 'new')]),
      ),
      same(current),
    );
  });
}
