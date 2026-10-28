/* Exercise the production listing code with deterministic libssh2, socket and
   monotonic-clock stubs. The shared variant also tests the real Dart FFI/retry. */
#define clock_gettime test_clock_gettime
#define select test_select
#define shutdown test_shutdown
#define libssh2_sftp_mkdir_ex test_sftp_mkdir_ex
#define libssh2_sftp_rename_ex test_sftp_rename_ex
#define libssh2_sftp_read test_sftp_read
#define libssh2_sftp_write test_sftp_write
#define libssh2_sftp_seek64 test_sftp_seek64
#define libssh2_sftp_open_ex test_sftp_open_ex
#define libssh2_sftp_readdir_ex test_sftp_readdir_ex
#define libssh2_sftp_close_handle test_sftp_close_handle
#define libssh2_session_get_blocking test_get_blocking
#define libssh2_session_set_blocking test_set_blocking
#define libssh2_session_last_errno test_last_errno
#define libssh2_session_block_directions test_block_directions
#define libssh2_sftp_last_error test_sftp_last_error
#ifdef VB_SFTP_TEST_SHARED
#define vb_sftp_connect test_unused_real_connect
#define vb_sftp_disconnect test_unused_real_disconnect
#endif
#include "../virtbackup_native.c"
#include <assert.h>

static long long test_now;
static int blocking = 1;
static int last_error;
static unsigned long sftp_status;
static int aborted;
static int opened;
static int closed;
static int read_count;
static int total_entries;
static int entry_delay;
static int opening_again;
static int clock_error;
static int socket_error;
static int interrupt_once;
static int open_stall;
static int read_stall;
static int close_stall;
static int missing_on_open;
static int missing_on_read;
static int connections;
static int disconnections;
static int fake_handle;
static int fake_file_handle;
static int transfer_mode;
static int transfer_stall;
static int transfer_delay;
static int transfer_offset;
static int transfer_error;
static int transfer_zero_write;

int test_sftp_mkdir_ex(LIBSSH2_SFTP *sftp, const char *path, unsigned int length, long mode) {
  (void)sftp; (void)path; (void)length; (void)mode; return 0;
}
int test_sftp_rename_ex(LIBSSH2_SFTP *sftp, const char *from, unsigned int from_len, const char *to, unsigned int to_len, long flags) {
  (void)sftp; (void)from; (void)from_len; (void)to; (void)to_len; (void)flags; return 0;
}
void test_sftp_seek64(LIBSSH2_SFTP_HANDLE *handle, libssh2_uint64_t offset) {
  (void)handle; transfer_offset = (int)offset;
}
ssize_t test_sftp_read(LIBSSH2_SFTP_HANDLE *handle, char *buffer, size_t length) {
  (void)handle; (void)length;
  assert(blocking == 0);
  if (transfer_stall) return LIBSSH2_ERROR_EAGAIN;
  if (transfer_error) return LIBSSH2_ERROR_SOCKET_RECV;
  if (transfer_offset >= 4) return 0;
  test_now += transfer_delay;
  *buffer = "blob"[transfer_offset++];
  return 1;
}
ssize_t test_sftp_write(LIBSSH2_SFTP_HANDLE *handle, const char *buffer, size_t length) {
  (void)handle; (void)buffer; (void)length;
  assert(blocking == 0);
  if (transfer_stall) return LIBSSH2_ERROR_EAGAIN;
  if (transfer_error) return LIBSSH2_ERROR_SOCKET_RECV;
  if (transfer_zero_write) return 0;
  test_now += transfer_delay;
  return 1;
}

int test_clock_gettime(clockid_t clock, struct timespec *now) {
  assert(clock == CLOCK_MONOTONIC);
  if (clock_error) return -1;
  now->tv_sec = test_now / 1000;
  now->tv_nsec = (test_now % 1000) * 1000000;
  return 0;
}

int test_get_blocking(LIBSSH2_SESSION *session) { (void)session; return blocking; }
void test_set_blocking(LIBSSH2_SESSION *session, int value) { (void)session; blocking = value; }
int test_last_errno(LIBSSH2_SESSION *session) { (void)session; return last_error; }
int test_block_directions(LIBSSH2_SESSION *session) { (void)session; return LIBSSH2_SESSION_BLOCK_INBOUND; }
unsigned long test_sftp_last_error(LIBSSH2_SFTP *sftp) { (void)sftp; return sftp_status; }
int test_shutdown(int socket, int how) { (void)socket; assert(how == SHUT_RDWR); aborted++; return 0; }

int test_select(int nfds, fd_set *reads, fd_set *writes, fd_set *errors, struct timeval *timeout) {
  (void)nfds; (void)reads; (void)writes; (void)errors;
  assert(blocking == 0);
  if (interrupt_once) { interrupt_once = 0; test_now++; errno = EINTR; return -1; }
  if (socket_error) { errno = EBADF; return -1; }
  if (opening_again) { opening_again = 0; test_now++; return 1; }
  test_now += timeout->tv_sec * 1000 + timeout->tv_usec / 1000;
  return 0;
}

LIBSSH2_SFTP_HANDLE *test_sftp_open_ex(LIBSSH2_SFTP *sftp, const char *path, unsigned int path_len, unsigned long flags, long mode, int open_type) {
  (void)sftp; (void)path; (void)path_len; (void)flags; (void)mode; (void)open_type;
  if (open_type == LIBSSH2_SFTP_OPENFILE) {
    transfer_offset = 0;
    return (LIBSSH2_SFTP_HANDLE *)&fake_file_handle;
  }
  assert(blocking == 0);
  if (missing_on_open) { last_error = LIBSSH2_ERROR_SFTP_PROTOCOL; sftp_status = LIBSSH2_FX_NO_SUCH_FILE; return NULL; }
  if (opening_again || open_stall) { last_error = LIBSSH2_ERROR_EAGAIN; return NULL; }
  opened++;
  return (LIBSSH2_SFTP_HANDLE *)&fake_handle;
}

int test_sftp_readdir_ex(LIBSSH2_SFTP_HANDLE *handle, char *buffer, size_t capacity, char *longentry, size_t longcapacity, LIBSSH2_SFTP_ATTRIBUTES *attrs) {
  (void)handle; (void)capacity; (void)longentry; (void)longcapacity;
  assert(blocking == 0);
  if (missing_on_read && read_count) { sftp_status = LIBSSH2_FX_NO_SUCH_FILE; return LIBSSH2_ERROR_SFTP_PROTOCOL; }
  if (read_stall && read_count) return LIBSSH2_ERROR_EAGAIN;
  if (read_count >= total_entries) return 0;
  test_now += entry_delay;
  read_count++;
  memcpy(buffer, "blob", 4);
  attrs->flags = LIBSSH2_SFTP_ATTR_PERMISSIONS;
  attrs->permissions = S_IFREG | 0600;
  return 4;
}

int test_sftp_close_handle(LIBSSH2_SFTP_HANDLE *handle) {
  if (handle == (LIBSSH2_SFTP_HANDLE *)&fake_file_handle) return 0;
  assert(blocking == 0);
  if (close_stall && !aborted) return LIBSSH2_ERROR_EAGAIN;
  assert(!closed);
  closed++;
  // Cleanup must not replace the operation's captured failure status.
  sftp_status = LIBSSH2_FX_NO_SUCH_FILE;
  return 0;
}

static vb_sftp_session test_session(void) {
  vb_sftp_session session = {0};
  session.sock = 3;
  session.session = (LIBSSH2_SESSION *)&fake_handle;
  session.sftp = (LIBSSH2_SFTP *)&fake_handle;
  return session;
}

#ifdef VB_SFTP_TEST_SHARED
#undef vb_sftp_connect
#undef vb_sftp_disconnect
void *vb_sftp_connect(const char *host, int port, const char *user, const char *password) {
  (void)host; (void)port; (void)user; (void)password;
  connections++;
  vb_sftp_session *session = malloc(sizeof(*session));
  *session = test_session();
  blocking = 1; aborted = 0; opened = 0; closed = 0; read_count = 0;
  open_stall = !transfer_mode && connections == 1;
  transfer_stall = transfer_mode && connections == 1;
  total_entries = 1;
  return session;
}
void vb_sftp_disconnect(void *session) { disconnections++; free(session); }
void vb_test_transfer_mode(int mode) { transfer_mode = mode; }
int vb_test_connection_count(void) { return connections; }
int vb_test_disconnection_count(void) { return disconnections; }
#else
static void reset(void) {
  transfer_stall = 0; transfer_delay = 0; transfer_offset = 0; transfer_error = 0; transfer_zero_write = 0;
  test_now = 0; blocking = 1; last_error = 0; sftp_status = 0;
  aborted = 0; opened = 0; closed = 0; read_count = 0;
  total_entries = 2; entry_delay = 2; opening_again = 0;
  open_stall = 0; read_stall = 0; close_stall = 0;
  missing_on_open = 0; missing_on_read = 0; clock_error = 0;
  socket_error = 0; interrupt_once = 0;
}

static void expect_timeout(vb_sftp_session *session) {
  int status = 0;
  assert(vb_sftp_listdir_timed(session, "/directory", 30, &status) == NULL);
  assert(status == LIBSSH2_ERROR_TIMEOUT);
  assert(test_now <= 30);
  assert(aborted == 1);
  assert(blocking == 0);
  assert(closed == opened);
}

int main(void) {
  vb_sftp_session session = test_session();
  int status;
  reset(); opening_again = 1;
  vb_sftp_dir_list *list = vb_sftp_listdir_timed(&session, "/directory", 30, &status);
  assert(list && list->count == 2 && status == 0 && blocking == 1 && !aborted && closed == 1);
  vb_sftp_free_dir_list(list);
  reset(); total_entries = 0;
  list = vb_sftp_listdir_timed(&session, "/empty", 30, &status);
  assert(list && list->count == 0 && status == 0);
  vb_sftp_free_dir_list(list);
  reset(); open_stall = 1; expect_timeout(&session);
  reset(); read_stall = 1; expect_timeout(&session);
  reset(); total_entries = 100; entry_delay = 6; expect_timeout(&session);
  reset(); close_stall = 1; expect_timeout(&session);
  reset(); missing_on_open = 1;
  assert(!vb_sftp_listdir_timed(&session, "/missing", 30, &status));
  assert(status == LIBSSH2_FX_NO_SUCH_FILE && !aborted && blocking == 1);
  reset(); missing_on_read = 1;
  assert(!vb_sftp_listdir_timed(&session, "/partial", 30, &status));
  assert(status == LIBSSH2_ERROR_SFTP_PROTOCOL && aborted == 1 && closed == 1);
  reset(); opening_again = 1; interrupt_once = 1;
  list = vb_sftp_listdir_timed(&session, "/interrupted", 30, &status);
  assert(list && status == 0 && !aborted);
  vb_sftp_free_dir_list(list);
  reset(); open_stall = 1; socket_error = 1;
  assert(!vb_sftp_listdir_timed(&session, "/broken", 30, &status));
  assert(status == LIBSSH2_ERROR_SOCKET_RECV && aborted == 1);
  reset(); clock_error = 1;
  assert(!vb_sftp_listdir_timed(&session, "/clock", 30, &status));
  assert(status == LIBSSH2_ERROR_INVAL && !opened);
  assert(!vb_sftp_listdir_timed(&session, "/invalid", 0, &status));
  assert(status == LIBSSH2_ERROR_INVAL);
  vb_sftp_file file = {0};
  file.sess = &session;
  file.handle = (LIBSSH2_SFTP_HANDLE *)&fake_file_handle;
  unsigned char buffer[8] = {0};
  for (int writing = 0; writing <= 1; writing++) {
    reset(); transfer_delay = 1;
    int result = writing ? vb_sftp_write(&file, buffer, 4, 30) : vb_sftp_read(&file, 0, buffer, 4, 30);
    assert(result == 4 && test_now == 4 && blocking == 1 && !aborted);
    reset(); transfer_delay = 2;
    result = writing ? vb_sftp_write(&file, buffer, 4, 5) : vb_sftp_read(&file, 0, buffer, 4, 5);
    assert(result == LIBSSH2_ERROR_TIMEOUT && test_now == 6 && aborted == 1 && blocking == 0);
    reset(); transfer_stall = 1; interrupt_once = 1;
    result = writing ? vb_sftp_write(&file, buffer, 4, 30) : vb_sftp_read(&file, 0, buffer, 4, 30);
    assert(result == LIBSSH2_ERROR_TIMEOUT && test_now == 30 && aborted == 1);
    reset(); transfer_error = 1;
    result = writing ? vb_sftp_write(&file, buffer, 4, 30) : vb_sftp_read(&file, 0, buffer, 4, 30);
    assert(result == LIBSSH2_ERROR_SOCKET_RECV && aborted == 1);
    reset(); clock_error = 1;
    result = writing ? vb_sftp_write(&file, buffer, 4, 30) : vb_sftp_read(&file, 0, buffer, 4, 30);
    assert(result == LIBSSH2_ERROR_INVAL && !aborted && blocking == 1);
  }
  reset(); transfer_zero_write = 1;
  assert(vb_sftp_write(&file, buffer, 4, 30) == LIBSSH2_ERROR_TIMEOUT && aborted == 1);
  reset(); file.last_offset = -1;
  assert(vb_sftp_read(&file, 0, buffer, 8, 30) == 4 && blocking == 1 && !aborted);
  reset(); blocking = 0;
  assert(vb_sftp_write(&file, buffer, 4, 30) == 4 && blocking == 0);
  assert(vb_sftp_read(&file, 0, buffer, 4, 0) == LIBSSH2_ERROR_INVAL);
  puts("Native SFTP listing and transfer deadline scenarios passed");
  return 0;
}
#endif
