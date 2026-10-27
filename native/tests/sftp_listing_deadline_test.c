/* Exercise the production listing code with deterministic libssh2, socket and
   monotonic-clock stubs. The shared variant also tests the real Dart FFI/retry. */
#define clock_gettime test_clock_gettime
#define select test_select
#define shutdown test_shutdown
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
  (void)handle;
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
  open_stall = connections == 1;
  total_entries = 1;
  return session;
}
void vb_sftp_disconnect(void *session) { disconnections++; free(session); }
int vb_test_connection_count(void) { return connections; }
int vb_test_disconnection_count(void) { return disconnections; }
#else
static void reset(void) {
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
  puts("12 native SFTP listing deadline scenarios passed");
  return 0;
}
#endif
