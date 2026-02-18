#include <libssh2.h>
#include <libssh2_sftp.h>
#include <openssl/evp.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/select.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

typedef struct {
  int sock;
  LIBSSH2_SESSION *session;
  LIBSSH2_SFTP *sftp;
} vb_sftp_session;

typedef struct {
  LIBSSH2_SFTP_HANDLE *handle;
  vb_sftp_session *sess;
  long long last_offset;
} vb_sftp_file;

static pthread_mutex_t g_libssh2_lock = PTHREAD_MUTEX_INITIALIZER;
static int g_libssh2_refcount = 0;
static const int VB_SFTP_TIMEOUT_MS = 8000;
static const int VB_SHA256_DIGEST_LENGTH = 32;
static const int VB_SHA256_HEX_LENGTH = 65;

static double now_seconds(void);
static int wait_socket_ready(vb_sftp_session *sess);
static void hex_encode(const unsigned char *src, int src_len, char *dst);

static int ensure_libssh2_init(void) {
  pthread_mutex_lock(&g_libssh2_lock);
  if (g_libssh2_refcount == 0) {
    if (libssh2_init(0) != 0) {
      pthread_mutex_unlock(&g_libssh2_lock);
      return -1;
    }
  }
  g_libssh2_refcount += 1;
  pthread_mutex_unlock(&g_libssh2_lock);
  return 0;
}

static void release_libssh2_init(void) {
  pthread_mutex_lock(&g_libssh2_lock);
  if (g_libssh2_refcount > 0) {
    g_libssh2_refcount -= 1;
    if (g_libssh2_refcount == 0) {
      libssh2_exit();
    }
  }
  pthread_mutex_unlock(&g_libssh2_lock);
}

static double now_seconds(void) {
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return (double)tv.tv_sec + (double)tv.tv_usec / 1000000.0;
}

static int wait_socket_ready(vb_sftp_session *sess) {
  if (!sess || !sess->session || sess->sock < 0) {
    return -1;
  }
  fd_set readfds;
  fd_set writefds;
  FD_ZERO(&readfds);
  FD_ZERO(&writefds);

  int directions = libssh2_session_block_directions(sess->session);
  if (directions & LIBSSH2_SESSION_BLOCK_INBOUND) {
    FD_SET(sess->sock, &readfds);
  }
  if (directions & LIBSSH2_SESSION_BLOCK_OUTBOUND) {
    FD_SET(sess->sock, &writefds);
  }
  if ((directions & (LIBSSH2_SESSION_BLOCK_INBOUND | LIBSSH2_SESSION_BLOCK_OUTBOUND)) == 0) {
    FD_SET(sess->sock, &readfds);
    FD_SET(sess->sock, &writefds);
  }

  struct timeval timeout;
  timeout.tv_sec = VB_SFTP_TIMEOUT_MS / 1000;
  timeout.tv_usec = (VB_SFTP_TIMEOUT_MS % 1000) * 1000;
  int rc = select(sess->sock + 1, &readfds, &writefds, NULL, &timeout);
  return rc > 0 ? 0 : -1;
}

static void hex_encode(const unsigned char *src, int src_len, char *dst) {
  static const char hexdigits[] = "0123456789abcdef";
  for (int i = 0; i < src_len; i++) {
    const unsigned char b = src[i];
    dst[i * 2] = hexdigits[(b >> 4) & 0x0f];
    dst[i * 2 + 1] = hexdigits[b & 0x0f];
  }
  dst[src_len * 2] = '\0';
}

int vb_sha256_hex(const unsigned char *data, int length, char *out_hex, int out_len) {
  if (!out_hex || out_len < VB_SHA256_HEX_LENGTH || length < 0) {
    return -1;
  }
  if (length > 0 && !data) {
    return -1;
  }

  unsigned char digest[EVP_MAX_MD_SIZE];
  unsigned int digest_len = 0;
  if (EVP_Digest(data, (size_t)length, digest, &digest_len, EVP_sha256(), NULL) != 1) {
    return -1;
  }
  if ((int)digest_len != VB_SHA256_DIGEST_LENGTH) {
    return -1;
  }
  hex_encode(digest, VB_SHA256_DIGEST_LENGTH, out_hex);
  return 0;
}

static int connect_tcp(const char *host, int port) {
  struct addrinfo hints;
  struct addrinfo *res = NULL;
  char port_str[16];
  int sock = -1;

  memset(&hints, 0, sizeof(hints));
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  snprintf(port_str, sizeof(port_str), "%d", port);
  if (getaddrinfo(host, port_str, &hints, &res) != 0) {
    return -1;
  }

  for (struct addrinfo *p = res; p != NULL; p = p->ai_next) {
    sock = socket(p->ai_family, p->ai_socktype, p->ai_protocol);
    if (sock < 0) {
      continue;
    }
#ifdef __APPLE__
    int set = 1;
    setsockopt(sock, SOL_SOCKET, SO_NOSIGPIPE, &set, sizeof(set));
#endif
    if (connect(sock, p->ai_addr, p->ai_addrlen) == 0) {
      break;
    }
    close(sock);
    sock = -1;
  }

  freeaddrinfo(res);
  return sock;
}

int vb_scp_speed_test(const char *host, int port, const char *user, const char *key_path,
                      const char *password, const char *remote_path, int interval_sec) {
  LIBSSH2_SESSION *session = NULL;
  LIBSSH2_CHANNEL *channel = NULL;
  int sock = -1;
  int rc = 0;
  char buffer[32768];
  ssize_t n;
  size_t total = 0;
  size_t window = 0;
  double last_log = 0.0;
  double start = 0.0;

  if (interval_sec <= 0) {
    interval_sec = 5;
  }

  if (libssh2_init(0) != 0) {
    return 2;
  }

  sock = connect_tcp(host, port);
  if (sock < 0) {
    libssh2_exit();
    return 3;
  }

  session = libssh2_session_init();
  if (!session) {
    close(sock);
    libssh2_exit();
    return 4;
  }

  libssh2_session_set_blocking(session, 1);
  if (libssh2_session_handshake(session, sock) != 0) {
    libssh2_session_free(session);
    close(sock);
    libssh2_exit();
    return 5;
  }

  if (key_path && key_path[0] != '\0') {
    rc = libssh2_userauth_publickey_fromfile(session, user, NULL, key_path, password);
  } else {
    rc = libssh2_userauth_password(session, user, password ? password : "");
  }
  if (rc != 0) {
    libssh2_session_disconnect(session, "auth failed");
    libssh2_session_free(session);
    close(sock);
    libssh2_exit();
    return 6;
  }

  struct stat sb;
  channel = libssh2_scp_recv2(session, remote_path, &sb);
  if (!channel) {
    libssh2_session_disconnect(session, "scp recv failed");
    libssh2_session_free(session);
    close(sock);
    libssh2_exit();
    return 7;
  }

  start = now_seconds();
  last_log = start;

  while ((n = libssh2_channel_read(channel, buffer, sizeof(buffer))) > 0) {
    total += (size_t)n;
    window += (size_t)n;
    double now = now_seconds();
    if (now - last_log >= interval_sec) {
      double mb = (double)total / (1024.0 * 1024.0);
      double mbps = ((double)window / (1024.0 * 1024.0)) / (now - last_log);
      fprintf(stdout, "SCP read: %.1fMB/s total=%.1fMB\n", mbps, mb);
      fflush(stdout);
      window = 0;
      last_log = now;
    }
  }

  if (n < 0) {
    rc = 8;
  }

  libssh2_channel_close(channel);
  libssh2_channel_free(channel);
  libssh2_session_disconnect(session, "done");
  libssh2_session_free(session);
  close(sock);
  libssh2_exit();
  return rc;
}

void *vb_sftp_connect(const char *host, int port, const char *user, const char *password) {
  if (ensure_libssh2_init() != 0) {
    return NULL;
  }

  int sock = connect_tcp(host, port);
  if (sock < 0) {
    release_libssh2_init();
    return NULL;
  }

  LIBSSH2_SESSION *session = libssh2_session_init();
  if (!session) {
    close(sock);
    release_libssh2_init();
    return NULL;
  }

  libssh2_session_set_blocking(session, 1);
  libssh2_session_set_timeout(session, VB_SFTP_TIMEOUT_MS);
  if (libssh2_session_handshake(session, sock) != 0) {
    libssh2_session_free(session);
    close(sock);
    release_libssh2_init();
    return NULL;
  }

  if (libssh2_userauth_password(session, user, password ? password : "") != 0) {
    libssh2_session_disconnect(session, "auth failed");
    libssh2_session_free(session);
    close(sock);
    release_libssh2_init();
    return NULL;
  }

  LIBSSH2_SFTP *sftp = libssh2_sftp_init(session);
  if (!sftp) {
    libssh2_session_disconnect(session, "sftp init failed");
    libssh2_session_free(session);
    close(sock);
    release_libssh2_init();
    return NULL;
  }

  vb_sftp_session *handle = (vb_sftp_session *)calloc(1, sizeof(vb_sftp_session));
  if (!handle) {
    libssh2_sftp_shutdown(sftp);
    libssh2_session_disconnect(session, "alloc failed");
    libssh2_session_free(session);
    close(sock);
    release_libssh2_init();
    return NULL;
  }
  handle->sock = sock;
  handle->session = session;
  handle->sftp = sftp;
  return (void *)handle;
}

void vb_sftp_disconnect(void *session_ptr) {
  if (!session_ptr) {
    return;
  }
  vb_sftp_session *sess = (vb_sftp_session *)session_ptr;
  if (sess->sftp) {
    libssh2_sftp_shutdown(sess->sftp);
  }
  if (sess->session) {
    libssh2_session_disconnect(sess->session, "done");
    libssh2_session_free(sess->session);
  }
  if (sess->sock >= 0) {
    close(sess->sock);
  }
  free(sess);
  release_libssh2_init();
}

void *vb_sftp_open_read(void *session_ptr, const char *path) {
  if (!session_ptr || !path) {
    return NULL;
  }
  vb_sftp_session *sess = (vb_sftp_session *)session_ptr;
  LIBSSH2_SFTP_HANDLE *handle = libssh2_sftp_open(sess->sftp, path, LIBSSH2_FXF_READ, 0);
  if (!handle) {
    return NULL;
  }
  vb_sftp_file *file = (vb_sftp_file *)calloc(1, sizeof(vb_sftp_file));
  if (!file) {
    libssh2_sftp_close(handle);
    return NULL;
  }
  file->handle = handle;
  file->sess = sess;
  file->last_offset = -1;
  return (void *)file;
}

void *vb_sftp_open_write(void *session_ptr, const char *path, int truncate) {
  if (!session_ptr || !path) {
    return NULL;
  }
  vb_sftp_session *sess = (vb_sftp_session *)session_ptr;
  long flags = LIBSSH2_FXF_WRITE | LIBSSH2_FXF_CREAT;
  if (truncate) {
    flags |= LIBSSH2_FXF_TRUNC;
  }
  LIBSSH2_SFTP_HANDLE *handle = libssh2_sftp_open(sess->sftp, path, flags, 0644);
  if (!handle) {
    return NULL;
  }
  vb_sftp_file *file = (vb_sftp_file *)calloc(1, sizeof(vb_sftp_file));
  if (!file) {
    libssh2_sftp_close(handle);
    return NULL;
  }
  file->handle = handle;
  file->sess = sess;
  file->last_offset = -1;
  return (void *)file;
}

int vb_sftp_read(void *file_ptr, long long offset, unsigned char *buffer, int length) {
  if (!file_ptr || !buffer || length <= 0) {
    return -1;
  }
  vb_sftp_file *file = (vb_sftp_file *)file_ptr;
  if (file->last_offset != offset) {
    libssh2_sftp_seek64(file->handle, (libssh2_uint64_t)offset);
  }
  int total = 0;
  while (total < length) {
    ssize_t n = libssh2_sftp_read(file->handle, (char *)buffer + total, length - total);
    if (n < 0) {
      if (total > 0) {
        break;
      }
      return -1;
    }
    if (n == 0) {
      break;
    }
    total += (int)n;
  }
  file->last_offset = offset + (long long)total;
  return total;
}

int vb_sftp_write(void *file_ptr, const unsigned char *buffer, int length) {
  if (!file_ptr || !buffer || length <= 0) {
    return -1;
  }
  vb_sftp_file *file = (vb_sftp_file *)file_ptr;
  if (!file->sess || !file->sess->session) {
    return -1;
  }
  LIBSSH2_SESSION *session = file->sess->session;
  libssh2_session_set_blocking(session, 0);
  int total = 0;
  while (total < length) {
    ssize_t n = libssh2_sftp_write(file->handle, (const char *)buffer + total, length - total);
    if (n == LIBSSH2_ERROR_EAGAIN || n == 0) {
      if (wait_socket_ready(file->sess) != 0) {
        libssh2_session_set_blocking(session, 1);
        return total > 0 ? total : -1;
      }
      continue;
    }
    if (n < 0) {
      libssh2_session_set_blocking(session, 1);
      return total > 0 ? total : -1;
    }
    total += (int)n;
  }
  libssh2_session_set_blocking(session, 1);
  return total;
}

void vb_sftp_close_file(void *file_ptr) {
  if (!file_ptr) {
    return;
  }
  vb_sftp_file *file = (vb_sftp_file *)file_ptr;
  if (file->handle) {
    libssh2_sftp_close(file->handle);
  }
  free(file);
}
