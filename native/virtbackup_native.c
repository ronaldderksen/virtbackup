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
  EVP_MD_CTX *write_sha256_ctx;
} vb_sftp_file;

typedef struct {
  char *name;
  int is_dir;
  long long size;
} vb_sftp_dir_entry;

typedef struct {
  int count;
  vb_sftp_dir_entry *entries;
} vb_sftp_dir_list;

static pthread_mutex_t g_libssh2_lock = PTHREAD_MUTEX_INITIALIZER;
static int g_libssh2_refcount = 0;
static const int VB_SFTP_TIMEOUT_MS = 8000;
static const int VB_SHA256_DIGEST_LENGTH = 32;
static const int VB_SHA256_HEX_LENGTH = 65;

static double now_seconds(void);
static int wait_socket_ready_for(vb_sftp_session *sess, int timeout_ms);
static int transfer_remaining_ms(double deadline);
static int sftp_last_error(vb_sftp_session *sess);
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

static int wait_socket_ready_for(vb_sftp_session *sess, int timeout_ms) {
  if (!sess || !sess->session || sess->sock < 0) {
    return -1;
  }
  if (timeout_ms <= 0) {
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
  timeout.tv_sec = timeout_ms / 1000;
  timeout.tv_usec = (timeout_ms % 1000) * 1000;
  int rc = select(sess->sock + 1, &readfds, &writefds, NULL, &timeout);
  return rc > 0 ? 0 : -1;
}

static int transfer_remaining_ms(double deadline) {
  double remaining = deadline - now_seconds();
  if (remaining <= 0.0) {
    return 0;
  }
  int remaining_ms = (int)(remaining * 1000.0);
  if (remaining_ms <= 0) {
    return 1;
  }
  return remaining_ms;
}

static int sftp_last_error(vb_sftp_session *sess) {
  if (!sess || !sess->sftp) {
    return -1;
  }
  unsigned long code = libssh2_sftp_last_error(sess->sftp);
  return code == 0 ? -1 : (int)code;
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

int vb_sftp_last_error(void *session_ptr) {
  if (!session_ptr) {
    return -1;
  }
  return sftp_last_error((vb_sftp_session *)session_ptr);
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
  file->write_sha256_ctx = NULL;
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
  file->write_sha256_ctx = EVP_MD_CTX_new();
  if (!file->write_sha256_ctx || EVP_DigestInit_ex(file->write_sha256_ctx, EVP_sha256(), NULL) != 1) {
    if (file->write_sha256_ctx) {
      EVP_MD_CTX_free(file->write_sha256_ctx);
    }
    free(file);
    libssh2_sftp_close(handle);
    return NULL;
  }
  return (void *)file;
}

int vb_sftp_stat(void *session_ptr, const char *path, long long *size_out, int *is_dir_out) {
  if (!session_ptr || !path || !size_out || !is_dir_out) {
    return -1;
  }
  vb_sftp_session *sess = (vb_sftp_session *)session_ptr;
  LIBSSH2_SFTP_ATTRIBUTES attrs;
  memset(&attrs, 0, sizeof(attrs));
  int rc = libssh2_sftp_stat_ex(sess->sftp, path, (unsigned int)strlen(path), LIBSSH2_SFTP_STAT, &attrs);
  if (rc != 0) {
    return sftp_last_error(sess);
  }
  *size_out = (long long)attrs.filesize;
  *is_dir_out = (attrs.flags & LIBSSH2_SFTP_ATTR_PERMISSIONS) && S_ISDIR(attrs.permissions) ? 1 : 0;
  return 0;
}

int vb_sftp_mkdir(void *session_ptr, const char *path) {
  if (!session_ptr || !path) {
    return -1;
  }
  vb_sftp_session *sess = (vb_sftp_session *)session_ptr;
  int rc = libssh2_sftp_mkdir(sess->sftp, path, 0755);
  return rc == 0 ? 0 : sftp_last_error(sess);
}

int vb_sftp_rename(void *session_ptr, const char *from_path, const char *to_path) {
  if (!session_ptr || !from_path || !to_path) {
    return -1;
  }
  vb_sftp_session *sess = (vb_sftp_session *)session_ptr;
  int rc = libssh2_sftp_rename(sess->sftp, from_path, to_path);
  return rc == 0 ? 0 : sftp_last_error(sess);
}

int vb_sftp_remove(void *session_ptr, const char *path) {
  if (!session_ptr || !path) {
    return -1;
  }
  vb_sftp_session *sess = (vb_sftp_session *)session_ptr;
  int rc = libssh2_sftp_unlink(sess->sftp, path);
  return rc == 0 ? 0 : sftp_last_error(sess);
}

void *vb_sftp_listdir(void *session_ptr, const char *path) {
  if (!session_ptr || !path) {
    return NULL;
  }
  vb_sftp_session *sess = (vb_sftp_session *)session_ptr;
  LIBSSH2_SFTP_HANDLE *dir = libssh2_sftp_opendir(sess->sftp, path);
  if (!dir) {
    return NULL;
  }

  vb_sftp_dir_list *list = (vb_sftp_dir_list *)calloc(1, sizeof(vb_sftp_dir_list));
  if (!list) {
    libssh2_sftp_closedir(dir);
    return NULL;
  }

  int capacity = 32;
  list->entries = (vb_sftp_dir_entry *)calloc((size_t)capacity, sizeof(vb_sftp_dir_entry));
  if (!list->entries) {
    libssh2_sftp_closedir(dir);
    free(list);
    return NULL;
  }

  while (1) {
    char name[1024];
    LIBSSH2_SFTP_ATTRIBUTES attrs;
    memset(&attrs, 0, sizeof(attrs));
    int rc = libssh2_sftp_readdir_ex(dir, name, sizeof(name), NULL, 0, &attrs);
    if (rc == 0) {
      break;
    }
    if (rc < 0) {
      for (int i = 0; i < list->count; i++) {
        free(list->entries[i].name);
      }
      free(list->entries);
      free(list);
      libssh2_sftp_closedir(dir);
      return NULL;
    }
    if (list->count >= capacity) {
      capacity *= 2;
      vb_sftp_dir_entry *expanded = (vb_sftp_dir_entry *)realloc(list->entries, (size_t)capacity * sizeof(vb_sftp_dir_entry));
      if (!expanded) {
        for (int i = 0; i < list->count; i++) {
          free(list->entries[i].name);
        }
        free(list->entries);
        free(list);
        libssh2_sftp_closedir(dir);
        return NULL;
      }
      list->entries = expanded;
    }
    char *copy = (char *)calloc((size_t)rc + 1, sizeof(char));
    if (!copy) {
      for (int i = 0; i < list->count; i++) {
        free(list->entries[i].name);
      }
      free(list->entries);
      free(list);
      libssh2_sftp_closedir(dir);
      return NULL;
    }
    memcpy(copy, name, (size_t)rc);
    copy[rc] = '\0';
    list->entries[list->count].name = copy;
    list->entries[list->count].is_dir = (attrs.flags & LIBSSH2_SFTP_ATTR_PERMISSIONS) && S_ISDIR(attrs.permissions) ? 1 : 0;
    list->entries[list->count].size = (long long)attrs.filesize;
    list->count += 1;
  }

  libssh2_sftp_closedir(dir);
  return (void *)list;
}

void vb_sftp_free_dir_list(void *list_ptr) {
  if (!list_ptr) {
    return;
  }
  vb_sftp_dir_list *list = (vb_sftp_dir_list *)list_ptr;
  for (int i = 0; i < list->count; i++) {
    free(list->entries[i].name);
  }
  free(list->entries);
  free(list);
}

int vb_sftp_read(void *file_ptr, long long offset, unsigned char *buffer, int length, int timeout_ms) {
  if (!file_ptr || !buffer || length <= 0 || timeout_ms <= 0) {
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

int vb_sftp_write(void *file_ptr, const unsigned char *buffer, int length, int timeout_ms) {
  if (!file_ptr || !buffer || length <= 0 || timeout_ms <= 0) {
    return -1;
  }
  vb_sftp_file *file = (vb_sftp_file *)file_ptr;
  if (!file->sess || !file->sess->session) {
    return -1;
  }
  LIBSSH2_SESSION *session = file->sess->session;
  double deadline = now_seconds() + ((double)timeout_ms / 1000.0);
  libssh2_session_set_blocking(session, 0);
  int total = 0;
  while (total < length) {
    ssize_t n = libssh2_sftp_write(file->handle, (const char *)buffer + total, length - total);
    if (n == LIBSSH2_ERROR_EAGAIN || n == 0) {
      int remaining_ms = transfer_remaining_ms(deadline);
      if (remaining_ms <= 0) {
        libssh2_session_set_blocking(session, 1);
        return -1;
      }
      if (wait_socket_ready_for(file->sess, remaining_ms < VB_SFTP_TIMEOUT_MS ? remaining_ms : VB_SFTP_TIMEOUT_MS) != 0) {
        libssh2_session_set_blocking(session, 1);
        return -1;
      }
      continue;
    }
    if (n < 0) {
      libssh2_session_set_blocking(session, 1);
      return total > 0 ? total : -1;
    }
    if (file->write_sha256_ctx && EVP_DigestUpdate(file->write_sha256_ctx, buffer + total, (size_t)n) != 1) {
      libssh2_session_set_blocking(session, 1);
      return total > 0 ? total : -1;
    }
    total += (int)n;
  }
  libssh2_session_set_blocking(session, 1);
  return total;
}

int vb_sftp_file_sha256_hex(void *file_ptr, char *out_hex, int out_len) {
  if (!file_ptr || !out_hex || out_len < VB_SHA256_HEX_LENGTH) {
    return -1;
  }
  vb_sftp_file *file = (vb_sftp_file *)file_ptr;
  if (!file->write_sha256_ctx) {
    return -1;
  }
  unsigned char digest[EVP_MAX_MD_SIZE];
  unsigned int digest_len = 0;
  if (EVP_DigestFinal_ex(file->write_sha256_ctx, digest, &digest_len) != 1) {
    return -1;
  }
  EVP_MD_CTX_free(file->write_sha256_ctx);
  file->write_sha256_ctx = NULL;
  if ((int)digest_len != VB_SHA256_DIGEST_LENGTH) {
    return -1;
  }
  hex_encode(digest, VB_SHA256_DIGEST_LENGTH, out_hex);
  return 0;
}

void vb_sftp_close_file(void *file_ptr) {
  if (!file_ptr) {
    return;
  }
  vb_sftp_file *file = (vb_sftp_file *)file_ptr;
  if (file->write_sha256_ctx) {
    EVP_MD_CTX_free(file->write_sha256_ctx);
  }
  if (file->handle) {
    libssh2_sftp_close(file->handle);
  }
  free(file);
}
