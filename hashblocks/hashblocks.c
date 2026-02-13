#define _POSIX_C_SOURCE 200809L
#include <errno.h>
#include <fcntl.h>
#include <openssl/evp.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

static void usage(const char *prog) {
  fprintf(stderr, "Usage: %s --read <path> [--block-size <bytes>]\n", prog);
  fprintf(stderr, "  --read <path>         Input file to hash (required)\n");
  fprintf(stderr, "  --block-size <bytes>  Block size in bytes (default: 8192)\n");
  fprintf(stderr, "  -h, --help            Show this help\n");
}

static int is_all_zero(const uint8_t *buf, size_t len) {
  for (size_t i = 0; i < len; i++) {
    if (buf[i] != 0) {
      return 0;
    }
  }
  return 1;
}

static void print_zero_run(uint64_t start, uint64_t end) {
  if (start == end) {
    printf("%llu -> ZERO\n", (unsigned long long)start);
  } else {
    printf("%llu-%llu -> ZERO\n", (unsigned long long)start, (unsigned long long)end);
  }
}

static void sha256_hex(const uint8_t *buf, size_t len, char out[65]) {
  unsigned char digest[EVP_MAX_MD_SIZE];
  unsigned int digest_len = 0;
  EVP_MD_CTX *ctx = EVP_MD_CTX_new();
  if (!ctx) {
    fprintf(stderr, "Failed to allocate EVP_MD_CTX.\\n");
    exit(1);
  }
  if (EVP_DigestInit_ex(ctx, EVP_sha256(), NULL) != 1 ||
      EVP_DigestUpdate(ctx, buf, len) != 1 ||
      EVP_DigestFinal_ex(ctx, digest, &digest_len) != 1) {
    EVP_MD_CTX_free(ctx);
    fprintf(stderr, "SHA256 failed.\\n");
    exit(1);
  }
  EVP_MD_CTX_free(ctx);

  static const char hex[] = "0123456789abcdef";
  for (unsigned int i = 0; i < digest_len; i++) {
    out[i * 2] = hex[(digest[i] >> 4) & 0xF];
    out[i * 2 + 1] = hex[digest[i] & 0xF];
  }
  out[digest_len * 2] = '\0';
}

static void handle_control(uint64_t *limit_index, int *stop_requested) {
  static char line_buf[256];
  static size_t line_len = 0;
  char buf[128];
  ssize_t n = 0;
  int saw_eof = 0;

  while ((n = read(STDIN_FILENO, buf, sizeof(buf))) > 0) {
    for (ssize_t i = 0; i < n; i++) {
      char c = buf[i];
      if (c == '\n' || c == '\r') {
        if (line_len > 0) {
          line_buf[line_len] = '\0';
          if (strncmp(line_buf, "LIMIT ", 6) == 0) {
            char *end = NULL;
            errno = 0;
            unsigned long long value = strtoull(line_buf + 6, &end, 10);
            if (errno == 0 && end != NULL && *end == '\0') {
              *limit_index = (uint64_t)value;
            }
          } else if (strcmp(line_buf, "STOP") == 0) {
            *stop_requested = 1;
          }
        }
        line_len = 0;
        continue;
      }
      if (line_len < sizeof(line_buf) - 1) {
        line_buf[line_len++] = c;
      }
    }
  }
  if (n == 0) {
    saw_eof = 1;
  }
  if (saw_eof) {
    *stop_requested = 1;
  }
}

int main(int argc, char **argv) {
  const char *path = NULL;
  size_t block_size = 8192;
  const uint64_t zero_run_chunk = 128;
  int stop_requested = 0;
  uint64_t limit_index = UINT64_MAX;

  setvbuf(stdout, NULL, _IONBF, 0);
  int stdin_flags = fcntl(STDIN_FILENO, F_GETFL, 0);
  if (stdin_flags >= 0) {
    fcntl(STDIN_FILENO, F_SETFL, stdin_flags | O_NONBLOCK);
  }

  for (int i = 1; i < argc; i++) {
    if (strcmp(argv[i], "--read") == 0) {
      if (i + 1 >= argc) {
        usage(argv[0]);
        return 2;
      }
      path = argv[++i];
    } else if (strcmp(argv[i], "--block-size") == 0) {
      if (i + 1 >= argc) {
        usage(argv[0]);
        return 2;
      }
      char *end = NULL;
      errno = 0;
      unsigned long long value = strtoull(argv[++i], &end, 10);
      if (errno != 0 || end == argv[i] || *end != '\0' || value == 0) {
        fprintf(stderr, "Invalid --block-size value.\n");
        return 2;
      }
      block_size = (size_t)value;
    } else if (strcmp(argv[i], "-h") == 0 || strcmp(argv[i], "--help") == 0) {
      usage(argv[0]);
      return 0;
    } else {
      usage(argv[0]);
      return 2;
    }
  }

  if (path == NULL) {
    usage(argv[0]);
    return 2;
  }

  int fd = open(path, O_RDONLY);
  if (fd < 0) {
    fprintf(stderr, "Failed to open %s: %s\n", path, strerror(errno));
    return 1;
  }

  uint8_t *buf = (uint8_t *)malloc(block_size);
  if (!buf) {
    fprintf(stderr, "Out of memory.\n");
    close(fd);
    return 1;
  }

  uint64_t index = 0;
  int in_zero_run = 0;
  uint64_t zero_start = 0;
  uint64_t zero_len = 0;

  while (1) {
    handle_control(&limit_index, &stop_requested);
    if (stop_requested) {
      free(buf);
      close(fd);
      return 2;
    }
    if (index > limit_index && !in_zero_run) {
      struct timespec ts;
      ts.tv_sec = 0;
      ts.tv_nsec = 100 * 1000 * 1000;
      nanosleep(&ts, NULL);
      continue;
    }
    ssize_t n = read(fd, buf, block_size);
    if (n < 0) {
      fprintf(stderr, "Read error: %s\n", strerror(errno));
      free(buf);
      close(fd);
      return 1;
    }
    if (n == 0) {
      break;
    }

    if (is_all_zero(buf, (size_t)n)) {
      if (!in_zero_run) {
        in_zero_run = 1;
        zero_start = index;
        zero_len = 0;
      }
      zero_len++;
      if (zero_len >= zero_run_chunk) {
        print_zero_run(zero_start, index);
        zero_start = index + 1;
        zero_len = 0;
      }
    } else {
      if (index > limit_index && in_zero_run) {
        /* End of ZERO run beyond LIMIT:
           - flush ZERO run quickly beyond LIMIT
           - rewind current non-zero block so it will be processed only after LIMIT advances */
        print_zero_run(zero_start, index - 1);
        in_zero_run = 0;
        zero_len = 0;
        if (lseek(fd, -(off_t)n, SEEK_CUR) == (off_t)-1) {
          fprintf(stderr, "Seek error after ZERO run: %s\n", strerror(errno));
          free(buf);
          close(fd);
          return 1;
        }
        struct timespec ts;
        ts.tv_sec = 0;
        ts.tv_nsec = 100 * 1000 * 1000;
        nanosleep(&ts, NULL);
        continue;
      }
      if (in_zero_run) {
        print_zero_run(zero_start, index - 1);
        in_zero_run = 0;
        zero_len = 0;
      }
      char hex[65];
      sha256_hex(buf, (size_t)n, hex);
      printf("%llu -> %s\n", (unsigned long long)index, hex);
    }

    index++;
  }

  if (in_zero_run) {
    print_zero_run(zero_start, index - 1);
  }
  printf("EOF\n");

  free(buf);
  close(fd);
  return 0;
}
