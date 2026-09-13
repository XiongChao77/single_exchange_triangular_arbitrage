#pragma once
#include <openssl/ssl.h>
#include <filesystem>
#include <stdexcept>
#include <cstring>
#include <fcntl.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>

namespace triangular {
// Context owns the descriptor; flock also serializes separate contexts/processes.
inline void enable_tls_keylog(SSL_CTX* context, const std::filesystem::path& path) {
    if (path.empty()) return;
    static const int index = SSL_CTX_get_ex_new_index(0, nullptr, nullptr, nullptr,
        [](void*, void* value, CRYPTO_EX_DATA*, int, long, void*) {
            if (value) { auto* descriptor = static_cast<int*>(value); ::close(*descriptor); delete descriptor; }
        });
    if (index < 0) throw std::runtime_error("Cannot allocate TLS keylog context");
    const int descriptor = ::open(path.c_str(), O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC | O_NOFOLLOW, 0600);
    struct stat info{};
    if (descriptor < 0) throw std::runtime_error("Cannot open TLS keylog file");
    if (::fstat(descriptor, &info) != 0 || !S_ISREG(info.st_mode) || (info.st_mode & 077) != 0 || info.st_uid != ::geteuid()) {
        ::close(descriptor);
        throw std::runtime_error("TLS keylog must be an owned regular file with permissions 0600");
    }
    auto* owned = new int(descriptor);
    if (!SSL_CTX_set_ex_data(context, index, owned)) {
        ::close(descriptor); delete owned;
        throw std::runtime_error("Cannot attach TLS keylog context");
    }
    SSL_CTX_set_keylog_callback(context, [](const SSL* ssl, const char* line) {
        auto* fd = static_cast<int*>(SSL_CTX_get_ex_data(SSL_get_SSL_CTX(ssl), index));
        if (!fd || ::flock(*fd, LOCK_EX) != 0) return;
        const auto size = std::strlen(line);
        std::size_t written = 0;
        while (written < size) {
            const auto count = ::write(*fd, line + written, size - written);
            if (count <= 0) break;
            written += static_cast<std::size_t>(count);
        }
        if (written == size) { const auto ignored = ::write(*fd, "\n", 1); (void)ignored; }
        ::flock(*fd, LOCK_UN);
    });
}
}
