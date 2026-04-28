#pragma once

#include <vector>
#include <string>
#include <cstdint>
#include <type_traits>
#include <cstring>

namespace rdbms {
namespace serialization {

// Append raw bytes to buffer
inline void append_bytes(std::vector<char> &buf, const void *data, size_t size) noexcept {
    const char *p = reinterpret_cast<const char*>(data);
    buf.insert(buf.end(), p, p + size);
}

template<typename T>
inline void write_pod(std::vector<char> &buf, const T &value) noexcept {
    static_assert(std::is_trivially_copyable<T>::value, "write_pod requires trivially copyable type");
    append_bytes(buf, &value, sizeof(T));
}

template<typename T>
inline bool read_pod(const std::vector<char> &buf, size_t &offset, T &out) noexcept {
    if (offset + sizeof(T) > buf.size()) return false;
    std::memcpy(&out, buf.data() + offset, sizeof(T));
    offset += sizeof(T);
    return true;
}

// Little-endian 32-bit length helper
inline void write_u32_le(std::vector<char> &buf, uint32_t v) noexcept {
    char bytes[4];
    bytes[0] = static_cast<char>(v & 0xFF);
    bytes[1] = static_cast<char>((v >> 8) & 0xFF);
    bytes[2] = static_cast<char>((v >> 16) & 0xFF);
    bytes[3] = static_cast<char>((v >> 24) & 0xFF);
    append_bytes(buf, bytes, 4);
}

inline bool read_u32_le(const std::vector<char> &buf, size_t &offset, uint32_t &out) noexcept {
    if (offset + 4 > buf.size()) return false;
    const unsigned char *p = reinterpret_cast<const unsigned char*>(buf.data() + offset);
    out = static_cast<uint32_t>(p[0]) | (static_cast<uint32_t>(p[1]) << 8) | (static_cast<uint32_t>(p[2]) << 16) | (static_cast<uint32_t>(p[3]) << 24);
    offset += 4;
    return true;
}

inline void write_string(std::vector<char> &buf, const std::string &s) noexcept {
    write_u32_le(buf, static_cast<uint32_t>(s.size()));
    if (!s.empty()) append_bytes(buf, s.data(), s.size());
}

inline bool read_string(const std::vector<char> &buf, size_t &offset, std::string &out) noexcept {
    uint32_t len = 0;
    if (!read_u32_le(buf, offset, len)) return false;
    if (offset + len > buf.size()) return false;
    out.assign(buf.data() + offset, buf.data() + offset + len);
    offset += len;
    return true;
}

template<typename T>
inline void write_vector(std::vector<char> &buf, const std::vector<T> &v) noexcept {
    static_assert(std::is_trivially_copyable<T>::value, "write_vector requires trivially copyable element type");
    write_u32_le(buf, static_cast<uint32_t>(v.size()));
    if (!v.empty()) append_bytes(buf, v.data(), v.size() * sizeof(T));
}

template<typename T>
inline bool read_vector(const std::vector<char> &buf, size_t &offset, std::vector<T> &out) noexcept {
    static_assert(std::is_trivially_copyable<T>::value, "read_vector requires trivially copyable element type");
    uint32_t count = 0;
    if (!read_u32_le(buf, offset, count)) return false;
    if (offset + static_cast<size_t>(count) * sizeof(T) > buf.size()) return false;
    out.resize(count);
    if (count > 0) std::memcpy(out.data(), buf.data() + offset, count * sizeof(T));
    offset += static_cast<size_t>(count) * sizeof(T);
    return true;
}

} // namespace serialization
} // namespace rdbms
