#pragma once

#include <string>
#include <vector>
#include <cstdint>

namespace rdbms {

class FileManager {
public:
    // 创建目录（包含父目录）。成功返回 true。
    static bool create_directory(const std::string &path) noexcept;

    // 删除目录（必须为空）。成功返回 true。
    static bool remove_directory(const std::string &path) noexcept;

    // 创建空文件（如不存在）。成功返回 true。
    static bool create_file(const std::string &path) noexcept;

    // 删除文件。成功返回 true。
    static bool remove_file(const std::string &path) noexcept;

    // 在指定偏移写入数据。成功返回 true。
    static bool write_at(const std::string &file_path, uint64_t offset, const std::vector<char> &data) noexcept;

    // 从指定偏移读取固定字节数。成功时填充 `out` 并返回 true。
    static bool read_at(const std::string &file_path, uint64_t offset, size_t size, std::vector<char> &out) noexcept;

    // 便捷：按页读取（page_no * page_size）。成功返回 true。
    static bool read_page(const std::string &file_path, uint64_t page_no, size_t page_size, std::vector<char> &out) noexcept;

    // 便捷：按页写入（page_no * page_size）。成功返回 true。
    static bool write_page(const std::string &file_path, uint64_t page_no, size_t page_size, const std::vector<char> &data) noexcept;
};

} // namespace rdbms
