#include "../include/FileManager.h"
#include <filesystem>
#include <fstream>

namespace rdbms {

bool FileManager::create_directory(const std::string &path) noexcept {
    try {
        return std::filesystem::create_directories(path);
    } catch (...) {
        return false;
    }
}

bool FileManager::remove_directory(const std::string &path) noexcept {
    try {
        return std::filesystem::remove(path);
    } catch (...) {
        return false;
    }
}

bool FileManager::create_file(const std::string &path) noexcept {
    try {
        std::filesystem::path p(path);
        if (std::filesystem::exists(p)) return true;
        auto parent = p.parent_path();
        if (!parent.empty()) std::filesystem::create_directories(parent);
        std::ofstream ofs(path, std::ios::binary);
        return ofs.good();
    } catch (...) {
        return false;
    }
}

bool FileManager::remove_file(const std::string &path) noexcept {
    try {
        return std::filesystem::remove(path);
    } catch (...) {
        return false;
    }
}

bool FileManager::write_at(const std::string &file_path, uint64_t offset, const std::vector<char> &data) noexcept {
    try {
        std::ofstream ofs(file_path, std::ios::binary | std::ios::in | std::ios::out);
        if (!ofs) {
                // 尝试创建文件
                std::ofstream create(file_path, std::ios::binary);
            if (!create) return false;
            create.close();
            ofs.open(file_path, std::ios::binary | std::ios::in | std::ios::out);
            if (!ofs) return false;
        }
        ofs.seekp(static_cast<std::streamoff>(offset));
        ofs.write(data.data(), static_cast<std::streamsize>(data.size()));
        return ofs.good();
    } catch (...) {
        return false;
    }
}

bool FileManager::read_at(const std::string &file_path, uint64_t offset, size_t size, std::vector<char> &out) noexcept {
    try {
        std::ifstream ifs(file_path, std::ios::binary);
        if (!ifs) return false;
        ifs.seekg(0, std::ios::end);
        std::streamoff file_size = ifs.tellg();
        if (static_cast<uint64_t>(file_size) < offset) return false;
        ifs.seekg(static_cast<std::streamoff>(offset));
        out.resize(size);
        ifs.read(out.data(), static_cast<std::streamsize>(size));
        std::streamsize n = ifs.gcount();
        if (static_cast<size_t>(n) != size) {
            out.resize(static_cast<size_t>(n));
        }
        return n > 0;
    } catch (...) {
        return false;
    }
}

bool FileManager::read_page(const std::string &file_path, uint64_t page_no, size_t page_size, std::vector<char> &out) noexcept {
    uint64_t offset = page_no * static_cast<uint64_t>(page_size);
    return read_at(file_path, offset, page_size, out);
}

bool FileManager::write_page(const std::string &file_path, uint64_t page_no, size_t page_size, const std::vector<char> &data) noexcept {
    if (data.size() != page_size) return false;
    uint64_t offset = page_no * static_cast<uint64_t>(page_size);
    return write_at(file_path, offset, data);
}

} // namespace rdbms
