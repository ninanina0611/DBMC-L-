#include "../include/DatabaseManager.h"
#include "../include/FileManager.h"
#include "../include/Serializer.h"

#include <filesystem>
#include <system_error>
#include <algorithm>

namespace fs = std::filesystem;

namespace rdbms {

DatabaseManager::DatabaseManager(const std::string &root_dir) noexcept
    : root_dir_(root_dir), current_db_(), db_path_() {
    if (root_dir_.empty()) root_dir_ = "data";
}

bool DatabaseManager::create_database(const std::string &db_name) noexcept {
    try {
        fs::path p = fs::path(root_dir_) / db_name;
        if (fs::exists(p) && fs::is_directory(p)) return true;
        return fs::create_directories(p);
    } catch (...) {
        return false;
    }
}

bool DatabaseManager::drop_database(const std::string &db_name) noexcept {
    try {
        fs::path p = fs::path(root_dir_) / db_name;
        if (!fs::exists(p)) return false;
        std::error_code ec;
        fs::remove_all(p, ec);
        return !ec && !fs::exists(p);
    } catch (...) {
        return false;
    }
}

bool DatabaseManager::use_database(const std::string &db_name) noexcept {
    try {
        fs::path p = fs::path(root_dir_) / db_name;
        if (!fs::exists(p) || !fs::is_directory(p)) return false;
        current_db_ = db_name;
        db_path_ = p.string();
        return true;
    } catch (...) {
        return false;
    }
}

std::string DatabaseManager::current_database() const noexcept {
    return current_db_;
}

std::string DatabaseManager::meta_file_path(const std::string &table_name) const noexcept {
    if (db_path_.empty()) return std::string();
    return db_path_ + "/" + table_name + ".meta";
}

std::string DatabaseManager::data_file_path(const std::string &table_name) const noexcept {
    if (db_path_.empty()) return std::string();
    return db_path_ + "/" + table_name + ".bin";
}

bool DatabaseManager::write_schema_file(const TableSchema &schema) noexcept {
    try {
        std::vector<char> buf;
        // write table name
        rdbms::serialization::write_string(buf, schema.table_name);
        // write column count
        rdbms::serialization::write_u32_le(buf, static_cast<uint32_t>(schema.columns.size()));
        // write columns
        for (const auto &c : schema.columns) {
            rdbms::serialization::write_string(buf, c.name);
            uint8_t t = static_cast<uint8_t>(c.type);
            rdbms::serialization::write_pod(buf, t);
            uint8_t pk = c.is_primary ? 1 : 0;
            rdbms::serialization::write_pod(buf, pk);
            uint8_t nn = c.not_null ? 1 : 0;
            rdbms::serialization::write_pod(buf, nn);
        }

        const std::string meta = meta_file_path(schema.table_name);
        if (meta.empty()) return false;
        if (!FileManager::create_file(meta)) return false;
        return FileManager::write_at(meta, 0, buf);
    } catch (...) {
        return false;
    }
}

bool DatabaseManager::read_schema_file(const std::string &table_name, TableSchema &schema) const noexcept {
    try {
        const std::string meta = meta_file_path(table_name);
        if (meta.empty()) return false;
        if (!fs::exists(meta)) return false;
        uint64_t sz = static_cast<uint64_t>(fs::file_size(meta));
        std::vector<char> buf;
        if (!FileManager::read_at(meta, 0, static_cast<size_t>(sz), buf)) return false;
        size_t offset = 0;
        // read table name
        if (!rdbms::serialization::read_string(buf, offset, schema.table_name)) return false;
        uint32_t col_count = 0;
        if (!rdbms::serialization::read_u32_le(buf, offset, col_count)) return false;
        schema.columns.clear();
        // Manual parse columns
        for (uint32_t i = 0; i < col_count; ++i) {
            Column c;
            if (!rdbms::serialization::read_string(buf, offset, c.name)) return false;
            uint8_t t = 0;
            if (!rdbms::serialization::read_pod(buf, offset, t)) return false;
            c.type = static_cast<Type>(t);
            uint8_t pk = 0;
            if (!rdbms::serialization::read_pod(buf, offset, pk)) return false;
            c.is_primary = pk != 0;
            uint8_t nn = 0;
            if (!rdbms::serialization::read_pod(buf, offset, nn)) return false;
            c.not_null = nn != 0;
            schema.columns.push_back(std::move(c));
        }

        return true;
    } catch (...) {
        return false;
    }
}

bool DatabaseManager::create_table(const TableSchema &schema) noexcept {
    if (current_db_.empty()) return false;
    try {
        const std::string dataf = data_file_path(schema.table_name);
        const std::string metaf = meta_file_path(schema.table_name);
        if (!FileManager::create_file(dataf)) return false;
        if (!write_schema_file(schema)) {
            FileManager::remove_file(dataf);
            return false;
        }
        return true;
    } catch (...) {
        return false;
    }
}

bool DatabaseManager::drop_table(const std::string &table_name) noexcept {
    if (current_db_.empty()) return false;
    try {
        const std::string dataf = data_file_path(table_name);
        const std::string metaf = meta_file_path(table_name);
        bool ok1 = true, ok2 = true;
        if (fs::exists(dataf)) ok1 = FileManager::remove_file(dataf);
        if (fs::exists(metaf)) ok2 = FileManager::remove_file(metaf);
        return ok1 && ok2;
    } catch (...) {
        return false;
    }
}

bool DatabaseManager::add_column(const std::string &table_name, const Column &col) noexcept {
    if (current_db_.empty()) return false;
    try {
        TableSchema schema;
        if (!read_schema_file(table_name, schema)) return false;
        for (const auto &c : schema.columns) if (c.name == col.name) return false;
        schema.columns.push_back(col);
        return write_schema_file(schema);
    } catch (...) {
        return false;
    }
}

bool DatabaseManager::remove_column(const std::string &table_name, const std::string &col_name) noexcept {
    if (current_db_.empty()) return false;
    try {
        TableSchema schema;
        if (!read_schema_file(table_name, schema)) return false;
        auto it = std::remove_if(schema.columns.begin(), schema.columns.end(), [&](const Column &c) { return c.name == col_name; });
        if (it == schema.columns.end()) return false;
        schema.columns.erase(it, schema.columns.end());
        return write_schema_file(schema);
    } catch (...) {
        return false;
    }
}

bool DatabaseManager::list_databases(std::vector<std::string> &out) const noexcept {
    try {
        out.clear();
        if (!fs::exists(root_dir_)) return true;
        for (auto &e : fs::directory_iterator(root_dir_)) {
            if (e.is_directory()) out.push_back(e.path().filename().string());
        }
        return true;
    } catch (...) {
        return false;
    }
}

bool DatabaseManager::list_tables(std::vector<std::string> &out) const noexcept {
    if (current_db_.empty()) return false;
    try {
        out.clear();
        if (!fs::exists(db_path_)) return true;
        for (auto &e : fs::directory_iterator(db_path_)) {
            if (!e.is_regular_file()) continue;
            const std::string name = e.path().filename().string();
            const std::string ext = e.path().extension().string();
            if (ext == ".meta") {
                out.push_back(e.path().stem().string());
            }
        }
        return true;
    } catch (...) {
        return false;
    }
}

bool DatabaseManager::get_schema(const std::string &table_name, TableSchema &out) const noexcept {
    if (current_db_.empty()) return false;
    return read_schema_file(table_name, out);
}

} // namespace rdbms
