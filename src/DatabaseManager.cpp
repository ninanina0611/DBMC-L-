#include "../include/DatabaseManager.h"
#include "../include/FileManager.h"
#include "../include/Serializer.h"

#include <filesystem>
#include <system_error>
#include <algorithm>
#include <fstream>
#include <sstream>

namespace fs = std::filesystem;

namespace rdbms {

namespace {
    // Helper to convert/normalize values when migrating between column types.
    static std::string convert_field(const std::string &old_val,
                                     DatabaseManager::Type old_t,
                                     DatabaseManager::Type new_t,
                                     const std::string &default_fill,
                                     bool new_not_null) noexcept {
        try {
            if (new_t == DatabaseManager::Type::STRING) {
                if (old_val.empty()) {
                    if (!default_fill.empty()) return default_fill;
                    if (new_not_null) return std::string(" ");
                    return std::string();
                }
                return old_val;
            }

            // target is numeric
            if (old_val.empty()) {
                if (!default_fill.empty()) return default_fill;
                return std::string("0");
            }
            try {
                long long v = std::stoll(old_val);
                return std::to_string(v);
            } catch (...) {
                if (!default_fill.empty()) return default_fill;
                return std::string("0");
            }
        } catch (...) {
            return std::string();
        }
    }

    // Migrate/transform table data from old schema to new schema, writing to same data file path.
    static bool migrate_table_data(const std::string &dataf,
                                   const DatabaseManager::TableSchema &old_schema,
                                   const DatabaseManager::TableSchema &new_schema,
                                   const std::string &default_fill) noexcept {
        try {
            namespace fs = std::filesystem;
            // If no existing data file, just create empty file and return success
            if (!fs::exists(dataf)) {
                return FileManager::create_file(dataf);
            }

            uint64_t fsize = static_cast<uint64_t>(fs::file_size(dataf));
            // nothing to migrate
            if (fsize == 0) return true;

            std::vector<char> buf;
            if (!FileManager::read_at(dataf, 0, static_cast<size_t>(fsize), buf)) return false;

            size_t offset = 0;
            std::vector<std::vector<std::string>> new_rows;

            while (offset < buf.size()) {
                uint32_t rec_sz = 0;
                if (!rdbms::serialization::read_u32_le(buf, offset, rec_sz)) return false;
                if (offset + rec_sz > buf.size()) return false;
                std::vector<char> rec(buf.begin() + offset, buf.begin() + offset + rec_sz);
                offset += rec_sz;

                // parse old record
                size_t roff = 0;
                std::vector<std::string> old_fields;
                for (const auto &c : old_schema.columns) {
                    if (c.type == DatabaseManager::Type::INT32) {
                        int32_t v = 0;
                        if (!rdbms::serialization::read_pod(rec, roff, v)) return false;
                        old_fields.push_back(std::to_string(v));
                    } else if (c.type == DatabaseManager::Type::INT64) {
                        int64_t v = 0;
                        if (!rdbms::serialization::read_pod(rec, roff, v)) return false;
                        old_fields.push_back(std::to_string(v));
                    } else {
                        std::string s;
                        if (!rdbms::serialization::read_string(rec, roff, s)) return false;
                        old_fields.push_back(std::move(s));
                    }
                }

                // build new row according to new_schema
                std::vector<std::string> new_fields;
                new_fields.reserve(new_schema.columns.size());
                for (const auto &nc : new_schema.columns) {
                    // find old index for this column name
                    int old_idx = -1;
                    for (size_t i = 0; i < old_schema.columns.size(); ++i) if (old_schema.columns[i].name == nc.name) { old_idx = static_cast<int>(i); break; }
                    if (old_idx >= 0) {
                        const auto &old_col = old_schema.columns[old_idx];
                        const std::string &old_val = old_fields[static_cast<size_t>(old_idx)];
                        std::string new_val = convert_field(old_val, old_col.type, nc.type, default_fill, nc.not_null);
                        new_fields.push_back(std::move(new_val));
                    } else {
                        // column newly added: use default_fill or sensible default
                        if (!default_fill.empty()) new_fields.push_back(default_fill);
                        else {
                            if (nc.type == DatabaseManager::Type::STRING) new_fields.push_back(nc.not_null ? std::string(" ") : std::string());
                            else new_fields.push_back(std::string("0"));
                        }
                    }
                }

                new_rows.push_back(std::move(new_fields));
            }

            // write new rows to temporary file
            const std::string tmp = dataf + ".tmp";
            std::ofstream ofs(tmp, std::ios::binary | std::ios::trunc);
            if (!ofs) return false;

            for (const auto &row : new_rows) {
                std::vector<char> recbuf;
                for (size_t i = 0; i < new_schema.columns.size(); ++i) {
                    const auto &col = new_schema.columns[i];
                    const std::string &val = row[i];
                    if (col.type == DatabaseManager::Type::INT32) {
                        int32_t v = 0;
                        if (!val.empty()) v = static_cast<int32_t>(std::stoll(val));
                        rdbms::serialization::write_pod(recbuf, v);
                    } else if (col.type == DatabaseManager::Type::INT64) {
                        int64_t v = 0;
                        if (!val.empty()) v = static_cast<int64_t>(std::stoll(val));
                        rdbms::serialization::write_pod(recbuf, v);
                    } else {
                        rdbms::serialization::write_string(recbuf, val);
                    }
                }
                std::vector<char> write_buf;
                rdbms::serialization::write_u32_le(write_buf, static_cast<uint32_t>(recbuf.size()));
                if (!recbuf.empty()) rdbms::serialization::append_bytes(write_buf, recbuf.data(), recbuf.size());
                ofs.write(write_buf.data(), static_cast<std::streamsize>(write_buf.size()));
                if (!ofs) {
                    ofs.close();
                    std::error_code _ec; std::filesystem::remove(tmp, _ec);
                    return false;
                }
            }
            ofs.close();

            // replace original file
            std::error_code ec;
            std::filesystem::remove(dataf, ec);
            std::filesystem::rename(tmp, dataf, ec);
            if (ec) return false;
            return true;
        } catch (...) {
            return false;
        }
    }
} // anonymous namespace


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
        TableSchema old_schema;
        if (!read_schema_file(table_name, old_schema)) return false;
        for (const auto &c : old_schema.columns) if (c.name == col.name) return false;
        // build new schema
        TableSchema new_schema = old_schema;
        new_schema.columns.push_back(col);
        const std::string dataf = data_file_path(table_name);
        // migrate existing data to include new column values (default fill empty)
        if (!migrate_table_data(dataf, old_schema, new_schema, std::string())) return false;
        return write_schema_file(new_schema);
    } catch (...) {
        return false;
    }
}

bool DatabaseManager::remove_column(const std::string &table_name, const std::string &col_name) noexcept {
    if (current_db_.empty()) return false;
    try {
        TableSchema old_schema;
        if (!read_schema_file(table_name, old_schema)) return false;
        TableSchema new_schema = old_schema;
        auto it = std::remove_if(new_schema.columns.begin(), new_schema.columns.end(), [&](const Column &c) { return c.name == col_name; });
        if (it == new_schema.columns.end()) return false;
        new_schema.columns.erase(it, new_schema.columns.end());
        const std::string dataf = data_file_path(table_name);
        if (!migrate_table_data(dataf, old_schema, new_schema, std::string())) return false;
        return write_schema_file(new_schema);
    } catch (...) {
        return false;
    }
}

bool DatabaseManager::modify_column(const std::string &table_name, const std::string &old_name, const Column &new_col, const std::string &default_fill) noexcept {
    if (current_db_.empty()) return false;
    try {
        TableSchema old_schema;
        if (!read_schema_file(table_name, old_schema)) return false;
        int idx = -1;
        for (size_t i = 0; i < old_schema.columns.size(); ++i) if (old_schema.columns[i].name == old_name) { idx = static_cast<int>(i); break; }
        if (idx < 0) return false;
        // check name collision when renaming
        if (new_col.name != old_name) {
            for (const auto &c : old_schema.columns) if (c.name == new_col.name) return false;
        }

        TableSchema new_schema = old_schema;
        new_schema.columns[static_cast<size_t>(idx)] = new_col;

        const std::string dataf = data_file_path(table_name);
        if (!migrate_table_data(dataf, old_schema, new_schema, default_fill)) return false;
        return write_schema_file(new_schema);
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
