#include "DatabaseManager.h"
#include "FileManager.h"
#include "Serializer.h"

#include <filesystem>
#include <system_error>
#include <algorithm>
#include <fstream>
#include <sstream>
#include <iomanip>
#include <ctime>

namespace fs = std::filesystem;

namespace rdbms {

namespace {
    static std::string format_numeric(const std::string &val, DatabaseManager::Type new_t,
                                      const std::string &scale_str) noexcept {
        try {
            double dv = val.empty() ? 0.0 : std::stod(val);
            if (new_t == DatabaseManager::Type::INT32 || new_t == DatabaseManager::Type::INT64) {
                return std::to_string(static_cast<long long>(dv));
            }
            int scale = 0;
            if (!scale_str.empty()) {
                try { scale = std::stoi(scale_str); } catch (...) {}
            }
            if (scale > 0) {
                std::ostringstream oss;
                oss << std::fixed << std::setprecision(scale) << dv;
                return oss.str();
            }
            std::ostringstream oss;
            oss << std::setprecision(15) << dv;
            return oss.str();
        } catch (...) {
            return std::string("0");
        }
    }

    static std::string convert_field(const std::string &old_val,
                                     DatabaseManager::Type old_t,
                                     const DatabaseManager::Column &new_col,
                                     const std::string &default_fill) noexcept {
        try {
            if (new_col.type == DatabaseManager::Type::STRING) {
                if (old_val.empty()) {
                    if (!default_fill.empty()) return default_fill;
                    if (new_col.not_null) return std::string(" ");
                    return std::string();
                }
                return old_val;
            }

            if (old_val.empty()) {
                if (!default_fill.empty()) return default_fill;
                return format_numeric("0", new_col.type, new_col.scale);
            }
            try {
                return format_numeric(old_val, new_col.type, new_col.scale);
            } catch (...) {
                if (!default_fill.empty()) return default_fill;
                return format_numeric("0", new_col.type, new_col.scale);
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
                        std::string new_val = convert_field(old_val, old_col.type, nc, default_fill);
                        new_fields.push_back(std::move(new_val));
                    } else {
                        // column newly added: prefer explicit default_fill (argument), then column default, then sensible default
                        std::string fill = default_fill.empty() ? nc.default_value : default_fill;
                        if (!fill.empty()) {
                            // support special CURRENT_TIMESTAMP
                            std::string up = fill;
                            std::transform(up.begin(), up.end(), up.begin(), [](unsigned char c){ return std::toupper(c); });
                            if (up == "CURRENT_TIMESTAMP" || up == "CURRENT_TIMESTAMP()") {
                                // produce current datetime string
                                std::time_t t = std::time(nullptr);
                                std::tm tm;
#ifdef _MSC_VER
                                localtime_s(&tm, &t);
#else
                                localtime_r(&t, &tm);
#endif
                                char buf[32];
                                if (nc.display_type == "DATE") std::strftime(buf, sizeof(buf), "%Y-%m-%d", &tm);
                                else std::strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", &tm);
                                new_fields.push_back(std::string(buf));
                            } else {
                                new_fields.push_back(fill);
                            }
                        } else {
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

std::string DatabaseManager::type_name(Type t) noexcept {
    switch (t) {
        case Type::INT32: return "INT32";
        case Type::INT64: return "INT64";
        case Type::STRING: return "STRING";
        case Type::FLOAT: return "FLOAT";
        case Type::DOUBLE: return "DOUBLE";
        case Type::BOOL: return "BOOL";
    }
    return "STRING";
}

DatabaseManager::Type DatabaseManager::type_from_name(const std::string &name) noexcept {
    if (name == "TINYINT" || name == "SMALLINT" || name == "MEDIUMINT" ||
        name == "INT" || name == "INTEGER" || name == "INT32") return Type::INT32;
    if (name == "BIGINT" || name == "INT64") return Type::INT64;
    if (name == "FLOAT" || name == "REAL") return Type::FLOAT;
    if (name == "DOUBLE" || name == "DOUBLE PRECISION") return Type::DOUBLE;
    // treat DECIMAL/NUMERIC/DEC as numeric (map to DOUBLE) so arithmetic works in UPDATE
    if (name == "DECIMAL" || name == "DEC" || name == "NUMERIC") return Type::DOUBLE;
    if (name == "BOOL" || name == "BOOLEAN" || name == "BIT") return Type::BOOL;
    if (name == "DATE" || name == "TIME" || name == "DATETIME" || name == "TIMESTAMP" ||
        name == "YEAR" || name == "ENUM" || name == "SET" || name == "JSON" ||
        name == "CHAR" || name == "VARCHAR" || name == "BINARY" || name == "VARBINARY" ||
        name == "TINYTEXT" || name == "TEXT" || name == "MEDIUMTEXT" || name == "LONGTEXT" ||
        name == "TINYBLOB" || name == "BLOB" || name == "MEDIUMBLOB" || name == "LONGBLOB")
        return Type::STRING;
    return Type::STRING;
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
    // if in transaction, use transactional copy in txn_dir_
    try {
        if (in_transaction_) {
            fs::path txn = fs::path(txn_dir_) / (table_name + ".meta");
            if (!fs::exists(txn)) {
                fs::path orig = fs::path(db_path_) / (table_name + ".meta");
                if (fs::exists(orig)) {
                    std::error_code ec;
                    fs::copy_file(orig, txn, ec);
                } else {
                    FileManager::create_file(txn.string());
                }
            }
            return txn.string();
        }
    } catch (...) {
        // fallthrough
    }
    return db_path_ + "/" + table_name + ".meta";
}

std::string DatabaseManager::data_file_path(const std::string &table_name) const noexcept {
    if (db_path_.empty()) return std::string();
    try {
        if (in_transaction_) {
            fs::path txn = fs::path(txn_dir_) / (table_name + ".bin");
            if (!fs::exists(txn)) {
                fs::path orig = fs::path(db_path_) / (table_name + ".bin");
                if (fs::exists(orig)) {
                    std::error_code ec;
                    fs::copy_file(orig, txn, ec);
                } else {
                    FileManager::create_file(txn.string());
                }
            }
            return txn.string();
        }
    } catch (...) {
        // fallthrough
    }
    return db_path_ + "/" + table_name + ".bin";
}

bool DatabaseManager::start_transaction() noexcept {
    if (current_db_.empty()) return false;
    if (in_transaction_) return false;
    try {
        fs::path td = fs::path(db_path_) / ".txn";
        if (!fs::exists(td)) fs::create_directories(td);
        txn_dir_ = td.string();
        in_transaction_ = true;
        return true;
    } catch (...) {
        return false;
    }
}

bool DatabaseManager::commit_transaction() noexcept {
    if (!in_transaction_) return false;
    try {
        std::error_code ec;
        for (auto &e : fs::directory_iterator(txn_dir_)) {
            if (!e.is_regular_file()) continue;
            fs::path src = e.path();
            fs::path dest = fs::path(db_path_) / src.filename();
            if (fs::exists(dest)) fs::remove(dest, ec);
            fs::rename(src, dest, ec);
            if (ec) return false;
        }
        fs::remove_all(txn_dir_, ec);
        in_transaction_ = false;
        txn_dir_.clear();
        return true;
    } catch (...) {
        return false;
    }
}

bool DatabaseManager::rollback_transaction() noexcept {
    if (!in_transaction_) return false;
    try {
        std::error_code ec;
        fs::remove_all(txn_dir_, ec);
        in_transaction_ = false;
        txn_dir_.clear();
        return true;
    } catch (...) {
        return false;
    }
}

bool DatabaseManager::in_transaction() const noexcept {
    return in_transaction_;
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
            rdbms::serialization::write_string(buf, c.display_type);
            rdbms::serialization::write_string(buf, c.length);
            rdbms::serialization::write_string(buf, c.scale);
            uint8_t pk = c.is_primary ? 1 : 0;
            rdbms::serialization::write_pod(buf, pk);
            uint8_t nn = c.not_null ? 1 : 0;
            rdbms::serialization::write_pod(buf, nn);
            uint8_t uq = c.is_unique ? 1 : 0;
            rdbms::serialization::write_pod(buf, uq);
            uint8_t ai = c.auto_increment ? 1 : 0;
            rdbms::serialization::write_pod(buf, ai);
            // optional default value (newer schema versions)
            rdbms::serialization::write_string(buf, c.default_value);
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
            if (offset < buf.size()) {
                rdbms::serialization::read_string(buf, offset, c.display_type);
            }
            if (c.display_type.empty()) c.display_type = type_name(c.type);
            if (offset < buf.size()) {
                rdbms::serialization::read_string(buf, offset, c.length);
            }
            if (offset < buf.size()) {
                rdbms::serialization::read_string(buf, offset, c.scale);
            }
            uint8_t pk = 0;
            if (!rdbms::serialization::read_pod(buf, offset, pk)) return false;
            c.is_primary = pk != 0;
            uint8_t nn = 0;
            if (!rdbms::serialization::read_pod(buf, offset, nn)) return false;
            c.not_null = nn != 0;
            uint8_t uq = 0;
            if (offset < buf.size()) {
                if (!rdbms::serialization::read_pod(buf, offset, uq)) return false;
            }
            c.is_unique = uq != 0;
            uint8_t ai = 0;
            if (offset < buf.size()) {
                if (!rdbms::serialization::read_pod(buf, offset, ai)) return false;
            }
            c.auto_increment = ai != 0;
            // optional default value (newer schema versions)
            if (offset < buf.size()) {
                std::string dv;
                if (!rdbms::serialization::read_string(buf, offset, dv)) return false;
                c.default_value = dv;
            }
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
