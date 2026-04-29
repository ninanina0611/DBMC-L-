#include "../include/DataManager.h"
#include "../include/DatabaseManager.h"
#include "../include/FileManager.h"
#include "../include/Serializer.h"

#include <filesystem>
#include <algorithm>
#include <unordered_map>
#include "../include/ConstraintValidator.h"

namespace fs = std::filesystem;

namespace rdbms {

DataManager::DataManager(DatabaseManager &db) noexcept : db_(db) {}

std::string DataManager::data_file_path(const std::string &table) const noexcept {
    return db_.data_file_path(table);
}

bool DataManager::load_raw_records(const std::string &table, std::vector<std::vector<char>> &recs) const noexcept {
    try {
        const std::string dataf = data_file_path(table);
        recs.clear();
        if (!fs::exists(dataf)) return true;
        uint64_t fsize = static_cast<uint64_t>(fs::file_size(dataf));
        if (fsize == 0) return true;
        std::vector<char> buf;
        if (!FileManager::read_at(dataf, 0, static_cast<size_t>(fsize), buf)) return false;
        size_t offset = 0;
        while (offset < buf.size()) {
            uint32_t rec_sz = 0;
            if (!rdbms::serialization::read_u32_le(buf, offset, rec_sz)) return false;
            if (offset + rec_sz > buf.size()) return false;
            std::vector<char> rec(buf.begin() + offset, buf.begin() + offset + rec_sz);
            recs.push_back(std::move(rec));
            offset += rec_sz;
        }
        return true;
    } catch (...) {
        return false;
    }
}

bool DataManager::parse_record(const std::vector<char> &recbuf,
                               const std::vector<DatabaseManager::Column> &columns,
                               std::vector<std::string> &out_fields) const noexcept {
    try {
        out_fields.clear();
        size_t offset = 0;
        for (const auto &col : columns) {
            if (col.type == DatabaseManager::Type::INT32) {
                int32_t v = 0;
                if (!rdbms::serialization::read_pod(recbuf, offset, v)) return false;
                out_fields.push_back(std::to_string(v));
            } else if (col.type == DatabaseManager::Type::INT64) {
                int64_t v = 0;
                if (!rdbms::serialization::read_pod(recbuf, offset, v)) return false;
                out_fields.push_back(std::to_string(v));
            } else { // STRING
                std::string s;
                if (!rdbms::serialization::read_string(recbuf, offset, s)) return false;
                out_fields.push_back(std::move(s));
            }
        }
        return true;
    } catch (...) {
        return false;
    }
}

bool DataManager::build_record_buffer(const std::vector<DatabaseManager::Column> &columns,
                                     const std::vector<std::string> &field_values,
                                     std::vector<char> &outbuf) const noexcept {
    try {
        outbuf.clear();
        if (field_values.size() != columns.size()) return false;
        for (size_t i = 0; i < columns.size(); ++i) {
            const auto &col = columns[i];
            const std::string &val = field_values[i];
            if (col.type == DatabaseManager::Type::INT32) {
                int32_t v = 0;
                if (!val.empty()) v = static_cast<int32_t>(std::stoll(val));
                rdbms::serialization::write_pod(outbuf, v);
            } else if (col.type == DatabaseManager::Type::INT64) {
                int64_t v = 0;
                if (!val.empty()) v = static_cast<int64_t>(std::stoll(val));
                rdbms::serialization::write_pod(outbuf, v);
            } else { // STRING
                rdbms::serialization::write_string(outbuf, val);
            }
        }
        return true;
    } catch (...) {
        return false;
    }
}

bool DataManager::insert_row(const std::string &table, const std::vector<std::pair<std::string,std::string>> &col_values, std::string *errmsg) noexcept {
    try {
        DatabaseManager::TableSchema schema;
        if (!db_.get_schema(table, schema)) {
            if (errmsg) {
                if (db_.current_database().empty()) *errmsg = "no database selected";
                else *errmsg = std::string("unknown table: ") + table;
            }
            return false;
        }

        std::unordered_map<std::string, std::string> mp;
        for (const auto &p : col_values) mp[p.first] = p.second;

        std::vector<std::string> fields;
        for (const auto &c : schema.columns) {
            auto it = mp.find(c.name);
            std::string v = (it != mp.end()) ? it->second : std::string();
            fields.push_back(std::move(v));
        }

        // load existing records and parse to strings for uniqueness checks
        std::vector<std::vector<char>> recs;
        if (!load_raw_records(table, recs)) {
            if (errmsg) *errmsg = "failed to load table data";
            return false;
        }
        std::vector<std::vector<std::string>> parsed_recs;
        for (const auto &r : recs) {
            std::vector<std::string> pf;
            if (!parse_record(r, schema.columns, pf)) return false;
            parsed_recs.push_back(std::move(pf));
        }

        std::string vmsg;
        if (!rdbms::ConstraintValidator::validate_row(schema, fields, parsed_recs, -1, vmsg)) {
            if (errmsg) *errmsg = vmsg;
            return false;
        }

        std::vector<char> recbuf;
        if (!build_record_buffer(schema.columns, fields, recbuf)) {
            if (errmsg) *errmsg = "failed to build record buffer";
            return false;
        }

        std::vector<char> write_buf;
        rdbms::serialization::write_u32_le(write_buf, static_cast<uint32_t>(recbuf.size()));
        if (!recbuf.empty()) rdbms::serialization::append_bytes(write_buf, recbuf.data(), recbuf.size());

        const std::string dataf = data_file_path(table);
        if (!FileManager::create_file(dataf)) {
            if (errmsg) *errmsg = "failed to create data file";
            return false;
        }
        uint64_t offset = 0;
        if (fs::exists(dataf)) offset = static_cast<uint64_t>(fs::file_size(dataf));
        bool w = FileManager::write_at(dataf, offset, write_buf);
        if (!w) {
            if (errmsg) *errmsg = "failed to write to data file";
            return false;
        }
        return true;
    } catch (...) {
        if (errmsg) *errmsg = "exception in insert_row";
        return false;
    }
}

bool DataManager::select_rows(const std::string &table,
                              const std::vector<std::string> &cols,
                              const std::string &where_col,
                              const std::string &where_val,
                              std::vector<std::vector<std::string>> &out_rows,
                              std::vector<std::string> &out_columns,
                              std::string *errmsg) noexcept {
    try {
        DatabaseManager::TableSchema schema;
        if (!db_.get_schema(table, schema)) {
            if (errmsg) {
                if (db_.current_database().empty()) *errmsg = "no database selected";
                else *errmsg = std::string("unknown table: ") + table;
            }
            return false;
        }

        std::vector<std::vector<char>> recs;
        if (!load_raw_records(table, recs)) {
            if (errmsg) *errmsg = "failed to load table data";
            return false;
        }

        // Determine column indices to return
        std::vector<size_t> return_indices;
        if (cols.empty() || (cols.size() == 1 && cols[0] == "*")) {
            for (size_t i = 0; i < schema.columns.size(); ++i) return_indices.push_back(i);
        } else {
            for (const auto &cname : cols) {
                size_t idx = 0;
                bool found = false;
                for (; idx < schema.columns.size(); ++idx) if (schema.columns[idx].name == cname) { found = true; break; }
                if (!found) {
                    if (errmsg) *errmsg = "unknown column: " + cname;
                    return false;
                }
                return_indices.push_back(idx);
            }
        }

        // prepare output column names
        out_columns.clear();
        for (auto idx : return_indices) out_columns.push_back(schema.columns[idx].name);

        // find where index if specified
        int where_idx = -1;
        if (!where_col.empty()) {
            for (size_t i = 0; i < schema.columns.size(); ++i) if (schema.columns[i].name == where_col) { where_idx = static_cast<int>(i); break; }
            if (where_idx < 0) {
                if (errmsg) *errmsg = "unknown where column: " + where_col;
                return false;
            }
        }

        out_rows.clear();
        for (const auto &rec : recs) {
            std::vector<std::string> fields;
            if (!parse_record(rec, schema.columns, fields)) {
                if (errmsg) *errmsg = "failed to parse record";
                return false;
            }
            bool match = true;
            if (where_idx >= 0) {
                match = (fields[where_idx] == where_val);
            }
            if (match) {
                std::vector<std::string> row;
                for (auto idx : return_indices) row.push_back(fields[idx]);
                out_rows.push_back(std::move(row));
            }
        }
        return true;
    } catch (...) {
        if (errmsg) *errmsg = "exception in select_rows";
        return false;
    }
}

bool DataManager::update_rows(const std::string &table,
                              const std::vector<std::pair<std::string,std::string>> &set_values,
                              const std::string &where_col,
                              const std::string &where_val,
                              size_t &affected,
                              std::string *errmsg) noexcept {
    affected = 0;
    try {
        DatabaseManager::TableSchema schema;
        if (!db_.get_schema(table, schema)) {
            if (errmsg) {
                if (db_.current_database().empty()) *errmsg = "no database selected";
                else *errmsg = std::string("unknown table: ") + table;
            }
            return false;
        }

        std::vector<std::vector<char>> recs;
        if (!load_raw_records(table, recs)) {
            if (errmsg) *errmsg = "failed to load table data";
            return false;
        }

        // parse all records first
        std::vector<std::vector<std::string>> parsed_recs;
        parsed_recs.reserve(recs.size());
        for (const auto &rec : recs) {
            std::vector<std::string> pf;
            if (!parse_record(rec, schema.columns, pf)) return false;
            parsed_recs.push_back(std::move(pf));
        }

        // prepare set map
        std::unordered_map<std::string, std::string> setmap;
        for (const auto &p : set_values) setmap[p.first] = p.second;

        int where_idx = -1;
        if (!where_col.empty()) {
            for (size_t i = 0; i < schema.columns.size(); ++i) if (schema.columns[i].name == where_col) { where_idx = static_cast<int>(i); break; }
            if (where_idx < 0) return false;
        }

        std::vector<std::vector<char>> new_recs;
        for (size_t rec_i = 0; rec_i < parsed_recs.size(); ++rec_i) {
            std::vector<std::string> fields = parsed_recs[rec_i];
            bool match = true;
            if (where_idx >= 0) match = (fields[where_idx] == where_val);
            if (match) {
                // apply set
                for (size_t i = 0; i < schema.columns.size(); ++i) {
                    const auto &col = schema.columns[i];
                    auto it = setmap.find(col.name);
                    if (it != setmap.end()) fields[i] = it->second;
                }
                ++affected;
            }

            // validate row after possible modification (check types/not-null/uniqueness)
            std::string vmsg;
            if (!rdbms::ConstraintValidator::validate_row(schema, fields, parsed_recs, static_cast<int>(rec_i), vmsg)) {
                if (errmsg) *errmsg = vmsg;
                return false;
            }

            // update parsed_recs so subsequent uniqueness checks see changes
            parsed_recs[rec_i] = fields;

            // rebuild record buffer for this row
            std::vector<char> newbuf;
            if (!build_record_buffer(schema.columns, fields, newbuf)) {
                if (errmsg) *errmsg = "failed to build record buffer";
                return false;
            }
            new_recs.push_back(std::move(newbuf));
        }

        // rewrite file
        const std::string dataf = data_file_path(table);
        if (!FileManager::remove_file(dataf)) {
            if (errmsg) *errmsg = "failed to rewrite data file";
            return false;
        }
        if (!FileManager::create_file(dataf)) {
            if (errmsg) *errmsg = "failed to rewrite data file";
            return false;
        }
        uint64_t offset = 0;
        for (const auto &rb : new_recs) {
            std::vector<char> write_buf;
            rdbms::serialization::write_u32_le(write_buf, static_cast<uint32_t>(rb.size()));
            if (!rb.empty()) rdbms::serialization::append_bytes(write_buf, rb.data(), rb.size());
            if (!FileManager::write_at(dataf, offset, write_buf)) {
                if (errmsg) *errmsg = "failed to write updated data";
                return false;
            }
            offset += write_buf.size();
        }

        return true;
    } catch (...) {
        if (errmsg) *errmsg = "exception in update_rows";
        return false;
    }
}

bool DataManager::delete_rows(const std::string &table,
                              const std::string &where_col,
                              const std::string &where_val,
                              size_t &affected,
                              std::string *errmsg) noexcept {
    affected = 0;
    try {
        DatabaseManager::TableSchema schema;
        if (!db_.get_schema(table, schema)) {
            if (errmsg) {
                if (db_.current_database().empty()) *errmsg = "no database selected";
                else *errmsg = std::string("unknown table: ") + table;
            }
            return false;
        }

        std::vector<std::vector<char>> recs;
        if (!load_raw_records(table, recs)) {
            if (errmsg) *errmsg = "failed to load table data";
            return false;
        }

        int where_idx = -1;
        if (!where_col.empty()) {
            for (size_t i = 0; i < schema.columns.size(); ++i) if (schema.columns[i].name == where_col) { where_idx = static_cast<int>(i); break; }
            if (where_idx < 0) {
                if (errmsg) *errmsg = "unknown where column: " + where_col;
                return false;
            }
        }

        std::vector<std::vector<char>> new_recs;
        for (const auto &rec : recs) {
            std::vector<std::string> fields;
            if (!parse_record(rec, schema.columns, fields)) {
                if (errmsg) *errmsg = "failed to parse record";
                return false;
            }
            bool match = true;
            if (where_idx >= 0) match = (fields[where_idx] == where_val);
            if (match) {
                ++affected;
                continue; // skip this record
            }
            new_recs.push_back(rec);
        }

        // rewrite file
        const std::string dataf = data_file_path(table);
        if (!FileManager::remove_file(dataf)) {
            if (errmsg) *errmsg = "failed to rewrite data file";
            return false;
        }
        if (!FileManager::create_file(dataf)) {
            if (errmsg) *errmsg = "failed to rewrite data file";
            return false;
        }
        uint64_t offset = 0;
        for (const auto &rb : new_recs) {
            std::vector<char> write_buf;
            rdbms::serialization::write_u32_le(write_buf, static_cast<uint32_t>(rb.size()));
            if (!rb.empty()) rdbms::serialization::append_bytes(write_buf, rb.data(), rb.size());
            if (!FileManager::write_at(dataf, offset, write_buf)) {
                if (errmsg) *errmsg = "failed to write updated data";
                return false;
            }
            offset += write_buf.size();
        }

        return true;
    } catch (...) {
        if (errmsg) *errmsg = "exception in delete_rows";
        return false;
    }
}

} // namespace rdbms
