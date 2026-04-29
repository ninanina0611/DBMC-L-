#pragma once

#include <string>
#include <vector>
#include <utility>

#include "DatabaseManager.h"

namespace rdbms {

class DataManager {
public:
    explicit DataManager(DatabaseManager &db) noexcept;

    // Insert row using column->value pairs. Values are strings and will be converted
    // to column types according to schema.
    bool insert_row(const std::string &table, const std::vector<std::pair<std::string,std::string>> &col_values) noexcept;

    // Select rows. If `cols` is empty or contains "*" then all columns are returned.
    // where_col/where_val may be empty to indicate no condition (match all).
    bool select_rows(const std::string &table,
                     const std::vector<std::string> &cols,
                     const std::string &where_col,
                     const std::string &where_val,
                     std::vector<std::vector<std::string>> &out_rows,
                     std::vector<std::string> &out_columns) noexcept;

    // Update rows matching where condition. Returns number of affected rows in `affected`.
    bool update_rows(const std::string &table,
                     const std::vector<std::pair<std::string,std::string>> &set_values,
                     const std::string &where_col,
                     const std::string &where_val,
                     size_t &affected) noexcept;

    // Delete rows matching where condition. Returns number deleted in `affected`.
    bool delete_rows(const std::string &table,
                     const std::string &where_col,
                     const std::string &where_val,
                     size_t &affected) noexcept;

private:
    DatabaseManager &db_;

    std::string data_file_path(const std::string &table) const noexcept;

    bool load_raw_records(const std::string &table, std::vector<std::vector<char>> &recs) const noexcept;
    bool parse_record(const std::vector<char> &recbuf,
                      const std::vector<DatabaseManager::Column> &columns,
                      std::vector<std::string> &out_fields) const noexcept;
    bool build_record_buffer(const std::vector<DatabaseManager::Column> &columns,
                             const std::vector<std::string> &field_values,
                             std::vector<char> &outbuf) const noexcept;
};

} // namespace rdbms
