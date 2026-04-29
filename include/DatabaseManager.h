#pragma once

#include <string>
#include <vector>
#include <cstdint>

namespace rdbms {

class DatabaseManager {
public:
    enum class Type : uint8_t {
        INT32 = 1,
        INT64 = 2,
        STRING = 3
    };

    struct Column {
        std::string name;
        Type type = Type::STRING;
        bool is_primary = false;
        bool not_null = false;
    };

    struct TableSchema {
        std::string table_name;
        std::vector<Column> columns;
    };

    explicit DatabaseManager(const std::string &root_dir = "data") noexcept;

    // Database operations
    bool create_database(const std::string &db_name) noexcept;
    bool drop_database(const std::string &db_name) noexcept;
    bool use_database(const std::string &db_name) noexcept;
    std::string current_database() const noexcept;

    // Table operations (require a selected database)
    bool create_table(const TableSchema &schema) noexcept;
    bool drop_table(const std::string &table_name) noexcept;
    bool add_column(const std::string &table_name, const Column &col) noexcept;
    bool remove_column(const std::string &table_name, const std::string &col_name) noexcept;
    // Modify an existing column (rename and/or change type/constraints).
    // `old_name` is the existing column name; `new_col` describes the desired column
    // definition (may change `name`, `type`, `is_primary`, `not_null`).
    // `default_fill` is used when migrating existing data where conversion fails
    // or when filling values for newly added columns.
    bool modify_column(const std::string &table_name, const std::string &old_name, const Column &new_col, const std::string &default_fill = std::string()) noexcept;

    bool list_databases(std::vector<std::string> &out) const noexcept;
    bool list_tables(std::vector<std::string> &out) const noexcept;
    bool get_schema(const std::string &table_name, TableSchema &out) const noexcept;

    // Expose file path helpers so other modules can access table files
    std::string meta_file_path(const std::string &table_name) const noexcept;
    std::string data_file_path(const std::string &table_name) const noexcept;

private:
    std::string root_dir_;
    std::string current_db_;
    std::string db_path_;

    bool write_schema_file(const TableSchema &schema) noexcept;
    bool read_schema_file(const std::string &table_name, TableSchema &schema) const noexcept;
};

} // namespace rdbms
