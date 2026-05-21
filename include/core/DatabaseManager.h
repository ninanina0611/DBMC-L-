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
        STRING = 3,
        FLOAT = 4,
        DOUBLE = 5,
        BOOL = 6
    };

    struct Column {
        std::string name;
        Type type = Type::STRING;
        std::string display_type;
        std::string length;
        std::string scale;
        bool is_primary = false;
        bool not_null = false;
        bool is_unique = false;
        bool auto_increment = false;
        std::string default_value;
    };

    struct TableSchema {
        std::string table_name;
        std::vector<Column> columns;
    };

    explicit DatabaseManager(const std::string &root_dir = "data") noexcept;

    // Transaction control
    bool start_transaction() noexcept;
    bool commit_transaction() noexcept;
    bool rollback_transaction() noexcept;
    bool in_transaction() const noexcept;

    bool create_database(const std::string &db_name) noexcept;
    bool drop_database(const std::string &db_name) noexcept;
    bool use_database(const std::string &db_name) noexcept;
    std::string current_database() const noexcept;

    bool create_table(const TableSchema &schema) noexcept;
    bool drop_table(const std::string &table_name) noexcept;
    bool add_column(const std::string &table_name, const Column &col) noexcept;
    bool remove_column(const std::string &table_name, const std::string &col_name) noexcept;
    bool modify_column(const std::string &table_name, const std::string &old_name, const Column &new_col, const std::string &default_fill = std::string()) noexcept;

    bool list_databases(std::vector<std::string> &out) const noexcept;
    bool list_tables(std::vector<std::string> &out) const noexcept;
    bool get_schema(const std::string &table_name, TableSchema &out) const noexcept;

    std::string meta_file_path(const std::string &table_name) const noexcept;
    std::string data_file_path(const std::string &table_name) const noexcept;

    static std::string type_name(Type t) noexcept;
    static Type type_from_name(const std::string &name) noexcept;

private:
    std::string root_dir_;
    std::string current_db_;
    std::string db_path_;
    bool in_transaction_ = false;
    std::string txn_dir_;

    bool write_schema_file(const TableSchema &schema) noexcept;
    bool read_schema_file(const std::string &table_name, TableSchema &schema) const noexcept;
};

} // namespace rdbms
