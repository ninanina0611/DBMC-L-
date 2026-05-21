#pragma once

#include "SQLLexer.h"
#include "DatabaseManager.h"
#include <string>
#include <vector>
#include <utility>

namespace rdbms {

struct SQLStatement {
    enum class Type {
        Select,
        Insert,
        Update,
        Delete,
        // DDL
        CreateDatabase,
        DropDatabase,
        CreateTable,
        DropTable,
        RenameTable,
        AlterTableAddColumn,
        AlterTableDropColumn,
        AlterTableModifyColumn,
        AlterTableRenameColumn,
        UseDatabase,
        // Transaction control
        StartTransaction,
        Commit,
        Rollback,
        Unknown
    } type = Type::Unknown;

    // General
    std::string table;
    std::vector<std::string> columns; // select list or insert columns
    std::vector<std::string> values;  // insert values
    // For INSERT with multiple value tuples: each inner vector is one row
    std::vector<std::vector<std::string>> values_rows;
    std::vector<std::pair<std::string,std::string>> assignments; // update set list
    std::string where_col;
    std::string where_op = "=";
    std::string where_val;

    // ORDER / LIMIT / OFFSET (for SELECT)
    std::string order_by; // column name to order by (empty = none)
    std::string order_dir; // "ASC" or "DESC"
    int limit = -1;        // -1 = no limit
    int offset = 0;        // default offset

    // GROUP BY / HAVING / aggregates (for SELECT)
    std::string group_by;
    std::string having_col;
    std::string having_op = "=";
    std::string having_val;
    std::vector<std::string> select_aliases;    // AS aliases (parallel to columns)
    std::vector<std::string> select_funcs;      // aggregate func names (parallel to columns, empty=plain col)
    std::vector<std::string> select_func_args;  // aggregate func args (parallel to columns)

    // DDL-specific
    std::string db_name; // for CREATE/DROP/USE DATABASE
    bool if_not_exists = false; // for CREATE DATABASE IF NOT EXISTS
        bool if_exists = false; // for DROP TABLE/DB IF EXISTS
    DatabaseManager::TableSchema create_table_schema; // for CREATE TABLE
    std::string alter_table; // target table for ALTER
    DatabaseManager::Column alter_column; // column definition for ALTER ADD
    std::string alter_column_name; // column name for ALTER DROP
    // For RENAME TABLE a list of (old_name, new_name) pairs
    std::vector<std::pair<std::string,std::string>> rename_pairs;
};

class SQLParser {
public:
    // Parse tokens (produced by SQLLexer) into SQLStatement. Returns false and sets errmsg on failure.
    static bool parse(const std::vector<SQLToken> &tokens, SQLStatement &out_stmt, std::string *errmsg = nullptr) noexcept;
};

} // namespace rdbms
