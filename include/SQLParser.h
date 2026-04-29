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
        AlterTableAddColumn,
        AlterTableDropColumn,
        UseDatabase,
        Unknown
    } type = Type::Unknown;

    // General
    std::string table;
    std::vector<std::string> columns; // select list or insert columns
    std::vector<std::string> values;  // insert values
    std::vector<std::pair<std::string,std::string>> assignments; // update set list
    std::string where_col;
    std::string where_val;

    // DDL-specific
    std::string db_name; // for CREATE/DROP/USE DATABASE
    DatabaseManager::TableSchema create_table_schema; // for CREATE TABLE
    std::string alter_table; // target table for ALTER
    DatabaseManager::Column alter_column; // column definition for ALTER ADD
    std::string alter_column_name; // column name for ALTER DROP
};

class SQLParser {
public:
    // Parse tokens (produced by SQLLexer) into SQLStatement. Returns false and sets errmsg on failure.
    static bool parse(const std::vector<SQLToken> &tokens, SQLStatement &out_stmt, std::string *errmsg = nullptr) noexcept;
};

} // namespace rdbms
