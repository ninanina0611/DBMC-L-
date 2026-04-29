#pragma once

#include "SQLLexer.h"
#include <string>
#include <vector>
#include <utility>

namespace rdbms {

struct SQLStatement {
    enum class Type { Select, Insert, Update, Delete, Unknown } type = Type::Unknown;
    std::string table;
    std::vector<std::string> columns; // select list or insert columns
    std::vector<std::string> values;  // insert values
    std::vector<std::pair<std::string,std::string>> assignments; // update set list
    std::string where_col;
    std::string where_val;
};

class SQLParser {
public:
    // Parse tokens (produced by SQLLexer) into SQLStatement. Returns false and sets errmsg on failure.
    static bool parse(const std::vector<SQLToken> &tokens, SQLStatement &out_stmt, std::string *errmsg = nullptr) noexcept;
};

} // namespace rdbms
