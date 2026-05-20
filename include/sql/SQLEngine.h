#pragma once

#include <string>
#include <vector>

namespace rdbms {

class DatabaseManager;
class DataManager;

class SQLEngine {
public:
    SQLEngine(DatabaseManager &db, DataManager &dm) noexcept;

    // Execute a single SQL statement. For SELECT, out_rows/out_columns will be filled.
    bool execute(const std::string &sql,
                 std::vector<std::vector<std::string>> &out_rows,
                 std::vector<std::string> &out_columns,
                 std::string &message) noexcept;

private:
    DatabaseManager &db_;
    DataManager &dm_;
};

} // namespace rdbms
