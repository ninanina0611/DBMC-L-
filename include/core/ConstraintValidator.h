#pragma once

#include <string>
#include <vector>
#include "DatabaseManager.h"

namespace rdbms {

class ConstraintValidator {
public:
    // Validate a row (for insert or update). If self_index == -1 it's an insert,
    // otherwise it's an update and uniqueness checks ignore the row at self_index.
    static bool validate_row(const DatabaseManager::TableSchema &schema,
                             const std::vector<std::string> &fields,
                             const std::vector<std::vector<std::string>> &existing_rows,
                             int self_index,
                             std::string &errmsg) noexcept;
};

} // namespace rdbms
