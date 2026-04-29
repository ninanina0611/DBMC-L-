#include "../include/ConstraintValidator.h"

#include <limits>
#include <cstdint>
#include <cstdlib>

namespace rdbms {

static bool check_type_and_not_null(const DatabaseManager::Column &col, const std::string &val, std::string &errmsg) noexcept {
    // empty value
    if (val.empty()) {
        if (col.not_null) {
            errmsg = "column '" + col.name + "' cannot be null";
            return false;
        }
        return true;
    }

    try {
        if (col.type == DatabaseManager::Type::INT32) {
            long long v = std::stoll(val);
            if (v < std::numeric_limits<int32_t>::min() || v > std::numeric_limits<int32_t>::max()) {
                errmsg = "value out of range for INT32 in column '" + col.name + "'";
                return false;
            }
            return true;
        } else if (col.type == DatabaseManager::Type::INT64) {
            long long v = std::stoll(val);
            (void)v;
            return true;
        } else { // STRING
            return true;
        }
    } catch (...) {
        errmsg = "type conversion error for column '" + col.name + "'";
        return false;
    }
}

bool ConstraintValidator::validate_row(const DatabaseManager::TableSchema &schema,
                                       const std::vector<std::string> &fields,
                                       const std::vector<std::vector<std::string>> &existing_rows,
                                       int self_index,
                                       std::string &errmsg) noexcept {
    errmsg.clear();
    if (fields.size() != schema.columns.size()) { errmsg = "field count mismatch"; return false; }

    // type and not-null checks
    for (size_t i = 0; i < schema.columns.size(); ++i) {
        const auto &col = schema.columns[i];
        if (!check_type_and_not_null(col, fields[i], errmsg)) return false;
    }

    // primary key uniqueness
    std::vector<size_t> pk_idx;
    for (size_t i = 0; i < schema.columns.size(); ++i) if (schema.columns[i].is_primary) pk_idx.push_back(i);
    if (pk_idx.empty()) return true;

    auto make_key = [&](const std::vector<std::string> &row) {
        std::string k;
        for (size_t j = 0; j < pk_idx.size(); ++j) {
            if (j) k.push_back('|');
            size_t idx = pk_idx[j];
            if (idx < row.size()) k += row[idx];
        }
        return k;
    };

    const std::string mykey = make_key(fields);
    for (size_t i = 0; i < existing_rows.size(); ++i) {
        if (static_cast<int>(i) == self_index) continue;
        const auto &r = existing_rows[i];
        if (r.size() != schema.columns.size()) continue;
        if (make_key(r) == mykey) {
            errmsg = "duplicate primary key";
            return false;
        }
    }

    return true;
}

} // namespace rdbms
