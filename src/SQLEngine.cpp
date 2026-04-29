#include "../include/SQLEngine.h"
#include "../include/DatabaseManager.h"
#include "../include/DataManager.h"

#include <algorithm>
#include <cctype>
#include <sstream>

namespace rdbms {

static inline std::string trim(const std::string &s) {
    size_t a = 0, b = s.size();
    while (a < b && std::isspace(static_cast<unsigned char>(s[a]))) ++a;
    while (b > a && std::isspace(static_cast<unsigned char>(s[b-1]))) --b;
    return s.substr(a, b - a);
}

static inline std::string to_upper(const std::string &s) {
    std::string r = s;
    std::transform(r.begin(), r.end(), r.begin(), [](unsigned char c){ return std::toupper(c); });
    return r;
}

// split comma separated list, ignoring commas inside single or double quotes
static std::vector<std::string> split_csv(const std::string &s) {
    std::vector<std::string> out;
    std::string cur;
    bool in_quote = false;
    char q = 0;
    for (size_t i = 0; i < s.size(); ++i) {
        char c = s[i];
        if (in_quote) {
            if (c == q) { in_quote = false; cur.push_back(c); }
            else cur.push_back(c);
        } else {
            if (c == '\'' || c == '"') { in_quote = true; q = c; cur.push_back(c); }
            else if (c == ',') { out.push_back(trim(cur)); cur.clear(); }
            else cur.push_back(c);
        }
    }
    if (!cur.empty()) out.push_back(trim(cur));
    return out;
}

static std::string strip_quotes(const std::string &s) {
    std::string t = trim(s);
    if (t.size() >= 2 && ((t.front() == '\'' && t.back() == '\'') || (t.front() == '"' && t.back() == '"'))) {
        return t.substr(1, t.size() - 2);
    }
    return t;
}

SQLEngine::SQLEngine(DatabaseManager &db, DataManager &dm) noexcept : db_(db), dm_(dm) {}

bool SQLEngine::execute(const std::string &sql_raw,
                        std::vector<std::vector<std::string>> &out_rows,
                        std::vector<std::string> &out_columns,
                        std::string &message) noexcept {
    out_rows.clear(); out_columns.clear(); message.clear();
    std::string sql = trim(sql_raw);
    if (sql.empty()) { message = "empty"; return false; }
    // remove trailing semicolon
    if (!sql.empty() && sql.back() == ';') sql.pop_back();

    std::string up = to_upper(sql);

    try {
        if (up.rfind("INSERT", 0) == 0) {
            // INSERT INTO table (c1,c2) VALUES (v1,v2)
            size_t pos_into = up.find("INTO");
            if (pos_into == std::string::npos) { message = "malformed INSERT"; return false; }
            size_t p = pos_into + 4;
            while (p < up.size() && std::isspace((unsigned char)up[p])) ++p;
            // read table name until space or (
            size_t table_end = p;
            while (table_end < up.size() && !std::isspace((unsigned char)up[table_end]) && up[table_end] != '(') ++table_end;
            std::string table = trim(sql.substr(p, table_end - p));

            size_t pos_par_open = up.find('(', table_end);
            size_t pos_par_close = up.find(')', pos_par_open);
            if (pos_par_open == std::string::npos || pos_par_close == std::string::npos) { message = "malformed INSERT columns"; return false; }
            std::string cols_str = sql.substr(pos_par_open + 1, pos_par_close - pos_par_open - 1);
            auto cols = split_csv(cols_str);

            size_t pos_values = up.find("VALUES", pos_par_close);
            if (pos_values == std::string::npos) { message = "missing VALUES"; return false; }
            size_t val_open = up.find('(', pos_values);
            size_t val_close = up.find(')', val_open);
            if (val_open == std::string::npos || val_close == std::string::npos) { message = "malformed VALUES"; return false; }
            std::string vals_str = sql.substr(val_open + 1, val_close - val_open - 1);
            auto vals = split_csv(vals_str);

            if (cols.size() != vals.size()) { message = "column/value count mismatch"; return false; }
            std::vector<std::pair<std::string,std::string>> pairs;
            for (size_t i = 0; i < cols.size(); ++i) pairs.emplace_back(trim(cols[i]), strip_quotes(vals[i]));
            bool ok = dm_.insert_row(table, pairs);
            message = ok ? "OK" : "INSERT failed";
            return ok;
        } else if (up.rfind("SELECT", 0) == 0) {
            // SELECT cols FROM table [WHERE col = val]
            size_t pos_from = up.find(" FROM ");
            if (pos_from == std::string::npos) { message = "malformed SELECT"; return false; }
            std::string cols_part = sql.substr(6, pos_from - 6);
            auto cols = split_csv(cols_part);
            size_t pos_where = up.find(" WHERE ", pos_from);
            std::string table;
            if (pos_where == std::string::npos) {
                table = trim(sql.substr(pos_from + 6));
            } else {
                table = trim(sql.substr(pos_from + 6, pos_where - (pos_from + 6)));
            }
            std::string where_col, where_val;
            if (pos_where != std::string::npos) {
                std::string cond = sql.substr(pos_where + 7);
                size_t eq = cond.find('=');
                if (eq == std::string::npos) { message = "unsupported WHERE"; return false; }
                where_col = trim(cond.substr(0, eq));
                where_val = strip_quotes(cond.substr(eq + 1));
            }
            bool ok = dm_.select_rows(table, cols, where_col, where_val, out_rows, out_columns);
            message = ok ? "OK" : "SELECT failed";
            return ok;
        } else if (up.rfind("UPDATE", 0) == 0) {
            // UPDATE table SET c1=v1, c2=v2 [WHERE col=val]
            size_t pos_set = up.find(" SET ");
            if (pos_set == std::string::npos) { message = "malformed UPDATE"; return false; }
            std::string table = trim(sql.substr(6, pos_set - 6));
            size_t pos_where = up.find(" WHERE ", pos_set);
            std::string set_part;
            std::string where_col, where_val;
            if (pos_where == std::string::npos) set_part = sql.substr(pos_set + 5);
            else { set_part = sql.substr(pos_set + 5, pos_where - (pos_set + 5)); cond:; }
            // parse set list
            auto assigns = split_csv(set_part);
            std::vector<std::pair<std::string,std::string>> sets;
            for (auto &a : assigns) {
                size_t eq = a.find('=');
                if (eq == std::string::npos) { message = "malformed SET"; return false; }
                std::string left = trim(a.substr(0, eq));
                std::string right = strip_quotes(a.substr(eq + 1));
                sets.emplace_back(left, right);
            }
            if (pos_where != std::string::npos) {
                std::string cond = sql.substr(pos_where + 7);
                size_t eq = cond.find('=');
                if (eq == std::string::npos) { message = "unsupported WHERE"; return false; }
                where_col = trim(cond.substr(0, eq));
                where_val = strip_quotes(cond.substr(eq + 1));
            }
            size_t affected = 0;
            bool ok = dm_.update_rows(table, sets, where_col, where_val, affected);
            message = ok ? ("OK " + std::to_string(affected)) : "UPDATE failed";
            return ok;
        } else if (up.rfind("DELETE", 0) == 0) {
            // DELETE FROM table [WHERE col=val]
            size_t pos_from = up.find(" FROM ");
            if (pos_from == std::string::npos) { message = "malformed DELETE"; return false; }
            size_t pos_where = up.find(" WHERE ", pos_from);
            std::string table;
            std::string where_col, where_val;
            if (pos_where == std::string::npos) table = trim(sql.substr(pos_from + 6));
            else { table = trim(sql.substr(pos_from + 6, pos_where - (pos_from + 6))); std::string cond = sql.substr(pos_where + 7); size_t eq = cond.find('='); if (eq == std::string::npos) { message = "unsupported WHERE"; return false; } where_col = trim(cond.substr(0, eq)); where_val = strip_quotes(cond.substr(eq + 1)); }
            size_t affected = 0;
            bool ok = dm_.delete_rows(table, where_col, where_val, affected);
            message = ok ? ("OK " + std::to_string(affected)) : "DELETE failed";
            return ok;
        }
    } catch (const std::exception &e) {
        message = std::string("exception: ") + e.what();
        return false;
    }

    message = "unsupported statement";
    return false;
}

} // namespace rdbms
