#include "../include/SQLEngine.h"
#include "../include/DatabaseManager.h"
#include "../include/DataManager.h"

#include <algorithm>
#include <cctype>
#include <sstream>
#include "../include/SQLLexer.h"
#include "../include/SQLParser.h"

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

    // Use lexer + parser to build an execution plan, then dispatch to DataManager
    std::vector<SQLToken> toks;
    std::string lex_err;
    if (!SQLLexer::tokenize(sql, toks, &lex_err)) { message = lex_err.empty() ? "lex error" : lex_err; return false; }

    SQLStatement stmt;
    std::string parse_err;
    if (!SQLParser::parse(toks, stmt, &parse_err)) { message = parse_err.empty() ? "parse error" : parse_err; return false; }

    // dispatch
    if (stmt.type == SQLStatement::Type::Insert) {
        if (stmt.columns.size() != stmt.values.size()) { message = "column/value count mismatch"; return false; }
        std::vector<std::pair<std::string,std::string>> pairs;
        for (size_t i = 0; i < stmt.columns.size(); ++i) pairs.emplace_back(stmt.columns[i], stmt.values[i]);
        std::string dm_err;
        bool ok = dm_.insert_row(stmt.table, pairs, &dm_err);
        message = ok ? "OK" : (dm_err.empty() ? std::string("INSERT failed") : dm_err);
        return ok;
    } else if (stmt.type == SQLStatement::Type::Select) {
        std::string dm_err;
        bool ok = dm_.select_rows(stmt.table, stmt.columns, stmt.where_col, stmt.where_val, out_rows, out_columns, &dm_err);
        message = ok ? "OK" : (dm_err.empty() ? std::string("SELECT failed") : dm_err);
        return ok;
    } else if (stmt.type == SQLStatement::Type::Update) {
        size_t affected = 0;
        std::string dm_err;
        bool ok = dm_.update_rows(stmt.table, stmt.assignments, stmt.where_col, stmt.where_val, affected, &dm_err);
        message = ok ? ("OK " + std::to_string(affected)) : (dm_err.empty() ? std::string("UPDATE failed") : dm_err);
        return ok;
    } else if (stmt.type == SQLStatement::Type::Delete) {
        size_t affected = 0;
        std::string dm_err;
        bool ok = dm_.delete_rows(stmt.table, stmt.where_col, stmt.where_val, affected, &dm_err);
        message = ok ? ("OK " + std::to_string(affected)) : (dm_err.empty() ? std::string("DELETE failed") : dm_err);
        return ok;
    }

    message = "unsupported statement";
    return false;
}

} // namespace rdbms
