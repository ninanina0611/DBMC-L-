#include "SQLParser.h"

#include <cctype>
#include <string>
#include <iostream>

namespace rdbms {

namespace {
    const SQLToken &peek_token(const std::vector<SQLToken> &toks, size_t idx) {
        if (idx < toks.size()) return toks[idx];
        static SQLToken endt{SQLTokenType::End, std::string()};
        return endt;
    }

    static inline bool is_kw(const SQLToken &tk, const char *kw) {
        return tk.type == SQLTokenType::Keyword && tk.text == kw;
    }

    void apply_type(const std::string &tname, DatabaseManager::Column &col) {
        col.display_type = tname;
        col.type = DatabaseManager::type_from_name(tname);
    }

    static bool accept_token(const std::vector<SQLToken> &toks, size_t &idx, SQLTokenType ty) {
        if (idx < toks.size() && toks[idx].type == ty) { ++idx; return true; }
        return false;
    }

    bool parse_type_params(const std::vector<SQLToken> &tokens, size_t &idx,
                           DatabaseManager::Column &col, std::string *errmsg) {
        if (peek_token(tokens, idx).type != SQLTokenType::LParen) return true;
        idx++;
        std::string params;
        bool first = true;
        while (peek_token(tokens, idx).type != SQLTokenType::RParen) {
            if (!first) {
                if (peek_token(tokens, idx).type == SQLTokenType::Comma) {
                    params += ",";
                    idx++;
                }
            }
            const SQLToken &tk = peek_token(tokens, idx);
            if (tk.type == SQLTokenType::Number || tk.type == SQLTokenType::Identifier) {
                params += tk.text;
                idx++;
            } else {
                if (errmsg) *errmsg = "expected number in type parameters";
                return false;
            }
            first = false;
        }
        if (!accept_token(tokens, idx, SQLTokenType::RParen)) {
            if (errmsg) *errmsg = "expected ')' after type parameters";
            return false;
        }
        col.length = params;
        auto commaPos = params.find(',');
        if (commaPos != std::string::npos) {
            col.length = params.substr(0, commaPos);
            col.scale = params.substr(commaPos + 1);
        }
        return true;
    }
}

bool SQLParser::parse(const std::vector<SQLToken> &tokens, SQLStatement &out_stmt, std::string *errmsg) noexcept {
    try {
        out_stmt = SQLStatement();
        size_t idx = 0;
        bool debug_parse = false;
        try { const char *env = std::getenv("LIGHTDB_DEBUG_PARSE"); if (env && env[0] == '1') debug_parse = true; } catch(...) {}
        const auto accept = [&](SQLTokenType ty) -> bool {
            if (peek_token(tokens, idx).type == ty) { ++idx; return true; }
            return false;
        };
        const auto expect = [&](SQLTokenType ty, const char *err) -> bool {
            if (accept(ty)) return true;
            if (errmsg) *errmsg = err;
            return false;
        };

        const SQLToken &first = peek_token(tokens, idx);
        // DDL: CREATE / DROP / ALTER / USE
        if (first.type == SQLTokenType::Keyword && first.text == "CREATE") {
            // CREATE DATABASE name
            idx++;
            if (is_kw(peek_token(tokens, idx), "DATABASE")) {
                idx++;
                // support optional IF NOT EXISTS
                if (is_kw(peek_token(tokens, idx), "IF")) {
                    idx++;
                    if (!is_kw(peek_token(tokens, idx), "NOT")) { if (errmsg) *errmsg = "expected NOT after IF"; return false; }
                    idx++;
                    if (!is_kw(peek_token(tokens, idx), "EXISTS")) { if (errmsg) *errmsg = "expected EXISTS after NOT"; return false; }
                    idx++;
                    out_stmt.if_not_exists = true;
                }
                if (peek_token(tokens, idx).type != SQLTokenType::Identifier) { if (errmsg) *errmsg = "expected database name"; return false; }
                out_stmt.type = SQLStatement::Type::CreateDatabase;
                out_stmt.db_name = peek_token(tokens, idx).text; idx++;
                return true;
            }
            // CREATE TABLE name ( col_def, ... )
            if (is_kw(peek_token(tokens, idx), "TABLE")) {
                idx++;
                if (peek_token(tokens, idx).type != SQLTokenType::Identifier) { if (errmsg) *errmsg = "expected table name"; return false; }
                out_stmt.create_table_schema = DatabaseManager::TableSchema();
                out_stmt.create_table_schema.table_name = peek_token(tokens, idx).text; idx++;
                if (!accept(SQLTokenType::LParen)) { if (errmsg) *errmsg = "expected '(' after table name"; return false; }
                // parse column definitions
                while (true) {
                    if (peek_token(tokens, idx).type == SQLTokenType::RParen) { idx++; break; }
                    if (peek_token(tokens, idx).type != SQLTokenType::Identifier) { if (errmsg) *errmsg = "expected column name in table definition"; return false; }
                    DatabaseManager::Column col; col.name = peek_token(tokens, idx).text; idx++;

                    // type (allow keyword or identifier)
                    const SQLToken &typetk = peek_token(tokens, idx);
                    if (!(typetk.type == SQLTokenType::Keyword || typetk.type == SQLTokenType::Identifier)) { if (errmsg) *errmsg = "expected type for column"; return false; }
                    std::string tname = typetk.text; idx++;
                    apply_type(tname, col);
                    if (!parse_type_params(tokens, idx, col, errmsg)) return false;

                    // optional modifiers: PRIMARY KEY, NOT NULL, UNIQUE, DEFAULT, AUTO_INCREMENT, UNSIGNED (order-insensitive)
                    while (true) {
                        if (is_kw(peek_token(tokens, idx), "PRIMARY")) {
                            idx++;
                            if (!is_kw(peek_token(tokens, idx), "KEY")) { if (errmsg) *errmsg = "expected KEY after PRIMARY"; return false; }
                            idx++;
                            col.is_primary = true; continue;
                        }
                        if (is_kw(peek_token(tokens, idx), "NOT")) {
                            idx++;
                            if (!is_kw(peek_token(tokens, idx), "NULL")) { if (errmsg) *errmsg = "expected NULL after NOT"; return false; }
                            idx++;
                            col.not_null = true; continue;
                        }
                        if (is_kw(peek_token(tokens, idx), "UNIQUE")) {
                            idx++;
                            col.is_unique = true; continue;
                        }
                        if (is_kw(peek_token(tokens, idx), "DEFAULT")) {
                            idx++;
                            const SQLToken &valtk = peek_token(tokens, idx);
                            if (valtk.type == SQLTokenType::String || valtk.type == SQLTokenType::Number || valtk.type == SQLTokenType::Identifier || valtk.type == SQLTokenType::Keyword) {
                                col.default_value = valtk.text;
                                idx++;
                                continue;
                            } else { if (errmsg) *errmsg = "expected value after DEFAULT"; return false; }
                        }
                        if (is_kw(peek_token(tokens, idx), "AUTO_INCREMENT")) {
                            idx++;
                            col.auto_increment = true;
                            continue;
                        }
                        if (is_kw(peek_token(tokens, idx), "UNSIGNED")) {
                            idx++; continue;
                        }
                        break;
                    }

                    out_stmt.create_table_schema.columns.push_back(std::move(col));
                    if (accept(SQLTokenType::Comma)) continue;
                    if (peek_token(tokens, idx).type == SQLTokenType::RParen) { idx++; break; }
                    if (errmsg) *errmsg = "expected ',' or ')' in column list";
                    if (debug_parse) {
                        const SQLToken &tk = peek_token(tokens, idx);
                        std::cerr << "PARSE_DEBUG: at idx=" << idx << " token.type=" << static_cast<int>(tk.type) << " token.text='" << tk.text << "'\n";
                        // also dump remaining tokens
                        for (size_t dump = idx; dump < tokens.size(); ++dump) {
                            const SQLToken &dt = tokens[dump];
                            std::cerr << "TOK[" << dump << "] type=" << static_cast<int>(dt.type) << " text='" << dt.text << "'\n";
                        }
                    }
                    return false;
                }
                out_stmt.type = SQLStatement::Type::CreateTable;
                return true;
            }
            if (errmsg) *errmsg = "unsupported CREATE target";
            return false;
        }
        if (first.type == SQLTokenType::Keyword && first.text == "DROP") {
            idx++;
            if (is_kw(peek_token(tokens, idx), "DATABASE")) {
                idx++;
                // optional IF EXISTS
                if (is_kw(peek_token(tokens, idx), "IF")) {
                    idx++;
                    if (!is_kw(peek_token(tokens, idx), "EXISTS")) { if (errmsg) *errmsg = "expected EXISTS after IF"; return false; }
                    idx++;
                    out_stmt.if_exists = true;
                }
                if (peek_token(tokens, idx).type != SQLTokenType::Identifier) { if (errmsg) *errmsg = "expected database name"; return false; }
                out_stmt.type = SQLStatement::Type::DropDatabase;
                out_stmt.db_name = peek_token(tokens, idx).text; idx++;
                return true;
            }
            if (is_kw(peek_token(tokens, idx), "TABLE")) {
                idx++;
                // optional IF EXISTS
                if (is_kw(peek_token(tokens, idx), "IF")) {
                    idx++;
                    if (!is_kw(peek_token(tokens, idx), "EXISTS")) { if (errmsg) *errmsg = "expected EXISTS after IF"; return false; }
                    idx++;
                    out_stmt.if_exists = true;
                }
                if (peek_token(tokens, idx).type != SQLTokenType::Identifier) { if (errmsg) *errmsg = "expected table name"; return false; }
                out_stmt.type = SQLStatement::Type::DropTable;
                out_stmt.table = peek_token(tokens, idx).text; idx++;
                return true;
            }
            if (errmsg) *errmsg = "unsupported DROP target";
            return false;
        }
        if (first.type == SQLTokenType::Keyword && first.text == "RENAME") {
            idx++;
            if (!is_kw(peek_token(tokens, idx), "TABLE")) { if (errmsg) *errmsg = "expected TABLE after RENAME"; return false; }
            idx++;
            // parse one or more rename pairs: old TO new [, old2 TO new2]
            while (true) {
                if (peek_token(tokens, idx).type != SQLTokenType::Identifier) { if (errmsg) *errmsg = "expected table name to rename"; return false; }
                std::string oldname = peek_token(tokens, idx).text; idx++;
                if (!is_kw(peek_token(tokens, idx), "TO")) { if (errmsg) *errmsg = "expected TO in RENAME"; return false; }
                idx++;
                if (peek_token(tokens, idx).type != SQLTokenType::Identifier) { if (errmsg) *errmsg = "expected new table name"; return false; }
                std::string newname = peek_token(tokens, idx).text; idx++;
                out_stmt.rename_pairs.emplace_back(oldname, newname);
                if (accept(SQLTokenType::Comma)) continue;
                break;
            }
            out_stmt.type = SQLStatement::Type::RenameTable;
            return true;
        }
        if (first.type == SQLTokenType::Keyword && first.text == "ALTER") {
            idx++;
            if (!is_kw(peek_token(tokens, idx), "TABLE")) { if (errmsg) *errmsg = "expected TABLE after ALTER"; return false; }
            idx++;
            if (peek_token(tokens, idx).type != SQLTokenType::Identifier) { if (errmsg) *errmsg = "expected table name"; return false; }
            out_stmt.alter_table = peek_token(tokens, idx).text; idx++;
            if (is_kw(peek_token(tokens, idx), "ADD")) {
                idx++;
                // optional COLUMN
                if (is_kw(peek_token(tokens, idx), "COLUMN")) idx++;
                // expect column def similar to CREATE TABLE single column
                if (peek_token(tokens, idx).type != SQLTokenType::Identifier) { if (errmsg) *errmsg = "expected column name"; return false; }
                out_stmt.alter_column = DatabaseManager::Column();
                out_stmt.alter_column.name = peek_token(tokens, idx).text; idx++;
                const SQLToken &typetk = peek_token(tokens, idx);
                if (!(typetk.type == SQLTokenType::Keyword || typetk.type == SQLTokenType::Identifier)) { if (errmsg) *errmsg = "expected type for column"; return false; }
                std::string tname = typetk.text; idx++;
                apply_type(tname, out_stmt.alter_column);
                if (!parse_type_params(tokens, idx, out_stmt.alter_column, errmsg)) return false;
                // optional modifiers
                // parse optional modifiers for ALTER ADD COLUMN
                while (true) {
                    if (is_kw(peek_token(tokens, idx), "NOT")) { idx++; if (!is_kw(peek_token(tokens, idx), "NULL")) { if (errmsg) *errmsg = "expected NULL after NOT"; return false; } idx++; out_stmt.alter_column.not_null = true; continue; }
                    if (is_kw(peek_token(tokens, idx), "PRIMARY")) { idx++; if (!is_kw(peek_token(tokens, idx), "KEY")) { if (errmsg) *errmsg = "expected KEY after PRIMARY"; return false; } idx++; out_stmt.alter_column.is_primary = true; continue; }
                    if (is_kw(peek_token(tokens, idx), "UNIQUE")) { idx++; out_stmt.alter_column.is_unique = true; continue; }
                    if (is_kw(peek_token(tokens, idx), "DEFAULT")) { idx++; const SQLToken &valtk = peek_token(tokens, idx); if (valtk.type == SQLTokenType::String || valtk.type == SQLTokenType::Number || valtk.type == SQLTokenType::Identifier || valtk.type == SQLTokenType::Keyword) { out_stmt.alter_column.default_value = valtk.text; idx++; continue; } else { if (errmsg) *errmsg = "expected value after DEFAULT"; return false; } }
                    if (is_kw(peek_token(tokens, idx), "AUTO_INCREMENT")) { idx++; out_stmt.alter_column.auto_increment = true; continue; }
                    if (is_kw(peek_token(tokens, idx), "UNSIGNED")) { idx++; continue; }
                    break;
                }
                out_stmt.type = SQLStatement::Type::AlterTableAddColumn;
                return true;
            }
            if (is_kw(peek_token(tokens, idx), "DROP")) {
                idx++;
                if (is_kw(peek_token(tokens, idx), "COLUMN")) idx++;
                if (peek_token(tokens, idx).type != SQLTokenType::Identifier) { if (errmsg) *errmsg = "expected column name to drop"; return false; }
                out_stmt.alter_column_name = peek_token(tokens, idx).text; idx++;
                out_stmt.type = SQLStatement::Type::AlterTableDropColumn;
                return true;
            }
            // ALTER TABLE ... MODIFY [COLUMN] col TYPE [NOT NULL] [PRIMARY KEY]
            if (is_kw(peek_token(tokens, idx), "MODIFY") || is_kw(peek_token(tokens, idx), "CHANGE")) {
                idx++;
                if (is_kw(peek_token(tokens, idx), "COLUMN")) idx++;
                // CHANGE may provide old and new names: "CHANGE old_name new_name TYPE"
                if (is_kw(peek_token(tokens, idx), "COLUMN")) idx++;
                if (peek_token(tokens, idx).type != SQLTokenType::Identifier) { if (errmsg) *errmsg = "expected column name"; return false; }
                std::string first_name = peek_token(tokens, idx).text; idx++;
                std::string second_name;
                // if CHANGE form (two identifiers) detect by looking ahead
                if (is_kw(tokens.size() > idx ? peek_token(tokens, idx) : SQLToken{SQLTokenType::End, std::string()}, "") && false) {
                    // unreachable placeholder
                }
                // If next token is identifier and next after that is a type keyword/identifier, treat as CHANGE old new type
                if (peek_token(tokens, idx).type == SQLTokenType::Identifier && (peek_token(tokens, idx+1).type == SQLTokenType::Keyword || peek_token(tokens, idx+1).type == SQLTokenType::Identifier)) {
                    // treat as CHANGE old_name new_name TYPE...
                    second_name = peek_token(tokens, idx).text; idx++;
                }
                DatabaseManager::Column newcol;
                // if second_name set, newcol.name = second_name; else newcol.name = first_name
                newcol.name = second_name.empty() ? first_name : second_name;
                // parse type
                const SQLToken &typetk = peek_token(tokens, idx);
                if (!(typetk.type == SQLTokenType::Keyword || typetk.type == SQLTokenType::Identifier)) { if (errmsg) *errmsg = "expected type for column"; return false; }
                std::string tname = typetk.text; idx++;
                apply_type(tname, newcol);
                if (!parse_type_params(tokens, idx, newcol, errmsg)) return false;
                // optional modifiers for MODIFY/CHANGE
                while (true) {
                    if (is_kw(peek_token(tokens, idx), "NOT")) { idx++; if (!is_kw(peek_token(tokens, idx), "NULL")) { if (errmsg) *errmsg = "expected NULL after NOT"; return false; } idx++; newcol.not_null = true; continue; }
                    if (is_kw(peek_token(tokens, idx), "PRIMARY")) { idx++; if (!is_kw(peek_token(tokens, idx), "KEY")) { if (errmsg) *errmsg = "expected KEY after PRIMARY"; return false; } idx++; newcol.is_primary = true; continue; }
                    if (is_kw(peek_token(tokens, idx), "UNIQUE")) { idx++; newcol.is_unique = true; continue; }
                    if (is_kw(peek_token(tokens, idx), "DEFAULT")) { idx++; const SQLToken &valtk = peek_token(tokens, idx); if (valtk.type == SQLTokenType::String || valtk.type == SQLTokenType::Number || valtk.type == SQLTokenType::Identifier || valtk.type == SQLTokenType::Keyword) { newcol.default_value = valtk.text; idx++; continue; } else { if (errmsg) *errmsg = "expected value after DEFAULT"; return false; } }
                    if (is_kw(peek_token(tokens, idx), "AUTO_INCREMENT")) { idx++; newcol.auto_increment = true; continue; }
                    if (is_kw(peek_token(tokens, idx), "UNSIGNED")) { idx++; continue; }
                    break;
                }
                out_stmt.alter_column = std::move(newcol);
                out_stmt.alter_column_name = first_name; // old name
                out_stmt.type = SQLStatement::Type::AlterTableModifyColumn;
                return true;
            }
            // ALTER TABLE ... RENAME [COLUMN] old TO new
            if (is_kw(peek_token(tokens, idx), "RENAME")) {
                idx++;
                if (is_kw(peek_token(tokens, idx), "COLUMN")) idx++;
                if (peek_token(tokens, idx).type != SQLTokenType::Identifier) { if (errmsg) *errmsg = "expected column name to rename"; return false; }
                std::string oldname = peek_token(tokens, idx).text; idx++;
                if (!is_kw(peek_token(tokens, idx), "TO")) { if (errmsg) *errmsg = "expected TO in RENAME"; return false; }
                idx++;
                if (peek_token(tokens, idx).type != SQLTokenType::Identifier) { if (errmsg) *errmsg = "expected new column name"; return false; }
                std::string newname = peek_token(tokens, idx).text; idx++;
                out_stmt.alter_column_name = oldname;
                out_stmt.alter_column = DatabaseManager::Column();
                out_stmt.alter_column.name = newname;
                out_stmt.type = SQLStatement::Type::AlterTableRenameColumn;
                return true;
            }
            if (errmsg) *errmsg = "unsupported ALTER action";
            return false;
        }
        if (first.type == SQLTokenType::Keyword && first.text == "USE") {
            idx++;
            if (peek_token(tokens, idx).type != SQLTokenType::Identifier) { if (errmsg) *errmsg = "expected database name after USE"; return false; }
            out_stmt.type = SQLStatement::Type::UseDatabase;
            out_stmt.db_name = peek_token(tokens, idx).text; idx++;
            return true;
        }
        // Transaction control: START TRANSACTION / BEGIN, COMMIT, ROLLBACK
        if (first.type == SQLTokenType::Keyword && (first.text == "START" || first.text == "BEGIN")) {
            idx++;
            // optional TRANSACTION keyword
            if (is_kw(peek_token(tokens, idx), "TRANSACTION")) idx++;
            out_stmt.type = SQLStatement::Type::StartTransaction;
            return true;
        }
        if (first.type == SQLTokenType::Keyword && first.text == "COMMIT") {
            idx++;
            out_stmt.type = SQLStatement::Type::Commit;
            return true;
        }
        if (first.type == SQLTokenType::Keyword && first.text == "ROLLBACK") {
            idx++;
            out_stmt.type = SQLStatement::Type::Rollback;
            return true;
        }
        if (first.type == SQLTokenType::Keyword && first.text == "INSERT") {
            // INSERT INTO table (c1,c2) VALUES (v1,v2)
            idx++;
            if (!is_kw(peek_token(tokens, idx), "INTO")) { if (errmsg) *errmsg = "expected INTO"; return false; }
            idx++;
            // table name
            if (peek_token(tokens, idx).type != SQLTokenType::Identifier) { if (errmsg) *errmsg = "expected table name"; return false; }
            out_stmt.type = SQLStatement::Type::Insert;
            out_stmt.table = peek_token(tokens, idx).text; idx++;

            if (!accept(SQLTokenType::LParen)) { if (errmsg) *errmsg = "expected '(' after table"; return false; }
            // parse column list
            while (true) {
                if (peek_token(tokens, idx).type == SQLTokenType::RParen) { idx++; break; }
                if (peek_token(tokens, idx).type != SQLTokenType::Identifier) { if (errmsg) *errmsg = "expected column name"; return false; }
                out_stmt.columns.push_back(peek_token(tokens, idx).text); idx++;
                if (accept(SQLTokenType::Comma)) continue;
                if (peek_token(tokens, idx).type == SQLTokenType::RParen) { idx++; break; }
                if (errmsg) *errmsg = "expected ',' or ')' in column list";
                return false;
            }

            if (!is_kw(peek_token(tokens, idx), "VALUES")) { if (errmsg) *errmsg = "expected VALUES"; return false; }
            idx++;

            // parse one or more value tuples: (v1,v2,...) [, (v1,v2,...)]
            while (true) {
                if (!accept(SQLTokenType::LParen)) { if (errmsg) *errmsg = "expected '(' before VALUES list"; return false; }
                std::vector<std::string> row;
                // parse value list for this row
                while (true) {
                    const SQLToken &tk = peek_token(tokens, idx);
                    if (tk.type == SQLTokenType::Comma) {
                        // empty value
                        row.push_back(std::string());
                        idx++;
                        continue;
                    }
                    if (tk.type == SQLTokenType::RParen) { idx++; break; }
                    if (tk.type == SQLTokenType::String || tk.type == SQLTokenType::Number || tk.type == SQLTokenType::Identifier || tk.type == SQLTokenType::Keyword) {
                        row.push_back(tk.text);
                        idx++;
                    } else {
                        if (errmsg) *errmsg = "expected value"; return false;
                    }
                    if (accept(SQLTokenType::Comma)) continue;
                    if (peek_token(tokens, idx).type == SQLTokenType::RParen) { idx++; break; }
                }
                out_stmt.values_rows.push_back(row);
                // keep single-row compatibility: set out_stmt.values to first row
                if (out_stmt.values_rows.size() == 1) out_stmt.values = row;
                // if another tuple follows, it will be separated by a comma
                if (accept(SQLTokenType::Comma)) continue;
                break;
            }

            return true;
        } else if (first.type == SQLTokenType::Keyword && first.text == "SELECT") {
            idx++;
            // select list
            if (peek_token(tokens, idx).type == SQLTokenType::Star) {
                out_stmt.columns.push_back("*");
                out_stmt.select_aliases.push_back("");
                out_stmt.select_funcs.push_back("");
                out_stmt.select_func_args.push_back("");
                idx++;
            } else {
                while (true) {
                    const SQLToken &tk = peek_token(tokens, idx);
                    bool is_agg = (tk.type == SQLTokenType::Keyword &&
                                   (tk.text == "COUNT" || tk.text == "AVG" || tk.text == "SUM" ||
                                    tk.text == "MIN" || tk.text == "MAX"));
                    if (is_agg) {
                        std::string func_name = tk.text; idx++;
                        if (!accept(SQLTokenType::LParen)) { if (errmsg) *errmsg = "expected '(' after aggregate function"; return false; }
                        std::string arg;
                        const SQLToken &argtk = peek_token(tokens, idx);
                        if (argtk.type == SQLTokenType::Star) { arg = "*"; idx++; }
                        else if (argtk.type == SQLTokenType::Identifier || argtk.type == SQLTokenType::Keyword) { arg = argtk.text; idx++; }
                        else { if (errmsg) *errmsg = "expected column name or * in aggregate"; return false; }
                        if (!accept(SQLTokenType::RParen)) { if (errmsg) *errmsg = "expected ')' after aggregate argument"; return false; }
                        std::string raw = func_name + "(" + arg + ")";
                        std::string alias;
                        if (is_kw(peek_token(tokens, idx), "AS")) { idx++; if (peek_token(tokens, idx).type != SQLTokenType::Identifier) { if (errmsg) *errmsg = "expected alias after AS"; return false; } alias = peek_token(tokens, idx).text; idx++; }
                        out_stmt.columns.push_back(raw);
                        out_stmt.select_aliases.push_back(alias);
                        out_stmt.select_funcs.push_back(func_name);
                        out_stmt.select_func_args.push_back(arg);
                    } else if (tk.type == SQLTokenType::Identifier) {
                        std::string col_name = tk.text; idx++;
                        std::string alias;
                        if (is_kw(peek_token(tokens, idx), "AS")) { idx++; if (peek_token(tokens, idx).type != SQLTokenType::Identifier) { if (errmsg) *errmsg = "expected alias after AS"; return false; } alias = peek_token(tokens, idx).text; idx++; }
                        out_stmt.columns.push_back(col_name);
                        out_stmt.select_aliases.push_back(alias);
                        out_stmt.select_funcs.push_back("");
                        out_stmt.select_func_args.push_back("");
                    } else {
                        if (errmsg) *errmsg = "expected select column or aggregate function"; return false;
                    }
                    if (accept(SQLTokenType::Comma)) continue;
                    break;
                }
            }
            if (!is_kw(peek_token(tokens, idx), "FROM")) { if (errmsg) *errmsg = "expected FROM"; return false; }
            idx++;
            if (peek_token(tokens, idx).type != SQLTokenType::Identifier) { if (errmsg) *errmsg = "expected table name"; return false; }
            out_stmt.table = peek_token(tokens, idx).text; idx++;
            if (is_kw(peek_token(tokens, idx), "WHERE")) {
                idx++;
                if (peek_token(tokens, idx).type != SQLTokenType::Identifier) { if (errmsg) *errmsg = "expected where column"; return false; }
                out_stmt.where_col = peek_token(tokens, idx).text; idx++;
                const SQLToken &optk = peek_token(tokens, idx);
                if (optk.type == SQLTokenType::Equals) { out_stmt.where_op = "="; idx++; }
                else if (optk.type == SQLTokenType::Less) { out_stmt.where_op = "<"; idx++; }
                else if (optk.type == SQLTokenType::Greater) { out_stmt.where_op = ">"; idx++; }
                else if (optk.type == SQLTokenType::LessEq) { out_stmt.where_op = "<="; idx++; }
                else if (optk.type == SQLTokenType::GreaterEq) { out_stmt.where_op = ">="; idx++; }
                else if (optk.type == SQLTokenType::NotEquals) { out_stmt.where_op = "!="; idx++; }
                else if (optk.type == SQLTokenType::Keyword && optk.text == "LIKE") { out_stmt.where_op = "LIKE"; idx++; }
                else if (optk.type == SQLTokenType::Keyword && optk.text == "IN") {
                    out_stmt.where_op = "IN"; idx++;
                    if (!accept(SQLTokenType::LParen)) { if (errmsg) *errmsg = "expected '(' after IN"; return false; }
                    std::string combined;
                    bool first_item = true;
                    while (peek_token(tokens, idx).type != SQLTokenType::RParen) {
                        const SQLToken &it = peek_token(tokens, idx);
                        if (it.type == SQLTokenType::String || it.type == SQLTokenType::Number || it.type == SQLTokenType::Identifier || it.type == SQLTokenType::Keyword) {
                            if (!first_item) combined += ",";
                            combined += it.text;
                            idx++;
                            first_item = false;
                        } else { if (errmsg) *errmsg = "expected value in IN list"; return false; }
                        if (accept(SQLTokenType::Comma)) continue;
                        if (peek_token(tokens, idx).type == SQLTokenType::RParen) break;
                    }
                    if (!accept(SQLTokenType::RParen)) { if (errmsg) *errmsg = "expected ')' after IN list"; return false; }
                    out_stmt.where_val = combined;
                }
                else { if (errmsg) *errmsg = "expected operator in WHERE"; return false; }
                if (out_stmt.where_op != "IN") {
                    const SQLToken &valtk = peek_token(tokens, idx);
                    if (valtk.type == SQLTokenType::String || valtk.type == SQLTokenType::Number || valtk.type == SQLTokenType::Identifier) {
                        out_stmt.where_val = valtk.text; idx++;
                    } else {
                        if (errmsg) *errmsg = "expected value in WHERE"; return false;
                    }
                }
            }
            // optional GROUP BY
            if (is_kw(peek_token(tokens, idx), "GROUP")) {
                idx++;
                if (!is_kw(peek_token(tokens, idx), "BY")) { if (errmsg) *errmsg = "expected BY after GROUP"; return false; }
                idx++;
                if (peek_token(tokens, idx).type != SQLTokenType::Identifier) { if (errmsg) *errmsg = "expected column name after GROUP BY"; return false; }
                out_stmt.group_by = peek_token(tokens, idx).text; idx++;
            }
            // optional HAVING
            if (is_kw(peek_token(tokens, idx), "HAVING")) {
                idx++;
                if (peek_token(tokens, idx).type != SQLTokenType::Identifier) { if (errmsg) *errmsg = "expected column in HAVING"; return false; }
                out_stmt.having_col = peek_token(tokens, idx).text; idx++;
                const SQLToken &hoptk = peek_token(tokens, idx);
                if (hoptk.type == SQLTokenType::Equals) { out_stmt.having_op = "="; idx++; }
                else if (hoptk.type == SQLTokenType::Less) { out_stmt.having_op = "<"; idx++; }
                else if (hoptk.type == SQLTokenType::Greater) { out_stmt.having_op = ">"; idx++; }
                else if (hoptk.type == SQLTokenType::LessEq) { out_stmt.having_op = "<="; idx++; }
                else if (hoptk.type == SQLTokenType::GreaterEq) { out_stmt.having_op = ">="; idx++; }
                else if (hoptk.type == SQLTokenType::NotEquals) { out_stmt.having_op = "!="; idx++; }
                else { if (errmsg) *errmsg = "expected operator in HAVING"; return false; }
                const SQLToken &hvaltk = peek_token(tokens, idx);
                if (hvaltk.type == SQLTokenType::String || hvaltk.type == SQLTokenType::Number || hvaltk.type == SQLTokenType::Identifier) {
                    out_stmt.having_val = hvaltk.text; idx++;
                } else { if (errmsg) *errmsg = "expected value in HAVING"; return false; }
            }
            // optional ORDER BY / LIMIT / OFFSET
            while (true) {
                const SQLToken &next = peek_token(tokens, idx);
                if (next.type == SQLTokenType::Keyword && next.text == "ORDER") {
                    idx++;
                    if (!is_kw(peek_token(tokens, idx), "BY")) { if (errmsg) *errmsg = "expected BY after ORDER"; return false; }
                    idx++;
                    if (peek_token(tokens, idx).type != SQLTokenType::Identifier) { if (errmsg) *errmsg = "expected column name after ORDER BY"; return false; }
                    out_stmt.order_by = peek_token(tokens, idx).text; idx++;
                    if (peek_token(tokens, idx).type == SQLTokenType::Keyword && (peek_token(tokens, idx).text == "ASC" || peek_token(tokens, idx).text == "DESC")) {
                        out_stmt.order_dir = peek_token(tokens, idx).text; idx++;
                    }
                    continue;
                }
                if (next.type == SQLTokenType::Keyword && next.text == "LIMIT") {
                    idx++;
                    const SQLToken &limtk = peek_token(tokens, idx);
                    if (limtk.type != SQLTokenType::Number) { if (errmsg) *errmsg = "expected number after LIMIT"; return false; }
                    try { out_stmt.limit = std::stoi(limtk.text); } catch(...) { out_stmt.limit = -1; }
                    idx++;
                    continue;
                }
                if (next.type == SQLTokenType::Keyword && next.text == "OFFSET") {
                    idx++;
                    const SQLToken &oftk = peek_token(tokens, idx);
                    if (oftk.type != SQLTokenType::Number) { if (errmsg) *errmsg = "expected number after OFFSET"; return false; }
                    try { out_stmt.offset = std::stoi(oftk.text); } catch(...) { out_stmt.offset = 0; }
                    idx++;
                    continue;
                }
                break;
            }
            out_stmt.type = SQLStatement::Type::Select;
            return true;
        } else if (first.type == SQLTokenType::Keyword && first.text == "UPDATE") {
            // UPDATE table SET a=1, b='x' [WHERE col=val]
            idx++;
            if (peek_token(tokens, idx).type != SQLTokenType::Identifier) { if (errmsg) *errmsg = "expected table name"; return false; }
            out_stmt.table = peek_token(tokens, idx).text; idx++;
            if (!is_kw(peek_token(tokens, idx), "SET")) { if (errmsg) *errmsg = "expected SET"; return false; }
            idx++;
            // assignments
            while (true) {
                if (peek_token(tokens, idx).type != SQLTokenType::Identifier) { if (errmsg) *errmsg = "expected assignment target"; return false; }
                std::string left = peek_token(tokens, idx).text; idx++;
                if (!accept(SQLTokenType::Equals)) { if (errmsg) *errmsg = "expected '=' in assignment"; return false; }
                // parse simple expression for RHS: allow literals or identifier [op literal|identifier]...
                const SQLToken &firstval = peek_token(tokens, idx);
                if (firstval.type == SQLTokenType::String || firstval.type == SQLTokenType::Number || firstval.type == SQLTokenType::Identifier) {
                    std::string expr = firstval.text; idx++;
                    while (true) {
                        const SQLToken &optk = peek_token(tokens, idx);
                        std::string op;
                        if (optk.type == SQLTokenType::Star) op = "*";
                        else if (optk.type == SQLTokenType::Plus) op = "+";
                        else if (optk.type == SQLTokenType::Minus) op = "-";
                        else if (optk.type == SQLTokenType::Slash) op = "/";
                        else break;
                        idx++;
                        const SQLToken &rt = peek_token(tokens, idx);
                        if (rt.type == SQLTokenType::String || rt.type == SQLTokenType::Number || rt.type == SQLTokenType::Identifier) {
                            expr += " "; expr += op; expr += " "; expr += rt.text; idx++; continue;
                        } else {
                            if (errmsg) *errmsg = "expected value after operator in assignment"; return false;
                        }
                    }
                    out_stmt.assignments.emplace_back(left, expr);
                } else {
                    if (errmsg) *errmsg = "expected value in assignment"; return false;
                }
                if (accept(SQLTokenType::Comma)) continue;
                break;
            }
            if (is_kw(peek_token(tokens, idx), "WHERE")) {
                idx++;
                if (peek_token(tokens, idx).type != SQLTokenType::Identifier) { if (errmsg) *errmsg = "expected where column"; return false; }
                out_stmt.where_col = peek_token(tokens, idx).text; idx++;
                const SQLToken &optk = peek_token(tokens, idx);
                if (optk.type == SQLTokenType::Equals) { out_stmt.where_op = "="; idx++; }
                else if (optk.type == SQLTokenType::Less) { out_stmt.where_op = "<"; idx++; }
                else if (optk.type == SQLTokenType::Greater) { out_stmt.where_op = ">"; idx++; }
                else if (optk.type == SQLTokenType::LessEq) { out_stmt.where_op = "<="; idx++; }
                else if (optk.type == SQLTokenType::GreaterEq) { out_stmt.where_op = ">="; idx++; }
                else if (optk.type == SQLTokenType::NotEquals) { out_stmt.where_op = "!="; idx++; }
                else if (optk.type == SQLTokenType::Keyword && optk.text == "LIKE") { out_stmt.where_op = "LIKE"; idx++; }
                else if (optk.type == SQLTokenType::Keyword && optk.text == "IN") {
                    out_stmt.where_op = "IN"; idx++;
                    if (!accept(SQLTokenType::LParen)) { if (errmsg) *errmsg = "expected '(' after IN"; return false; }
                    std::string combined;
                    bool first_item = true;
                    while (peek_token(tokens, idx).type != SQLTokenType::RParen) {
                        const SQLToken &it = peek_token(tokens, idx);
                        if (it.type == SQLTokenType::String || it.type == SQLTokenType::Number || it.type == SQLTokenType::Identifier || it.type == SQLTokenType::Keyword) {
                            if (!first_item) combined += ",";
                            combined += it.text;
                            idx++;
                            first_item = false;
                        } else { if (errmsg) *errmsg = "expected value in IN list"; return false; }
                        if (accept(SQLTokenType::Comma)) continue;
                        if (peek_token(tokens, idx).type == SQLTokenType::RParen) break;
                    }
                    if (!accept(SQLTokenType::RParen)) { if (errmsg) *errmsg = "expected ')' after IN list"; return false; }
                    out_stmt.where_val = combined;
                }
                else { if (errmsg) *errmsg = "expected operator in WHERE"; return false; }
                if (out_stmt.where_op != "IN") {
                    const SQLToken &valtk = peek_token(tokens, idx);
                    if (valtk.type == SQLTokenType::String || valtk.type == SQLTokenType::Number || valtk.type == SQLTokenType::Identifier) {
                        out_stmt.where_val = valtk.text; idx++; 
                    } else { if (errmsg) *errmsg = "expected value in WHERE"; return false; }
                }
            }
            out_stmt.type = SQLStatement::Type::Update;
            return true;
        } else if (first.type == SQLTokenType::Keyword && first.text == "DELETE") {
            // DELETE FROM table [WHERE col=val]
            idx++;
            if (!is_kw(peek_token(tokens, idx), "FROM")) { if (errmsg) *errmsg = "expected FROM"; return false; }
            idx++;
            if (peek_token(tokens, idx).type != SQLTokenType::Identifier) { if (errmsg) *errmsg = "expected table name"; return false; }
            out_stmt.table = peek_token(tokens, idx).text; idx++;
            if (is_kw(peek_token(tokens, idx), "WHERE")) {
                idx++;
                if (peek_token(tokens, idx).type != SQLTokenType::Identifier) { if (errmsg) *errmsg = "expected where column"; return false; }
                out_stmt.where_col = peek_token(tokens, idx).text; idx++;
                // accept comparison operator: =, <, >, <=, >=, !=, LIKE
                const SQLToken &optk = peek_token(tokens, idx);
                if (optk.type == SQLTokenType::Equals) { out_stmt.where_op = "="; idx++; }
                else if (optk.type == SQLTokenType::Less) { out_stmt.where_op = "<"; idx++; }
                else if (optk.type == SQLTokenType::Greater) { out_stmt.where_op = ">"; idx++; }
                else if (optk.type == SQLTokenType::LessEq) { out_stmt.where_op = "<="; idx++; }
                else if (optk.type == SQLTokenType::GreaterEq) { out_stmt.where_op = ">="; idx++; }
                else if (optk.type == SQLTokenType::NotEquals) { out_stmt.where_op = "!="; idx++; }
                else if (optk.type == SQLTokenType::Keyword && optk.text == "LIKE") { out_stmt.where_op = "LIKE"; idx++; }
                else if (optk.type == SQLTokenType::Keyword && optk.text == "IN") {
                    out_stmt.where_op = "IN"; idx++;
                    if (!accept(SQLTokenType::LParen)) { if (errmsg) *errmsg = "expected '(' after IN"; return false; }
                    std::string combined;
                    bool first_item = true;
                    while (peek_token(tokens, idx).type != SQLTokenType::RParen) {
                        const SQLToken &it = peek_token(tokens, idx);
                        if (it.type == SQLTokenType::String || it.type == SQLTokenType::Number || it.type == SQLTokenType::Identifier || it.type == SQLTokenType::Keyword) {
                            if (!first_item) combined += ",";
                            combined += it.text;
                            idx++;
                            first_item = false;
                        } else { if (errmsg) *errmsg = "expected value in IN list"; return false; }
                        if (accept(SQLTokenType::Comma)) continue;
                        if (peek_token(tokens, idx).type == SQLTokenType::RParen) break;
                    }
                    if (!accept(SQLTokenType::RParen)) { if (errmsg) *errmsg = "expected ')' after IN list"; return false; }
                    out_stmt.where_val = combined;
                }
                else { if (errmsg) *errmsg = "expected operator in WHERE"; return false; }
                if (out_stmt.where_op != "IN") {
                    const SQLToken &valtk = peek_token(tokens, idx);
                    if (valtk.type == SQLTokenType::String || valtk.type == SQLTokenType::Number || valtk.type == SQLTokenType::Identifier) {
                        out_stmt.where_val = valtk.text; idx++;
                    } else { if (errmsg) *errmsg = "expected value in WHERE"; return false; }
                }
            }
            out_stmt.type = SQLStatement::Type::Delete;
            return true;
        }

        if (errmsg) *errmsg = "unsupported or empty statement";
        return false;
    } catch (...) {
        if (errmsg) *errmsg = "exception in parser";
        return false;
    }
}

} // namespace rdbms
