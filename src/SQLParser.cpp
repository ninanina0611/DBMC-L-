#include "../include/SQLParser.h"

#include <cctype>
#include <string>

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
}

bool SQLParser::parse(const std::vector<SQLToken> &tokens, SQLStatement &out_stmt, std::string *errmsg) noexcept {
    try {
        out_stmt = SQLStatement();
        size_t idx = 0;
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
                    // handle VARCHAR(size)
                    if (tname == "VARCHAR" && peek_token(tokens, idx).type == SQLTokenType::LParen) {
                        // skip ( N )
                        idx++; // (
                        if (peek_token(tokens, idx).type == SQLTokenType::Number) idx++;
                        if (!accept(SQLTokenType::RParen)) { if (errmsg) *errmsg = "expected ')' after type size"; return false; }
                    }

                    // map to DatabaseManager::Type
                    if (tname == "INT" || tname == "INT32") col.type = DatabaseManager::Type::INT32;
                    else if (tname == "INT64" || tname == "BIGINT") col.type = DatabaseManager::Type::INT64;
                    else col.type = DatabaseManager::Type::STRING;

                    // optional modifiers: PRIMARY KEY, NOT NULL (order-insensitive)
                    bool seen_any = true;
                    while (seen_any) {
                        seen_any = false;
                        if (is_kw(peek_token(tokens, idx), "PRIMARY")) {
                            idx++;
                            if (!is_kw(peek_token(tokens, idx), "KEY")) { if (errmsg) *errmsg = "expected KEY after PRIMARY"; return false; }
                            idx++;
                            col.is_primary = true; seen_any = true; continue;
                        }
                        if (is_kw(peek_token(tokens, idx), "NOT")) {
                            idx++;
                            if (!is_kw(peek_token(tokens, idx), "NULL")) { if (errmsg) *errmsg = "expected NULL after NOT"; return false; }
                            idx++;
                            col.not_null = true; seen_any = true; continue;
                        }
                    }

                    out_stmt.create_table_schema.columns.push_back(std::move(col));
                    if (accept(SQLTokenType::Comma)) continue;
                    if (peek_token(tokens, idx).type == SQLTokenType::RParen) { idx++; break; }
                    if (errmsg) *errmsg = "expected ',' or ')' in column list";
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
                if (peek_token(tokens, idx).type != SQLTokenType::Identifier) { if (errmsg) *errmsg = "expected database name"; return false; }
                out_stmt.type = SQLStatement::Type::DropDatabase;
                out_stmt.db_name = peek_token(tokens, idx).text; idx++;
                return true;
            }
            if (is_kw(peek_token(tokens, idx), "TABLE")) {
                idx++;
                if (peek_token(tokens, idx).type != SQLTokenType::Identifier) { if (errmsg) *errmsg = "expected table name"; return false; }
                out_stmt.type = SQLStatement::Type::DropTable;
                out_stmt.table = peek_token(tokens, idx).text; idx++;
                return true;
            }
            if (errmsg) *errmsg = "unsupported DROP target";
            return false;
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
                if (tname == "VARCHAR" && peek_token(tokens, idx).type == SQLTokenType::LParen) {
                    idx++; if (peek_token(tokens, idx).type == SQLTokenType::Number) idx++; if (!accept(SQLTokenType::RParen)) { if (errmsg) *errmsg = "expected ')' after type size"; return false; }
                }
                if (tname == "INT" || tname == "INT32") out_stmt.alter_column.type = DatabaseManager::Type::INT32;
                else if (tname == "INT64" || tname == "BIGINT") out_stmt.alter_column.type = DatabaseManager::Type::INT64;
                else out_stmt.alter_column.type = DatabaseManager::Type::STRING;
                // optional modifiers
                if (is_kw(peek_token(tokens, idx), "NOT")) { idx++; if (!is_kw(peek_token(tokens, idx), "NULL")) { if (errmsg) *errmsg = "expected NULL after NOT"; return false; } idx++; out_stmt.alter_column.not_null = true; }
                if (is_kw(peek_token(tokens, idx), "PRIMARY")) { idx++; if (!is_kw(peek_token(tokens, idx), "KEY")) { if (errmsg) *errmsg = "expected KEY after PRIMARY"; return false; } idx++; out_stmt.alter_column.is_primary = true; }
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
                if (tname == "VARCHAR" && peek_token(tokens, idx).type == SQLTokenType::LParen) { idx++; if (peek_token(tokens, idx).type == SQLTokenType::Number) idx++; if (!accept(SQLTokenType::RParen)) { if (errmsg) *errmsg = "expected ')' after type size"; return false; } }
                if (tname == "INT" || tname == "INT32") newcol.type = DatabaseManager::Type::INT32;
                else if (tname == "INT64" || tname == "BIGINT") newcol.type = DatabaseManager::Type::INT64;
                else newcol.type = DatabaseManager::Type::STRING;
                if (is_kw(peek_token(tokens, idx), "NOT")) { idx++; if (!is_kw(peek_token(tokens, idx), "NULL")) { if (errmsg) *errmsg = "expected NULL after NOT"; return false; } idx++; newcol.not_null = true; }
                if (is_kw(peek_token(tokens, idx), "PRIMARY")) { idx++; if (!is_kw(peek_token(tokens, idx), "KEY")) { if (errmsg) *errmsg = "expected KEY after PRIMARY"; return false; } idx++; newcol.is_primary = true; }
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

            if (!accept(SQLTokenType::LParen)) { if (errmsg) *errmsg = "expected '(' before VALUES list"; return false; }
            // parse value list (allow empty values between commas)
            while (true) {
                const SQLToken &tk = peek_token(tokens, idx);
                if (tk.type == SQLTokenType::Comma) {
                    // empty value
                    out_stmt.values.push_back(std::string());
                    idx++;
                    continue;
                }
                if (tk.type == SQLTokenType::RParen) { idx++; break; }
                if (tk.type == SQLTokenType::String || tk.type == SQLTokenType::Number || tk.type == SQLTokenType::Identifier) {
                    out_stmt.values.push_back(tk.text);
                    idx++;
                } else {
                    if (errmsg) *errmsg = "expected value"; return false;
                }
                if (accept(SQLTokenType::Comma)) continue;
                if (peek_token(tokens, idx).type == SQLTokenType::RParen) { idx++; break; }
            }

            return true;
        } else if (first.type == SQLTokenType::Keyword && first.text == "SELECT") {
            // SELECT cols FROM table [WHERE col = val]
            idx++;
            // select list
            if (peek_token(tokens, idx).type == SQLTokenType::Star) { out_stmt.columns.push_back("*"); idx++; }
            else {
                while (true) {
                    if (peek_token(tokens, idx).type != SQLTokenType::Identifier) { if (errmsg) *errmsg = "expected select column"; return false; }
                    out_stmt.columns.push_back(peek_token(tokens, idx).text); idx++;
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
                if (!accept(SQLTokenType::Equals)) { if (errmsg) *errmsg = "expected '=' in WHERE"; return false; }
                const SQLToken &valtk = peek_token(tokens, idx);
                if (valtk.type == SQLTokenType::String || valtk.type == SQLTokenType::Number || valtk.type == SQLTokenType::Identifier) {
                    out_stmt.where_val = valtk.text; idx++;
                } else {
                    if (errmsg) *errmsg = "expected value in WHERE"; return false;
                }
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
                const SQLToken &valtk = peek_token(tokens, idx);
                if (valtk.type == SQLTokenType::String || valtk.type == SQLTokenType::Number || valtk.type == SQLTokenType::Identifier) {
                    out_stmt.assignments.emplace_back(left, valtk.text); idx++;
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
                if (!accept(SQLTokenType::Equals)) { if (errmsg) *errmsg = "expected '=' in WHERE"; return false; }
                const SQLToken &valtk = peek_token(tokens, idx);
                if (valtk.type == SQLTokenType::String || valtk.type == SQLTokenType::Number || valtk.type == SQLTokenType::Identifier) {
                    out_stmt.where_val = valtk.text; idx++;
                } else { if (errmsg) *errmsg = "expected value in WHERE"; return false; }
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
                if (!accept(SQLTokenType::Equals)) { if (errmsg) *errmsg = "expected '=' in WHERE"; return false; }
                const SQLToken &valtk = peek_token(tokens, idx);
                if (valtk.type == SQLTokenType::String || valtk.type == SQLTokenType::Number || valtk.type == SQLTokenType::Identifier) {
                    out_stmt.where_val = valtk.text; idx++;
                } else { if (errmsg) *errmsg = "expected value in WHERE"; return false; }
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
