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
