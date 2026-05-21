#include "SQLLexer.h"

#include <cctype>
#include <algorithm>
#include <unordered_set>
#include <iostream>

namespace rdbms {

static inline std::string to_upper_str(const std::string &s) {
    std::string r = s;
    std::transform(r.begin(), r.end(), r.begin(), [](unsigned char c){ return std::toupper(c); });
    return r;
}

bool SQLLexer::tokenize(const std::string &sql, std::vector<SQLToken> &out_tokens, std::string *errmsg) noexcept {
    try {
        out_tokens.clear();
            const std::unordered_set<std::string> keywords = {
            "SELECT","INSERT","INTO","VALUES","FROM","WHERE","UPDATE","SET","DELETE",
            // DDL / schema keywords
            "CREATE","DROP","ALTER","DATABASE","TABLE","ADD","COLUMN","USE","MODIFY","RENAME","TO","CHANGE",
            // IF/EXISTS
            "IF","NOT","EXISTS",
            // CHARACTER SET / COLLATE
            "CHARACTER","SET","COLLATE","CHARSET",
            // constraints / modifiers
            "PRIMARY","KEY","NULL","DEFAULT","AUTO_INCREMENT","UNSIGNED","UNIQUE",
            // engine / table options
            "ENGINE","INNODB","CHARACTER_SET","CHARSET","CURRENT_TIMESTAMP",
            // transaction / control
            "START","BEGIN","TRANSACTION","COMMIT","ROLLBACK","SAVEPOINT","RELEASE","ROLLBACK",
            // pattern matching / logical
            "LIKE","IN","AND","OR","BETWEEN",
            // ordering / pagination
            "ORDER","BY","ASC","DESC","LIMIT","OFFSET",
            // grouping / filtering
            "GROUP","HAVING","AS",
            // aggregate functions
            "COUNT","AVG","SUM","MIN","MAX",
            // common types
            "INT","INT32","INT64","BIGINT","SMALLINT","VARCHAR","CHAR","TEXT","STRING","DECIMAL","DATE","DATETIME","TIMESTAMP","BOOLEAN"
        };

        size_t i = 0, n = sql.size();
        bool debug = false;
        try { const char *env = std::getenv("LIGHTDB_DEBUG_LEX"); if (env && env[0] == '1') debug = true; } catch(...) {}
        while (i < n) {
            unsigned char c = static_cast<unsigned char>(sql[i]);
            // skip whitespace
            if (std::isspace(c)) { ++i; continue; }

            // single-line comment: -- comment till end of line
            if (c == '-' && i + 1 < n && sql[i+1] == '-') {
                size_t j = i + 2;
                while (j < n && sql[j] != '\n' && sql[j] != '\r') ++j;
                i = j;
                continue;
            }
            // single-line comment: # comment till end of line
            if (c == '#') {
                size_t j = i + 1;
                while (j < n && sql[j] != '\n' && sql[j] != '\r') ++j;
                i = j;
                continue;
            }
            // multi-line comment: /* ... */
            if (c == '/' && i + 1 < n && sql[i+1] == '*') {
                size_t j = i + 2;
                while (j + 1 < n && !(sql[j] == '*' && sql[j+1] == '/')) ++j;
                if (j + 1 >= n) {
                    if (errmsg) *errmsg = "unterminated comment";
                    return false;
                }
                i = j + 2;
                continue;
            }

            if (std::isalpha(c) || c == '_') {
                size_t j = i + 1;
                while (j < n && (std::isalnum(static_cast<unsigned char>(sql[j])) || sql[j] == '_')) ++j;
                std::string tok = sql.substr(i, j - i);
                std::string up = to_upper_str(tok);
                if (keywords.find(up) != keywords.end()) {
                    out_tokens.push_back({SQLTokenType::Keyword, up});
                    if (debug) std::cout << "LEX: Keyword(" << up << ")\n";
                } else {
                    out_tokens.push_back({SQLTokenType::Identifier, tok});
                    if (debug) std::cout << "LEX: Identifier(" << tok << ")\n";
                }
                i = j;
                continue;
            }

            if (std::isdigit(c)) {
                size_t j = i + 1;
                bool seen_dot = false;
                while (j < n) {
                    unsigned char cc = static_cast<unsigned char>(sql[j]);
                    if (std::isdigit(cc)) { ++j; continue; }
                    if (cc == '.' && !seen_dot && j + 1 < n && std::isdigit(static_cast<unsigned char>(sql[j+1]))) { seen_dot = true; ++j; continue; }
                    break;
                }
                out_tokens.push_back({SQLTokenType::Number, sql.substr(i, j - i)});
                if (debug) std::cout << "LEX: Number(" << sql.substr(i, j - i) << ")\n";
                i = j;
                continue;
            }

            if (c == '\'' || c == '"') {
                char q = static_cast<char>(c);
                size_t j = i + 1;
                std::string acc;
                while (j < n) {
                    char cc = sql[j];
                    if (cc == q) { ++j; break; }
                    if (cc == '\\' && j + 1 < n) { acc.push_back(sql[j+1]); j += 2; }
                    else { acc.push_back(cc); ++j; }
                }
                // if we reached end without closing quote, error
                if (j > n) {
                    if (errmsg) *errmsg = "unterminated string literal";
                    return false;
                }
                out_tokens.push_back({SQLTokenType::String, acc});
                if (debug) std::cout << "LEX: String('" << acc << "')\n";
                i = j;
                continue;
            }

            switch (c) {
                case '*': out_tokens.push_back({SQLTokenType::Star, std::string("*")}); if (debug) std::cout << "LEX: Star(*)\n"; ++i; break;
                case ',': out_tokens.push_back({SQLTokenType::Comma, ","}); if (debug) std::cout << "LEX: Comma(,)\n"; ++i; break;
                case '(' : out_tokens.push_back({SQLTokenType::LParen, "("}); if (debug) std::cout << "LEX: LParen(()\n"; ++i; break;
                case ')' : out_tokens.push_back({SQLTokenType::RParen, ")"}); if (debug) std::cout << "LEX: RParen())\n"; ++i; break;
                case '=' : out_tokens.push_back({SQLTokenType::Equals, "="}); if (debug) std::cout << "LEX: Equals(=)\n"; ++i; break;
                case '+': out_tokens.push_back({SQLTokenType::Plus, "+"}); if (debug) std::cout << "LEX: Plus(+)\n"; ++i; break;
                case '-': out_tokens.push_back({SQLTokenType::Minus, "-"}); if (debug) std::cout << "LEX: Minus(-)\n"; ++i; break;
                case '/': out_tokens.push_back({SQLTokenType::Slash, "/"}); if (debug) std::cout << "LEX: Slash(/)\n"; ++i; break;
                case '<':
                    if (i + 1 < n && sql[i+1] == '=') { out_tokens.push_back({SQLTokenType::LessEq, "<="}); if (debug) std::cout << "LEX: LessEq(<=)\n"; i += 2; }
                    else { out_tokens.push_back({SQLTokenType::Less, "<"}); if (debug) std::cout << "LEX: Less(<)\n"; ++i; }
                    break;
                case '>':
                    if (i + 1 < n && sql[i+1] == '=') { out_tokens.push_back({SQLTokenType::GreaterEq, ">="}); if (debug) std::cout << "LEX: GreaterEq(>=)\n"; i += 2; }
                    else { out_tokens.push_back({SQLTokenType::Greater, ">"}); if (debug) std::cout << "LEX: Greater(>)\n"; ++i; }
                    break;
                case '!':
                    if (i + 1 < n && sql[i+1] == '=') { out_tokens.push_back({SQLTokenType::NotEquals, "!="}); if (debug) std::cout << "LEX: NotEquals(!=)\n"; i += 2; }
                    else { if (errmsg) *errmsg = std::string("unexpected character: ") + sql[i]; return false; }
                    break;
                case ';' : out_tokens.push_back({SQLTokenType::Semicolon, ";"}); if (debug) std::cout << "LEX: Semicolon(;)\n"; ++i; break;
                default:
                    if (errmsg) *errmsg = std::string("unexpected character: ") + sql[i];
                    return false;
            }
        }

        out_tokens.push_back({SQLTokenType::End, std::string()});
        return true;
    } catch (...) {
        if (errmsg) *errmsg = "exception in lexer";
        return false;
    }
}

} // namespace rdbms
