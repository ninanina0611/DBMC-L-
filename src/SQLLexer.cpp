#include "../include/SQLLexer.h"

#include <cctype>
#include <algorithm>
#include <unordered_set>

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
            "CREATE","DROP","ALTER","DATABASE","TABLE","ADD","COLUMN","USE",
            // constraints / modifiers
            "PRIMARY","KEY","NOT","NULL",
            // common types
            "INT","INT32","INT64","BIGINT","VARCHAR","TEXT","STRING"
        };

        size_t i = 0, n = sql.size();
        while (i < n) {
            unsigned char c = static_cast<unsigned char>(sql[i]);
            if (std::isspace(c)) { ++i; continue; }

            if (std::isalpha(c) || c == '_') {
                size_t j = i + 1;
                while (j < n && (std::isalnum(static_cast<unsigned char>(sql[j])) || sql[j] == '_')) ++j;
                std::string tok = sql.substr(i, j - i);
                std::string up = to_upper_str(tok);
                if (keywords.find(up) != keywords.end()) out_tokens.push_back({SQLTokenType::Keyword, up});
                else out_tokens.push_back({SQLTokenType::Identifier, tok});
                i = j;
                continue;
            }

            if (std::isdigit(c)) {
                size_t j = i + 1;
                while (j < n && std::isdigit(static_cast<unsigned char>(sql[j]))) ++j;
                out_tokens.push_back({SQLTokenType::Number, sql.substr(i, j - i)});
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
                i = j;
                continue;
            }

            switch (c) {
                case '*': out_tokens.push_back({SQLTokenType::Star, std::string("*")}); ++i; break;
                case ',': out_tokens.push_back({SQLTokenType::Comma, ","}); ++i; break;
                case '(' : out_tokens.push_back({SQLTokenType::LParen, "("}); ++i; break;
                case ')' : out_tokens.push_back({SQLTokenType::RParen, ")"}); ++i; break;
                case '=' : out_tokens.push_back({SQLTokenType::Equals, "="}); ++i; break;
                case ';' : out_tokens.push_back({SQLTokenType::Semicolon, ";"}); ++i; break;
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
