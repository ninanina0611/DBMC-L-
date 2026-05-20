#pragma once

#include <string>
#include <vector>

namespace rdbms {

enum class SQLTokenType {
    Identifier,
    Number,
    String,
    Keyword,
    Star,
    Comma,
    LParen,
    RParen,
    Equals,
    Semicolon,
    End
};

struct SQLToken {
    SQLTokenType type;
    std::string text; // original text (keywords stored uppercase)
};

class SQLLexer {
public:
    // Tokenize SQL text into tokens. Returns false and sets errmsg on failure.
    static bool tokenize(const std::string &sql, std::vector<SQLToken> &out_tokens, std::string *errmsg = nullptr) noexcept;
};

} // namespace rdbms
