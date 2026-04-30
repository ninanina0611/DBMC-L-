#include <algorithm>
#include <cctype>
#include <filesystem>
#include <fstream>
#include <memory>
#include <iomanip>
#include <iostream>
#include <string>
#include <vector>
#include "../include/Test.h"
#include "../include/FileManager.h"
#include "../include/Serializer.h"
#include "../include/DatabaseManager.h"
#include "../include/DataManager.h"
#include "../include/SQLEngine.h"
#include "../include/SQLLexer.h"
#include "../include/SQLParser.h"

namespace {

// Tee streambuf: writes to two streambufs
class TeeBuf : public std::streambuf {
public:
    TeeBuf(std::streambuf *a, std::streambuf *b) : sb1(a), sb2(b) {}
protected:
    int overflow(int c) override {
        if (c == EOF) return !EOF;
        const int r1 = sb1->sputc(c);
        const int r2 = sb2 ? sb2->sputc(c) : r1;
        return (r1 == EOF || r2 == EOF) ? EOF : c;
    }
    int sync() override {
        const int r1 = sb1->pubsync();
        const int r2 = sb2 ? sb2->pubsync() : r1;
        return (r1 == 0 && r2 == 0) ? 0 : -1;
    }
private:
    std::streambuf *sb1;
    std::streambuf *sb2;
};


void print_step(int step, const std::string &title) {
    std::cout << "\n[STEP " << step << "] " << title << '\n';
}

void print_ok(bool ok) {
    std::cout << "  result: " << (ok ? "OK" : "FAIL") << '\n';
}

void print_path_state(const std::string &path) {
    std::cout << "  path: " << path << '\n';
    std::cout << "  exists: " << (std::filesystem::exists(path) ? "yes" : "no") << '\n';
    if (std::filesystem::exists(path) && std::filesystem::is_regular_file(path)) {
        std::cout << "  file_size: " << std::filesystem::file_size(path) << " bytes\n";
    }
}

void dump_bytes(const std::vector<char> &data, std::size_t limit, std::size_t ascii_limit = 0) {
    const std::size_t count = std::min(limit, data.size());
    const std::size_t ascii_count = ascii_limit == 0 ? count : std::min(ascii_limit, data.size());
    std::cout << "  hex preview (" << count << " bytes):\n    ";

    for (std::size_t i = 0; i < count; ++i) {
        const unsigned char ch = static_cast<unsigned char>(data[i]);
        std::cout << std::hex << std::setw(2) << std::setfill('0') << static_cast<int>(ch) << ' ';
        if ((i + 1) % 16 == 0 && i + 1 < count) {
            std::cout << "\n    ";
        }
    }

    std::cout << std::dec << '\n';
    // restore default fill char (spaces) so later formatted output isn't zero-filled
    std::cout << std::setfill(' ');

    std::cout << "  ascii preview (" << ascii_count << " bytes): \"";
    for (std::size_t i = 0; i < ascii_count; ++i) {
        const unsigned char ch = static_cast<unsigned char>(data[i]);
        std::cout << (std::isprint(ch) ? static_cast<char>(ch) : '.');
    }
    std::cout << "\"\n";
}

} // namespace

// Helpers to make SQL test outputs clearer
namespace {
void print_sql(const std::string &sql) {
    std::cout << "  SQL: " << sql << '\n';
}

void print_cmd(const std::string &cmd, bool verbose) {
    if (verbose) std::cout << "  CMD: " << cmd << '\n';
}

bool exec_sql(rdbms::SQLEngine &engine, const std::string &sql,
              std::vector<std::vector<std::string>> &out_rows,
              std::vector<std::string> &out_cols,
              std::string &msg) {
    print_sql(sql);
    bool ok = engine.execute(sql, out_rows, out_cols, msg);
    print_ok(ok);
    if (ok && (!out_cols.empty() || !out_rows.empty())) {
        // prepare table-like output with column widths and separators
        size_t ncols = std::max(out_cols.size(), out_rows.empty() ? size_t(0) : out_rows[0].size());
        if (ncols == 0) return ok;
        std::vector<std::string> headers(ncols);
        for (size_t i = 0; i < ncols; ++i) headers[i] = (i < out_cols.size() ? out_cols[i] : (std::string("col") + std::to_string(i+1)));
        std::vector<size_t> widths(ncols, 0);
        for (size_t i = 0; i < ncols; ++i) widths[i] = headers[i].size();
        for (const auto &r : out_rows) {
            for (size_t i = 0; i < r.size() && i < ncols; ++i) widths[i] = std::max(widths[i], r[i].size());
        }

        auto print_sep = [&](void) {
            std::cout << "  +";
            for (size_t i = 0; i < ncols; ++i) {
                std::cout << std::string(widths[i] + 2, '-') << "+";
            }
            std::cout << '\n';
        };

        print_sep();
        std::cout << "  |";
        for (size_t i = 0; i < ncols; ++i) {
            std::cout << ' ' << std::left << std::setw(widths[i]) << headers[i] << ' ' << "|";
        }
        std::cout << '\n';
        print_sep();

        for (const auto &r : out_rows) {
            std::cout << "  |";
            for (size_t i = 0; i < ncols; ++i) {
                const std::string v = (i < r.size() ? r[i] : std::string());
                std::cout << ' ' << std::left << std::setw(widths[i]) << v << ' ' << "|";
            }
            std::cout << '\n';
        }
        print_sep();
    }
    return ok;
}
} // namespace

int run_visual_test(const std::string &mode) {
    using namespace rdbms;

    const bool verbose = (mode != "compact");

    // always write test log to default path
    static const std::string default_log = "data/lightdb_test_output.txt";
    static std::ofstream s_log;
    static std::unique_ptr<TeeBuf> s_tee;
    static std::streambuf *s_old = nullptr;
    if (!s_log.is_open()) {
        try {
            auto p = std::filesystem::path(default_log).parent_path();
            if (!p.empty()) std::filesystem::create_directories(p);
        } catch (...) {}
        s_log.open(default_log, std::ios::trunc);
    }
    if (s_log.is_open() && !s_tee) {
        s_tee = std::make_unique<TeeBuf>(std::cout.rdbuf(), s_log.rdbuf());
        s_old = std::cout.rdbuf(s_tee.get());
        (void)s_old;
    }

    const std::string dir = "data/db1";
    const std::string filepath = dir + "/table1.bin";
    const std::size_t page_size = 4096;

    std::cout << "lightdb test started.\n\n";
    std::cout << "target dir : " << dir << '\n';
    std::cout << "target file: " << filepath << '\n';
    std::cout << "page size  : " << page_size << " bytes\n";

    print_step(1, "Create directory");
    print_cmd(std::string("FileManager::create_directory(\"") + dir + "\")", verbose);
    bool ok = FileManager::create_directory(dir);
    // treat existing directory as success for repeatable runs
    print_ok(ok || std::filesystem::exists(dir));
    print_path_state(dir);
    if (!ok && !std::filesystem::exists(dir)) {
        std::cerr << "create_directory failed\n";
        return 1;
    }

    print_step(2, "Create file");
    print_cmd(std::string("FileManager::create_file(\"") + filepath + "\")", verbose);
    ok = FileManager::create_file(filepath);
    print_ok(ok);
    print_path_state(filepath);
    if (!ok) {
        std::cerr << "create_file failed\n";
        return 1;
    }

    print_step(3, "Prepare page buffer");
    std::vector<char> page(page_size, 0);
    const std::string text = "Hello RDBMS binary page";
    std::copy(text.begin(), text.end(), page.begin());
    std::cout << "  payload text: " << text << '\n';
    std::cout << "  payload len : " << text.size() << " bytes\n";
    dump_bytes(page, 64, text.size());

    print_step(4, "Write page 0");
    print_cmd(std::string("FileManager::write_page(\"") + filepath + "\", 0, page_size, page)", verbose);
    ok = FileManager::write_page(filepath, 0, page_size, page);
    print_ok(ok);
    print_path_state(filepath);
    if (!ok) {
        std::cerr << "write_page failed\n";
        return 1;
    }

    print_step(5, "Read page 0");
    print_cmd(std::string("FileManager::read_page(\"") + filepath + "\", 0, page_size, out)", verbose);
    std::vector<char> out;
    ok = FileManager::read_page(filepath, 0, page_size, out);
    print_ok(ok);
    std::cout << "  read size: " << out.size() << " bytes\n";
    dump_bytes(out, 64, text.size());
    if (!ok) {
        std::cerr << "read_page failed\n";
        return 1;
    }

    print_step(6, "Verify content");
    const bool size_match = out.size() == page.size();
    const bool data_match = size_match && std::equal(page.begin(), page.end(), out.begin());
    std::cout << "  size match: " << (size_match ? "yes" : "no") << '\n';
    std::cout << "  data match: " << (data_match ? "yes" : "no") << '\n';
    std::cout << "  text check: ";
    const std::string s(out.begin(), out.end());
    std::cout << s.substr(0, text.size()) << '\n';
    if (!data_match) {
        std::cerr << "content verification failed\n";
        return 1;
    }

    // --- Serialization test: custom record -> binary -> file -> memory -> object ---
    print_step(7, "Serialize custom record into page 1");
    struct Record {
        uint32_t id;
        std::string name;
        std::vector<int32_t> values;
    };

    Record rec{42, "Alice", {100, 200, 300}};
    std::vector<char> rec_buf;
    // serialize fields into rec_buf
    print_cmd("serialize Record -> rec_buf", verbose);
    rdbms::serialization::write_pod(rec_buf, rec.id);
    rdbms::serialization::write_string(rec_buf, rec.name);
    rdbms::serialization::write_vector(rec_buf, rec.values);

    std::vector<char> page2(page_size, 0);
    if (rec_buf.size() > page2.size()) {
        std::cerr << "record too large for page\n";
        return 1;
    }
    std::copy(rec_buf.begin(), rec_buf.end(), page2.begin());

    print_cmd(std::string("FileManager::write_page(\"") + filepath + "\", 1, page_size, page2)", verbose);
    bool ok2 = FileManager::write_page(filepath, 1, page_size, page2);
    print_ok(ok2);
    if (!ok2) {
        std::cerr << "write_page (record) failed\n";
        return 1;
    }

    print_step(8, "Read and deserialize record from page 1");
    std::vector<char> rec_out;
    print_cmd(std::string("FileManager::read_page(\"") + filepath + "\", 1, page_size, rec_out)", verbose);
    bool ok3 = FileManager::read_page(filepath, 1, page_size, rec_out);
    print_ok(ok3);
    if (!ok3) {
        std::cerr << "read_page (record) failed\n";
        return 1;
    }

    size_t offset = 0;
    Record rec2;
    if (!rdbms::serialization::read_pod(rec_out, offset, rec2.id)) {
        std::cerr << "failed to read id\n";
        return 1;
    }
    if (!rdbms::serialization::read_string(rec_out, offset, rec2.name)) {
        std::cerr << "failed to read name\n";
        return 1;
    }
    if (!rdbms::serialization::read_vector(rec_out, offset, rec2.values)) {
        std::cerr << "failed to read values\n";
        return 1;
    }

    std::cout << "  deserialized id: " << rec2.id << '\n';
    std::cout << "  deserialized name: " << rec2.name << '\n';
    std::cout << "  deserialized values:";
    for (auto v : rec2.values) std::cout << ' ' << v;
    std::cout << '\n';

    // verify record
    const bool rec_match = (rec.id == rec2.id) && (rec.name == rec2.name) && (rec.values == rec2.values);
    std::cout << "  record match: " << (rec_match ? "yes" : "no") << '\n';
    if (!rec_match) {
        std::cerr << "record verification failed\n";
        return 1;
    }

    print_step(9, "DatabaseManager test: create/use db and table");
    rdbms::DatabaseManager mgr("data");
    print_cmd("DatabaseManager::create_database(\"test_db\")", verbose);
    bool ok_db = mgr.create_database("test_db");
    print_ok(ok_db);
    if (!ok_db) {
        std::cerr << "create_database failed\n";
        return 1;
    }
    print_cmd("DatabaseManager::use_database(\"test_db\")", verbose);
    bool ok_use = mgr.use_database("test_db");
    print_ok(ok_use);
    if (!ok_use) {
        std::cerr << "use_database failed\n";
        return 1;
    }

    rdbms::DatabaseManager::TableSchema schema2;
    schema2.table_name = "users";
    schema2.columns = {
        rdbms::DatabaseManager::Column{"id", rdbms::DatabaseManager::Type::INT32, true, true},
        rdbms::DatabaseManager::Column{"name", rdbms::DatabaseManager::Type::STRING, false, false}
    };

    bool ok_create_table2 = mgr.create_table(schema2);
    print_ok(ok_create_table2);
    if (!ok_create_table2) {
        std::cerr << "create_table failed\n";
        return 1;
    }

    std::vector<std::string> tables;
    print_cmd("DatabaseManager::list_tables()", verbose);
    bool ok_list_tables = mgr.list_tables(tables);
    print_ok(ok_list_tables);
    if (ok_list_tables) {
        std::cout << "  tables:";
        for (const auto &tn : tables) std::cout << ' ' << tn;
        std::cout << '\n';
    }

    rdbms::DatabaseManager::TableSchema got_schema;
    print_cmd("DatabaseManager::get_schema(\"users\")", verbose);
    bool ok_get_schema2 = mgr.get_schema("users", got_schema);
    print_ok(ok_get_schema2);
    if (ok_get_schema2) {
        std::cout << "  schema table: " << got_schema.table_name << '\n';
        std::cout << "  columns:";
        for (const auto &c : got_schema.columns) std::cout << ' ' << c.name;
        std::cout << '\n';
    }

    // --- SQL parser test (lexer + parser) ---
    print_step(10, "SQL parser test: tokenize & parse sample");
    {
        const std::string sample = "INSERT INTO users (id, name) VALUES (1, 'Alice');";
        std::vector<rdbms::SQLToken> toks;
        std::string lex_err;
        bool oklex = rdbms::SQLLexer::tokenize(sample, toks, &lex_err);
        print_ok(oklex);
        if (!oklex) {
            std::cerr << "  lexer error: " << lex_err << '\n';
        } else {
            std::cout << "  tokens:\n";
            for (const auto &t : toks) {
                if (t.type == rdbms::SQLTokenType::End) break;
                std::cout << "   - ";
                switch (t.type) {
                    case rdbms::SQLTokenType::Identifier: std::cout << "Identifier: "; break;
                    case rdbms::SQLTokenType::Number: std::cout << "Number: "; break;
                    case rdbms::SQLTokenType::String: std::cout << "String: "; break;
                    case rdbms::SQLTokenType::Keyword: std::cout << "Keyword: "; break;
                    case rdbms::SQLTokenType::Star: std::cout << "Star: "; break;
                    case rdbms::SQLTokenType::Comma: std::cout << "Comma: "; break;
                    case rdbms::SQLTokenType::LParen: std::cout << "LParen: "; break;
                    case rdbms::SQLTokenType::RParen: std::cout << "RParen: "; break;
                    case rdbms::SQLTokenType::Equals: std::cout << "Equals: "; break;
                    case rdbms::SQLTokenType::Semicolon: std::cout << "Semicolon: "; break;
                    default: std::cout << "Other: "; break;
                }
                std::cout << t.text << '\n';
            }

            rdbms::SQLStatement st;
            std::string perr;
            bool okp = rdbms::SQLParser::parse(toks, st, &perr);
            print_ok(okp);
            if (!okp) std::cerr << "  parse error: " << perr << '\n';
            else {
                std::cout << "  parsed: type=";
                switch (st.type) {
                    case rdbms::SQLStatement::Type::Insert: std::cout << "INSERT"; break;
                    case rdbms::SQLStatement::Type::Select: std::cout << "SELECT"; break;
                    case rdbms::SQLStatement::Type::Update: std::cout << "UPDATE"; break;
                    case rdbms::SQLStatement::Type::Delete: std::cout << "DELETE"; break;
                    default: std::cout << "UNKNOWN"; break;
                }
                std::cout << " table=" << st.table << '\n';
                if (!st.columns.empty()) {
                    std::cout << "  columns:";
                    for (auto &c : st.columns) std::cout << ' ' << c;
                    std::cout << '\n';
                }
                if (!st.values.empty()) {
                    std::cout << "  values:";
                    for (auto &v : st.values) std::cout << ' ' << v;
                    std::cout << '\n';
                }
            }
        }
    }

    // --- SQL engine test using DataManager ---
    rdbms::DataManager dm(mgr);
    rdbms::SQLEngine engine(mgr, dm);

    std::vector<std::vector<std::string>> rows_out;
    std::vector<std::string> cols_out;
    std::string msg;

    print_step(11, "SQL DDL test: CREATE/USE/ALTER/DROP via engine");
    {
        std::vector<std::vector<std::string>> dummy_rows;
        std::vector<std::string> dummy_cols;
        std::string dm_msg;
        bool ok;
        ok = exec_sql(engine, "CREATE DATABASE test_db_sql;", dummy_rows, dummy_cols, dm_msg);
        if (!ok) std::cerr << "  " << dm_msg << '\n';
        ok = exec_sql(engine, "USE test_db_sql;", dummy_rows, dummy_cols, dm_msg);
        if (!ok) std::cerr << "  " << dm_msg << '\n';
        ok = exec_sql(engine, "CREATE TABLE test_users (id INT PRIMARY KEY NOT NULL, name STRING);", dummy_rows, dummy_cols, dm_msg);
        if (!ok) std::cerr << "  " << dm_msg << '\n';
        ok = exec_sql(engine, "ALTER TABLE test_users MODIFY COLUMN name VARCHAR(100) NOT NULL;", dummy_rows, dummy_cols, dm_msg);
        if (!ok) std::cerr << "  " << dm_msg << '\n';
        ok = exec_sql(engine, "ALTER TABLE test_users RENAME COLUMN name TO fullname;", dummy_rows, dummy_cols, dm_msg);
        if (!ok) std::cerr << "  " << dm_msg << '\n';
        ok = exec_sql(engine, "ALTER TABLE test_users ADD COLUMN age INT;", dummy_rows, dummy_cols, dm_msg);
        if (!ok) std::cerr << "  " << dm_msg << '\n';
        ok = exec_sql(engine, "ALTER TABLE test_users DROP COLUMN age;", dummy_rows, dummy_cols, dm_msg);
        if (!ok) std::cerr << "  " << dm_msg << '\n';
        ok = exec_sql(engine, "DROP TABLE test_users;", dummy_rows, dummy_cols, dm_msg);
        if (!ok) std::cerr << "  " << dm_msg << '\n';
        ok = exec_sql(engine, "DROP DATABASE test_db_sql;", dummy_rows, dummy_cols, dm_msg);
        if (!ok) std::cerr << "  " << dm_msg << '\n';
        // restore previously used database for subsequent DML test
        ok = exec_sql(engine, "USE test_db;", dummy_rows, dummy_cols, dm_msg);
        if (!ok) std::cerr << "  " << dm_msg << '\n';
    }

    print_step(12, "Error handling test: no DB / unknown table / unknown column");
    {
        std::vector<std::vector<std::string>> dummy_rows2;
        std::vector<std::string> dummy_cols2;
        std::string dm_msg2;
        bool ok2;

        // New DatabaseManager instance without selecting a database -> expect "no database selected"
        rdbms::DatabaseManager mgr_no_db("data");
        rdbms::DataManager dm_no_db(mgr_no_db);
        rdbms::SQLEngine engine_no_db(mgr_no_db, dm_no_db);
        ok2 = exec_sql(engine_no_db, "CREATE TABLE should_fail (id INT);", dummy_rows2, dummy_cols2, dm_msg2);
        if (!ok2) std::cerr << "  expected failure (no db): " << dm_msg2 << '\n';

        // unknown table
        ok2 = exec_sql(engine, "DROP TABLE nonexist_table;", dummy_rows2, dummy_cols2, dm_msg2);
        if (!ok2) std::cerr << "  expected failure (unknown table): " << dm_msg2 << '\n';

        // unknown column: create a temp table then try to drop a non-existent column
        ok2 = exec_sql(engine, "CREATE TABLE tmp_col_test (id INT, name STRING);", dummy_rows2, dummy_cols2, dm_msg2);
        ok2 = exec_sql(engine, "ALTER TABLE tmp_col_test DROP COLUMN not_a_column;", dummy_rows2, dummy_cols2, dm_msg2);
        if (!ok2) std::cerr << "  expected failure (unknown column): " << dm_msg2 << '\n';
        ok2 = exec_sql(engine, "DROP TABLE tmp_col_test;", dummy_rows2, dummy_cols2, dm_msg2);
    }

    print_step(13, "SQL engine test: INSERT / SELECT / UPDATE / DELETE");
    if (!exec_sql(engine, "INSERT INTO users (id, name) VALUES (1, 'Alice');", rows_out, cols_out, msg)) { std::cerr << msg << '\n'; return 1; }
    if (!exec_sql(engine, "INSERT INTO users (id, name) VALUES (2, 'Bob');", rows_out, cols_out, msg)) { std::cerr << msg << '\n'; return 1; }

    print_step(14, "Constraint validation test");
    // duplicate primary key
    bool ok_dup = exec_sql(engine, "INSERT INTO users (id, name) VALUES (1, 'Charlie');", rows_out, cols_out, msg);
    if (ok_dup) { std::cerr << "unexpected duplicate insert success\n"; return 1; }
    else { std::cout << "  expected failure: " << msg << '\n'; }

    // wrong type for INT32 id
    bool ok_badtype = exec_sql(engine, "INSERT INTO users (id, name) VALUES ('abc', 'Bad');", rows_out, cols_out, msg);
    if (ok_badtype) { std::cerr << "unexpected bad-type insert success\n"; return 1; }
    else { std::cout << "  expected failure: " << msg << '\n'; }

    // not-null violation (empty id)
    bool ok_notnull = exec_sql(engine, "INSERT INTO users (id, name) VALUES (, 'NoId');", rows_out, cols_out, msg);
    if (ok_notnull) { std::cerr << "unexpected not-null insert success\n"; return 1; }
    else { std::cout << "  expected failure: " << msg << '\n'; }

    if (!exec_sql(engine, "SELECT id, name FROM users;", rows_out, cols_out, msg)) {
        std::cerr << msg << '\n';
        return 1;
    }

    if (!exec_sql(engine, "UPDATE users SET name = 'AliceSmith' WHERE id = 1;", rows_out, cols_out, msg)) { std::cerr << msg << '\n'; return 1; }
    if (!exec_sql(engine, "SELECT id, name FROM users WHERE id = 1;", rows_out, cols_out, msg)) { std::cerr << msg << '\n'; return 1; }
    if (!exec_sql(engine, "DELETE FROM users WHERE id = 2;", rows_out, cols_out, msg)) { std::cerr << msg << '\n'; return 1; }
    if (!exec_sql(engine, "SELECT id, name FROM users;", rows_out, cols_out, msg)) { std::cerr << msg << '\n'; return 1; }

    bool ok_drop_table2 = mgr.drop_table("users");
    print_ok(ok_drop_table2);

    bool ok_drop_db2 = mgr.drop_database("test_db");
    print_ok(ok_drop_db2);

    print_step(15, "Cleanup");
    print_cmd(std::string("FileManager::remove_file(\"") + filepath + "\")", verbose);
    const bool removed_file = FileManager::remove_file(filepath);
    print_cmd(std::string("FileManager::remove_directory(\"") + dir + "\")", verbose);
    const bool removed_dir = FileManager::remove_directory(dir);
    std::cout << "  remove_file: " << (removed_file ? "OK" : "FAIL") << '\n';
    std::cout << "  remove_directory: " << (removed_dir ? "OK" : "FAIL") << '\n';
    std::cout << "  file exists after cleanup: " << (std::filesystem::exists(filepath) ? "yes" : "no") << '\n';
    std::cout << "  dir exists after cleanup : " << (std::filesystem::exists(dir) ? "yes" : "no") << '\n';

    std::cout << "\ntest finished successfully.\n";
    return 0;
}