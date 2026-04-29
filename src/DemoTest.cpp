#include <algorithm>
#include <cctype>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <string>
#include <vector>
#include "../include/DemoTest.h"
#include "../include/FileManager.h"
#include "../include/Serializer.h"
#include "../include/DatabaseManager.h"
#include "../include/DataManager.h"
#include "../include/SQLEngine.h"
#include "../include/SQLLexer.h"
#include "../include/SQLParser.h"

namespace {

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

    std::cout << "  ascii preview (" << ascii_count << " bytes): \"";
    for (std::size_t i = 0; i < ascii_count; ++i) {
        const unsigned char ch = static_cast<unsigned char>(data[i]);
        std::cout << (std::isprint(ch) ? static_cast<char>(ch) : '.');
    }
    std::cout << "\"\n";
}

} // namespace

int run_file_manager_visual_test() {
    using namespace rdbms;

    const std::string dir = "data/db1";
    const std::string filepath = dir + "/table1.bin";
    const std::size_t page_size = 4096;

    std::cout << "=== FileManager visual test ===\n";
    std::cout << "target dir : " << dir << '\n';
    std::cout << "target file: " << filepath << '\n';
    std::cout << "page size  : " << page_size << " bytes\n";

    print_step(1, "Create directory");
    bool ok = FileManager::create_directory(dir);
    // treat existing directory as success for repeatable runs
    print_ok(ok || std::filesystem::exists(dir));
    print_path_state(dir);
    if (!ok && !std::filesystem::exists(dir)) {
        std::cerr << "create_directory failed\n";
        return 1;
    }

    print_step(2, "Create file");
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
    ok = FileManager::write_page(filepath, 0, page_size, page);
    print_ok(ok);
    print_path_state(filepath);
    if (!ok) {
        std::cerr << "write_page failed\n";
        return 1;
    }

    print_step(5, "Read page 0");
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

    // --- Serialization demo: custom record -> binary -> file -> memory -> object ---
    print_step(7, "Serialize custom record into page 1");
    struct Record {
        uint32_t id;
        std::string name;
        std::vector<int32_t> values;
    };

    Record rec{42, "Alice", {100, 200, 300}};
    std::vector<char> rec_buf;
    // serialize fields into rec_buf
    rdbms::serialization::write_pod(rec_buf, rec.id);
    rdbms::serialization::write_string(rec_buf, rec.name);
    rdbms::serialization::write_vector(rec_buf, rec.values);

    std::vector<char> page2(page_size, 0);
    if (rec_buf.size() > page2.size()) {
        std::cerr << "record too large for page\n";
        return 1;
    }
    std::copy(rec_buf.begin(), rec_buf.end(), page2.begin());

    bool ok2 = FileManager::write_page(filepath, 1, page_size, page2);
    print_ok(ok2);
    if (!ok2) {
        std::cerr << "write_page (record) failed\n";
        return 1;
    }

    print_step(8, "Read and deserialize record from page 1");
    std::vector<char> rec_out;
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

    print_step(9, "DatabaseManager demo: create/use db and table");
    rdbms::DatabaseManager mgr("data");
    bool ok_db = mgr.create_database("demo_db");
    print_ok(ok_db);
    if (!ok_db) {
        std::cerr << "create_database failed\n";
        return 1;
    }
    bool ok_use = mgr.use_database("demo_db");
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
    bool ok_list_tables = mgr.list_tables(tables);
    print_ok(ok_list_tables);
    if (ok_list_tables) {
        std::cout << "  tables:";
        for (const auto &tn : tables) std::cout << ' ' << tn;
        std::cout << '\n';
    }

    rdbms::DatabaseManager::TableSchema got_schema;
    bool ok_get_schema2 = mgr.get_schema("users", got_schema);
    print_ok(ok_get_schema2);
    if (ok_get_schema2) {
        std::cout << "  schema table: " << got_schema.table_name << '\n';
        std::cout << "  columns:";
        for (const auto &c : got_schema.columns) std::cout << ' ' << c.name;
        std::cout << '\n';
    }

    // --- SQL parser demo (lexer + parser) ---
    print_step(10, "SQL parser demo: tokenize & parse sample");
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

    // --- SQL engine demo using DataManager ---
    rdbms::DataManager dm(mgr);
    rdbms::SQLEngine engine(mgr, dm);

    std::vector<std::vector<std::string>> rows_out;
    std::vector<std::string> cols_out;
    std::string msg;

    print_step(11, "SQL DDL demo: CREATE/USE/ALTER/DROP via engine");
    {
        std::vector<std::vector<std::string>> dummy_rows;
        std::vector<std::string> dummy_cols;
        std::string dm_msg;
        bool ok;
        ok = engine.execute("CREATE DATABASE demo_db_sql;", dummy_rows, dummy_cols, dm_msg);
        print_ok(ok); if (!ok) std::cerr << "  " << dm_msg << '\n';
        ok = engine.execute("USE demo_db_sql;", dummy_rows, dummy_cols, dm_msg);
        print_ok(ok); if (!ok) std::cerr << "  " << dm_msg << '\n';
        ok = engine.execute("CREATE TABLE demo_users (id INT PRIMARY KEY NOT NULL, name STRING);", dummy_rows, dummy_cols, dm_msg);
        print_ok(ok); if (!ok) std::cerr << "  " << dm_msg << '\n';
        ok = engine.execute("ALTER TABLE demo_users ADD COLUMN age INT;", dummy_rows, dummy_cols, dm_msg);
        print_ok(ok); if (!ok) std::cerr << "  " << dm_msg << '\n';
        ok = engine.execute("ALTER TABLE demo_users DROP COLUMN age;", dummy_rows, dummy_cols, dm_msg);
        print_ok(ok); if (!ok) std::cerr << "  " << dm_msg << '\n';
        ok = engine.execute("DROP TABLE demo_users;", dummy_rows, dummy_cols, dm_msg);
        print_ok(ok); if (!ok) std::cerr << "  " << dm_msg << '\n';
        ok = engine.execute("DROP DATABASE demo_db_sql;", dummy_rows, dummy_cols, dm_msg);
        print_ok(ok); if (!ok) std::cerr << "  " << dm_msg << '\n';
        // restore previously used database for subsequent DML demo
        ok = engine.execute("USE demo_db;", dummy_rows, dummy_cols, dm_msg);
        print_ok(ok); if (!ok) std::cerr << "  " << dm_msg << '\n';
    }

    print_step(12, "SQL engine demo: INSERT / SELECT / UPDATE / DELETE");
    bool ok_ins1 = engine.execute("INSERT INTO users (id, name) VALUES (1, 'Alice');", rows_out, cols_out, msg);
    print_ok(ok_ins1);
    if (!ok_ins1) { std::cerr << msg << '\n'; return 1; }

    bool ok_ins2 = engine.execute("INSERT INTO users (id, name) VALUES (2, 'Bob');", rows_out, cols_out, msg);
    print_ok(ok_ins2);
    if (!ok_ins2) { std::cerr << msg << '\n'; return 1; }

    print_step(13, "Constraint validation demo");
    // duplicate primary key
    bool ok_dup = engine.execute("INSERT INTO users (id, name) VALUES (1, 'Charlie');", rows_out, cols_out, msg);
    print_ok(ok_dup);
    if (ok_dup) { std::cerr << "unexpected duplicate insert success\n"; return 1; }
    else { std::cout << "  expected failure: " << msg << '\n'; }

    // wrong type for INT32 id
    bool ok_badtype = engine.execute("INSERT INTO users (id, name) VALUES ('abc', 'Bad');", rows_out, cols_out, msg);
    print_ok(ok_badtype);
    if (ok_badtype) { std::cerr << "unexpected bad-type insert success\n"; return 1; }
    else { std::cout << "  expected failure: " << msg << '\n'; }

    // not-null violation (empty id)
    bool ok_notnull = engine.execute("INSERT INTO users (id, name) VALUES (, 'NoId');", rows_out, cols_out, msg);
    print_ok(ok_notnull);
    if (ok_notnull) { std::cerr << "unexpected not-null insert success\n"; return 1; }
    else { std::cout << "  expected failure: " << msg << '\n'; }

    bool ok_sel1 = engine.execute("SELECT id, name FROM users;", rows_out, cols_out, msg);
    print_ok(ok_sel1);
    if (ok_sel1) {
        std::cout << "  columns:";
        for (auto &c : cols_out) std::cout << ' ' << c;
        std::cout << '\n';
        for (auto &r : rows_out) {
            std::cout << "   row:";
            for (auto &v : r) std::cout << ' ' << v;
            std::cout << '\n';
        }
    }

    bool ok_upd = engine.execute("UPDATE users SET name = 'AliceSmith' WHERE id = 1;", rows_out, cols_out, msg);
    print_ok(ok_upd);
    if (!ok_upd) { std::cerr << msg << '\n'; return 1; }

    bool ok_sel2 = engine.execute("SELECT id, name FROM users WHERE id = 1;", rows_out, cols_out, msg);
    print_ok(ok_sel2);
    if (ok_sel2) {
        for (auto &r : rows_out) {
            std::cout << "   row:";
            for (auto &v : r) std::cout << ' ' << v;
            std::cout << '\n';
        }
    }

    bool ok_del = engine.execute("DELETE FROM users WHERE id = 2;", rows_out, cols_out, msg);
    print_ok(ok_del);
    if (!ok_del) { std::cerr << msg << '\n'; return 1; }

    bool ok_sel3 = engine.execute("SELECT id, name FROM users;", rows_out, cols_out, msg);
    print_ok(ok_sel3);
    if (ok_sel3) {
        for (auto &r : rows_out) {
            std::cout << "   row:";
            for (auto &v : r) std::cout << ' ' << v;
            std::cout << '\n';
        }
    }

    bool ok_drop_table2 = mgr.drop_table("users");
    print_ok(ok_drop_table2);

    bool ok_drop_db2 = mgr.drop_database("demo_db");
    print_ok(ok_drop_db2);

    print_step(14, "Cleanup");
    const bool removed_file = FileManager::remove_file(filepath);
    const bool removed_dir = FileManager::remove_directory(dir);
    std::cout << "  remove_file: " << (removed_file ? "OK" : "FAIL") << '\n';
    std::cout << "  remove_directory: " << (removed_dir ? "OK" : "FAIL") << '\n';
    std::cout << "  file exists after cleanup: " << (std::filesystem::exists(filepath) ? "yes" : "no") << '\n';
    std::cout << "  dir exists after cleanup : " << (std::filesystem::exists(dir) ? "yes" : "no") << '\n';

    std::cout << "\n=== test finished successfully ===\n";
    return 0;
}