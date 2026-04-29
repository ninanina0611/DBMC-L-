#include <algorithm>
#include <cctype>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>
#include <memory>

#include "../include/CLI.h"
#include "../include/DemoTest.h"
#include "../include/DatabaseManager.h"
#include "../include/DataManager.h"
#include "../include/SQLEngine.h"

namespace rdbms {

static std::string trim(const std::string &s) {
    size_t a = 0;
    while (a < s.size() && std::isspace(static_cast<unsigned char>(s[a]))) ++a;
    size_t b = s.size();
    while (b > a && std::isspace(static_cast<unsigned char>(s[b-1]))) --b;
    return s.substr(a, b - a);
}

static void print_help() {
    std::cout << "lightdb interactive CLI\n";
    std::cout << "Commands:\n";
    std::cout << "  help             Show this help\n";
    std::cout << "  exit, quit       Exit the CLI\n";
    std::cout << "  databases        List databases\n";
    std::cout << "  tables           List tables in current database\n";
    std::cout << "  use <db>         Select database\n";
    std::cout << "  test             Run scripted demo/test\n";
    std::cout << "  root <dir>       Change data root directory at runtime\n";
    std::cout << "You can also enter SQL statements terminated by a semicolon (;).\n";
}

static void print_rows(const std::vector<std::vector<std::string>> &rows, const std::vector<std::string> &cols) {
    size_t ncols = std::max(cols.size(), rows.empty() ? size_t(0) : rows[0].size());
    if (ncols == 0) return;
    std::vector<std::string> headers(ncols);
    for (size_t i = 0; i < ncols; ++i) headers[i] = (i < cols.size() ? cols[i] : (std::string("col") + std::to_string(i+1)));
    std::vector<size_t> widths(ncols, 0);
    for (size_t i = 0; i < ncols; ++i) widths[i] = headers[i].size();
    for (const auto &r : rows) {
        for (size_t i = 0; i < r.size() && i < ncols; ++i) widths[i] = std::max(widths[i], r[i].size());
    }

    auto print_sep = [&](void) {
        std::cout << "+";
        for (size_t i = 0; i < ncols; ++i) {
            std::cout << std::string(widths[i] + 2, '-') << "+";
        }
        std::cout << "\n";
    };

    print_sep();
    std::cout << "|";
    for (size_t i = 0; i < ncols; ++i) {
        std::cout << ' ' << std::left << std::setw(widths[i]) << headers[i] << ' ' << "|";
    }
    std::cout << "\n";
    print_sep();

    for (const auto &r : rows) {
        std::cout << "|";
        for (size_t i = 0; i < ncols; ++i) {
            const std::string v = (i < r.size() ? r[i] : std::string());
            std::cout << ' ' << std::left << std::setw(widths[i]) << v << ' ' << "|";
        }
        std::cout << "\n";
    }
    print_sep();
}

int run_interactive_cli(int argc, char **argv) noexcept {
    try {
        // CLI runs in interactive mode only; all functionality is exposed via REPL commands.
        std::string root_dir = "data";

        std::unique_ptr<DatabaseManager> mgr = std::make_unique<DatabaseManager>(root_dir);
        std::unique_ptr<DataManager> dm = std::make_unique<DataManager>(*mgr);
        std::unique_ptr<SQLEngine> engine = std::make_unique<SQLEngine>(*mgr, *dm);

        std::cout << "lightdb interactive CLI\n";
        std::cout << "Type help for commands, or enter SQL terminated by a semicolon (;).\n\n";

        // no auto-run on startup; use test command inside REPL to run scripted demo

        std::string buffer;
        std::string line;
        while (true) {
            const std::string prompt = mgr->current_database().empty() ? "lightdb> " : ("lightdb(" + mgr->current_database() + ")> ");
            std::cout << prompt;
            if (!std::getline(std::cin, line)) { std::cout << "\n"; break; }
            const std::string tline = trim(line);
            if (tline.empty()) continue;

            // commands (no leading dot required)
            {
                std::istringstream iss(tline);
                std::string cmd;
                iss >> cmd;
                if (!cmd.empty() && cmd[0] == '.') cmd.erase(0, 1);
                const bool has_semicolon = (tline.find(';') != std::string::npos);
                if (!has_semicolon && (cmd == "help" || cmd == "test" || cmd == "exit" || cmd == "quit" || cmd == "databases" || cmd == "tables" || cmd == "use" || cmd == "root")) {
                    if (cmd == "help") { print_help(); continue; }
                    if (cmd == "test") { int ret = run_visual_test(); continue; }
                    if (cmd == "exit" || cmd == "quit") { break; }
                        if (cmd == "databases") {
                            std::vector<std::string> dbs;
                            if (mgr->list_databases(dbs)) {
                                if (dbs.empty()) {
                                    std::cout << "No databases\n";
                                } else {
                                    std::cout << "Databases:\n";
                                    for (const auto &d : dbs) std::cout << "  " << d << "\n";
                                }
                            } else {
                                std::cout << "Failed to list databases\n";
                            }
                            continue;
                        }
                    if (cmd == "tables") {
                        std::vector<std::string> tabs;
                        if (mgr->list_tables(tabs)) {
                            if (tabs.empty()) {
                                std::cout << "No tables\n";
                            } else {
                                std::cout << "Tables:\n";
                                for (const auto &t : tabs) std::cout << "  " << t << "\n";
                            }
                        } else {
                            std::cout << "No database selected or failed to list tables\n";
                        }
                        continue;
                    }
                    if (cmd == "use") {
                        std::string dbn;
                        iss >> dbn;
                        if (dbn.empty()) { std::cout << "Usage: use <database>\n"; }
                        else { if (mgr->use_database(dbn)) std::cout << "Using database " << dbn << "\n"; else std::cout << "Failed to use database: " << dbn << "\n"; }
                        continue;
                    }
                    if (cmd == "root") {
                        std::string nd;
                        iss >> nd;
                        if (nd.empty()) { std::cout << "Usage: root <dir>\n"; }
                        else {
                            // attempt to switch root by creating a new manager
                            auto new_mgr = std::make_unique<DatabaseManager>(nd);
                            std::vector<std::string> tmp;
                            if (!new_mgr->list_databases(tmp)) {
                                std::cout << "Failed to switch root to: " << nd << "\n";
                            } else {
                                mgr = std::move(new_mgr);
                                dm = std::make_unique<DataManager>(*mgr);
                                engine = std::make_unique<SQLEngine>(*mgr, *dm);
                                std::cout << "Root changed to: " << nd << "\n";
                            }
                        }
                        continue;
                    }
                }
                // not a recognized bare command; fall through to SQL accumulation
            }

            // accumulate SQL until semicolon
            buffer += line;
            buffer += '\n';
            size_t pos = 0;
            while ((pos = buffer.find(';')) != std::string::npos) {
                std::string stmt = buffer.substr(0, pos + 1);
                buffer.erase(0, pos + 1);
                stmt = trim(stmt);
                if (stmt.empty()) continue;

                std::vector<std::vector<std::string>> out_rows;
                std::vector<std::string> out_cols;
                std::string msg;
                bool ok = engine->execute(stmt, out_rows, out_cols, msg);
                if (ok) {
                    std::cout << "OK\n";
                    if (!out_cols.empty() || !out_rows.empty()) print_rows(out_rows, out_cols);
                } else {
                    std::cout << "ERROR: " << msg << "\n";
                }
            }
        }

        std::cout << "Bye.\n";
        return 0;
    } catch (...) {
        return 1;
    }
}

} // namespace rdbms
