#include <algorithm>
#include <cctype>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <string>
#include <vector>
#include "../include/DemoTest.h"
#include "../include/FileManager.h"

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
    print_ok(ok);
    print_path_state(dir);
    if (!ok) {
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

    print_step(7, "Cleanup");
    const bool removed_file = FileManager::remove_file(filepath);
    const bool removed_dir = FileManager::remove_directory(dir);
    std::cout << "  remove_file: " << (removed_file ? "OK" : "FAIL") << '\n';
    std::cout << "  remove_directory: " << (removed_dir ? "OK" : "FAIL") << '\n';
    std::cout << "  file exists after cleanup: " << (std::filesystem::exists(filepath) ? "yes" : "no") << '\n';
    std::cout << "  dir exists after cleanup : " << (std::filesystem::exists(dir) ? "yes" : "no") << '\n';

    std::cout << "\n=== test finished successfully ===\n";
    return 0;
}