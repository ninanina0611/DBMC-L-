#include <iostream>
#include <vector>
#include <string>
#include "../include/FileManager.h"

int main() {
    using namespace rdbms;
    std::string dir = "data/db1";
    std::string filepath = dir + "/table1.bin";

    if (!FileManager::create_directory(dir)) {
        std::cerr << "create_directory failed\n";
        return 1;
    }

    if (!FileManager::create_file(filepath)) {
        std::cerr << "create_file failed\n";
        return 1;
    }

    // 写入一个页面
    const size_t page_size = 4096;
    std::vector<char> page(page_size, 0);
    std::string text = "Hello RDBMS binary page";
    std::copy(text.begin(), text.end(), page.begin());

    if (!FileManager::write_page(filepath, 0, page_size, page)) {
        std::cerr << "write_page failed\n";
        return 1;
    }

    std::vector<char> out;
    if (!FileManager::read_page(filepath, 0, page_size, out)) {
        std::cerr << "read_page failed\n";
        return 1;
    }

    std::string s(out.begin(), out.end());
    std::cout << "Read content (prefix): " << s.substr(0, text.size()) << "\n";

    // 清理
    FileManager::remove_file(filepath);
    // 删除目录（在某些平台若非空可能会失败）
    FileManager::remove_directory(dir);

    return 0;
}
