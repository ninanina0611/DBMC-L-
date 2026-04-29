#include "../include/DemoTest.h"
#include <iostream>

int main(int argc, char **argv) {
    std::string mode = "verbose"; // verbose | compact
    for (int i = 1; i < argc; ++i) {
        std::string a(argv[i]);
        if (a == "--help" || a == "-h") {
            std::cout << "Usage: lightdb_demo [--mode <verbose|compact>] [--help]\n";
            std::cout << "  --mode <mode>   test mode: verbose (default) or compact\n";
            return 0;
        }
        if (a == "--mode" || a == "-m") {
            if (i + 1 < argc) { mode = argv[++i]; }
            else { std::cerr << "--mode requires a value\n"; return 1; }
            continue;
        }
    }
    return run_file_manager_visual_test(mode);
}
