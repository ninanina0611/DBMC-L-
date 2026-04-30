#include "../include/CLI.h"
#include <iostream>
#include <string>

int main(int argc, char **argv) {
    // always forward arguments to CLI which will parse and act accordingly
    return rdbms::run_interactive_cli(argc, argv);
}
