#pragma once
#include <string>

namespace rdbms {

// Start an interactive command-line SQL REPL.
// All functionality is exposed via REPL commands (e.g., test, root).
// Returns 0 on clean exit, non-zero on error.
int run_interactive_cli(int argc, char **argv) noexcept;

} // namespace rdbms
