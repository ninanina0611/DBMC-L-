#include <QApplication>
#include "MainWindow.h"
#include <fstream>

static void gui_log(const std::string &msg) {
    try {
        std::ofstream f("gui_debug.log", std::ios::app);
        if (f) f << msg << std::endl;
    } catch (...) {}
}

int main(int argc, char **argv) {
    // ensure an early startup log even if QApplication fails to initialize
    try {
        std::ofstream f("gui_startup.log", std::ios::app);
        if (f) f << "main: starting process" << std::endl;
    } catch (...) {}

    QApplication app(argc, argv);
    app.setApplicationName("LightDB");
    app.setApplicationVersion("1.0");

    gui_log("main: app initialized");
    gui_log("main: before creating MainWindow");
    MainWindow window;
    gui_log("main: after creating MainWindow");
    window.show();
    gui_log("main: window shown");

    return app.exec();
}
