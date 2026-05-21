cmake_minimum_required(VERSION 3.16)
project(lightdb LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include_directories(
    ${CMAKE_SOURCE_DIR}/include/core
    ${CMAKE_SOURCE_DIR}/include/sql
    ${CMAKE_SOURCE_DIR}/include/data
    ${CMAKE_SOURCE_DIR}/include/cli
)

add_library(lightdb_core STATIC
    src/core/Test.cpp
    src/core/FileManager.cpp
    src/core/DatabaseManager.cpp
    src/core/ConstraintValidator.cpp
    src/sql/SQLLexer.cpp
    src/sql/SQLParser.cpp
    src/sql/SQLEngine.cpp
    src/data/DataManager.cpp
)

add_executable(lightdb src/cli/main.cpp src/cli/CLI.cpp)
target_link_libraries(lightdb PRIVATE lightdb_core)

option(BUILD_GUI "Build the Qt GUI application" ON)

if(BUILD_GUI)
    if(NOT DEFINED CMAKE_PREFIX_PATH AND NOT DEFINED ENV{CMAKE_PREFIX_PATH})
        if(MSVC)
            set(_qt_hint "C:/Qt/6.9.2/msvc2022_64")
        else()
            set(_qt_hint "C:/Qt/6.9.2/mingw_64")
        endif()
        if(EXISTS "${_qt_hint}")
            list(APPEND CMAKE_PREFIX_PATH "${_qt_hint}")
        endif()
        unset(_qt_hint)
    endif()

    find_package(Qt6 COMPONENTS Core Gui Widgets QUIET)

    if(Qt6_FOUND)
        set(CMAKE_AUTOMOC ON)
        set(CMAKE_AUTORCC ON)
        set(CMAKE_AUTOUIC ON)

        add_executable(lightdb_gui
            src/gui/gui_main.cpp
            src/gui/MainWindow.cpp
            src/gui/DatabaseBrowser.cpp
            src/gui/SQLEditor.cpp
            src/gui/TableDataView.cpp
            src/gui/NewDatabaseDialog.cpp
            src/gui/NewTableDialog.cpp
            src/gui/FieldManagerWidget.cpp
            src/gui/LogWidget.cpp
            include/gui/MainWindow.h
            include/gui/DatabaseBrowser.h
            include/gui/SQLEditor.h
            include/gui/TableDataView.h
            include/gui/NewDatabaseDialog.h
            include/gui/NewTableDialog.h
            include/gui/FieldManagerWidget.h
            include/gui/LogWidget.h
        )
        target_include_directories(lightdb_gui PRIVATE
            ${CMAKE_SOURCE_DIR}/include/core
            ${CMAKE_SOURCE_DIR}/include/sql
            ${CMAKE_SOURCE_DIR}/include/data
            ${CMAKE_SOURCE_DIR}/include/cli
            ${CMAKE_SOURCE_DIR}/include/gui
        )
        target_link_libraries(lightdb_gui PRIVATE lightdb_core Qt6::Core Qt6::Gui Qt6::Widgets)
    else()
        message(WARNING "Qt6 not found or incompatible (Qt6 requires 64-bit). GUI will not be built. "
                        "To fix: use -A x64 instead of -A win32, or set CMAKE_PREFIX_PATH to your Qt6 installation.")
    endif()
endif()
