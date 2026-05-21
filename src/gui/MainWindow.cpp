#include "MainWindow.h"
#include "DatabaseBrowser.h"
#include "SQLEditor.h"
#include "TableDataView.h"
#include "FieldManagerWidget.h"
#include "LogWidget.h"
#include "NewDatabaseDialog.h"
#include "NewTableDialog.h"

#include "DatabaseManager.h"
#include "DataManager.h"
#include "SQLEngine.h"

#include <QMenuBar>
#include <QStatusBar>
#include <QSplitter>
#include <QMessageBox>
#include <QInputDialog>
#include <QApplication>
#include <QVBoxLayout>
#include <QTabWidget>
#include <QFileDialog>
#include <QTextStream>
#include <QCloseEvent>
#include <QDir>
#include <chrono>

MainWindow::MainWindow(const QString &rootDir, QWidget *parent)
    : QMainWindow(parent)
{
    dbMgr_ = std::make_unique<rdbms::DatabaseManager>(rootDir.toStdString());
    dataMgr_ = std::make_unique<rdbms::DataManager>(*dbMgr_);
    engine_ = std::make_unique<rdbms::SQLEngine>(*dbMgr_, *dataMgr_);

    setWindowTitle("LightDB");
    resize(1200, 750);

    setupMenuBar();
    setupCentralWidget();
    setupStatusBar();

    logWidget_->loadLog(logFilePath());
}

MainWindow::~MainWindow() = default;

void MainWindow::closeEvent(QCloseEvent *event) {
    if (logWidget_) {
        logWidget_->saveLog(logFilePath());
    }
    event->accept();
}

QString MainWindow::logFilePath() const {
    QString dir = QApplication::applicationDirPath() + "/data";
    QDir().mkpath(dir);
    return dir + "/lightdb_log.txt";
}

void MainWindow::setupMenuBar() {
    auto *fileMenu = menuBar()->addMenu("文件");
    fileMenu->addAction("导入 SQL 脚本", this, &MainWindow::onOpenScript);
    fileMenu->addAction("切换数据根目录", this, &MainWindow::onChangeRoot);
    fileMenu->addSeparator();
    fileMenu->addAction("退出", this, &QWidget::close);

    auto *dbMenu = menuBar()->addMenu("数据库");
    dbMenu->addAction("新建数据库", this, &MainWindow::onNewDatabase);
    dropDbAction_ = dbMenu->addAction("删除数据库", this, [this]() {
        onDropDatabase(browser_->selectedDatabase());
    });
    dropDbAction_->setEnabled(false);

    auto *tableMenu = menuBar()->addMenu("表");
    tableMenu->addAction("新建表", this, &MainWindow::onNewTable);
    dropTableAction_ = tableMenu->addAction("删除表", this, [this]() {
        onDropTable(browser_->selectedTable());
    });
    dropTableAction_->setEnabled(false);
    tableMenu->addSeparator();
    manageFieldsAction_ = tableMenu->addAction("字段管理", this, [this]() {
        onManageFields(browser_->selectedTable());
    });
    manageFieldsAction_->setEnabled(false);

    auto *helpMenu = menuBar()->addMenu("帮助");
    helpMenu->addAction("关于", this, &MainWindow::onAbout);
}

void MainWindow::setupStatusBar() {
    statusBar()->showMessage("就绪");
}

void MainWindow::setupCentralWidget() {
    browser_ = new DatabaseBrowser(*dbMgr_, this);

    sqlEditor_ = new SQLEditor(this);

    dataView_ = new TableDataView(this);
    dataView_->setEngine(*dbMgr_, *dataMgr_, *engine_);

    // Use a vertical splitter so the SQL editor and data view can be resized vertically.
    auto *editorSplitter = new QSplitter(Qt::Vertical);
    editorSplitter->addWidget(sqlEditor_);
    editorSplitter->addWidget(dataView_);
    editorSplitter->setStretchFactor(0, 0);
    editorSplitter->setStretchFactor(1, 1);
    editorSplitter->setCollapsible(0, false);
    editorSplitter->setCollapsible(1, false);

    auto *dataTabLayout = new QVBoxLayout;
    dataTabLayout->setContentsMargins(0, 0, 0, 0);
    dataTabLayout->setSpacing(2);
    dataTabLayout->addWidget(editorSplitter, 1);

    auto *dataTab = new QWidget;
    dataTab->setLayout(dataTabLayout);

    fieldMgr_ = new FieldManagerWidget(*dbMgr_);

    rightTab_ = new QTabWidget;
    rightTab_->addTab(dataTab, "数据");
    rightTab_->addTab(fieldMgr_, "字段管理");

    auto *mainSplitter = new QSplitter(Qt::Horizontal);
    mainSplitter->addWidget(browser_);
    mainSplitter->addWidget(rightTab_);
    mainSplitter->setStretchFactor(0, 1);
    mainSplitter->setStretchFactor(1, 4);

    logWidget_ = new LogWidget(this);

    auto *topLevelSplitter = new QSplitter(Qt::Vertical);
    topLevelSplitter->addWidget(mainSplitter);
    topLevelSplitter->addWidget(logWidget_);
    topLevelSplitter->setStretchFactor(0, 1);
    topLevelSplitter->setStretchFactor(1, 0);
    topLevelSplitter->setCollapsible(0, false);
    topLevelSplitter->setCollapsible(1, false);

    setCentralWidget(topLevelSplitter);

    connect(sqlEditor_, &SQLEditor::executeRequested, this, &MainWindow::onExecuteSQL);
    connect(logWidget_, &LogWidget::sqlReexecuteRequested, this, [this](const QString &sql) {
        sqlEditor_->setSqlText(sql);
        onExecuteSQL(sql);
    });
    connect(browser_, &DatabaseBrowser::tableDoubleClicked, this, &MainWindow::onTableSelected);
    connect(browser_, &DatabaseBrowser::databaseSelected, this, &MainWindow::onDatabaseSelected);
    connect(browser_, &DatabaseBrowser::manageFieldsRequested, this, &MainWindow::onManageFields);
    connect(browser_, &DatabaseBrowser::refreshRequested, this, &MainWindow::onDatabaseBrowserRefresh);
    connect(browser_, &DatabaseBrowser::newDatabaseRequested, this, &MainWindow::onNewDatabase);
    connect(browser_, &DatabaseBrowser::newTableRequested, this, &MainWindow::onNewTable);
    connect(browser_, &DatabaseBrowser::dropDatabaseRequested, this, &MainWindow::onDropDatabase);
    connect(browser_, &DatabaseBrowser::dropTableRequested, this, &MainWindow::onDropTable);
    connect(browser_, &DatabaseBrowser::selectionChanged, this, &MainWindow::updateMenuState);
    connect(dataView_, &TableDataView::tableDataChanged, this, &MainWindow::onDatabaseBrowserRefresh);
    connect(dataView_, &TableDataView::messageRequested, this, [this](const QString &msg, bool isError) {
        statusBar()->showMessage(isError ? "ERROR: " + msg : msg);
        if (isError) {
            logWidget_->onError(msg);
        } else {
            logWidget_->onInfo(msg);
        }
    });
    connect(fieldMgr_, &FieldManagerWidget::fieldsChanged, this, [this]() {
        browser_->refresh();
        statusBar()->showMessage("字段变更已应用");
        logWidget_->onInfo("字段变更已应用: " + fieldMgr_->currentTable());
        if (dataView_->currentTable() == fieldMgr_->currentTable()) {
            dataView_->loadTable(fieldMgr_->currentTable());
        }
    });

    browser_->refresh();
}

void MainWindow::onTableSelected(const QString &tableName) {
    dataView_->loadTable(tableName);
    fieldMgr_->setTable(tableName);
    sqlEditor_->setSqlText(QString("SELECT * FROM %1;").arg(tableName));
    rightTab_->setCurrentIndex(0);
    statusBar()->showMessage("已选择表: " + tableName);
    logWidget_->onInfo("选择表: " + tableName);
}

void MainWindow::onDatabaseSelected(const QString &dbName) {
    statusBar()->showMessage("已选择数据库: " + dbName);
    setWindowTitle(QString("LightDB - %1").arg(dbName));
    logWidget_->onInfo("切换数据库: " + dbName);
}

void MainWindow::onExecuteSQL(const QString &sql) {
    if (sql.trimmed().isEmpty()) return;

    auto isSelectVerb = [](const QString &stmt) -> bool {
        QString v = stmt.trimmed().section(' ', 0, 0).toLower();
        return v == "select";
    };

    auto isModifyVerb = [](const QString &stmt) -> bool {
        QString v = stmt.trimmed().section(' ', 0, 0).toLower();
        const QStringList modifyVerbs = {"insert", "update", "delete", "alter",
            "create", "drop", "truncate", "replace", "rename", "use",
            "start", "begin", "commit", "rollback"};
        return modifyVerbs.contains(v);
    };

    // 拆分语句（支持字符串字面量与行注释）
    QString content = sql;
    QString current;
    bool inString = false;
    QChar stringChar;
    QStringList statements;
    for (int i = 0; i < content.size(); ++i) {
        QChar c = content[i];
        if (inString) {
            current += c;
            if (c == stringChar && (i + 1 >= content.size() || content[i + 1] != stringChar)) {
                inString = false;
            }
            continue;
        }
        if (c == '\'' || c == '"') {
            inString = true;
            stringChar = c;
            current += c;
            continue;
        }
        if (c == '-' && i + 1 < content.size() && content[i + 1] == '-') {
            while (i < content.size() && content[i] != '\n') ++i;
            continue;
        }
        if (c == ';') {
            QString stmt = current.trimmed();
            if (!stmt.isEmpty()) statements.append(stmt);
            current.clear();
            continue;
        }
        current += c;
    }
    QString last = current.trimmed();
    if (!last.isEmpty()) statements.append(last);

    // 单条语句
    if (statements.size() == 1) {
        auto start = std::chrono::high_resolution_clock::now();
        std::vector<std::vector<std::string>> rows;
        std::vector<std::string> cols;
        std::string msg;
        bool ok = engine_->execute(statements[0].toStdString(), rows, cols, msg);
        auto end = std::chrono::high_resolution_clock::now();
        double elapsed = std::chrono::duration<double, std::milli>(end - start).count();

        if (ok) {
            if (!cols.empty() || !rows.empty()) {
                dataView_->showQueryResult(rows, cols);
                statusBar()->showMessage(
                    QString("OK | %1 行 | 耗时 %2 ms").arg(rows.size()).arg(elapsed, 0, 'f', 1));
            } else {
                statusBar()->showMessage(
                    QString("OK | 耗时 %1 ms").arg(elapsed, 0, 'f', 1) +
                    (msg.empty() ? QString() : QString(" | ") + QString::fromStdString(msg)));
            }
            if (isModifyVerb(statements[0]) && !dataView_->currentTable().isEmpty()) {
                dataView_->loadTable(dataView_->currentTable());
            }

            logWidget_->onSQL(statements[0], true, elapsed);

            browser_->refresh();
            QString currentDb = QString::fromStdString(dbMgr_->current_database());
            if (!currentDb.isEmpty()) {
                setWindowTitle(QString("LightDB - %1").arg(currentDb));
            } else {
                setWindowTitle("LightDB");
            }
        } else {
            statusBar()->showMessage("ERROR: " + QString::fromStdString(msg));
            logWidget_->onSQL(statements[0], false, elapsed);
            logWidget_->onError(QString::fromStdString(msg));
        }

        return;
    }

    // 批量执行多条语句
    int okCount = 0, errCount = 0;
    QString lastError;
    std::vector<std::vector<std::string>> lastRows;
    std::vector<std::string> lastCols;
    bool lastStmtIsSelect = false;
    bool anyModify = false;
    auto totalStart = std::chrono::high_resolution_clock::now();

    for (int si = 0; si < statements.size(); ++si) {
        const QString &stmt = statements[si];
        auto stmtStart = std::chrono::high_resolution_clock::now();
        std::vector<std::vector<std::string>> rows;
        std::vector<std::string> cols;
        std::string msg;
        bool ok = engine_->execute(stmt.toStdString(), rows, cols, msg);
        auto stmtEnd = std::chrono::high_resolution_clock::now();
        double stmtElapsed = std::chrono::duration<double, std::milli>(stmtEnd - stmtStart).count();

        if (ok) {
            ++okCount;
            lastStmtIsSelect = isSelectVerb(stmt);
            if (isModifyVerb(stmt)) anyModify = true;
            if (!cols.empty() || !rows.empty()) {
                lastRows = rows;
                lastCols = cols;
            }
            logWidget_->onSQL(stmt, true, stmtElapsed);
            browser_->refresh();
            QString currentDb = QString::fromStdString(dbMgr_->current_database());
            if (!currentDb.isEmpty()) setWindowTitle(QString("LightDB - %1").arg(currentDb));
            else setWindowTitle("LightDB");
        } else {
            ++errCount;
            lastError = QString::fromStdString(msg);
            logWidget_->onSQL(stmt, false, stmtElapsed);
            logWidget_->onError(QString("[%1/%2] %3").arg(si + 1).arg(statements.size()).arg(lastError));
        }
    }

    auto totalEnd = std::chrono::high_resolution_clock::now();
    double totalElapsed = std::chrono::duration<double, std::milli>(totalEnd - totalStart).count();

    // 如果最后一条是非SELECT语句，刷新当前表视图
    if (!lastStmtIsSelect && anyModify && !dataView_->currentTable().isEmpty()) {
        dataView_->loadTable(dataView_->currentTable());
    }

    if (!lastCols.empty() || !lastRows.empty()) {
        dataView_->showQueryResult(lastRows, lastCols);
        statusBar()->showMessage(QString("OK | %1 行 | 耗时 %2 ms").arg(lastRows.size()).arg(totalElapsed, 0, 'f', 1));
    } else {
        if (errCount == 0) {
            statusBar()->showMessage(
                QString("执行完成 | %1 条语句成功 | 耗时 %2 ms").arg(okCount).arg(totalElapsed, 0, 'f', 1));
        } else {
            QMessageBox::warning(this, "脚本执行出错",
                QString("执行完成: %1 成功, %2 失败\n\n最后错误: %3").arg(okCount).arg(errCount).arg(lastError));
            statusBar()->showMessage(
                QString("执行完成 | %1 成功 | %2 失败 | 耗时 %3 ms").arg(okCount).arg(errCount).arg(totalElapsed, 0, 'f', 1));
        }
    }
}

void MainWindow::onDatabaseBrowserRefresh() {
    browser_->refresh();
    statusBar()->showMessage("已刷新");
    logWidget_->onInfo("浏览器已刷新");
}

void MainWindow::onNewDatabase() {
    NewDatabaseDialog dlg(this);
    if (dlg.exec() == QDialog::Accepted) {
        QString name = dlg.databaseName();
        if (name.isEmpty()) return;
        if (dbMgr_->create_database(name.toStdString())) {
            statusBar()->showMessage("数据库 " + name + " 创建成功");
            logWidget_->onInfo("创建数据库: " + name);
            browser_->refresh();
        } else {
            statusBar()->showMessage("创建数据库失败: " + name);
            logWidget_->onError("创建数据库失败: " + name);
        }
    }
}

void MainWindow::onDropDatabase(const QString &name) {
    QString dbName = name;
    if (dbName.isEmpty()) {
        QStringList dbs;
        std::vector<std::string> dbList;
        if (dbMgr_->list_databases(dbList)) {
            for (const auto &d : dbList) dbs << QString::fromStdString(d);
        }
        if (dbs.isEmpty()) {
            QMessageBox::information(this, "提示", "没有可删除的数据库");
            return;
        }
        bool ok = false;
        dbName = QInputDialog::getItem(this, "删除数据库", "选择数据库:", dbs, 0, false, &ok);
        if (!ok || dbName.isEmpty()) return;
    }
    if (QMessageBox::question(this, "确认", "确定要删除数据库 " + dbName + " 吗？") != QMessageBox::Yes) return;
    if (dbMgr_->drop_database(dbName.toStdString())) {
        statusBar()->showMessage("数据库 " + dbName + " 已删除");
        logWidget_->onInfo("删除数据库: " + dbName);
        dataView_->clear();
        browser_->refresh();
    } else {
        statusBar()->showMessage("删除数据库失败: " + dbName);
        logWidget_->onError("删除数据库失败: " + dbName);
    }
}

void MainWindow::onNewTable() {
    if (dbMgr_->current_database().empty()) {
        QMessageBox::warning(this, "提示", "请先选择一个数据库");
        return;
    }
    NewTableDialog dlg(this);
    if (dlg.exec() == QDialog::Accepted) {
        auto schema = dlg.tableSchema();
        if (schema.table_name.empty()) return;
        if (dbMgr_->create_table(schema)) {
            statusBar()->showMessage("表 " + QString::fromStdString(schema.table_name) + " 创建成功");
            logWidget_->onInfo("创建表: " + QString::fromStdString(schema.table_name));
            browser_->refresh();
        } else {
            statusBar()->showMessage("创建表失败");
            logWidget_->onError("创建表失败: " + QString::fromStdString(schema.table_name));
        }
    }
}

void MainWindow::onDropTable(const QString &name) {
    if (dbMgr_->current_database().empty()) {
        QMessageBox::warning(this, "提示", "请先选择一个数据库");
        return;
    }
    QString tblName = name;
    if (tblName.isEmpty()) {
        QStringList tables;
        std::vector<std::string> tableList;
        if (dbMgr_->list_tables(tableList)) {
            for (const auto &t : tableList) tables << QString::fromStdString(t);
        }
        if (tables.isEmpty()) {
            QMessageBox::information(this, "提示", "没有可删除的表");
            return;
        }
        bool ok = false;
        tblName = QInputDialog::getItem(this, "删除表", "选择表:", tables, 0, false, &ok);
        if (!ok || tblName.isEmpty()) return;
    }
    if (QMessageBox::question(this, "确认", "确定要删除表 " + tblName + " 吗？") != QMessageBox::Yes) return;
    if (dbMgr_->drop_table(tblName.toStdString())) {
        statusBar()->showMessage("表 " + tblName + " 已删除");
        logWidget_->onInfo("删除表: " + tblName);
        dataView_->clear();
        browser_->refresh();
    } else {
        statusBar()->showMessage("删除表失败: " + tblName);
        logWidget_->onError("删除表失败: " + tblName);
    }
}

void MainWindow::onManageFields(const QString &tableName) {
    if (dbMgr_->current_database().empty()) {
        QMessageBox::warning(this, "提示", "请先选择一个数据库");
        return;
    }

    QString name = tableName;
    if (name.isEmpty()) {
        QStringList tables;
        std::vector<std::string> tableList;
        if (dbMgr_->list_tables(tableList)) {
            for (const auto &t : tableList) tables << QString::fromStdString(t);
        }
        if (tables.isEmpty()) {
            QMessageBox::information(this, "提示", "没有可管理的表");
            return;
        }
        bool ok = false;
        name = QInputDialog::getItem(this, "字段管理", "选择表:", tables, 0, false, &ok);
        if (!ok || name.isEmpty()) return;
    }

    fieldMgr_->setTable(name);
    rightTab_->setCurrentIndex(1);
    statusBar()->showMessage("字段管理: " + name);
    logWidget_->onInfo("字段管理: " + name);
}

void MainWindow::onOpenScript() {
    if (sqlEditor_) sqlEditor_->onImportScript();
}

void MainWindow::onChangeRoot() {
    bool ok = false;
    QString dir = QInputDialog::getText(this, "切换数据根目录", "根目录路径:", QLineEdit::Normal, "data", &ok);
    if (!ok || dir.isEmpty()) return;

    auto newMgr = std::make_unique<rdbms::DatabaseManager>(dir.toStdString());
    std::vector<std::string> tmp;
    if (!newMgr->list_databases(tmp)) {
        QMessageBox::warning(this, "错误", "无法切换到目录: " + dir);
        return;
    }
    dbMgr_ = std::move(newMgr);
    dataMgr_ = std::make_unique<rdbms::DataManager>(*dbMgr_);
    engine_ = std::make_unique<rdbms::SQLEngine>(*dbMgr_, *dataMgr_);
    browser_->setManager(*dbMgr_);
    browser_->refresh();
    dataView_->setEngine(*dbMgr_, *dataMgr_, *engine_);
    dataView_->clear();
    statusBar()->showMessage("根目录已切换到: " + dir);
    logWidget_->onInfo("切换数据根目录: " + dir);
    setWindowTitle("LightDB");
}

void MainWindow::updateMenuState() {
    if (dropDbAction_) dropDbAction_->setEnabled(!browser_->selectedDatabase().isEmpty());
    bool hasTable = !browser_->selectedTable().isEmpty();
    if (dropTableAction_) dropTableAction_->setEnabled(hasTable);
    if (manageFieldsAction_) manageFieldsAction_->setEnabled(hasTable);
}

void MainWindow::onAbout() {
    QMessageBox::about(this, "关于 LightDB",
        "LightDB v1.0\n\n"
        "轻量级关系型数据库管理系统\n"
        "基于 C++17 / Qt6 开发\n\n"
        "功能：\n"
        "· 数据库与表的创建、删除、管理\n"
        "· 可视化字段管理（类型、约束、长度、小数位）\n"
        "· SQL 编辑器（CREATE / INSERT / SELECT / UPDATE / DELETE / ALTER）\n"
        "· 数据的增删改查与批量提交\n"
        "· 二进制序列化存储，向后兼容");
}
