#pragma once

#include <QMainWindow>
#include <memory>

class QCloseEvent;

namespace rdbms {
class DatabaseManager;
class DataManager;
class SQLEngine;
}

class DatabaseBrowser;
class SQLEditor;
class TableDataView;
class FieldManagerWidget;
class LogWidget;
class QTabWidget;

class MainWindow : public QMainWindow {
    Q_OBJECT

public:
    explicit MainWindow(const QString &rootDir = "data", QWidget *parent = nullptr);
    ~MainWindow() override;

protected:
    void closeEvent(QCloseEvent *event) override;

private slots:
    void onExecuteSQL(const QString &sql);
    void onDatabaseBrowserRefresh();
    void onNewDatabase();
    void onDropDatabase(const QString &name = QString());
    void onNewTable();
    void onDropTable(const QString &name = QString());
    void onManageFields(const QString &tableName = QString());
    void onChangeRoot();
    void onOpenScript();
    void onAbout();
    void onTableSelected(const QString &tableName);
    void onDatabaseSelected(const QString &dbName);

private:
    void setupMenuBar();
    void setupStatusBar();
    void setupCentralWidget();
    void updateMenuState();
    QString logFilePath() const;

    std::unique_ptr<rdbms::DatabaseManager> dbMgr_;
    std::unique_ptr<rdbms::DataManager> dataMgr_;
    std::unique_ptr<rdbms::SQLEngine> engine_;

    DatabaseBrowser *browser_ = nullptr;
    SQLEditor *sqlEditor_ = nullptr;
    TableDataView *dataView_ = nullptr;
    FieldManagerWidget *fieldMgr_ = nullptr;
    LogWidget *logWidget_ = nullptr;
    QTabWidget *rightTab_ = nullptr;

    QAction *dropDbAction_ = nullptr;
    QAction *dropTableAction_ = nullptr;
    QAction *manageFieldsAction_ = nullptr;
};
