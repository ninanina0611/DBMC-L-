#pragma once

#include <QWidget>
#include <vector>
#include <string>
#include <QSet>
#include <QMap>

class QTableWidget;
class QPushButton;
class QLabel;

namespace rdbms {
class DatabaseManager;
class DataManager;
class SQLEngine;
}

class TableDataView : public QWidget {
    Q_OBJECT

public:
    explicit TableDataView(QWidget *parent = nullptr);

    void setEngine(rdbms::DatabaseManager &dbMgr,
                   rdbms::DataManager &dataMgr,
                   rdbms::SQLEngine &engine);

    void loadTable(const QString &tableName);
    void showQueryResult(const std::vector<std::vector<std::string>> &rows,
                         const std::vector<std::string> &columns);
    void clear();

    QString currentTable() const;

signals:
    void tableDataChanged();
    void messageRequested(const QString &msg, bool isError);

private slots:
    void onCellChanged(int row, int col);
    void onAddRow();
    void onDeleteRow();
    void onApply();
    void onDiscard();

private:
    bool hasPendingChanges() const;

    rdbms::DatabaseManager *dbMgr_ = nullptr;
    rdbms::DataManager *dataMgr_ = nullptr;
    rdbms::SQLEngine *engine_ = nullptr;

    QTableWidget *table_ = nullptr;
    QPushButton *addRowBtn_ = nullptr;
    QPushButton *deleteRowBtn_ = nullptr;
    QPushButton *applyBtn_ = nullptr;
    QPushButton *discardBtn_ = nullptr;
    QLabel *infoLabel_ = nullptr;

    QString currentTable_;
    std::vector<std::string> columnNames_;
    QSet<int> newRows_;
    QMap<int, QString> dirtyRows_;
    bool loading_ = false;
};
