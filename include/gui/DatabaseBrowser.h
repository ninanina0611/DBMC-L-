#pragma once

#include <QWidget>
#include <QTreeWidget>

class QPushButton;
class QVBoxLayout;

namespace rdbms {
class DatabaseManager;
}

class DatabaseBrowser : public QWidget {
    Q_OBJECT

public:
    explicit DatabaseBrowser(rdbms::DatabaseManager &mgr, QWidget *parent = nullptr);

    void setManager(rdbms::DatabaseManager &mgr);
    void refresh();

    QString selectedDatabase() const;
    QString selectedTable() const;

signals:
    void tableDoubleClicked(const QString &tableName);
    void databaseSelected(const QString &dbName);
    void refreshRequested();
    void newDatabaseRequested();
    void newTableRequested();
    void manageFieldsRequested(const QString &tableName);
    void dropDatabaseRequested(const QString &dbName);
    void dropTableRequested(const QString &tableName);
    void selectionChanged();

private slots:
    void onItemClicked(QTreeWidgetItem *item, int column);
    void onCustomContextMenu(const QPoint &pos);

private:
    rdbms::DatabaseManager *mgr_;
    QTreeWidget *tree_ = nullptr;
    QPushButton *refreshBtn_ = nullptr;
    QPushButton *newDbBtn_ = nullptr;
    QPushButton *newTblBtn_ = nullptr;
};
