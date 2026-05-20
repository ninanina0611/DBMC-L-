#include "DatabaseBrowser.h"
#include "DatabaseManager.h"

#include <QMenu>
#include <QHeaderView>
#include <QPushButton>
#include <QVBoxLayout>
#include <QHBoxLayout>
#include <QPainter>
#include <QApplication>

namespace {

QPixmap makeColorIcon(const QColor &color, int size = 16) {
    QPixmap pix(size, size);
    pix.fill(Qt::transparent);
    QPainter p(&pix);
    p.setRenderHint(QPainter::Antialiasing);
    p.setBrush(color);
    p.setPen(Qt::NoPen);
    p.drawRoundedRect(1, 1, size - 2, size - 2, 3, 3);
    return pix;
}

}

DatabaseBrowser::DatabaseBrowser(rdbms::DatabaseManager &mgr, QWidget *parent)
    : QWidget(parent), mgr_(&mgr)
{
    auto *layout = new QVBoxLayout(this);
    layout->setContentsMargins(0, 0, 0, 0);
    layout->setSpacing(0);

    auto *toolbar = new QHBoxLayout;
    toolbar->setContentsMargins(4, 2, 4, 2);
    newDbBtn_ = new QPushButton("建库");
    newTblBtn_ = new QPushButton("建表");
    refreshBtn_ = new QPushButton("刷新");
    toolbar->addWidget(newDbBtn_);
    toolbar->addWidget(newTblBtn_);
    toolbar->addStretch();
    toolbar->addWidget(refreshBtn_);
    layout->addLayout(toolbar);

    tree_ = new QTreeWidget;
    tree_->setHeaderLabel("数据库浏览器");
    tree_->setColumnCount(1);
    tree_->header()->setStretchLastSection(true);
    tree_->setContextMenuPolicy(Qt::CustomContextMenu);
    layout->addWidget(tree_);

    connect(refreshBtn_, &QPushButton::clicked, this, &DatabaseBrowser::refreshRequested);
    connect(newDbBtn_, &QPushButton::clicked, this, &DatabaseBrowser::newDatabaseRequested);
    connect(newTblBtn_, &QPushButton::clicked, this, &DatabaseBrowser::newTableRequested);
    connect(tree_, &QTreeWidget::itemClicked, this, &DatabaseBrowser::onItemClicked);
    connect(tree_, &QTreeWidget::customContextMenuRequested, this, &DatabaseBrowser::onCustomContextMenu);
    connect(tree_, &QTreeWidget::itemSelectionChanged, this, &DatabaseBrowser::selectionChanged);
}

void DatabaseBrowser::setManager(rdbms::DatabaseManager &mgr) {
    mgr_ = &mgr;
}

QString DatabaseBrowser::selectedDatabase() const {
    auto *item = tree_->currentItem();
    if (!item) return {};
    QString type = item->data(0, Qt::UserRole).toString();
    if (type == "database") return item->data(0, Qt::UserRole + 1).toString();
    if (type == "table" || type == "column") {
        auto *parent = item->parent();
        if (parent && parent->data(0, Qt::UserRole).toString() == "database")
            return parent->data(0, Qt::UserRole + 1).toString();
    }
    return {};
}

QString DatabaseBrowser::selectedTable() const {
    auto *item = tree_->currentItem();
    if (!item) return {};
    QString type = item->data(0, Qt::UserRole).toString();
    if (type == "table") return item->data(0, Qt::UserRole + 1).toString();
    if (type == "column") {
        auto *parent = item->parent();
        if (parent && parent->data(0, Qt::UserRole).toString() == "table")
            return parent->data(0, Qt::UserRole + 1).toString();
    }
    return {};
}

void DatabaseBrowser::refresh() {
    tree_->clear();

    std::vector<std::string> databases;
    if (!mgr_->list_databases(databases)) return;

    std::string savedDb = mgr_->current_database();
    QString currentDb = QString::fromStdString(savedDb);

    for (const auto &dbName : databases) {
        auto *dbItem = new QTreeWidgetItem(tree_, {QString::fromStdString(dbName)});
        dbItem->setData(0, Qt::UserRole, QString("database"));
        dbItem->setData(0, Qt::UserRole + 1, QString::fromStdString(dbName));
        dbItem->setIcon(0, QIcon(makeColorIcon(QColor(52, 120, 198))));

        QFont dbFont = dbItem->font(0);
        dbFont.setBold(true);
        dbFont.setPointSize(dbFont.pointSize() + 1);
        if (QString::fromStdString(dbName) == currentDb) {
            dbFont.setItalic(true);
        }
        dbItem->setFont(0, dbFont);

        mgr_->use_database(dbName);

        std::vector<std::string> tables;
        if (mgr_->list_tables(tables)) {
            for (const auto &tblName : tables) {
                auto *tblItem = new QTreeWidgetItem(dbItem, {QString::fromStdString(tblName)});
                tblItem->setData(0, Qt::UserRole, QString("table"));
                tblItem->setData(0, Qt::UserRole + 1, QString::fromStdString(tblName));
                tblItem->setIcon(0, QIcon(makeColorIcon(QColor(46, 160, 67))));

                rdbms::DatabaseManager::TableSchema schema;
                if (mgr_->get_schema(tblName, schema)) {
                    for (const auto &col : schema.columns) {
                        QString typeStr = QString::fromStdString(col.display_type.empty() ?
                            rdbms::DatabaseManager::type_name(col.type) : col.display_type);
                        if (!col.length.empty()) typeStr += "(" + QString::fromStdString(col.length) + ")";
                        QString colText = QString::fromStdString(col.name) + " (" + typeStr + ")";
                        if (col.is_primary) colText += " [PK]";
                        if (col.not_null) colText += " [NOT NULL]";
                        if (col.is_unique) colText += " [UNIQUE]";
                        auto *colItem = new QTreeWidgetItem(tblItem, {colText});
                        colItem->setData(0, Qt::UserRole, QString("column"));
                        colItem->setData(0, Qt::UserRole + 1, QString::fromStdString(col.name));
                        colItem->setIcon(0, QIcon(makeColorIcon(QColor(140, 140, 140))));

                        QFont colFont = colItem->font(0);
                        colFont.setPointSize(colFont.pointSize() - 1);
                        colItem->setFont(0, colFont);
                    }
                }
            }
        }
    }

    if (!savedDb.empty()) {
        mgr_->use_database(savedDb);
    }

    tree_->expandAll();
}

void DatabaseBrowser::onItemClicked(QTreeWidgetItem *item, int /*column*/) {
    QString type = item->data(0, Qt::UserRole).toString();
    if (type == "table") {
        QString tableName = item->data(0, Qt::UserRole + 1).toString();
        emit tableDoubleClicked(tableName);
    } else if (type == "database") {
        QString dbName = item->data(0, Qt::UserRole + 1).toString();
        mgr_->use_database(dbName.toStdString());
        emit databaseSelected(dbName);
        refresh();
    }
}

void DatabaseBrowser::onCustomContextMenu(const QPoint &pos) {
    QTreeWidgetItem *item = tree_->itemAt(pos);
    if (!item) return;

    QString type = item->data(0, Qt::UserRole).toString();
    QMenu menu(this);

    if (type == "database") {
        QString dbName = item->data(0, Qt::UserRole + 1).toString();
        menu.addAction("删除数据库", this, [this, dbName]() {
            emit dropDatabaseRequested(dbName);
        });
    } else if (type == "table") {
        QString tableName = item->data(0, Qt::UserRole + 1).toString();
        menu.addAction("查看表", this, [this, tableName]() {
            emit tableDoubleClicked(tableName);
        });
        menu.addAction("字段管理", this, [this, tableName]() {
            emit manageFieldsRequested(tableName);
        });
        menu.addAction("删除表", this, [this, tableName]() {
            emit dropTableRequested(tableName);
        });
    }

    if (!menu.actions().isEmpty()) {
        menu.exec(tree_->mapToGlobal(pos));
    }
}
