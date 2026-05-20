#include "TableDataView.h"
#include "DatabaseManager.h"
#include "DataManager.h"
#include "SQLEngine.h"

#include <QTableWidget>
#include <QPushButton>
#include <QLabel>
#include <QVBoxLayout>
#include <QHBoxLayout>
#include <QHeaderView>
#include <QMessageBox>

TableDataView::TableDataView(QWidget *parent)
    : QWidget(parent)
{
    auto *layout = new QVBoxLayout(this);
    layout->setContentsMargins(0, 0, 0, 0);
    layout->setSpacing(2);

    table_ = new QTableWidget;
    table_->setEditTriggers(QAbstractItemView::DoubleClicked | QAbstractItemView::EditKeyPressed);
    table_->setSelectionBehavior(QAbstractItemView::SelectItems);
    table_->setAlternatingRowColors(true);
    table_->horizontalHeader()->setSectionResizeMode(QHeaderView::Stretch);
    table_->verticalHeader()->setDefaultSectionSize(24);
    layout->addWidget(table_);

    auto *toolbar = new QHBoxLayout;
    addRowBtn_ = new QPushButton("追加行");
    deleteRowBtn_ = new QPushButton("删除行");
    applyBtn_ = new QPushButton("应用");
    discardBtn_ = new QPushButton("放弃");
    infoLabel_ = new QLabel;

    toolbar->addWidget(addRowBtn_);
    toolbar->addWidget(deleteRowBtn_);
    toolbar->addWidget(applyBtn_);
    toolbar->addWidget(discardBtn_);
    toolbar->addStretch();
    toolbar->addWidget(infoLabel_);

    layout->addLayout(toolbar);

    connect(addRowBtn_, &QPushButton::clicked, this, &TableDataView::onAddRow);
    connect(deleteRowBtn_, &QPushButton::clicked, this, &TableDataView::onDeleteRow);
    connect(applyBtn_, &QPushButton::clicked, this, &TableDataView::onApply);
    connect(discardBtn_, &QPushButton::clicked, this, &TableDataView::onDiscard);
    connect(table_, &QTableWidget::cellChanged, this, &TableDataView::onCellChanged);
}

void TableDataView::setEngine(rdbms::DatabaseManager &dbMgr,
                               rdbms::DataManager &dataMgr,
                               rdbms::SQLEngine &engine) {
    dbMgr_ = &dbMgr;
    dataMgr_ = &dataMgr;
    engine_ = &engine;
}

QString TableDataView::currentTable() const {
    return currentTable_;
}

bool TableDataView::hasPendingChanges() const {
    return !newRows_.isEmpty() || !dirtyRows_.isEmpty();
}

void TableDataView::loadTable(const QString &tableName) {
    if (!dbMgr_ || !dataMgr_) return;

    currentTable_ = tableName;
    columnNames_.clear();
    newRows_.clear();
    dirtyRows_.clear();

    rdbms::DatabaseManager::TableSchema schema;
    if (dbMgr_->get_schema(tableName.toStdString(), schema)) {
        for (const auto &col : schema.columns) {
            columnNames_.push_back(col.name);
        }
    }

    std::vector<std::vector<std::string>> rows;
    std::vector<std::string> cols;
    std::string msg;

    bool ok = dataMgr_->select_rows(tableName.toStdString(),
                                     {}, "", "",
                                     rows, cols, &msg);

    if (ok) {
        showQueryResult(rows, cols);
        infoLabel_->setText(QString("%1 | %2 行").arg(tableName).arg(rows.size()));
    } else {
        emit messageRequested("加载表失败: " + QString::fromStdString(msg), true);
    }
}

void TableDataView::showQueryResult(const std::vector<std::vector<std::string>> &rows,
                                     const std::vector<std::string> &columns) {
    loading_ = true;

    int ncols = static_cast<int>(columns.size());
    int nrows = static_cast<int>(rows.size());

    table_->setColumnCount(ncols);
    table_->setRowCount(nrows);

    QStringList headers;
    for (const auto &col : columns) {
        headers << QString::fromStdString(col);
    }
    table_->setHorizontalHeaderLabels(headers);

    for (int r = 0; r < nrows; ++r) {
        for (int c = 0; c < ncols && c < static_cast<int>(rows[r].size()); ++c) {
            table_->setItem(r, c, new QTableWidgetItem(QString::fromStdString(rows[r][c])));
        }
    }

    loading_ = false;
}

void TableDataView::clear() {
    currentTable_.clear();
    columnNames_.clear();
    newRows_.clear();
    dirtyRows_.clear();
    table_->clear();
    table_->setRowCount(0);
    table_->setColumnCount(0);
    infoLabel_->clear();
}

void TableDataView::onCellChanged(int row, int col) {
    if (loading_ || currentTable_.isEmpty() || !dataMgr_) return;
    if (col < 0 || col >= static_cast<int>(columnNames_.size())) return;

    if (newRows_.contains(row)) return;

    if (!dirtyRows_.contains(row)) {
        auto *pkItem = table_->item(row, 0);
        if (pkItem) {
            dirtyRows_.insert(row, pkItem->text());
        }
    }
}

void TableDataView::onAddRow() {
    if (currentTable_.isEmpty()) return;

    int row = table_->rowCount();
    loading_ = true;
    table_->insertRow(row);

    for (int c = 0; c < table_->columnCount(); ++c) {
        table_->setItem(row, c, new QTableWidgetItem(""));
    }
    loading_ = false;
    newRows_.insert(row);
    table_->setCurrentCell(row, 0);
}

void TableDataView::onDeleteRow() {
    if (currentTable_.isEmpty() || !dataMgr_ || columnNames_.empty()) return;

    int row = table_->currentRow();
    if (row < 0) {
        QMessageBox::information(this, "提示", "请先选择要删除的行");
        return;
    }

    if (newRows_.contains(row)) {
        newRows_.remove(row);
        table_->removeRow(row);
        return;
    }

    auto *pkItem = table_->item(row, 0);
    if (!pkItem) return;
    QString pkValue = pkItem->text();

    QString pkColName = QString::fromStdString(columnNames_[0]);

    if (QMessageBox::question(this, "确认删除",
            QString("确定要删除 %1 = '%2' 的行吗？").arg(pkColName, pkValue)) != QMessageBox::Yes) {
        return;
    }

    size_t affected = 0;
    std::string msg;
    bool ok = dataMgr_->delete_rows(currentTable_.toStdString(),
                                     pkColName.toStdString(),
                                     pkValue.toStdString(),
                                     affected, &msg);

    if (ok) {
        loadTable(currentTable_);
        emit tableDataChanged();
    } else {
        QMessageBox::warning(this, "删除失败", QString::fromStdString(msg));
    }
}

void TableDataView::onApply() {
    if (currentTable_.isEmpty() || !dataMgr_ || columnNames_.empty()) return;
    if (!hasPendingChanges()) {
        infoLabel_->setText("没有待保存的变更");
        return;
    }

    int insertCount = 0;
    int updateCount = 0;
    bool hasError = false;

    QList<int> newRowList = newRows_.values();
    std::sort(newRowList.begin(), newRowList.end());

    for (int row : newRowList) {
        bool allEmpty = true;
        for (int c = 0; c < table_->columnCount(); ++c) {
            auto *cell = table_->item(row, c);
            if (cell && !cell->text().isEmpty()) { allEmpty = false; break; }
        }
        if (allEmpty) continue;

        std::vector<std::pair<std::string, std::string>> colValues;
        for (int c = 0; c < table_->columnCount(); ++c) {
            auto *cell = table_->item(row, c);
            std::string val = cell ? cell->text().toStdString() : "";
            colValues.emplace_back(columnNames_[static_cast<size_t>(c)], val);
        }
        std::string msg;
        if (dataMgr_->insert_row(currentTable_.toStdString(), colValues, &msg)) {
            insertCount++;
        } else {
            QMessageBox::warning(this, "插入失败",
                QString("第 %1 行: %2").arg(row + 1).arg(QString::fromStdString(msg)));
            hasError = true;
            break;
        }
    }

    if (!hasError) {
        QList<int> dirtyRowList = dirtyRows_.keys();
        for (int row : dirtyRowList) {
            if (row >= table_->rowCount()) continue;

            QString originalPk = dirtyRows_[row];
            QString pkColName = QString::fromStdString(columnNames_[0]);

            std::vector<std::pair<std::string, std::string>> setValues;
            for (int c = 0; c < table_->columnCount(); ++c) {
                auto *cell = table_->item(row, c);
                std::string val = cell ? cell->text().toStdString() : "";
                setValues.emplace_back(columnNames_[static_cast<size_t>(c)], val);
            }

            size_t affected = 0;
            std::string msg;
            if (dataMgr_->update_rows(currentTable_.toStdString(),
                                       setValues,
                                       pkColName.toStdString(),
                                       originalPk.toStdString(),
                                       affected, &msg)) {
                updateCount++;
            } else {
                QMessageBox::warning(this, "更新失败",
                    QString("第 %1 行: %2").arg(row + 1).arg(QString::fromStdString(msg)));
                hasError = true;
                break;
            }
        }
    }

    if (!hasError) {
        newRows_.clear();
        dirtyRows_.clear();
        infoLabel_->setText(QString("%1 | 已插入 %2 行，已更新 %3 行").arg(currentTable_).arg(insertCount).arg(updateCount));
        emit messageRequested(QString("已插入 %1 行，已更新 %2 行").arg(insertCount).arg(updateCount), false);
        emit tableDataChanged();
        loadTable(currentTable_);
    }
}

void TableDataView::onDiscard() {
    if (!currentTable_.isEmpty()) {
        newRows_.clear();
        dirtyRows_.clear();
        loadTable(currentTable_);
        emit messageRequested("已放弃未保存的修改", false);
    }
}
