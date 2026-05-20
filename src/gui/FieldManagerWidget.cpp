#include "FieldManagerWidget.h"
#include "DatabaseManager.h"

#include <QTableWidget>
#include <QPushButton>
#include <QVBoxLayout>
#include <QHBoxLayout>
#include <QHeaderView>
#include <QMessageBox>
#include <QComboBox>
#include <QLineEdit>
#include <QLabel>
#include <QCheckBox>
#include <QStandardItemModel>

static QStringList typeList() {
    return {
        "-- 整数 --",
        "TINYINT", "SMALLINT", "MEDIUMINT", "INT", "BIGINT",
        "-- 浮点 --",
        "FLOAT", "DOUBLE", "DECIMAL",
        "-- 字符串 --",
        "CHAR", "VARCHAR", "TINYTEXT", "TEXT", "MEDIUMTEXT", "LONGTEXT",
        "-- 日期时间 --",
        "DATE", "TIME", "DATETIME", "TIMESTAMP", "YEAR",
        "-- 二进制 --",
        "BINARY", "VARBINARY", "TINYBLOB", "BLOB", "MEDIUMBLOB", "LONGBLOB",
        "-- 其他 --",
        "BOOL", "BIT", "ENUM", "SET", "JSON"
    };
}

static bool isSeparator(const QString &s) {
    return s.startsWith("--");
}

static bool typeSupportsLength(const QString &tname) {
    static const QSet<QString> supported = {
        "TINYINT", "SMALLINT", "MEDIUMINT", "INT", "BIGINT",
        "CHAR", "VARCHAR", "BINARY", "VARBINARY",
        "DECIMAL", "NUMERIC", "FLOAT", "DOUBLE",
        "BIT", "ENUM", "SET"
    };
    return supported.contains(tname);
}

static bool typeSupportsScale(const QString &tname) {
    static const QSet<QString> supported = {
        "DECIMAL", "NUMERIC", "FLOAT", "DOUBLE"
    };
    return supported.contains(tname);
}

static void disableSeparators(QComboBox *combo) {
    auto *model = qobject_cast<QStandardItemModel *>(combo->model());
    if (!model) return;
    for (int i = 0; i < combo->count(); ++i) {
        if (isSeparator(combo->itemText(i))) {
            auto *item = model->item(i);
            item->setFlags(item->flags() & ~Qt::ItemIsEnabled);
        }
    }
}

static void setTypeCombo(QComboBox *combo, const std::string &displayType) {
    QString dt = QString::fromStdString(displayType);
    for (int i = 0; i < combo->count(); ++i) {
        if (isSeparator(combo->itemText(i))) continue;
        if (combo->itemText(i) == dt) {
            combo->setCurrentIndex(i);
            return;
        }
    }
    for (int i = 0; i < combo->count(); ++i) {
        if (isSeparator(combo->itemText(i))) continue;
        if (rdbms::DatabaseManager::type_from_name(combo->itemText(i).toStdString()) ==
            rdbms::DatabaseManager::type_from_name(displayType)) {
            combo->setCurrentIndex(i);
            return;
        }
    }
}

enum { COL_NAME = 0, COL_TYPE = 1, COL_LENGTH = 2, COL_SCALE = 3, COL_PK = 4, COL_NN = 5, COL_UQ = 6 };

FieldManagerWidget::FieldManagerWidget(rdbms::DatabaseManager &mgr,
                                       QWidget *parent)
    : QWidget(parent), mgr_(mgr)
{
    auto *layout = new QVBoxLayout(this);
    layout->setContentsMargins(0, 0, 0, 0);
    layout->setSpacing(2);

    infoLabel_ = new QLabel("选择一个表进行字段管理");
    layout->addWidget(infoLabel_);

    fieldTable_ = new QTableWidget;
    fieldTable_->setColumnCount(7);
    fieldTable_->setHorizontalHeaderLabels({"字段名", "类型", "长度", "小数位", "主键", "非空", "唯一"});
    fieldTable_->horizontalHeader()->setSectionResizeMode(QHeaderView::Stretch);
    fieldTable_->setSelectionBehavior(QAbstractItemView::SelectRows);
    fieldTable_->setAlternatingRowColors(true);
    layout->addWidget(fieldTable_);

    auto *btnLayout = new QHBoxLayout;
    addBtn_ = new QPushButton("添加字段");
    dropBtn_ = new QPushButton("删除字段");
    applyBtn_ = new QPushButton("应用");
    discardBtn_ = new QPushButton("放弃");
    btnLayout->addWidget(addBtn_);
    btnLayout->addWidget(dropBtn_);
    btnLayout->addStretch();
    btnLayout->addWidget(applyBtn_);
    btnLayout->addWidget(discardBtn_);
    layout->addLayout(btnLayout);

    connect(addBtn_, &QPushButton::clicked, this, &FieldManagerWidget::onAddField);
    connect(dropBtn_, &QPushButton::clicked, this, &FieldManagerWidget::onDropField);
    connect(applyBtn_, &QPushButton::clicked, this, &FieldManagerWidget::onApply);
    connect(discardBtn_, &QPushButton::clicked, this, &FieldManagerWidget::onDiscard);
}

QString FieldManagerWidget::currentTable() const {
    return tableName_;
}

void FieldManagerWidget::setTable(const QString &tableName) {
    tableName_ = tableName;
    if (tableName_.isEmpty()) {
        infoLabel_->setText("选择一个表进行字段管理");
        fieldTable_->setRowCount(0);
    } else {
        infoLabel_->setText(QString("表: %1  (直接在表格中编辑字段属性，完成后点击「应用」)").arg(tableName_));
        loadFields();
    }
}

void FieldManagerWidget::setupConstraintCascade(int row) {
    auto *pkCheck = qobject_cast<QCheckBox *>(fieldTable_->cellWidget(row, COL_PK));
    auto *nnCheck = qobject_cast<QCheckBox *>(fieldTable_->cellWidget(row, COL_NN));
    if (!pkCheck || !nnCheck) return;

    connect(pkCheck, &QCheckBox::toggled, this, [this, row](bool checked) {
        auto *nn = qobject_cast<QCheckBox *>(fieldTable_->cellWidget(row, COL_NN));
        if (checked && nn) {
            nn->setChecked(true);
        }
    });

    connect(nnCheck, &QCheckBox::toggled, this, [this, row](bool checked) {
        auto *pk = qobject_cast<QCheckBox *>(fieldTable_->cellWidget(row, COL_PK));
        auto *nn = qobject_cast<QCheckBox *>(fieldTable_->cellWidget(row, COL_NN));
        if (!checked && pk && pk->isChecked() && nn) {
            nn->setChecked(true);
        }
    });
}

void FieldManagerWidget::setupTypeLengthLink(int row) {
    auto *typeCombo = qobject_cast<QComboBox *>(fieldTable_->cellWidget(row, COL_TYPE));
    if (!typeCombo) return;

    connect(typeCombo, QOverload<int>::of(&QComboBox::currentIndexChanged), this, [this, row](int) {
        auto *combo = qobject_cast<QComboBox *>(fieldTable_->cellWidget(row, COL_TYPE));
        auto *lenItem = fieldTable_->item(row, COL_LENGTH);
        if (!combo || !lenItem) return;
        QString tname = combo->currentText();
        if (isSeparator(tname)) return;
        bool supports = typeSupportsLength(tname);
        Qt::ItemFlags flags = lenItem->flags();
        if (supports) {
            flags |= Qt::ItemIsEnabled | Qt::ItemIsEditable;
        } else {
            flags &= ~(Qt::ItemIsEnabled | Qt::ItemIsEditable);
            lenItem->setText("");
        }
        lenItem->setFlags(flags);
        auto *scaleItem = fieldTable_->item(row, COL_SCALE);
        if (scaleItem) {
            Qt::ItemFlags sflags = scaleItem->flags();
            if (typeSupportsScale(tname)) {
                sflags |= Qt::ItemIsEnabled | Qt::ItemIsEditable;
            } else {
                sflags &= ~(Qt::ItemIsEnabled | Qt::ItemIsEditable);
                scaleItem->setText("");
            }
            scaleItem->setFlags(sflags);
        }
    });
}

void FieldManagerWidget::insertFieldRow(int row, const rdbms::DatabaseManager::Column &col) {
    fieldTable_->insertRow(row);
    fieldTable_->setItem(row, COL_NAME, new QTableWidgetItem(QString::fromStdString(col.name)));

    auto *typeCombo = new QComboBox;
    typeCombo->addItems(typeList());
    disableSeparators(typeCombo);
    setTypeCombo(typeCombo, col.display_type);
    fieldTable_->setCellWidget(row, COL_TYPE, typeCombo);

    auto *lenItem = new QTableWidgetItem(QString::fromStdString(col.length));
    if (!typeSupportsLength(QString::fromStdString(col.display_type))) {
        lenItem->setFlags(lenItem->flags() & ~(Qt::ItemIsEnabled | Qt::ItemIsEditable));
    }
    fieldTable_->setItem(row, COL_LENGTH, lenItem);

    auto *scaleItem = new QTableWidgetItem(QString::fromStdString(col.scale));
    if (!typeSupportsScale(QString::fromStdString(col.display_type))) {
        scaleItem->setFlags(scaleItem->flags() & ~(Qt::ItemIsEnabled | Qt::ItemIsEditable));
    }
    fieldTable_->setItem(row, COL_SCALE, scaleItem);

    auto *pkCheck = new QCheckBox;
    pkCheck->setChecked(col.is_primary);
    fieldTable_->setCellWidget(row, COL_PK, pkCheck);

    auto *nnCheck = new QCheckBox;
    nnCheck->setChecked(col.not_null);
    fieldTable_->setCellWidget(row, COL_NN, nnCheck);

    auto *uqCheck = new QCheckBox;
    uqCheck->setChecked(col.is_unique);
    fieldTable_->setCellWidget(row, COL_UQ, uqCheck);

    setupConstraintCascade(row);
    setupTypeLengthLink(row);
}

void FieldManagerWidget::loadFields() {
    fieldTable_->setRowCount(0);

    rdbms::DatabaseManager::TableSchema schema;
    if (!mgr_.get_schema(tableName_.toStdString(), schema)) return;

    for (int i = 0; i < static_cast<int>(schema.columns.size()); ++i) {
        const auto &col = schema.columns[static_cast<size_t>(i)];
        insertFieldRow(i, col);
    }
}

rdbms::DatabaseManager::Column FieldManagerWidget::readRow(int row) const {
    rdbms::DatabaseManager::Column col;

    auto *nameItem = fieldTable_->item(row, COL_NAME);
    if (nameItem) col.name = nameItem->text().trimmed().toStdString();

    auto *typeCombo = qobject_cast<QComboBox *>(fieldTable_->cellWidget(row, COL_TYPE));
    if (typeCombo) {
        QString tname = typeCombo->currentText();
        if (isSeparator(tname)) tname = "TEXT";
        col.display_type = tname.toStdString();
        col.type = rdbms::DatabaseManager::type_from_name(col.display_type);
    }

    auto *lenItem = fieldTable_->item(row, COL_LENGTH);
    if (lenItem) col.length = lenItem->text().trimmed().toStdString();

    auto *scaleItem = fieldTable_->item(row, COL_SCALE);
    if (scaleItem) col.scale = scaleItem->text().trimmed().toStdString();

    auto *pkCheck = qobject_cast<QCheckBox *>(fieldTable_->cellWidget(row, COL_PK));
    if (pkCheck) col.is_primary = pkCheck->isChecked();

    auto *nnCheck = qobject_cast<QCheckBox *>(fieldTable_->cellWidget(row, COL_NN));
    if (nnCheck) col.not_null = nnCheck->isChecked();

    auto *uqCheck = qobject_cast<QCheckBox *>(fieldTable_->cellWidget(row, COL_UQ));
    if (uqCheck) col.is_unique = uqCheck->isChecked();

    return col;
}

void FieldManagerWidget::onAddField() {
    rdbms::DatabaseManager::Column empty;
    empty.type = rdbms::DatabaseManager::Type::STRING;
    empty.display_type = "TEXT";
    int row = fieldTable_->rowCount();
    insertFieldRow(row, empty);
    fieldTable_->setCurrentCell(row, 0);
}

void FieldManagerWidget::onDropField() {
    int row = fieldTable_->currentRow();
    if (row >= 0) {
        fieldTable_->removeRow(row);
    }
}

void FieldManagerWidget::onApply() {
    if (tableName_.isEmpty()) return;

    rdbms::DatabaseManager::TableSchema oldSchema;
    if (!mgr_.get_schema(tableName_.toStdString(), oldSchema)) {
        QMessageBox::warning(this, "失败", "无法读取当前表结构");
        return;
    }

    std::vector<rdbms::DatabaseManager::Column> newCols;
    for (int i = 0; i < fieldTable_->rowCount(); ++i) {
        auto col = readRow(i);
        if (col.name.empty()) continue;
        newCols.push_back(std::move(col));
    }

    for (const auto &newCol : newCols) {
        bool found = false;
        for (const auto &oldCol : oldSchema.columns) {
            if (oldCol.name == newCol.name) {
                found = true;
                if (oldCol.type != newCol.type || oldCol.display_type != newCol.display_type ||
                    oldCol.length != newCol.length ||
                    oldCol.is_primary != newCol.is_primary ||
                    oldCol.not_null != newCol.not_null || oldCol.is_unique != newCol.is_unique) {
                    if (!mgr_.modify_column(tableName_.toStdString(), oldCol.name, newCol)) {
                        QMessageBox::warning(this, "失败",
                            "修改字段 " + QString::fromStdString(oldCol.name) + " 失败");
                        loadFields();
                        return;
                    }
                }
                break;
            }
        }
        if (!found) {
            if (!mgr_.add_column(tableName_.toStdString(), newCol)) {
                QMessageBox::warning(this, "失败",
                    "添加字段 " + QString::fromStdString(newCol.name) + " 失败");
                loadFields();
                return;
            }
        }
    }

    for (const auto &oldCol : oldSchema.columns) {
        bool found = false;
        for (const auto &newCol : newCols) {
            if (oldCol.name == newCol.name) { found = true; break; }
        }
        if (!found) {
            if (!mgr_.remove_column(tableName_.toStdString(), oldCol.name)) {
                QMessageBox::warning(this, "失败",
                    "删除字段 " + QString::fromStdString(oldCol.name) + " 失败");
                loadFields();
                return;
            }
        }
    }

    loadFields();
    emit fieldsChanged();
}

void FieldManagerWidget::onDiscard() {
    loadFields();
}
