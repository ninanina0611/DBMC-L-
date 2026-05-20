#include "NewTableDialog.h"

#include <QLineEdit>
#include <QLabel>
#include <QVBoxLayout>
#include <QHBoxLayout>
#include <QDialogButtonBox>
#include <QTableWidget>
#include <QHeaderView>
#include <QPushButton>
#include <QComboBox>
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

enum { COL_NAME = 0, COL_TYPE = 1, COL_LENGTH = 2, COL_SCALE = 3, COL_PK = 4, COL_NN = 5, COL_UQ = 6 };

NewTableDialog::NewTableDialog(QWidget *parent)
    : QDialog(parent)
{
    setWindowTitle("新建表");
    resize(600, 400);

    auto *layout = new QVBoxLayout(this);

    auto *nameLabel = new QLabel("表名称:");
    layout->addWidget(nameLabel);

    nameEdit_ = new QLineEdit;
    nameEdit_->setPlaceholderText("请输入表名称");
    layout->addWidget(nameEdit_);

    auto *colLabel = new QLabel("字段定义:");
    layout->addWidget(colLabel);

    columnTable_ = new QTableWidget(0, 7);
    columnTable_->setHorizontalHeaderLabels({"字段名", "类型", "长度", "小数位", "主键", "非空", "唯一"});
    columnTable_->horizontalHeader()->setSectionResizeMode(QHeaderView::Stretch);
    columnTable_->setSelectionBehavior(QAbstractItemView::SelectRows);
    layout->addWidget(columnTable_);

    auto *colBtnLayout = new QHBoxLayout;
    addColBtn_ = new QPushButton("添加字段");
    removeColBtn_ = new QPushButton("删除字段");
    colBtnLayout->addWidget(addColBtn_);
    colBtnLayout->addWidget(removeColBtn_);
    colBtnLayout->addStretch();
    layout->addLayout(colBtnLayout);

    auto *buttons = new QDialogButtonBox(QDialogButtonBox::Ok | QDialogButtonBox::Cancel);
    layout->addWidget(buttons);

    connect(addColBtn_, &QPushButton::clicked, this, &NewTableDialog::addColumnRow);
    connect(removeColBtn_, &QPushButton::clicked, this, &NewTableDialog::removeColumnRow);
    connect(buttons, &QDialogButtonBox::accepted, this, &QDialog::accept);
    connect(buttons, &QDialogButtonBox::rejected, this, &QDialog::reject);

    addColumnRow();
}

QString NewTableDialog::tableName() const {
    return nameEdit_->text().trimmed();
}

rdbms::DatabaseManager::TableSchema NewTableDialog::tableSchema() const {
    rdbms::DatabaseManager::TableSchema schema;
    schema.table_name = tableName().toStdString();

    for (int i = 0; i < columnTable_->rowCount(); ++i) {
        auto *nameItem = columnTable_->item(i, COL_NAME);
        auto *typeCombo = qobject_cast<QComboBox *>(columnTable_->cellWidget(i, COL_TYPE));
        auto *lenItem = columnTable_->item(i, COL_LENGTH);
        auto *pkCheck = qobject_cast<QCheckBox *>(columnTable_->cellWidget(i, COL_PK));
        auto *nnCheck = qobject_cast<QCheckBox *>(columnTable_->cellWidget(i, COL_NN));
        auto *uqCheck = qobject_cast<QCheckBox *>(columnTable_->cellWidget(i, COL_UQ));

        if (!nameItem || nameItem->text().trimmed().isEmpty()) continue;

        rdbms::DatabaseManager::Column col;
        col.name = nameItem->text().trimmed().toStdString();

        if (typeCombo) {
            QString tname = typeCombo->currentText();
            if (isSeparator(tname)) tname = "TEXT";
            col.display_type = tname.toStdString();
            col.type = rdbms::DatabaseManager::type_from_name(col.display_type);
        }

        if (lenItem) col.length = lenItem->text().trimmed().toStdString();

        auto *scaleItem = columnTable_->item(i, COL_SCALE);
        if (scaleItem) col.scale = scaleItem->text().trimmed().toStdString();

        if (pkCheck) col.is_primary = pkCheck->isChecked();
        if (nnCheck) col.not_null = nnCheck->isChecked();
        if (uqCheck) col.is_unique = uqCheck->isChecked();

        schema.columns.push_back(col);
    }

    return schema;
}

void NewTableDialog::setupConstraintCascade(int row) {
    auto *pkCheck = qobject_cast<QCheckBox *>(columnTable_->cellWidget(row, COL_PK));
    auto *nnCheck = qobject_cast<QCheckBox *>(columnTable_->cellWidget(row, COL_NN));
    if (!pkCheck || !nnCheck) return;

    connect(pkCheck, &QCheckBox::toggled, this, [this, row](bool checked) {
        auto *nn = qobject_cast<QCheckBox *>(columnTable_->cellWidget(row, COL_NN));
        if (checked && nn) {
            nn->setChecked(true);
        }
    });

    connect(nnCheck, &QCheckBox::toggled, this, [this, row](bool checked) {
        auto *pk = qobject_cast<QCheckBox *>(columnTable_->cellWidget(row, COL_PK));
        auto *nn = qobject_cast<QCheckBox *>(columnTable_->cellWidget(row, COL_NN));
        if (!checked && pk && pk->isChecked() && nn) {
            nn->setChecked(true);
        }
    });
}

void NewTableDialog::setupTypeLengthLink(int row) {
    auto *typeCombo = qobject_cast<QComboBox *>(columnTable_->cellWidget(row, COL_TYPE));
    if (!typeCombo) return;

    connect(typeCombo, QOverload<int>::of(&QComboBox::currentIndexChanged), this, [this, row](int) {
        auto *combo = qobject_cast<QComboBox *>(columnTable_->cellWidget(row, COL_TYPE));
        auto *lenItem = columnTable_->item(row, COL_LENGTH);
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
        auto *scaleItem = columnTable_->item(row, COL_SCALE);
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

void NewTableDialog::addColumnRow() {
    int row = columnTable_->rowCount();
    columnTable_->insertRow(row);

    columnTable_->setItem(row, COL_NAME, new QTableWidgetItem);

    auto *typeCombo = new QComboBox;
    typeCombo->addItems(typeList());
    disableSeparators(typeCombo);
    columnTable_->setCellWidget(row, COL_TYPE, typeCombo);

    auto *lenItem = new QTableWidgetItem;
    lenItem->setFlags(lenItem->flags() & ~(Qt::ItemIsEnabled | Qt::ItemIsEditable));
    columnTable_->setItem(row, COL_LENGTH, lenItem);

    auto *scaleItem = new QTableWidgetItem;
    scaleItem->setFlags(scaleItem->flags() & ~(Qt::ItemIsEnabled | Qt::ItemIsEditable));
    columnTable_->setItem(row, COL_SCALE, scaleItem);

    auto *pkCheck = new QCheckBox;
    columnTable_->setCellWidget(row, COL_PK, pkCheck);

    auto *nnCheck = new QCheckBox;
    columnTable_->setCellWidget(row, COL_NN, nnCheck);

    auto *uqCheck = new QCheckBox;
    columnTable_->setCellWidget(row, COL_UQ, uqCheck);

    setupConstraintCascade(row);
    setupTypeLengthLink(row);
}

void NewTableDialog::removeColumnRow() {
    int row = columnTable_->currentRow();
    if (row >= 0) {
        columnTable_->removeRow(row);
    }
}
