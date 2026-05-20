#pragma once

#include <QDialog>
#include <vector>
#include "DatabaseManager.h"

class QLineEdit;
class QTableWidget;
class QPushButton;

class NewTableDialog : public QDialog {
    Q_OBJECT

public:
    explicit NewTableDialog(QWidget *parent = nullptr);

    QString tableName() const;
    rdbms::DatabaseManager::TableSchema tableSchema() const;

private slots:
    void addColumnRow();
    void removeColumnRow();

private:
    void setupConstraintCascade(int row);
    void setupTypeLengthLink(int row);
    QLineEdit *nameEdit_ = nullptr;
    QTableWidget *columnTable_ = nullptr;
    QPushButton *addColBtn_ = nullptr;
    QPushButton *removeColBtn_ = nullptr;
};
