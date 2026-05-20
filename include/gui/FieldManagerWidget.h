#pragma once

#include <QWidget>
#include "DatabaseManager.h"

class QTableWidget;
class QPushButton;
class QLabel;

class FieldManagerWidget : public QWidget {
    Q_OBJECT

public:
    explicit FieldManagerWidget(rdbms::DatabaseManager &mgr,
                                QWidget *parent = nullptr);

    void setTable(const QString &tableName);
    QString currentTable() const;

signals:
    void fieldsChanged();

private slots:
    void onAddField();
    void onDropField();
    void onApply();
    void onDiscard();

private:
    void loadFields();
    void insertFieldRow(int row, const rdbms::DatabaseManager::Column &col);
    void setupConstraintCascade(int row);
    void setupTypeLengthLink(int row);
    rdbms::DatabaseManager::Column readRow(int row) const;

    QString tableName_;
    rdbms::DatabaseManager &mgr_;
    QLabel *infoLabel_ = nullptr;
    QTableWidget *fieldTable_ = nullptr;
    QPushButton *addBtn_ = nullptr;
    QPushButton *dropBtn_ = nullptr;
    QPushButton *applyBtn_ = nullptr;
    QPushButton *discardBtn_ = nullptr;
};
