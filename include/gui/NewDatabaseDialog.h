#pragma once

#include <QDialog>

class QLineEdit;

class NewDatabaseDialog : public QDialog {
    Q_OBJECT

public:
    explicit NewDatabaseDialog(QWidget *parent = nullptr);

    QString databaseName() const;

private:
    QLineEdit *nameEdit_ = nullptr;
};
