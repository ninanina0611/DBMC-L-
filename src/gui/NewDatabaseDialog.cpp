#include "NewDatabaseDialog.h"

#include <QLineEdit>
#include <QLabel>
#include <QVBoxLayout>
#include <QDialogButtonBox>

NewDatabaseDialog::NewDatabaseDialog(QWidget *parent)
    : QDialog(parent)
{
    setWindowTitle("新建数据库");

    auto *layout = new QVBoxLayout(this);

    auto *label = new QLabel("数据库名称:");
    layout->addWidget(label);

    nameEdit_ = new QLineEdit;
    nameEdit_->setPlaceholderText("请输入数据库名称");
    layout->addWidget(nameEdit_);

    auto *buttons = new QDialogButtonBox(QDialogButtonBox::Ok | QDialogButtonBox::Cancel);
    layout->addWidget(buttons);

    connect(buttons, &QDialogButtonBox::accepted, this, &QDialog::accept);
    connect(buttons, &QDialogButtonBox::rejected, this, &QDialog::reject);
}

QString NewDatabaseDialog::databaseName() const {
    return nameEdit_->text().trimmed();
}
