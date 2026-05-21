#include "SQLEditor.h"

#include <QPlainTextEdit>
#include <QPushButton>
#include <QVBoxLayout>
#include <QHBoxLayout>
#include <QShortcut>
#include <QKeySequence>
#include <QFileDialog>
#include <QFile>
#include <QTextStream>
#include <QMessageBox>
#include <QStringConverter>

SQLEditor::SQLEditor(QWidget *parent)
    : QWidget(parent)
{
    auto *layout = new QVBoxLayout(this);
    layout->setContentsMargins(0, 0, 0, 0);

    editor_ = new QPlainTextEdit;
    editor_->setPlaceholderText("在此输入 SQL 语句（以分号结尾）...");
    editor_->setMinimumHeight(100);
    QFont monoFont("Consolas", 10);
    editor_->setFont(monoFont);
    layout->addWidget(editor_);

    auto *btnLayout = new QHBoxLayout();

    executeBtn_ = new QPushButton("执行 (Ctrl+Enter)");
    importBtn_ = new QPushButton("导入 SQL");
    clearBtn_ = new QPushButton("清空");

    btnLayout->addWidget(executeBtn_);
    btnLayout->addWidget(importBtn_);
    btnLayout->addWidget(clearBtn_);
    btnLayout->addStretch();

    layout->addLayout(btnLayout);

    connect(executeBtn_, &QPushButton::clicked, this, &SQLEditor::onExecute);
    connect(clearBtn_, &QPushButton::clicked, this, &SQLEditor::clearSql);
    connect(importBtn_, &QPushButton::clicked, this, &SQLEditor::onImportScript);

    auto *shortcut = new QShortcut(QKeySequence(Qt::CTRL | Qt::Key_Return), this);
    connect(shortcut, &QShortcut::activated, this, &SQLEditor::onExecute);
}

QString SQLEditor::sqlText() const {
    return editor_->toPlainText();
}

void SQLEditor::setSqlText(const QString &text) {
    editor_->setPlainText(text);
}

void SQLEditor::clearSql() {
    editor_->clear();
}

void SQLEditor::onExecute() {
    QString sql = sqlText().trimmed();
    if (!sql.isEmpty()) {
        emit executeRequested(sql);
    }
}

void SQLEditor::onImportScript() {
    QString path = QFileDialog::getOpenFileName(this, "导入 SQL 脚本", QString(), "SQL 文件 (*.sql);;所有文件 (*.*)");
    if (path.isEmpty()) return;

    QFile file(path);
    if (!file.open(QIODevice::ReadOnly | QIODevice::Text)) {
        QMessageBox::warning(this, "错误", "无法打开文件: " + path);
        return;
    }

    QTextStream in(&file);
    // 尝试以 UTF-8 读取
    in.setEncoding(QStringConverter::Utf8);
    QString content = in.readAll();
    file.close();

    setSqlText(content);
}
