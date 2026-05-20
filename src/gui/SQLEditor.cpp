#include "SQLEditor.h"

#include <QPlainTextEdit>
#include <QPushButton>
#include <QVBoxLayout>
#include <QHBoxLayout>
#include <QShortcut>
#include <QKeySequence>

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
    clearBtn_ = new QPushButton("清空");

    btnLayout->addWidget(executeBtn_);
    btnLayout->addWidget(clearBtn_);
    btnLayout->addStretch();

    layout->addLayout(btnLayout);

    connect(executeBtn_, &QPushButton::clicked, this, &SQLEditor::onExecute);
    connect(clearBtn_, &QPushButton::clicked, this, &SQLEditor::clearSql);

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
