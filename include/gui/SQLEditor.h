#pragma once

#include <QWidget>

class QPlainTextEdit;
class QPushButton;

class SQLEditor : public QWidget {
    Q_OBJECT

public:
    explicit SQLEditor(QWidget *parent = nullptr);

    QString sqlText() const;
    void setSqlText(const QString &text);
    void clearSql();

signals:
    void executeRequested(const QString &sql);

private slots:
    void onExecute();

private:
    QPlainTextEdit *editor_ = nullptr;
    QPushButton *executeBtn_ = nullptr;
    QPushButton *clearBtn_ = nullptr;
};
