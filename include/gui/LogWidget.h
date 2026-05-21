#pragma once

#include <QWidget>
#include <QDateTime>

class QTextBrowser;
class QPushButton;
class QComboBox;

class LogWidget : public QWidget {
    Q_OBJECT

public:
    enum class Level {
        Info = 0,
        Warning = 1,
        Error = 2,
        SQL = 3
    };

    explicit LogWidget(QWidget *parent = nullptr);

    void clearLog();
    void saveLog(const QString &filePath) const;
    void loadLog(const QString &filePath);

    static QString levelToString(Level level);
    static QString levelColor(Level level);
    static Level levelFromString(const QString &s);

signals:
    void sqlReexecuteRequested(const QString &sql);

public slots:
    void onInfo(const QString &msg);
    void onWarning(const QString &msg);
    void onError(const QString &msg);
    void onSQL(const QString &sql, bool ok, double elapsedMs = 0.0);

private slots:
    void onFilterChanged(int index);
    void onClear();
    void onAnchorClicked(const class QUrl &url);

private:
    void setupUi();
    void rebuildEntryHtml(int index);

    static bool isSelectStatement(const QString &sql);

    QTextBrowser *logView_ = nullptr;
    QPushButton *clearBtn_ = nullptr;
    QComboBox *filterCombo_ = nullptr;

    struct LogEntry {
        Level level;
        QDateTime timestamp;
        QString formatted;
        QString rawMessage;
        bool sqlOk = false;
        double sqlElapsed = 0.0;
    };

    QVector<LogEntry> entries_;
    int currentFilter_ = -1;
};
