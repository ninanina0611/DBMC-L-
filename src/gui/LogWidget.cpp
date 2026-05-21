#include "LogWidget.h"

#include <QTextBrowser>
#include <QPushButton>
#include <QComboBox>
#include <QVBoxLayout>
#include <QHBoxLayout>
#include <QScrollBar>
#include <QUrl>
#include <QFile>
#include <QTextStream>
#include <QDateTime>

LogWidget::LogWidget(QWidget *parent)
    : QWidget(parent)
{
    setupUi();
}

void LogWidget::setupUi() {
    auto *mainLayout = new QVBoxLayout(this);
    mainLayout->setContentsMargins(0, 0, 0, 0);
    mainLayout->setSpacing(2);

    auto *toolbar = new QHBoxLayout();
    toolbar->setSpacing(6);

    filterCombo_ = new QComboBox;
    filterCombo_->addItem("全部", -1);
    filterCombo_->addItem("INFO", static_cast<int>(Level::Info));
    filterCombo_->addItem("WARNING", static_cast<int>(Level::Warning));
    filterCombo_->addItem("ERROR", static_cast<int>(Level::Error));
    filterCombo_->addItem("SQL", static_cast<int>(Level::SQL));

    clearBtn_ = new QPushButton("清空日志");

    toolbar->addWidget(filterCombo_);
    toolbar->addStretch();
    toolbar->addWidget(clearBtn_);

    mainLayout->addLayout(toolbar);

    logView_ = new QTextBrowser;
    logView_->setReadOnly(true);
    logView_->setMaximumHeight(180);
    logView_->setMinimumHeight(80);
    QFont monoFont("Consolas", 9);
    logView_->setFont(monoFont);
    logView_->setPlaceholderText("数据库操作日志将在此显示...");
    logView_->setOpenLinks(false);

    mainLayout->addWidget(logView_);

    connect(filterCombo_, QOverload<int>::of(&QComboBox::currentIndexChanged),
            this, &LogWidget::onFilterChanged);
    connect(clearBtn_, &QPushButton::clicked, this, &LogWidget::onClear);
    connect(logView_, &QTextBrowser::anchorClicked, this, &LogWidget::onAnchorClicked);
}

QString LogWidget::levelToString(Level level) {
    switch (level) {
    case Level::Info:    return "INFO";
    case Level::Warning: return "WARN";
    case Level::Error:   return "ERROR";
    case Level::SQL:     return "SQL";
    }
    return "UNKNOWN";
}

LogWidget::Level LogWidget::levelFromString(const QString &s) {
    if (s == "INFO")  return Level::Info;
    if (s == "WARN")  return Level::Warning;
    if (s == "ERROR") return Level::Error;
    if (s == "SQL")   return Level::SQL;
    return Level::Info;
}

QString LogWidget::levelColor(Level level) {
    switch (level) {
    case Level::Info:    return "#2196F3";
    case Level::Warning: return "#FF9800";
    case Level::Error:   return "#F44336";
    case Level::SQL:     return "#4CAF50";
    }
    return "#999999";
}

bool LogWidget::isSelectStatement(const QString &sql) {
    QString v = sql.trimmed().section(' ', 0, 0).toLower();
    return v == "select";
}

void LogWidget::rebuildEntryHtml(int index) {
    auto &entry = entries_[index];

    QString ts = entry.timestamp.toString("HH:mm:ss");
    QString lvl = levelToString(entry.level);
    QString color = levelColor(entry.level);

    QString body;
    if (entry.level == Level::SQL) {
        QString sqlEscaped = entry.rawMessage.toHtmlEscaped();
        if (sqlEscaped.length() > 120) {
            sqlEscaped = sqlEscaped.left(117) + "...";
        }

        bool isSelect = isSelectStatement(entry.rawMessage);
        QString sqlPart;
        if (isSelect) {
            QByteArray encoded = QUrl::toPercentEncoding(entry.rawMessage);
            sqlPart = QString("<a href='sql:%1' style='color:#2196F3;text-decoration:underline;'>%2</a>")
                          .arg(QString::fromUtf8(encoded), sqlEscaped);
        } else {
            sqlPart = sqlEscaped;
        }

        QString status;
        if (entry.sqlOk) {
            status = "<span style='color:#4CAF50;font-weight:bold;'>&#x2713;</span>";
        } else {
            status = "<span style='color:#F44336;font-weight:bold;'>&#x2717;</span>";
        }

        QString timing;
        if (entry.sqlElapsed > 0) {
            timing = QString("<span style='color:#888;'>(%1 ms)</span>").arg(entry.sqlElapsed, 0, 'f', 1);
        }

        body = QString("%1 %2 %3").arg(sqlPart, status, timing);
    } else if (entry.level == Level::Error) {
        body = QString("<span style='color:#F44336;'>%1</span>").arg(entry.rawMessage.toHtmlEscaped());
    } else {
        body = entry.rawMessage.toHtmlEscaped();
    }

    entry.formatted = QString("<span style='color:%1;font-weight:bold;'>[%2] [%3]</span> %4")
                          .arg(color, ts, lvl, body);
}

void LogWidget::clearLog() {
    entries_.clear();
    logView_->clear();
}

void LogWidget::saveLog(const QString &filePath) const {
    QFile file(filePath);
    if (!file.open(QIODevice::WriteOnly | QIODevice::Text)) return;

    QTextStream out(&file);
    out.setEncoding(QStringConverter::Utf8);

    for (const auto &entry : entries_) {
        out << levelToString(entry.level) << "\t"
            << entry.timestamp.toString(Qt::ISODate) << "\t"
            << entry.rawMessage << "\t"
            << (entry.sqlOk ? "1" : "0") << "\t"
            << entry.sqlElapsed << "\n";
    }
    file.close();
}

void LogWidget::loadLog(const QString &filePath) {
    QFile file(filePath);
    if (!file.open(QIODevice::ReadOnly | QIODevice::Text)) return;

    QTextStream in(&file);
    in.setEncoding(QStringConverter::Utf8);

    while (!in.atEnd()) {
        QString line = in.readLine();
        if (line.isEmpty()) continue;

        QStringList parts = line.split("\t");
        if (parts.size() < 3) continue;

        Level level = levelFromString(parts[0]);
        QDateTime timestamp = QDateTime::fromString(parts[1], Qt::ISODate);
        QString rawMessage = parts[2];
        bool sqlOk = parts.size() > 3 ? parts[3] == "1" : false;
        double sqlElapsed = parts.size() > 4 ? parts[4].toDouble() : 0.0;

        LogEntry entry;
        entry.level = level;
        entry.timestamp = timestamp;
        entry.rawMessage = rawMessage;
        entry.sqlOk = sqlOk;
        entry.sqlElapsed = sqlElapsed;

        entries_.append(entry);
    }
    file.close();

    for (int i = 0; i < entries_.size(); ++i) {
        rebuildEntryHtml(i);
    }

    logView_->clear();
    for (const auto &entry : entries_) {
        if (currentFilter_ == -1 || static_cast<int>(entry.level) == currentFilter_) {
            logView_->append(entry.formatted);
        }
    }
    QScrollBar *bar = logView_->verticalScrollBar();
    bar->setValue(bar->maximum());
}

void LogWidget::onInfo(const QString &msg) {
    LogEntry entry;
    entry.level = Level::Info;
    entry.timestamp = QDateTime::currentDateTime();
    entry.rawMessage = msg;
    entry.sqlOk = false;
    entry.sqlElapsed = 0.0;

    QString ts = entry.timestamp.toString("HH:mm:ss");
    QString color = levelColor(Level::Info);
    entry.formatted = QString("<span style='color:%1;font-weight:bold;'>[%2] [INFO]</span> %3")
                          .arg(color, ts, msg.toHtmlEscaped());

    entries_.append(entry);

    if (currentFilter_ == -1 || static_cast<int>(Level::Info) == currentFilter_) {
        logView_->append(entry.formatted);
        QScrollBar *bar = logView_->verticalScrollBar();
        bar->setValue(bar->maximum());
    }
}

void LogWidget::onWarning(const QString &msg) {
    LogEntry entry;
    entry.level = Level::Warning;
    entry.timestamp = QDateTime::currentDateTime();
    entry.rawMessage = msg;
    entry.sqlOk = false;
    entry.sqlElapsed = 0.0;

    QString ts = entry.timestamp.toString("HH:mm:ss");
    QString color = levelColor(Level::Warning);
    entry.formatted = QString("<span style='color:%1;font-weight:bold;'>[%2] [WARN]</span> %3")
                          .arg(color, ts, msg.toHtmlEscaped());

    entries_.append(entry);

    if (currentFilter_ == -1 || static_cast<int>(Level::Warning) == currentFilter_) {
        logView_->append(entry.formatted);
        QScrollBar *bar = logView_->verticalScrollBar();
        bar->setValue(bar->maximum());
    }
}

void LogWidget::onError(const QString &msg) {
    LogEntry entry;
    entry.level = Level::Error;
    entry.timestamp = QDateTime::currentDateTime();
    entry.rawMessage = msg;
    entry.sqlOk = false;
    entry.sqlElapsed = 0.0;

    QString ts = entry.timestamp.toString("HH:mm:ss");
    QString color = levelColor(Level::Error);
    entry.formatted = QString("<span style='color:%1;font-weight:bold;'>[%2] [ERROR]</span> <span style='color:#F44336;'>%3</span>")
                          .arg(color, ts, msg.toHtmlEscaped());

    entries_.append(entry);

    if (currentFilter_ == -1 || static_cast<int>(Level::Error) == currentFilter_) {
        logView_->append(entry.formatted);
        QScrollBar *bar = logView_->verticalScrollBar();
        bar->setValue(bar->maximum());
    }
}

void LogWidget::onSQL(const QString &sql, bool ok, double elapsedMs) {
    QString sqlEscaped = sql.toHtmlEscaped();

    if (sqlEscaped.length() > 120) {
        sqlEscaped = sqlEscaped.left(117) + "...";
    }

    bool isSelect = isSelectStatement(sql);

    QString sqlPart;
    if (isSelect) {
        QByteArray encoded = QUrl::toPercentEncoding(sql);
        sqlPart = QString("<a href='sql:%1' style='color:#2196F3;text-decoration:underline;'>%2</a>")
                      .arg(QString::fromUtf8(encoded), sqlEscaped);
    } else {
        sqlPart = sqlEscaped;
    }

    QString status;
    if (ok) {
        status = "<span style='color:#4CAF50;font-weight:bold;'>&#x2713;</span>";
    } else {
        status = "<span style='color:#F44336;font-weight:bold;'>&#x2717;</span>";
    }

    QString timing;
    if (elapsedMs > 0) {
        timing = QString("<span style='color:#888;'>(%1 ms)</span>").arg(elapsedMs, 0, 'f', 1);
    }

    QString body = QString("%1 %2 %3").arg(sqlPart, status, timing);

    LogEntry entry;
    entry.level = Level::SQL;
    entry.timestamp = QDateTime::currentDateTime();
    entry.rawMessage = sql;
    entry.sqlOk = ok;
    entry.sqlElapsed = elapsedMs;

    QString ts = entry.timestamp.toString("HH:mm:ss");
    QString color = levelColor(Level::SQL);
    entry.formatted = QString("<span style='color:%1;font-weight:bold;'>[%2] [SQL]</span> %3")
                          .arg(color, ts, body);

    entries_.append(entry);

    if (currentFilter_ == -1 || static_cast<int>(Level::SQL) == currentFilter_) {
        logView_->append(entry.formatted);
        QScrollBar *bar = logView_->verticalScrollBar();
        bar->setValue(bar->maximum());
    }
}

void LogWidget::onAnchorClicked(const QUrl &url) {
    if (url.scheme() == "sql") {
        QString sql = QUrl::fromPercentEncoding(url.toString(QUrl::RemoveScheme).toUtf8());
        if (!sql.isEmpty()) {
            emit sqlReexecuteRequested(sql);
        }
    }
}

void LogWidget::onFilterChanged(int index) {
    currentFilter_ = filterCombo_->itemData(index).toInt();

    logView_->clear();
    for (const auto &entry : entries_) {
        if (currentFilter_ == -1 || static_cast<int>(entry.level) == currentFilter_) {
            logView_->append(entry.formatted);
        }
    }
    QScrollBar *bar = logView_->verticalScrollBar();
    bar->setValue(bar->maximum());
}

void LogWidget::onClear() {
    clearLog();
}
