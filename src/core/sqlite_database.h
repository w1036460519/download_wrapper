#pragma once

#include <sqlite3.h>
#include <string>

namespace dw {
    inline bool ensure_sqlite_column(sqlite3 *db, const char *table, const char *column, const char *migration) {
        return sqlite3_table_column_metadata(db, nullptr, table, column, nullptr, nullptr, nullptr, nullptr, nullptr)
                   == SQLITE_OK
            || sqlite3_exec(db, migration, nullptr, nullptr, nullptr) == SQLITE_OK;
    }

    /// 统一管理 SQLite 连接，表结构和业务 SQL 交给各 store。
    class SqliteDatabase {
    public:
        SqliteDatabase() = default;
        ~SqliteDatabase() { close(); }
        SqliteDatabase(const SqliteDatabase &) = delete;
        SqliteDatabase &operator=(const SqliteDatabase &) = delete;

        bool open(const std::string &path) {
            if (db_) return true;
            if (sqlite3_open(path.c_str(), &db_) != SQLITE_OK) {
                if (db_) sqlite3_close(db_);
                db_ = nullptr;
                return false;
            }
            if (sqlite3_exec(db_, "PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;",
                nullptr, nullptr, nullptr) == SQLITE_OK) return true;
            close();
            return false;
        }

        void close() {
            if (!db_) return;
            if (sqlite3_close(db_) == SQLITE_OK) db_ = nullptr;
        }

        sqlite3 *const &connection() const { return db_; }
    private:
        sqlite3 *db_ = nullptr;
    };
}
