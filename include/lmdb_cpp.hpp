// Copyright (c) 2020-2023, Brandon Lehmann
//
// Redistribution and use in source and binary forms, with or without modification, are
// permitted provided that the following conditions are met:
//
// 1. Redistributions of source code must retain the above copyright notice, this list of
//    conditions and the following disclaimer.
//
// 2. Redistributions in binary form must reproduce the above copyright notice, this list
//    of conditions and the following disclaimer in the documentation and/or other
//    materials provided with the distribution.
//
// 3. Neither the name of the copyright holder nor the names of its contributors may be
//    used to endorse or promote products derived from this software without specific
//    prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY
// EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
// MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
// THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
// PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
// INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
// STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF
// THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

#ifndef LMDB_CPP_HPP
#define LMDB_CPP_HPP

#include "lmdb_errors.hpp"
#include "thread_safe_map.hpp"

#include <lmdb.h>
#include <memory>
#include <mutex>
#include <tuple>
#include <vector>

namespace LMDB
{
    class Environment;

    class Database;

    class Transaction;

    class Cursor;

    /// The standard value type -- just a byte vector. Every key and value you
    /// read back from the database comes as one of these.
    typedef std::vector<unsigned char> mdb_result_t;

    /**
     * The top-level entry point for working with LMDB.
     *
     * Each Environment maps to a single file (or directory) on disk and acts as
     * a singleton: calling `Environment::instance("my.db")` twice gives you the
     * same shared pointer both times. From here you open databases, create
     * transactions, and manage the memory map.
     *
     *     auto env = LMDB::Environment::instance("my.db");
     *     auto db  = env->database("users");
     */
    class Environment
    {
        friend class Database;

        friend class Transaction;

      public:
        Environment() = delete;

        ~Environment();

        /**
         * Creates a full backup of this environment at the given path.
         *
         * If the target already exists it will be overwritten. Pass
         * MDB_CP_COMPACT as flags to compact the copy while you're at it.
         */
        [[nodiscard]] Error copy(const std::string &path, unsigned int flags = 0);

        /**
         * Opens (or returns an already-open) named database inside this
         * environment. Each database is its own key space.
         *
         * Set enable_compression to true if you want values Snappy-compressed
         * before they hit disk. Compression is per-database and transparent --
         * reads decompress automatically.
         *
         * @param name             logical name (empty string = the default DB)
         * @param enable_compression  compress values with Snappy before writing
         * @param flags            additional MDB_dbi flags
         */
        std::shared_ptr<Database>
            database(const std::string &name = "", bool enable_compression = false, int flags = 0);

        /**
         * Re-reads the memory map size from disk. Useful when another process
         * has grown the map. Requires that no read/write transactions are open.
         */
        [[nodiscard]] Error detect_map_size() const;

        /**
         * Grows the memory map by the growth factor that was set when the
         * environment was created. Requires no open read/write transactions.
         */
        [[nodiscard]] Error expand();

        /**
         * Grows the memory map by a specific number of pages.
         * Requires no open read/write transactions.
         */
        [[nodiscard]] Error expand(size_t pages);

        /**
         * Flushes OS-buffered data to disk. LMDB already flushes on every
         * commit unless you opened with MDB_NOSYNC, so you usually don't need
         * this. Set force=true for a synchronous flush.
         */
        [[nodiscard]] Error flush(bool force = false);

        /// Returns the environment's current flags (e.g. MDB_NOSUBDIR, MDB_RDONLY).
        [[nodiscard]] std::tuple<Error, unsigned int> get_flags() const;

        /// Returns low-level environment info (map size, last page, etc.).
        [[nodiscard]] std::tuple<Error, MDB_envinfo> info() const;

        /**
         * Opens (or returns the existing) LMDB environment at the given path.
         *
         * This is the main way you create an Environment. Calling it more than
         * once with the same path gives you the same shared pointer back.
         *
         * @param path            filesystem path to the environment file/directory
         * @param flags           MDB environment flags (default: MDB_NOSUBDIR)
         * @param mode            UNIX file permissions for the environment file
         * @param growth_factor   how many MB to grow the map each time expand() is called
         * @param max_databases   how many named databases this environment can hold
         */
        static std::shared_ptr<Environment> instance(
            const std::string &path,
            int flags = MDB_NOSUBDIR,
            int mode = 0600,
            size_t growth_factor = 8,
            unsigned int max_databases = 8);

        /// Returns the maximum key size (in bytes) that this environment supports.
        [[nodiscard]] std::tuple<Error, size_t> max_key_size() const;

        /// Returns the maximum number of reader slots for this environment.
        [[nodiscard]] std::tuple<Error, size_t> max_readers() const;

        /// Returns how many read/write transactions are currently open.
        size_t open_transactions() const;

        /// Changes the environment's flags at runtime.
        [[nodiscard]] Error set_flags(int flags, bool flag_state);

        /// Returns page-level statistics (page size, depth, entries, etc.).
        [[nodiscard]] std::tuple<Error, MDB_stat> stats() const;

        /**
         * Creates a new transaction that isn't yet attached to any database.
         * Call txn->use(db) before performing any operations.
         *
         * Prefer Database::transaction() when you're only working with a single
         * database -- it sets things up for you.
         *
         * @param readonly  true for a read-only transaction
         */
        std::shared_ptr<Transaction> transaction(bool readonly = false) const;

        /// Returns the underlying LMDB library version as {major, minor, patch}.
        static std::tuple<int, int, int> version();

      private:
        Environment(std::string env_path, size_t growth_factor);

        std::tuple<Error, size_t> memory_to_pages(size_t memory) const;

        void transaction_register();

        void transaction_unregister();

        std::shared_ptr<MDB_env *> env = std::make_shared<MDB_env *>();

        static inline ThreadSafeMap<std::string, std::shared_ptr<Environment>> environments;

        std::string path;

        size_t growth_factor = 0, open_txns = 0;

        mutable std::mutex mutex, txn_mutex;

        ThreadSafeMap<std::string, std::shared_ptr<Database>> databases;
    };

    /**
     * A named key-value store inside an Environment.
     *
     * You won't construct these directly -- use Environment::database() instead.
     * Each Database is a singleton per name within its environment.
     *
     * For simple one-shot operations (put a value, get a value, delete a key),
     * the convenience methods on this class handle the transaction for you and
     * automatically retry when the map runs out of space.
     *
     *     auto db = env->database("users");
     *     db->put_key(user_id, payload);
     *     auto [err, data] = db->get_key(user_id);
     */
    class Database
    {
        friend class Environment;

        friend class Transaction;

        friend class Cursor;

      public:
        Database() = delete;

        ~Database();

        /// Returns true if this database was opened with Snappy compression enabled.
        bool compressed() const;

        /// Counts and returns the total number of key-value pairs in the database.
        size_t count();

        /**
         * Deletes the given key (and its value). Opens a transaction, deletes,
         * and commits for you. Automatically retries after expanding the map if
         * it fills up.
         */
        [[nodiscard]] Error del(const void *key, size_t length);

        /// Template convenience for del() -- works with std::string, std::vector, etc.
        template<typename KeyType> [[nodiscard]] Error del_key(const KeyType &key)
        {
            return del(key.data(), key.size());
        }

        /**
         * Deletes a specific key+value pair. Useful with MDB_DUPSORT databases
         * where a single key can have multiple values.
         */
        [[nodiscard]] Error del(const void *key, size_t key_length, const void *value, size_t value_length);

        /// Template convenience for the key+value delete.
        template<typename KeyType, typename ValueType>
        [[nodiscard]] Error del_key(const KeyType &key, const ValueType &value)
        {
            return del(key.data(), key.size(), value.data(), value.size());
        }

        /**
         * Empties out every key-value pair in the database. If delete_db is
         * true, the database itself is also removed from the environment.
         */
        [[nodiscard]] Error drop(bool delete_db);

        /// Returns true if the key exists in the database.
        bool exists(const void *key, size_t length);

        /// Template convenience for exists().
        template<typename KeyType> bool exists_key(const KeyType &key)
        {
            return exists(key.data(), key.size());
        }

        /**
         * Retrieves the value stored at the given key. Returns {error, data}
         * where data is empty if the key wasn't found.
         */
        [[nodiscard]] std::tuple<Error, mdb_result_t> get(const void *key, size_t length);

        /// Template convenience for get().
        template<typename KeyType> [[nodiscard]] std::tuple<Error, mdb_result_t> get_key(const KeyType &key)
        {
            return get(key.data(), key.size());
        }

        /**
         * Retrieves every value in the database. Heads up -- this scans the
         * entire keyspace, so it will be slow on large databases.
         */
        std::vector<mdb_result_t> get_all();

        /// Returns the MDB_dbi flags for this database handle.
        [[nodiscard]] std::tuple<Error, unsigned int> get_flags();

        /**
         * Returns all keys in the database. Duplicate keys are skipped by
         * default (set ignore_duplicates=false to include them).
         */
        std::vector<mdb_result_t> list_keys(bool ignore_duplicates = true);

        /**
         * Stores a key-value pair. Opens a transaction, writes, and commits
         * for you. Automatically retries after expanding the map if it fills up.
         */
        [[nodiscard]] Error
            put(const void *key, size_t key_length, const void *value, size_t value_length, int flags = 0);

        /// Template convenience for put().
        template<typename KeyType, typename ValueType>
        [[nodiscard]] Error put_key(const KeyType &key, const ValueType &value, int flags = 0)
        {
            return put(key.data(), key.size(), value.data(), value.size(), flags);
        }

        /**
         * Opens a transaction scoped to this database.
         *
         * @param readonly  true for a read-only transaction
         */
        std::shared_ptr<Transaction> transaction(bool readonly = false);

      private:
        explicit Database(
            std::shared_ptr<Environment> &environment,
            const std::string &name = "",
            int flags = MDB_CREATE,
            bool enable_compression = false);

        MDB_dbi dbi = 0;

        std::string name;

        bool compression = false;

        std::shared_ptr<Environment> environment;

        mutable std::mutex mutex;
    };

    /**
     * An RAII transaction wrapper.
     *
     * If a Transaction goes out of scope without being committed, it
     * automatically aborts -- so if your code throws or returns early, the
     * database stays consistent.
     *
     * You can work across multiple databases in a single transaction by calling
     * use() to switch which database operations target:
     *
     *     auto txn = env->transaction();
     *     txn->use(db_users);
     *     txn->put_key(user_id, user_data);
     *     txn->use(db_sessions);
     *     txn->put_key(session_id, session_data);
     *     txn->commit();
     *
     * If you're using Transaction::put/del directly (not the Database
     * convenience wrappers), you'll need to handle MDB_MAP_FULL yourself by
     * aborting, expanding the environment, and retrying.
     */
    class Transaction
    {
        friend class Environment;

        friend class Database;

        friend class Cursor;

      public:
        Transaction() = delete;

        ~Transaction();

        /// Aborts the transaction, discarding all changes made since it was opened.
        void abort();

        /// Commits all changes made in this transaction to disk.
        [[nodiscard]] Error commit();

        /// Opens a cursor for iterating over key-value pairs within this transaction.
        std::shared_ptr<Cursor> cursor();

        /// Deletes the given key from the current database.
        [[nodiscard]] Error del(const void *key, size_t length);

        /// Template convenience for del().
        template<typename KeyType> [[nodiscard]] Error del_key(const KeyType &key)
        {
            return del(key.data(), key.size());
        }

        /**
         * Deletes a specific key+value pair. With MDB_DUPSORT databases, this
         * removes only the matching value rather than all values for that key.
         */
        [[nodiscard]] Error del(const void *key, size_t key_length, const void *value, size_t value_length);

        /// Template convenience for the key+value delete.
        template<typename KeyType, typename ValueType>
        [[nodiscard]] Error del_key(const KeyType &key, const ValueType &value)
        {
            return del(key.data(), key.size(), value.data(), value.size());
        }

        /// Returns true if the given key exists in the current database.
        bool exists(const void *key, size_t length);

        /// Template convenience for exists().
        template<typename KeyType> bool exists_key(const KeyType &key)
        {
            return exists(key.data(), key.size());
        }

        /// Retrieves the value stored at the given key. Returns {error, data}.
        [[nodiscard]] std::tuple<Error, mdb_result_t> get(const void *key, size_t length);

        /// Template convenience for get().
        template<typename KeyType> [[nodiscard]] std::tuple<Error, mdb_result_t> get_key(const KeyType &key)
        {
            return get(key.data(), key.size());
        }

        /**
         * Returns this transaction's ID. A return value of 0 means the
         * transaction has already been committed or aborted.
         */
        [[nodiscard]] std::tuple<Error, size_t> id() const;

        /**
         * Stores a key-value pair in the current database.
         *
         * Unlike Database::put(), this does NOT auto-retry on MDB_MAP_FULL.
         * You'll need to abort, call env->expand(), and retry yourself.
         */
        [[nodiscard]] Error
            put(const void *key, size_t key_length, const void *value, size_t value_length, int flags = 0);

        /// Template convenience for put().
        template<typename KeyType, typename ValueType>
        [[nodiscard]] Error put_key(const KeyType &key, const ValueType &value, int flags = 0)
        {
            return put(key.data(), key.size(), value.data(), value.size(), flags);
        }

        /// Returns true if this is a read-only transaction.
        [[nodiscard]] bool readonly() const;

        /// Renews a read-only transaction that was previously reset().
        [[nodiscard]] Error renew();

        /// Releases a read-only transaction's resources without destroying it.
        /// Call renew() to reuse it later.
        void reset();

        /**
         * Switches this transaction to target a different database. This is how
         * you perform operations across multiple databases in a single
         * atomic commit.
         */
        void use(const std::shared_ptr<Database> &database);

      private:
        explicit Transaction(std::shared_ptr<Environment> &environment, bool readonly = false);

        Transaction(
            std::shared_ptr<Environment> &environment,
            std::shared_ptr<Database> &database,
            bool readonly = false);

        void txn_setup();

        std::shared_ptr<MDB_txn *> txn;

        std::shared_ptr<Environment> environment;

        std::shared_ptr<Database> db;

        bool m_readonly = false;
    };

    /**
     * A positioned iterator over key-value pairs within a transaction.
     *
     * Cursors let you walk through the database in order, jump to a specific
     * key, and read or write at the current position. Create one from a
     * transaction:
     *
     *     auto txn    = db->transaction(true);
     *     auto cursor = txn->cursor();
     *     auto [err, key, value] = cursor->get(MDB_FIRST);
     */
    class Cursor
    {
        friend class Database;

        friend class Transaction;

      public:
        Cursor() = delete;

        ~Cursor();

        /// Returns how many duplicate values exist for the key at the current position.
        /// Only meaningful with MDB_DUPSORT databases.
        [[nodiscard]] std::tuple<Error, size_t> count();

        /// Deletes the key-value pair at the cursor's current position.
        [[nodiscard]] Error del(int flags = 0);

        /**
         * Moves the cursor according to the given operation (MDB_FIRST,
         * MDB_NEXT, MDB_LAST, MDB_PREV, etc.) and returns the key-value pair
         * at the new position.
         */
        [[nodiscard]] std::tuple<Error, mdb_result_t, mdb_result_t> get(const MDB_cursor_op &op = MDB_FIRST);

        /// Positions the cursor at the given key and returns the pair found there.
        [[nodiscard]] std::tuple<Error, mdb_result_t, mdb_result_t>
            get(const void *key, size_t length, const MDB_cursor_op &op = MDB_SET);

        /// Template convenience for get-by-key.
        template<typename KeyType>
        [[nodiscard]] std::tuple<Error, mdb_result_t, mdb_result_t>
            get_key(const KeyType &key, const MDB_cursor_op &op = MDB_SET)
        {
            return get(key.data(), key.size(), op);
        }

        /**
         * Retrieves all duplicate values for a single key (MDB_DUPSORT
         * databases). Returns the key plus a vector of all its values.
         */
        [[nodiscard]] std::tuple<Error, mdb_result_t, std::vector<mdb_result_t>>
            get_all(const void *key, size_t length);

        /// Template convenience for get_all().
        template<typename KeyType>
        [[nodiscard]] std::tuple<Error, mdb_result_t, std::vector<mdb_result_t>> get_all_key(const KeyType &key)
        {
            return get_all(key.data(), key.size());
        }

        /**
         * Writes a key-value pair and positions the cursor at the new entry.
         *
         * Like Transaction::put(), this does NOT auto-retry on MDB_MAP_FULL --
         * you'll need to handle that yourself.
         */
        [[nodiscard]] Error
            put(const void *key, size_t key_length, const void *value, size_t value_length, int flags = 0);

        /// Template convenience for put().
        template<typename KeyType, typename ValueType>
        [[nodiscard]] Error put_key(const KeyType &key, const ValueType &value, int flags = 0)
        {
            return put(key.data(), key.size(), value.data(), value.size(), flags);
        }

        /// Returns true if this cursor belongs to a read-only transaction.
        [[nodiscard]] bool readonly() const;

        /// Renews a cursor that was created in a renewed read-only transaction.
        [[nodiscard]] Error renew();

      private:
        Cursor(std::shared_ptr<MDB_txn *> &txn, std::shared_ptr<Database> &db, bool readonly = false);

        MDB_cursor *cursor = nullptr;

        std::shared_ptr<Database> db;

        std::shared_ptr<MDB_txn *> txn;

        bool m_readonly = false;
    };
} // namespace LMDB

#endif
