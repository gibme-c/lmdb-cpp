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
    /**
     * Forward declarations
     */
    class Environment;

    class Database;

    class Transaction;

    class Cursor;

    // shorthand typedef
    typedef std::vector<unsigned char> mdb_result_t;

    /**
     * Wraps the LMDB C API into an OOP model that allows for opening and using
     * multiple environments and databases at once.
     */
    class Environment
    {
        friend class Database;

        friend class Transaction;

      public:
        Environment() = delete;

        ~Environment();

        /**
         * Copies (creates a backup) the environment to the path specified.
         * This method will overwrite the target path
         *
         * Note: Compaction is available with the flag of MDB_CP_COMPACT
         *
         * @param path
         * @param flags
         * @return
         */
        Error copy(const std::string &path, unsigned int flags = 0);

        /**
         * Opens a database (separate key space) in the environment as a logical
         * partitioning of data.
         *
         * @param name
         * @param enable_compression -- if enabled, VALUES will be compressed before writing into the database
         * @param flags
         * @return
         */
        std::shared_ptr<Database>
            database(const std::string &name = "", bool enable_compression = false, int flags = 0);

        /**
         * Detects the current memory map size if it has been changed elsewhere
         * This requires that there are no open R/W transactions; otherwise, the method
         * will throw an exception.
         *
         * @return
         */
        Error detect_map_size() const;

        /**
         * Expands the memory map by the growth factor supplied to the constructor
         * This requires that there are no open R/W transactions; otherwise, the method
         * will throw an exception.
         *
         * @return
         */
        Error expand();

        /**
         * Expands the memory map by the number of pages specified.
         * This requires that there are no open R/W transactions; otherwise, the method
         * will throw an exception.
         *
         * @param pages
         * @return
         */
        Error expand(size_t pages);

        /**
         * Flush the data buffers to disk.
         * Data is always written to disk when a transaction is committed, but the operating system may keep it
         * buffered. LMDB always flushes the OS buffers upon commit as well, unless the environment was opened
         * with MDB_NOSYNC or in part MDB_NOMETASYNC.
         *
         * This call is not valid if the environment was opened with MDB_RDONLY.
         *
         * @param force a synchronous flush of the buffers to disk
         * @return
         */
        Error flush(bool force = false);

        /**
         * Retrieves the LMDB environment flags
         *
         * @return
         */
        std::tuple<Error, unsigned int> get_flags() const;

        /**
         * Retrieves the LMDB environment information
         *
         * @return
         */
        std::tuple<Error, MDB_envinfo> info() const;

        /**
         * Opens a LMDB environment using the specified parameters
         *
         * @param path the file system path to the environment
         * @param flags
         * @param mode
         * @param growth_factor the growth rate, in MB, of the environment. The initial environment size will be set
         *   to this value in MB and each `expand()` call will result in the environment growing by this many MB
         * @param max_databases
         * @return
         */
        static std::shared_ptr<Environment> instance(
            const std::string &path,
            int flags = MDB_NOSUBDIR,
            int mode = 0600,
            size_t growth_factor = 8,
            unsigned int max_databases = 8);

        /**
         * Retrieves the maximum byte size of a key in the LMDB environment
         *
         * @return
         */
        std::tuple<Error, size_t> max_key_size() const;

        /**
         * Retrieves the maximum number of readers for the LMDB environment
         *
         * @return
         */
        std::tuple<Error, size_t> max_readers() const;

        /**
         * Returns the number of open R/W transactions in the environment
         *
         * @return
         */
        size_t open_transactions() const;

        /**
         * Sets/changes the LMDB environment flags
         *
         * @param flags
         * @param flag_state
         * @return
         */
        Error set_flags(int flags, bool flag_state);

        /**
         * Retrieves the LMDB environment statistics
         *
         * @return
         */
        std::tuple<Error, MDB_stat> stats() const;

        /**
         * Retrieves the current LMDB library version
         *
         * @return [major, minor, patch]
         */
        static std::tuple<int, int, int> version();

      private:
        /**
         * Creates a new instance of an environment
         *
         * @param env_path
         * @param growth_factor
         */
        Environment(std::string env_path, size_t growth_factor);

        /**
         * Converts the bytes of memory specified into LMDB pages (rounded up)
         *
         * @param memory
         * @return
         */
        std::tuple<Error, size_t> memory_to_pages(size_t memory) const;

        /**
         * Registers a new transaction in the environment
         */
        void transaction_register();

        /**
         * Un-registers a transaction from the environment
         */
        void transaction_unregister();

        std::shared_ptr<MDB_env *> env = std::make_shared<MDB_env *>();

        static inline ThreadSafeMap<std::string, std::shared_ptr<Environment>> environments;

        std::string path;

        size_t growth_factor = 0, open_txns = 0;

        mutable std::mutex mutex, txn_mutex;

        ThreadSafeMap<std::string, std::shared_ptr<Database>> databases;
    };

    /**
     * Provides a Database model for use within an LMDB environment
     */
    class Database
    {
        friend class Environment;

        friend class Transaction;

        friend class Cursor;

      public:
        Database() = delete;

        ~Database();

        /**
         * Returns if the database keys and values are compressed
         *
         * @return
         */
        bool compressed() const;

        /**
         * Returns how many key/value pairs currently exist in the database
         *
         * @return
         */
        size_t count();

        /**
         * Simplified deletion of the given key and its value. Automatically opens a
         * transaction, deletes the key, and commits the transaction, then returns.
         *
         * If we encounter MDB_MAP_FULL, we will automatically retry the transaction after
         * attempting to expand the database
         *
         * @param key
         * @param length
         * @return
         */
        Error del(const void *key, size_t length);

        /**
         * Simplified deletion of the given key and its value. Automatically opens a
         * transaction, deletes the key, and commits the transaction, then returns.
         *
         * If we encounter MDB_MAP_FULL, we will automatically retry the transaction after
         * attempting to expand the database
         *
         * @tparam KeyType
         * @param key
         * @return
         */
        template<typename KeyType> Error del_key(const KeyType &key)
        {
            return del(key.data(), key.size());
        }

        /**
         * Simplified deletion of the given key with the given value. Automatically
         * opens a transaction, deletes the value, and commits the transaction, then
         * returns.
         *
         * If we encounter MDB_MAP_FULL, we will automatically retry the transaction after
         * attempting to expand the database
         *
         * @param key
         * @param key_length
         * @param value
         * @param value_length
         * @return
         */
        Error del(const void *key, size_t key_length, const void *value, size_t value_length);

        /**
         * Simplified deletion of the given key with the given value. Automatically
         * opens a transaction, deletes the value, and commits the transaction, then
         * returns.
         *
         * If we encounter MDB_MAP_FULL, we will automatically retry the transaction after
         * attempting to expand the database
         *
         * @tparam KeyType
         * @tparam ValueType
         * @param key
         * @param value
         * @return
         */
        template<typename KeyType, typename ValueType> Error del_key(const KeyType &key, const ValueType &value)
        {
            return del(key.data(), key.size(), value.data(), value.size());
        }

        /**
         * Empties all of the key/value pairs from the database
         *
         * @param delete_db if specified, also deletes the database from the environment
         * @return
         */
        Error drop(bool delete_db);

        /**
         * Returns if the key exists in the database
         *
         * @param key
         * @param length
         * @return
         */
        bool exists(const void *key, size_t length);

        /**
         * Returns if the key exists in the database
         *
         * @tparam KeyType
         * @param key
         * @return
         */
        template<typename KeyType> bool exists_key(const KeyType &key)
        {
            return exists(key.data(), key.size());
        }

        /**
         * Simplified retrieval of the value at the specified key which opens a new
         * readonly transaction, retrieves the value, and then returns it as the
         * specified type
         *
         * @param key
         * @param length
         * @return
         */
        std::tuple<Error, mdb_result_t> get(const void *key, size_t length);

        /**
         * Simplified retrieval of the value at the specified key which opens a new
         * readonly transaction, retrieves the value, and then returns it as the
         * specified type
         *
         * @tparam KeyType
         * @param key
         * @return
         */
        template<typename KeyType> std::tuple<Error, mdb_result_t> get_key(const KeyType &key)
        {
            return get(key.data(), key.size());
        }

        /**
         * Simplifies retrieval of all values for all keys in the database
         *
         * WARNING: Very likely slow with large key sets
         *
         * @return
         */
        std::vector<mdb_result_t> get_all();

        /**
         * Retrieves the database flags
         *
         * @return
         */
        std::tuple<Error, unsigned int> get_flags();

        /**
         * Lists all keys in the database
         *
         * @param ignore_duplicates
         * @return
         */
        std::vector<mdb_result_t> list_keys(bool ignore_duplicates = true);

        /**
         * Simplified put which opens a new transaction, puts the value, and then returns.
         *
         * If we encounter MDB_MAP_FULL, we will automatically retry the transaction after
         * attempting to expand the database
         *
         * @param key
         * @param key_length
         * @param value
         * @param value_length
         * @param flags
         * @return
         */
        Error put(const void *key, size_t key_length, const void *value, size_t value_length, int flags = 0);

        /**
         * Simplified put which opens a new transaction, puts the value, and then returns.
         *
         * If we encounter MDB_MAP_FULL, we will automatically retry the transaction after
         * attempting to expand the database
         *
         * @tparam KeyType
         * @tparam ValueType
         * @param key
         * @param value
         * @param flags
         * @return
         */
        template<typename KeyType, typename ValueType>
        Error put_key(const KeyType &key, const ValueType &value, int flags = 0)
        {
            return put(key.data(), key.size(), value.data(), value.size(), flags);
        }

        /**
         * Opens a transaction in the database
         *
         * @param readonly
         * @return
         */
        std::shared_ptr<Transaction> transaction(bool readonly = false);

      private:
        /**
         * Opens the database within the specified environment
         *
         * @param environment
         * @param name
         * @param flags
         * @param enable_compression
         */
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
     * Provides a transaction model for use within a LMDB database
     *
     * Please note: A transaction will abort() automatically if it has not been committed before
     * it leaves the scope it was created in. This helps to maintain database integrity as if
     * your work in pushing to a transaction fails and throws, the transaction will always be
     * reverted.
     */
    class Transaction
    {
        friend class Database;

        friend class Cursor;

      public:
        Transaction() = delete;

        ~Transaction();

        /**
         * Aborts the currently open transaction
         */
        void abort();

        /**
         * Commits the currently open transaction
         *
         * @return
         */
        Error commit();

        /**
         * Opens a LMDB cursor within the transaction
         *
         * @return
         */
        std::shared_ptr<Cursor> cursor();

        /**
         * Deletes the provided key
         *
         * @param key
         * @param length
         * @return
         */
        Error del(const void *key, size_t length);

        /**
         * Deletes the provided key
         *
         * @tparam KeyType
         * @param key
         * @return
         */
        template<typename KeyType> Error del_key(const KeyType &key)
        {
            return del(key.data(), key.size());
        }

        /**
         * Deletes the provided key with the provided value.
         *
         * If the database supports sorted duplicates and the data parameter is NULL, all of the duplicate data items
         * for the key will be deleted. Otherwise, if the data parameter is non-NULL only the matching data item will
         * be deleted.
         *
         * @param key
         * @param key_length
         * @param value
         * @param value_length
         * @return
         */
        Error del(const void *key, size_t key_length, const void *value, size_t value_length);

        /**
         * Deletes the provided key with the provided value.
         *
         * If the database supports sorted duplicates and the data parameter is NULL, all of the duplicate data items
         * for the key will be deleted. Otherwise, if the data parameter is non-NULL only the matching data item will
         * be deleted.
         *
         * @tparam KeyType
         * @tparam ValueType
         * @param key
         * @param value
         * @return
         */
        template<typename KeyType, typename ValueType> Error del_key(const KeyType &key, const ValueType &value)
        {
            return del(key.data(), key.size(), value.data(), value.size());
        }

        /**
         * Checks if the given key exists in the database
         *
         * @param key
         * @param length
         * @return
         */
        bool exists(const void *key, size_t length);

        /**
         * Checks if the given key exists in the database
         *
         * @tparam KeyType
         * @param key
         * @return
         */
        template<typename KeyType> bool exists_key(const KeyType &key)
        {
            return exists(key.data(), key.size());
        }

        /**
         * Retrieves the value stored with the specified key
         *
         * @param key
         * @param length
         * @return
         */
        std::tuple<Error, mdb_result_t> get(const void *key, size_t length);

        /**
         * Retrieves the value stored with the specified key
         *
         * @tparam KeyType
         * @param key
         * @return
         */
        template<typename KeyType> std::tuple<Error, mdb_result_t> get_key(const KeyType &key)
        {
            return get(key.data(), key.size());
        }

        /**
         * Returns the transaction ID
         *
         * If a 0 value is returned, the transaction is complete [abort() or commit() has been used]
         *
         * @return
         */
        [[nodiscard]] std::tuple<Error, size_t> id() const;

        /**
         * Puts the specified value with the specified key in the database using the specified flag(s)
         *
         * Note: You must check for MDB_MAP_FULL or MDB_TXN_FULL response values and handle those
         * yourself as you will very likely need to abort the current transaction and expand
         * the LMDB environment before re-attempting the transaction.
         *
         * @param key
         * @param key_length
         * @param value
         * @param value_length
         * @param flags
         * @return
         */
        Error put(const void *key, size_t key_length, const void *value, size_t value_length, int flags = 0);

        /**
         * Puts the specified value with the specified key in the database using the specified flag(s)
         *
         * Note: You must check for MDB_MAP_FULL or MDB_TXN_FULL response values and handle those
         * yourself as you will very likely need to abort the current transaction and expand
         * the LMDB environment before re-attempting the transaction.
         *
         * @tparam KeyType
         * @tparam ValueType
         * @param key
         * @param value
         * @param flags
         * @return
         */
        template<typename KeyType, typename ValueType>
        Error put_key(const KeyType &key, const ValueType &value, int flags = 0)
        {
            return put(key.data(), key.size(), value.data(), value.size(), flags);
        }

        /**
         * Returns if the transaction is readonly or not
         *
         * @return
         */
        [[nodiscard]] bool readonly() const;

        /**
         * Renew a read-only transaction that has been previously reset()
         *
         * @return
         */
        Error renew();

        /**
         * Reset a read-only transaction.
         */
        void reset();

      private:
        /**
         * Constructs a new transaction in the environment specified
         *
         * @param environment
         * @param readonly
         */
        explicit Transaction(std::shared_ptr<Environment> &environment, bool readonly = false);

        /**
         * Constructs a new transaction in the environment and database specified
         *
         * @param environment
         * @param database
         * @param readonly
         */
        Transaction(
            std::shared_ptr<Environment> &environment,
            std::shared_ptr<Database> &database,
            bool readonly = false);

        /**
         * Sets up the transaction via the multiple entry methods
         */
        void txn_setup();

        std::shared_ptr<MDB_txn *> txn;

        std::shared_ptr<Environment> environment;

        std::shared_ptr<Database> db;

        bool m_readonly = false;
    };

    /**
     * Provides a Cursor model for use within a LMDB transaction
     */
    class Cursor
    {
        friend class Database;

        friend class Transaction;

      public:
        Cursor() = delete;

        ~Cursor();

        /**
         * Return count of duplicates for current key.
         *
         * @return
         */
        std::tuple<Error, size_t> count();

        /**
         * Delete current key/data pair.
         *
         * @param flags
         * @return
         */
        Error del(int flags = 0);

        /**
         * Retrieve key/value pairs by cursor.
         *
         * @param op
         * @return
         */
        std::tuple<Error, mdb_result_t, mdb_result_t> get(const MDB_cursor_op &op = MDB_FIRST);

        /**
         * Retrieve key/value pairs by cursor.
         *
         * @param key
         * @param length
         * @param op
         * @return
         */
        std::tuple<Error, mdb_result_t, mdb_result_t>
            get(const void *key, size_t length, const MDB_cursor_op &op = MDB_SET);

        /**
         * Retrieve key/value pairs by cursor.
         *
         * @tparam KeyType
         * @param key
         * @param op
         * @return
         */
        template<typename KeyType>
        std::tuple<Error, mdb_result_t, mdb_result_t> get_key(const KeyType &key, const MDB_cursor_op &op = MDB_SET)
        {
            return get(key.data(), key.size(), op);
        }

        /**
         * Retrieve multiple values for a single key from the database
         *
         * Requires that MDB_DUPSORT was used when opening the database
         *
         * @param key
         * @param length
         * @return
         */
        std::tuple<Error, mdb_result_t, std::vector<mdb_result_t>> get_all(const void *key, size_t length);

        /**
         * Retrieve multiple values for a single key from the database
         *
         * Requires that MDB_DUPSORT was used when opening the database
         *
         * @tparam KeyType
         * @param key
         * @return
         */
        template<typename KeyType>
        std::tuple<Error, mdb_result_t, std::vector<mdb_result_t>> get_all_key(const KeyType &key)
        {
            return get_all(key.data(), key.size());
        }

        /**
         * Puts the specified value with the specified key in the database using the specified flag(s)
         * and places the cursor at the position of the new item or, near it upon failure.
         *
         * Note: You must check for MDB_MAP_FULL or MDB_TXN_FULL response values and handle those
         * yourself as you will very likely need to abort the current transaction and expand
         * the LMDB environment before re-attempting the transaction.
         *
         * @param key
         * @param key_length
         * @param value
         * @param value_length
         * @param flags
         * @return
         */
        Error put(const void *key, size_t key_length, const void *value, size_t value_length, int flags = 0);

        /**
         * Puts the specified value with the specified key in the database using the specified flag(s)
         * and places the cursor at the position of the new item or, near it upon failure.
         *
         * Note: You must check for MDB_MAP_FULL or MDB_TXN_FULL response values and handle those
         * yourself as you will very likely need to abort the current transaction and expand
         * the LMDB environment before re-attempting the transaction.
         *
         * @tparam KeyType
         * @tparam ValueType
         * @param key
         * @param value
         * @param flags
         * @return
         */
        template<typename KeyType, typename ValueType>
        Error put_key(const KeyType &key, const ValueType &value, int flags = 0)
        {
            return put(key.data(), key.size(), value.data(), value.size(), flags);
        }

        /**
         * Returns if the cursor is readonly
         *
         * @return
         */
        [[nodiscard]] bool readonly() const;

        /**
         * Renews the cursor
         *
         * @return
         */
        Error renew();

      private:
        /**
         * Creates a new LMDB Cursor within the specified transaction and database
         *
         * @param txn
         * @param db
         * @param readonly
         */
        Cursor(std::shared_ptr<MDB_txn *> &txn, std::shared_ptr<Database> &db, bool readonly = false);

        MDB_cursor *cursor = nullptr;

        std::shared_ptr<Database> db;

        std::shared_ptr<MDB_txn *> txn;

        bool m_readonly = false;
    };
} // namespace LMDB

#endif
