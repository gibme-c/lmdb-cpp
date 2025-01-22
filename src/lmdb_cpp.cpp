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

#include "lmdb_cpp.hpp"

#include <cmath>
#include <cppfs/FileHandle.h>
#include <cppfs/fs.h>
#include <snappy.h>
#include <utility>

#define MAKE_LMDB_ERROR(code) Error(code, __LINE__, __FILE__)
#define MAKE_LMDB_ERROR_MSG(code, message) Error(code, message, __LINE__, __FILE__)
#define LMDB_SPACE_MULTIPLIER (1024 * 1024) // to MB
#define LMDB_CHECK_TXN_EXPAND(error, env, txn, label)     \
    if (error == LMDB_MAP_FULL || error == LMDB_TXN_FULL) \
    {                                                     \
        txn->abort();                                     \
                                                          \
        const auto exp_error = env->expand();             \
                                                          \
        if (!exp_error)                                   \
        {                                                 \
            goto label;                                   \
        }                                                 \
    }
#define LMDB_LOAD_VALUE(input, length, output, compressed)      \
    auto output##_temp = load_value(input, length, compressed); \
    auto output = load_val(output##_temp)

namespace LMDB
{
    /**
     * Converts the LMDB error code into a string using `mdb_strerror()`
     *
     * @param retval
     * @return
     */
    static inline std::string mdb_error(const int retval)
    {
        return {mdb_strerror(retval)};
    }

    /**
     * Loads a pointer based value into a std::vector<unsigned char> so that we stop carrying
     * pointers around that are prone to issues when working with memory mapped databases.
     *
     * @param value
     * @param length
     * @param compressed if set, will compress the result using snappy compression before returning
     * @return
     */
    static inline mdb_result_t load_value(const void *value, size_t length, bool compressed)
    {
        auto result =
            mdb_result_t(static_cast<const unsigned char *>(value), static_cast<const unsigned char *>(value) + length);

        if (!compressed)
        {
            return result;
        }

        auto temp = std::string();

        snappy::Compress(reinterpret_cast<const char *>(result.data()), result.size(), &temp);

        return {temp.begin(), temp.end()};
    }

    /**
     * Loads a std::vector<unsigned char> into a MDB_val for low-level LMDB calls
     *
     * @param value
     * @return
     */
    static inline MDB_val load_val(const mdb_result_t &value)
    {
        return {value.size(), (void *)value.data()};
    }

    /**
     * Loads the results of a LMDB call from a MDB_val into a std::vector<unsigned char> so that we stop
     * carrying pointers around that are prone to issues when working with memory mapped databases.
     *
     * @param value
     * @return
     */
    static inline mdb_result_t load_result(const MDB_val &value)
    {
        auto result = mdb_result_t(
            static_cast<const unsigned char *>(value.mv_data),
            static_cast<const unsigned char *>(value.mv_data) + value.mv_size);

        auto uncompressed = std::string();

        if (snappy::Uncompress(reinterpret_cast<const char *>(result.data()), result.size(), &uncompressed))
        {
            return {uncompressed.begin(), uncompressed.end()};
        }

        return result;
    }

    Environment::Environment(std::string env_path, size_t growth_factor):
        path(std::move(env_path)), growth_factor(growth_factor)
    {
    }

    Environment::~Environment()
    {
        std::scoped_lock lock(mutex);

        // clear the list of databases, which will destruct them and close them
        databases.clear();

        // flush the database to disk
        flush(true);

        mdb_env_close(*env);

        env = nullptr;

        if (environments.contains(path))
        {
            environments.erase(path);
        }
    }

    Error Environment::copy(const std::string &dst_path, unsigned int flags)
    {
        std::scoped_lock lock(mutex);

        auto file = cppfs::fs::open(dst_path);

        auto [error, env_flags] = get_flags();

        if (error)
        {
            throw std::runtime_error("Could not get environment flags");
        }

        // if the new path already exists, we need to clear it
        if (file.exists())
        {
            if (file.isFile())
            {
                file.remove();
            }
            else
            {
                file.removeDirectoryRec();
            }
        }

        if (!(env_flags & MDB_NOSUBDIR))
        {
            file.createDirectory();
        }

        const auto result = mdb_env_copy2(*env, file.path().c_str(), flags);

        return MAKE_LMDB_ERROR_MSG(result, mdb_error(result));
    }

    std::shared_ptr<Database> Environment::database(const std::string &name, bool enable_compression, int flags)
    {
        // if we haven't already opened this name database, we need to do so now
        if (!databases.contains(name))
        {
            /**
             * We grab the Environment instance this way to make sure that we are using the
             * shared pointer to the instance so we do not have a bunch of copies of our env
             * running around
             */
            auto _env = instance(path);

            // we create the shared pointer this way as our constructor is private to avoid public calls to it
            std::shared_ptr<Database> db(new Database(_env, name, flags, enable_compression));

            databases.insert(name, db);
        }

        return databases.at(name);
    }

    Error Environment::detect_map_size() const
    {
        std::scoped_lock lock(mutex);

        if (open_transactions() != 0)
        {
            return MAKE_LMDB_ERROR_MSG(
                LMDB_ERROR, "Cannot detect LMDB environment map size while transactions are open");
        }

        const auto result = mdb_env_set_mapsize(*env, 0);

        return MAKE_LMDB_ERROR_MSG(result, mdb_error(result));
    }

    Error Environment::expand()
    {
        const auto [error, pages] = memory_to_pages(growth_factor * LMDB_SPACE_MULTIPLIER);

        if (error)
        {
            return error;
        }

        return expand(pages);
    }

    Error Environment::expand(size_t pages)
    {
        std::scoped_lock lock(mutex);

        if (open_transactions() != 0)
        {
            return MAKE_LMDB_ERROR_MSG(
                LMDB_ERROR, "Cannot expand LMDB environment map size while transactions are open");
        }

        const auto [info_error, l_info] = info();

        if (info_error)
        {
            return info_error;
        }

        const auto [stats_error, l_stats] = stats();

        if (stats_error)
        {
            return stats_error;
        }

        const auto new_size = (l_stats.ms_psize * pages) + l_info.me_mapsize;

        const auto result = mdb_env_set_mapsize(*env, new_size);

        return MAKE_LMDB_ERROR_MSG(result, mdb_error(result));
    }

    Error Environment::flush(bool force)
    {
        std::scoped_lock lock(mutex);

        const auto result = mdb_env_sync(*env, (force) ? 1 : 0);

        return MAKE_LMDB_ERROR_MSG(result, mdb_error(result));
    }

    std::tuple<Error, unsigned int> Environment::get_flags() const
    {
        unsigned int flags;

        const auto result = mdb_env_get_flags(*env, &flags);

        return {MAKE_LMDB_ERROR_MSG(result, mdb_error(result)), flags};
    }

    std::tuple<Error, MDB_envinfo> Environment::info() const
    {
        MDB_envinfo info;

        const auto result = mdb_env_info(*env, &info);

        return {MAKE_LMDB_ERROR_MSG(result, mdb_error(result)), info};
    }

    std::shared_ptr<Environment> Environment::instance(
        const std::string &path,
        int flags,
        int mode,
        size_t growth_factor,
        unsigned int max_databases)
    {
        auto file = cppfs::fs::open(path);

        // if we have not already opened the environment at the specified path, we will do so now
        if (!Environment::environments.contains(file.path()))
        {
            // we create the shared pointer this way because the constructor is private to prevent public calls to it
            std::shared_ptr<Environment> db(new Environment(file.path(), growth_factor));

            if (flags & MDB_NOSUBDIR)
            {
                if (file.exists() && !file.isFile())
                {
                    throw std::runtime_error("LMDB path must be a regular file");
                }
            }
            else if (!file.isDirectory())
            {
                file.createDirectory();
            }

            // attempt to create the environment in the pointer
            auto success = mdb_env_create(db->env.get());

            if (success != MDB_SUCCESS)
            {
                throw std::runtime_error("Could not create LMDB environment: " + mdb_error(success));
            }

            // attempt to set the map size, this sets the minimum, if the environment is already bigger than
            // this will have no effect
            success = mdb_env_set_mapsize(*db->env, growth_factor * LMDB_SPACE_MULTIPLIER);

            if (success != MDB_SUCCESS)
            {
                throw std::runtime_error("Could not allocate initial LMDB memory map: " + mdb_error(success));
            }

            // attempt to set the maximum number of databases that can be opened in the environment
            success = mdb_env_set_maxdbs(*db->env, max_databases);

            if (success != MDB_SUCCESS)
            {
                throw std::runtime_error("Could not set maximum number of LMDB named databases: " + mdb_error(success));
            }

            /**
             * A transaction and its cursors must only be used by a single thread, and a thread may only have a
             * single write transaction at a time. If MDB_NOTLS is in use, this does not apply to read-only
             * transactions. This call actually opens the environment.
             */
            success = mdb_env_open(*db->env, file.path().c_str(), flags | MDB_NOTLS, mode);

            if (success != MDB_SUCCESS)
            {
                mdb_env_close(*db->env);

                throw std::runtime_error("Could not open LMDB database file [" + path + "]: " + mdb_error(success));
            }

            Environment::environments.insert(db->path, db);
        }

        return Environment::environments.at(file.path());
    }

    std::tuple<Error, size_t> Environment::max_key_size() const
    {
        const auto result = mdb_env_get_maxkeysize(*env);

        return {MAKE_LMDB_ERROR(SUCCESS), result};
    }

    std::tuple<Error, size_t> Environment::max_readers() const
    {
        unsigned int readers = 0;

        const auto result = mdb_env_get_maxreaders(*env, &readers);

        return {MAKE_LMDB_ERROR_MSG(result, mdb_error(result)), readers};
    }

    std::tuple<Error, size_t> Environment::memory_to_pages(size_t memory) const
    {
        const auto [error, l_stats] = stats();

        if (error)
        {
            return {error, 0};
        }

        return {MAKE_LMDB_ERROR(SUCCESS), size_t(ceil(double(memory) / double(l_stats.ms_psize)))};
    }

    size_t Environment::open_transactions() const
    {
        std::scoped_lock lock(txn_mutex);

        return open_txns;
    }

    Error Environment::set_flags(int flags, bool flag_state)
    {
        std::scoped_lock lock(mutex);

        const auto result = mdb_env_set_flags(*env, flags, (flag_state) ? 1 : 0);

        return MAKE_LMDB_ERROR_MSG(result, mdb_error(result));
    }

    std::tuple<Error, MDB_stat> Environment::stats() const
    {
        MDB_stat stats;

        const auto result = mdb_env_stat(*env, &stats);

        return {MAKE_LMDB_ERROR_MSG(result, mdb_error(result)), stats};
    }

    void Environment::transaction_register()
    {
        std::scoped_lock lock(txn_mutex);

        open_txns++;
    }

    void Environment::transaction_unregister()
    {
        std::scoped_lock lock(txn_mutex);

        if (open_txns > 0)
        {
            open_txns--;
        }
    }

    std::shared_ptr<Transaction> Environment::transaction(bool readonly) const
    {
        auto _env = instance(path);

        return std::shared_ptr<Transaction>(new Transaction(_env, readonly));
    }

    std::tuple<int, int, int> Environment::version()
    {
        int major, minor, patch;

        mdb_version(&major, &minor, &patch);

        return {major, minor, patch};
    }

    Database::Database(
        std::shared_ptr<Environment> &environment,
        const std::string &name,
        int flags,
        bool enable_compression):
        environment(environment), dbi(0), name(name), compression(enable_compression)
    {
        const auto [error, env_flags] = environment->get_flags();

        const auto readonly = (env_flags & MDB_RDONLY);

        std::unique_ptr<Transaction> txn(new Transaction(environment, readonly));

        auto success = mdb_dbi_open(*txn->txn, name.empty() ? nullptr : name.c_str(), flags | MDB_CREATE, &dbi);

        if (success != MDB_SUCCESS)
        {
            throw std::runtime_error("Unable to open LMDB named database [" + name + "]: " + mdb_error(success));
        }

        if (dbi == 0)
        {
            throw std::runtime_error("Could not open LMDB named database [" + name + "]: No DBI handle");
        }

        if (!(env_flags & MDB_RDONLY))
        {
            if (txn->commit() != SUCCESS)
            {
                throw std::runtime_error("Could not commit to open LMDB named database: [" + name + "]");
            }
        }
    }

    Database::~Database()
    {
        mdb_dbi_close(*environment->env, dbi);

        dbi = 0;

        if (environment->databases.contains(name))
        {
            environment->databases.erase(name);
        }
    }

    bool Database::compressed() const
    {
        return compression;
    }

    size_t Database::count()
    {
        auto txn = transaction(true);

        auto cursor = txn->cursor();

        size_t count = 0;

        Error error = Error(LMDB_ERROR);

        do
        {
            std::tie(error, std::ignore, std::ignore) = cursor->get(count ? MDB_NEXT : MDB_FIRST);

            if (error == SUCCESS)
            {
                count++;
            }
        } while (error == SUCCESS);

        return count;
    }

    Error Database::del(const void *key, size_t length)
    {
    try_again:
        auto txn = transaction();

        auto error = txn->del(key, length);

        LMDB_CHECK_TXN_EXPAND(error, environment, txn, try_again)

        if (error)
        {
            return error;
        }

        error = txn->commit();

        LMDB_CHECK_TXN_EXPAND(error, environment, txn, try_again)

        return error;
    }

    Error Database::del(const void *key, size_t key_length, const void *value, size_t value_length)
    {
    try_again:
        auto txn = transaction();

        auto error = txn->del(key, key_length, value, value_length);

        LMDB_CHECK_TXN_EXPAND(error, environment, txn, try_again)

        if (error)
        {
            return error;
        }

        error = txn->commit();

        LMDB_CHECK_TXN_EXPAND(error, environment, txn, try_again)

        return error;
    }

    Error Database::drop(bool delete_db)
    {
        std::scoped_lock lock(mutex);

    try_again:
        std::unique_ptr<Transaction> txn(new Transaction(environment));

        auto result = mdb_drop(*txn->txn, dbi, (delete_db) ? 1 : 0);

        if (result == MDB_MAP_FULL)
        {
            txn->abort();

            environment->expand();

            goto try_again;
        }

        return txn->commit();
    }

    bool Database::exists(const void *key, size_t length)
    {
        return transaction(true)->exists(key, length);
    }

    std::tuple<Error, mdb_result_t> Database::get(const void *key, size_t length)
    {
        return transaction(true)->get(key, length);
    }

    std::vector<mdb_result_t> Database::get_all()
    {
        std::vector<mdb_result_t> results;

        const auto keys = list_keys();

        auto txn = transaction(true);

        for (const auto &key : keys)
        {
            const auto [error, value] = txn->get(key.data(), key.size());

            if (!error)
            {
                results.emplace_back(value);
            }
        }

        return results;
    }

    std::tuple<Error, unsigned int> Database::get_flags()
    {
        auto txn = transaction(true);

        unsigned int dbi_flags;

        const auto result = mdb_dbi_flags(*txn->txn, dbi, &dbi_flags);

        return {MAKE_LMDB_ERROR_MSG(result, mdb_error(result)), dbi_flags};
    }

    std::vector<mdb_result_t> Database::list_keys(bool ignore_duplicates)
    {
        auto txn = transaction(true);

        auto cursor = txn->cursor();

        std::vector<mdb_result_t> results;

        size_t count = 0;

        mdb_result_t last_key;

        Error error = Error(LMDB_ERROR);

        do
        {
            mdb_result_t key;

            std::tie(error, key, std::ignore) = cursor->get(count ? MDB_NEXT : MDB_FIRST);

            if (error == SUCCESS)
            {
                if (ignore_duplicates && key == last_key)
                {
                    continue;
                }

                results.emplace_back(key);

                last_key = key;

                count++;
            }
        } while (error == SUCCESS);

        return results;
    }

    Error Database::put(const void *key, size_t key_length, const void *value, size_t value_length, int flags)
    {
    try_again:
        auto txn = transaction();

        auto error = txn->put(key, key_length, value, value_length, flags);

        LMDB_CHECK_TXN_EXPAND(error, environment, txn, try_again)

        if (error)
        {
            return error;
        }

        error = txn->commit();

        LMDB_CHECK_TXN_EXPAND(error, environment, txn, try_again)

        return error;
    }

    std::shared_ptr<Transaction> Database::transaction(bool readonly)
    {
        std::scoped_lock lock(mutex);

        /**
         * We fetch the database instance this way to make sure that we do not
         * have a bunch of extra instances running around
         */
        auto db = environment->database(name);

        return std::shared_ptr<Transaction>(new Transaction(environment, db, readonly));
    }

    Transaction::Transaction(std::shared_ptr<Environment> &environment, bool readonly):
        environment(environment), m_readonly(readonly)
    {
        txn_setup();
    }

    Transaction::Transaction(
        std::shared_ptr<Environment> &environment,
        std::shared_ptr<Database> &database,
        bool readonly):
        environment(environment), db(database), m_readonly(readonly)
    {
        txn_setup();
    }

    Transaction::~Transaction()
    {
        // default action is to abort if the Transaction leaves scope
        abort();
    }

    void Transaction::abort()
    {
        if (!txn)
        {
            return;
        }

        mdb_txn_abort(*txn);

        if (!readonly())
        {
            environment->transaction_unregister();
        }

        txn = nullptr;
    }

    Error Transaction::commit()
    {
        if (!txn)
        {
            return MAKE_LMDB_ERROR_MSG(LMDB_BAD_TXN, mdb_error(MDB_BAD_TXN));
        }

        const auto result = mdb_txn_commit(*txn);

        if (!readonly())
        {
            environment->transaction_unregister();
        }

        txn = nullptr;

        return MAKE_LMDB_ERROR_MSG(result, mdb_error(result));
    }

    std::shared_ptr<Cursor> Transaction::cursor()
    {
        return std::shared_ptr<Cursor>(new Cursor(txn, db, m_readonly));
    }

    Error Transaction::del(const void *key, size_t length)
    {
        LMDB_LOAD_VALUE(key, length, i_key, false);

        const auto result = mdb_del(*txn, db->dbi, &i_key, nullptr);

        return MAKE_LMDB_ERROR_MSG(result, mdb_error(result));
    }

    Error Transaction::del(const void *key, size_t key_length, const void *value, size_t value_length)
    {
        LMDB_LOAD_VALUE(key, key_length, i_key, false);

        LMDB_LOAD_VALUE(value, value_length, i_value, db->compressed());

        const auto result = mdb_del(*txn, db->dbi, &i_key, &i_value);

        return MAKE_LMDB_ERROR_MSG(result, mdb_error(result));
    }

    bool Transaction::exists(const void *key, size_t length)
    {
        LMDB_LOAD_VALUE(key, length, i_key, false);

        MDB_val value;

        const auto result = mdb_get(*txn, db->dbi, &i_key, &value);

        return result == MDB_SUCCESS;
    }

    std::tuple<Error, std::vector<unsigned char>> Transaction::get(const void *key, size_t length)
    {
        LMDB_LOAD_VALUE(key, length, i_key, false);

        MDB_val value;

        const auto result = mdb_get(*txn, db->dbi, &i_key, &value);

        mdb_result_t r_value;

        if (result == MDB_SUCCESS)
        {
            r_value = load_result(value);
        }

        return {MAKE_LMDB_ERROR_MSG(result, mdb_error(result)), r_value};
    }

    std::tuple<Error, size_t> Transaction::id() const
    {
        if (!txn)
        {
            return {MAKE_LMDB_ERROR_MSG(LMDB_BAD_TXN, mdb_error(MDB_BAD_TXN)), 0};
        }

        const auto result = mdb_txn_id(*txn);

        return {MAKE_LMDB_ERROR(SUCCESS), result};
    }

    Error Transaction::put(const void *key, size_t key_length, const void *value, size_t value_length, int flags)
    {
        LMDB_LOAD_VALUE(key, key_length, i_key, false);

        LMDB_LOAD_VALUE(value, value_length, i_value, db->compressed());

        const auto result = mdb_put(*txn, db->dbi, &i_key, &i_value, flags);

        return MAKE_LMDB_ERROR_MSG(result, mdb_error(result));
    }

    bool Transaction::readonly() const
    {
        return m_readonly;
    }

    Error Transaction::renew()
    {
        if (!txn || !m_readonly)
        {
            return MAKE_LMDB_ERROR_MSG(LMDB_BAD_TXN, "Transaction does not exist or is not readonly");
        }

        const auto result = mdb_txn_renew(*txn);

        return MAKE_LMDB_ERROR_MSG(result, mdb_error(result));
    }

    void Transaction::reset()
    {
        if (!txn || !m_readonly)
        {
            throw std::runtime_error("Transaction does not exist or is not readonly");
        }

        return mdb_txn_reset(*txn);
    }

    void Transaction::use(const std::shared_ptr<Database> &database)
    {
        db = database;
    }


    void Transaction::txn_setup()
    {
        MDB_txn *result;

        for (int i = 0; i < 3; ++i)
        {
            const auto mdb_result = mdb_txn_begin(*environment->env, nullptr, (m_readonly) ? MDB_RDONLY : 0, &result);

            if (mdb_result == MDB_SUCCESS)
            {
                break;
            }

            if (mdb_result == MDB_MAP_RESIZED && i < 2)
            {
                if (const auto error = environment->detect_map_size())
                {
                    throw std::runtime_error("Failed to re-initialize map");
                }

                continue;
            }

            throw std::runtime_error("Unable to start LMDB transaction: " + mdb_error(mdb_result));
        }

        txn = std::make_unique<MDB_txn *>(result);

        if (!readonly())
        {
            environment->transaction_register();
        }
    }

    Cursor::Cursor(std::shared_ptr<MDB_txn *> &txn, std::shared_ptr<Database> &db, bool readonly):
        txn(txn), db(db), m_readonly(readonly)
    {
        const auto result = mdb_cursor_open(*txn, db->dbi, &cursor);

        if (result != MDB_SUCCESS)
        {
            throw std::runtime_error("Could not open LMDB cursor: " + mdb_error(result));
        }
    }

    Cursor::~Cursor()
    {
        if (cursor == nullptr || !m_readonly)
        {
            return;
        }

        mdb_cursor_close(cursor);

        cursor = nullptr;
    }

    std::tuple<Error, size_t> Cursor::count()
    {
        if (cursor == nullptr)
        {
            return {MAKE_LMDB_ERROR_MSG(LMDB_ERROR, "Cursor does not exist"), 0};
        }

        size_t count = 0;

        const auto result = mdb_cursor_count(cursor, &count);

        return {MAKE_LMDB_ERROR_MSG(result, mdb_error(result)), count};
    }

    Error Cursor::del(int flags)
    {
        if (cursor == nullptr || m_readonly)
        {
            return MAKE_LMDB_ERROR_MSG(LMDB_ERROR, "Cursor does not exist or is readonly");
        }

        const auto result = mdb_cursor_del(cursor, flags);

        return MAKE_LMDB_ERROR_MSG(result, mdb_error(result));
    }

    std::tuple<Error, mdb_result_t, mdb_result_t> Cursor::get(const MDB_cursor_op &op)
    {
        mdb_result_t r_key;

        mdb_result_t r_value;

        if (cursor == nullptr)
        {
            return {MAKE_LMDB_ERROR_MSG(LMDB_ERROR, "Cursor does not exist"), r_key, r_value};
        }

        MDB_val i_key, i_value;

        const auto result = mdb_cursor_get(cursor, &i_key, &i_value, op);

        if (result == MDB_SUCCESS)
        {
            r_key = load_result(i_key);

            r_value = load_result(i_value);
        }

        return {MAKE_LMDB_ERROR_MSG(result, mdb_error(result)), r_key, r_value};
    }

    std::tuple<Error, mdb_result_t, mdb_result_t> Cursor::get(const void *key, size_t length, const MDB_cursor_op &op)
    {
        mdb_result_t r_key;

        mdb_result_t r_value;

        if (cursor == nullptr)
        {
            return {MAKE_LMDB_ERROR_MSG(LMDB_ERROR, "Cursor does not exist"), r_key, r_value};
        }

        LMDB_LOAD_VALUE(key, length, i_key, false);

        MDB_val i_value;

        const auto result = mdb_cursor_get(cursor, &i_key, &i_value, op);

        if (result == MDB_SUCCESS)
        {
            r_key = load_result(i_key);

            r_value = load_result(i_value);
        }

        return {MAKE_LMDB_ERROR_MSG(result, mdb_error(result)), r_key, r_value};
    }

    std::tuple<Error, mdb_result_t, std::vector<mdb_result_t>> Cursor::get_all(const void *key, size_t length)
    {
        mdb_result_t l_key;

        std::vector<mdb_result_t> results;

        bool success = false;

        do
        {
            auto [error, r_key, r_value] = get(key, length, (!success) ? MDB_SET : MDB_NEXT_DUP);

            if (!error)
            {
                results.emplace_back(r_value);

                if (l_key.empty())
                {
                    l_key = r_key;
                }
            }

            success = error == SUCCESS;
        } while (success);

        Error error = (!results.empty()) ? SUCCESS : LMDB_EMPTY;

        return {error, l_key, results};
    }

    Error Cursor::put(const void *key, size_t key_length, const void *value, size_t value_length, int flags)
    {
        if (cursor == nullptr || m_readonly)
        {
            return MAKE_LMDB_ERROR_MSG(LMDB_ERROR, "Cursor does not exist or is readonly");
        }

        LMDB_LOAD_VALUE(key, key_length, i_key, false);

        LMDB_LOAD_VALUE(value, value_length, i_value, db->compressed());

        const auto result = mdb_cursor_put(cursor, &i_key, &i_value, flags);

        return MAKE_LMDB_ERROR_MSG(result, mdb_error(result));
    }

    bool Cursor::readonly() const
    {
        return m_readonly;
    }

    Error Cursor::renew()
    {
        if (cursor == nullptr || m_readonly)
        {
            return MAKE_LMDB_ERROR_MSG(LMDB_ERROR, "Cursor does not exist or is readonly");
        }

        const auto result = mdb_cursor_renew(*txn, cursor);

        return MAKE_LMDB_ERROR_MSG(result, mdb_error(result));
    }
} // namespace LMDB
