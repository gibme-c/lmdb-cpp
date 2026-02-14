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

#include <iostream>
#include <string>

#include "lmdb_cpp.hpp"

using namespace LMDB;

static int failures = 0;

#define ASSERT_TRUE(expr)                                                                   \
    do                                                                                      \
    {                                                                                       \
        if (!(expr))                                                                        \
        {                                                                                   \
            std::cerr << "FAIL: " << #expr << " (" << __FILE__ << ":" << __LINE__ << ")\n"; \
            failures++;                                                                     \
        }                                                                                   \
    } while (false)

#define ASSERT_EQ(a, b)                                                          \
    do                                                                           \
    {                                                                            \
        if ((a) != (b))                                                          \
        {                                                                        \
            std::cerr << "FAIL: " << #a << " != " << #b << " (" << __FILE__      \
                      << ":" << __LINE__ << ")\n";                               \
            failures++;                                                          \
        }                                                                        \
    } while (false)

#define ASSERT_GT(a, b)                                                          \
    do                                                                           \
    {                                                                            \
        if (!((a) > (b)))                                                        \
        {                                                                        \
            std::cerr << "FAIL: " << #a << " > " << #b << " (" << __FILE__       \
                      << ":" << __LINE__ << ")\n";                               \
            failures++;                                                          \
        }                                                                        \
    } while (false)

static std::string from_result(const mdb_result_t &r)
{
    return {r.begin(), r.end()};
}

// ============================================================
// Environment tests
// ============================================================

static void test_environment_version()
{
    std::cout << "== test_environment_version ==" << std::endl;

    const auto [major, minor, patch] = Environment::version();

    ASSERT_TRUE(major > 0 || minor > 0);
}

static void test_environment_singleton()
{
    std::cout << "== test_environment_singleton ==" << std::endl;

    auto env1 = Environment::instance("test.db", MDB_NOSUBDIR, 0600, 8, 32);
    auto env2 = Environment::instance("test.db", MDB_NOSUBDIR, 0600, 8, 32);

    ASSERT_TRUE(env1.get() == env2.get());
}

static void test_environment_info_and_stats()
{
    std::cout << "== test_environment_info_and_stats ==" << std::endl;

    auto env = Environment::instance("test.db", MDB_NOSUBDIR, 0600, 8, 32);

    {
        const auto [error, env_info] = env->info();

        ASSERT_TRUE(!error);
        ASSERT_GT(env_info.me_mapsize, 0u);
    }

    {
        const auto [error, env_stats] = env->stats();

        ASSERT_TRUE(!error);
        ASSERT_GT(env_stats.ms_psize, 0u);
    }
}

static void test_environment_flags()
{
    std::cout << "== test_environment_flags ==" << std::endl;

    auto env = Environment::instance("test.db", MDB_NOSUBDIR, 0600, 8, 32);

    const auto [error, flags] = env->get_flags();

    ASSERT_TRUE(!error);
    ASSERT_TRUE(flags & MDB_NOSUBDIR);
}

static void test_environment_max_key_size()
{
    std::cout << "== test_environment_max_key_size ==" << std::endl;

    auto env = Environment::instance("test.db", MDB_NOSUBDIR, 0600, 8, 32);

    const auto [error, max_key] = env->max_key_size();

    ASSERT_TRUE(!error);
    ASSERT_GT(max_key, 0u);
}

static void test_environment_max_readers()
{
    std::cout << "== test_environment_max_readers ==" << std::endl;

    auto env = Environment::instance("test.db", MDB_NOSUBDIR, 0600, 8, 32);

    const auto [error, max_readers] = env->max_readers();

    ASSERT_TRUE(!error);
    ASSERT_GT(max_readers, 0u);
}

static void test_environment_open_transactions()
{
    std::cout << "== test_environment_open_transactions ==" << std::endl;

    auto env = Environment::instance("test.db", MDB_NOSUBDIR, 0600, 8, 32);
    auto db = env->database("test_open_txns");

    ASSERT_EQ(env->open_transactions(), 0u);

    {
        auto txn = db->transaction();

        ASSERT_EQ(env->open_transactions(), 1u);
    }

    // Transaction left scope without commit, RAII aborted it
    ASSERT_EQ(env->open_transactions(), 0u);
}

static void test_environment_flush()
{
    std::cout << "== test_environment_flush ==" << std::endl;

    auto env = Environment::instance("test.db", MDB_NOSUBDIR, 0600, 8, 32);

    auto error = env->flush(true);

    ASSERT_TRUE(!error);
}

static void test_environment_copy()
{
    std::cout << "== test_environment_copy ==" << std::endl;

    auto env = Environment::instance("test.db", MDB_NOSUBDIR, 0600, 8, 32);
    auto db = env->database("test_copy_src");

    const auto k = std::string("copy_key");
    const auto v = std::string("copy_val");

    auto error = db->put_key(k, v);

    ASSERT_TRUE(!error);

    error = env->copy("test_copy.db");

    ASSERT_TRUE(!error);

    // Open the copy and verify the data survived
    auto env2 = Environment::instance("test_copy.db", MDB_NOSUBDIR, 0600, 8, 32);
    auto db2 = env2->database("test_copy_src");

    const auto [get_error, data] = db2->get_key(k);

    ASSERT_TRUE(!get_error);
    ASSERT_EQ(from_result(data), v);
}

// ============================================================
// Database tests
// ============================================================

static void test_uncompressed_basic()
{
    std::cout << "== test_uncompressed_basic ==" << std::endl;

    const auto key_base = std::string("key_uncompressed_");
    const auto val_base = std::string("val_uncompressed_");

    auto env = Environment::instance("test.db", MDB_NOSUBDIR, 0600, 8, 32);
    auto db = env->database("test_uc");

    ASSERT_TRUE(!db->compressed());

    // Insert 10 entries
    for (size_t i = 0; i < 10; ++i)
    {
        const auto k = key_base + std::to_string(i);
        const auto v = val_base + std::to_string(i);

        auto error = db->put(k.data(), k.size(), v.data(), v.size());

        ASSERT_TRUE(!error);
    }

    // Count matches expected
    ASSERT_EQ(db->count(), 10u);

    // Retrieve and verify each value
    for (size_t i = 0; i < 10; ++i)
    {
        const auto k = key_base + std::to_string(i);
        const auto expected_v = val_base + std::to_string(i);

        const auto [error, data] = db->get(k.data(), k.size());

        ASSERT_TRUE(!error);
        ASSERT_EQ(from_result(data), expected_v);
    }

    // Existence checks
    {
        const auto k = key_base + "0";

        ASSERT_TRUE(db->exists(k.data(), k.size()));

        const auto missing = std::string("nonexistent_key");

        ASSERT_TRUE(!db->exists(missing.data(), missing.size()));
    }

    // List keys count matches
    {
        const auto keys = db->list_keys();

        ASSERT_EQ(keys.size(), 10u);
    }

    // Delete a key and verify it's gone
    {
        const auto k = key_base + "5";

        auto error = db->del(k.data(), k.size());

        ASSERT_TRUE(!error);
        ASSERT_TRUE(!db->exists(k.data(), k.size()));
        ASSERT_EQ(db->count(), 9u);
    }
}

static void test_template_helpers()
{
    std::cout << "== test_template_helpers ==" << std::endl;

    auto env = Environment::instance("test.db", MDB_NOSUBDIR, 0600, 8, 32);
    auto db = env->database("test_templates");

    const auto k = std::string("tmpl_key");
    const auto v = std::string("tmpl_val");

    // put_key
    auto error = db->put_key(k, v);

    ASSERT_TRUE(!error);

    // exists_key
    ASSERT_TRUE(db->exists_key(k));

    // get_key
    {
        const auto [get_error, data] = db->get_key(k);

        ASSERT_TRUE(!get_error);
        ASSERT_EQ(from_result(data), v);
    }

    // del_key
    error = db->del_key(k);

    ASSERT_TRUE(!error);
    ASSERT_TRUE(!db->exists_key(k));
}

static void test_put_overwrite()
{
    std::cout << "== test_put_overwrite ==" << std::endl;

    auto env = Environment::instance("test.db", MDB_NOSUBDIR, 0600, 8, 32);
    auto db = env->database("test_overwrite");

    const auto k = std::string("ow_key");
    const auto v1 = std::string("original_value");
    const auto v2 = std::string("updated_value");

    auto error = db->put_key(k, v1);

    ASSERT_TRUE(!error);

    // Overwrite same key
    error = db->put_key(k, v2);

    ASSERT_TRUE(!error);

    // Should get the updated value
    const auto [get_error, data] = db->get_key(k);

    ASSERT_TRUE(!get_error);
    ASSERT_EQ(from_result(data), v2);

    // Count should still be 1
    ASSERT_EQ(db->count(), 1u);
}

static void test_get_nonexistent()
{
    std::cout << "== test_get_nonexistent ==" << std::endl;

    auto env = Environment::instance("test.db", MDB_NOSUBDIR, 0600, 8, 32);
    auto db = env->database("test_get_missing");

    const auto k = std::string("no_such_key");
    const auto [error, data] = db->get_key(k);

    ASSERT_TRUE(error);
    ASSERT_EQ(error.code(), LMDB_NOTFOUND);
    ASSERT_TRUE(data.empty());
}

static void test_del_nonexistent()
{
    std::cout << "== test_del_nonexistent ==" << std::endl;

    auto env = Environment::instance("test.db", MDB_NOSUBDIR, 0600, 8, 32);
    auto db = env->database("test_del_missing");

    const auto k = std::string("no_such_key");
    auto error = db->del_key(k);

    ASSERT_TRUE(error);
    ASSERT_EQ(error.code(), LMDB_NOTFOUND);
}

static void test_database_get_flags()
{
    std::cout << "== test_database_get_flags ==" << std::endl;

    auto env = Environment::instance("test.db", MDB_NOSUBDIR, 0600, 8, 32);
    auto db = env->database("test_db_flags");

    const auto [error, flags] = db->get_flags();

    ASSERT_TRUE(!error);
}

static void test_database_reopen_same_name()
{
    std::cout << "== test_database_reopen_same_name ==" << std::endl;

    auto env = Environment::instance("test.db", MDB_NOSUBDIR, 0600, 8, 32);

    auto db1 = env->database("test_reopen");
    auto db2 = env->database("test_reopen");

    // Should return the same shared_ptr (singleton per name)
    ASSERT_TRUE(db1.get() == db2.get());
}

// ============================================================
// Compressed database tests
// ============================================================

static void test_compressed_roundtrip()
{
    std::cout << "== test_compressed_roundtrip ==" << std::endl;

    const auto key_base = std::string("key_compressed_");
    const auto val_base =
        std::string("ABCDEFGHIJKLMNOPQRSTUVWXYZABCDEFGHIJKLMNOPQRSTUVWXYZ_");

    auto env = Environment::instance("test.db", MDB_NOSUBDIR, 0600, 8, 32);
    auto db = env->database("test_comp", true);

    ASSERT_TRUE(db->compressed());

    // Insert entries
    for (size_t i = 0; i < 10; ++i)
    {
        const auto k = key_base + std::to_string(i);
        const auto v = val_base + std::to_string(i);

        auto error = db->put_key(k, v);

        ASSERT_TRUE(!error);
    }

    ASSERT_EQ(db->count(), 10u);

    // Verify round-trip: values read back match what was written
    for (size_t i = 0; i < 10; ++i)
    {
        const auto k = key_base + std::to_string(i);
        const auto expected_v = val_base + std::to_string(i);

        const auto [error, data] = db->get_key(k);

        ASSERT_TRUE(!error);
        ASSERT_EQ(from_result(data), expected_v);
    }

    // get_all returns correct number of values
    {
        const auto values = db->get_all();

        ASSERT_EQ(values.size(), 10u);
    }
}

static void test_compressed_delete_and_verify()
{
    std::cout << "== test_compressed_delete_and_verify ==" << std::endl;

    auto env = Environment::instance("test.db", MDB_NOSUBDIR, 0600, 8, 32);
    auto db = env->database("test_comp_del", true);

    const auto k = std::string("comp_del_key");
    const auto v = std::string("REPEATEDREPEATEDREPEATEDREPEATED");

    auto error = db->put_key(k, v);

    ASSERT_TRUE(!error);
    ASSERT_TRUE(db->exists_key(k));

    error = db->del_key(k);

    ASSERT_TRUE(!error);
    ASSERT_TRUE(!db->exists_key(k));
    ASSERT_EQ(db->count(), 0u);
}

// ============================================================
// Transaction tests
// ============================================================

static void test_transaction_raii_abort()
{
    std::cout << "== test_transaction_raii_abort ==" << std::endl;

    auto env = Environment::instance("test.db", MDB_NOSUBDIR, 0600, 8, 32);
    auto db = env->database("test_raii_abort");

    const auto k = std::string("raii_key");
    const auto v = std::string("raii_val");

    // Transaction goes out of scope without commit — should auto-abort
    {
        auto txn = db->transaction();

        auto error = txn->put_key(k, v);

        ASSERT_TRUE(!error);
        // no commit — destructor aborts
    }

    // Key should NOT exist
    ASSERT_TRUE(!db->exists_key(k));
}

static void test_transaction_explicit_abort()
{
    std::cout << "== test_transaction_explicit_abort ==" << std::endl;

    auto env = Environment::instance("test.db", MDB_NOSUBDIR, 0600, 8, 32);
    auto db = env->database("test_explicit_abort");

    const auto k = std::string("abort_key");
    const auto v = std::string("abort_val");

    auto txn = db->transaction();

    auto error = txn->put_key(k, v);

    ASSERT_TRUE(!error);

    txn->abort();

    ASSERT_TRUE(!db->exists_key(k));
}

static void test_transaction_readonly()
{
    std::cout << "== test_transaction_readonly ==" << std::endl;

    auto env = Environment::instance("test.db", MDB_NOSUBDIR, 0600, 8, 32);
    auto db = env->database("test_txn_ro");

    const auto k = std::string("ro_key");
    const auto v = std::string("ro_val");

    // Insert via write transaction first
    auto error = db->put_key(k, v);

    ASSERT_TRUE(!error);

    // Open a readonly transaction
    auto txn = db->transaction(true);

    ASSERT_TRUE(txn->readonly());

    // Read via readonly transaction
    const auto [get_error, data] = txn->get_key(k);

    ASSERT_TRUE(!get_error);
    ASSERT_EQ(from_result(data), v);

    // exists via readonly transaction
    ASSERT_TRUE(txn->exists_key(k));
}

static void test_transaction_id()
{
    std::cout << "== test_transaction_id ==" << std::endl;

    auto env = Environment::instance("test.db", MDB_NOSUBDIR, 0600, 8, 32);
    auto db = env->database("test_txn_id");

    auto txn = db->transaction(true);

    const auto [error, txn_id] = txn->id();

    ASSERT_TRUE(!error);
    ASSERT_GT(txn_id, 0u);
}

static void test_transaction_multiple_operations()
{
    std::cout << "== test_transaction_multiple_operations ==" << std::endl;

    auto env = Environment::instance("test.db", MDB_NOSUBDIR, 0600, 8, 32);
    auto db = env->database("test_txn_multi_ops");

    // Put several keys, delete one, then commit — all in one transaction
    auto txn = db->transaction();

    for (int i = 0; i < 5; ++i)
    {
        const auto k = std::string("mop_key_") + std::to_string(i);
        const auto v = std::string("mop_val_") + std::to_string(i);

        auto error = txn->put_key(k, v);

        ASSERT_TRUE(!error);
    }

    // Delete key 2 within the same transaction
    {
        const auto k = std::string("mop_key_2");

        auto error = txn->del_key(k);

        ASSERT_TRUE(!error);
    }

    auto error = txn->commit();

    ASSERT_TRUE(!error);

    // Verify: 4 keys remain, key 2 is gone
    ASSERT_EQ(db->count(), 4u);

    const auto k2 = std::string("mop_key_2");

    ASSERT_TRUE(!db->exists_key(k2));

    const auto k3 = std::string("mop_key_3");
    const auto [get_error, data] = db->get_key(k3);

    ASSERT_TRUE(!get_error);
    ASSERT_EQ(from_result(data), std::string("mop_val_3"));
}

static void test_multi_database_transaction()
{
    std::cout << "== test_multi_database_transaction ==" << std::endl;

    const auto key = std::string("multi_txn_key");
    const auto val = std::string("multi_txn_val");

    auto env = Environment::instance("test.db", MDB_NOSUBDIR, 0600, 8, 32);
    const auto db1 = env->database("test_multi1", true);
    const auto db2 = env->database("test_multi2", true);

    // Write to both databases in a single transaction
    {
        const auto txn = env->transaction();

        txn->use(db1);

        auto error = txn->put(key.data(), key.size(), val.data(), val.size());

        ASSERT_TRUE(!error);

        txn->use(db2);

        error = txn->put(key.data(), key.size(), val.data(), val.size());

        ASSERT_TRUE(!error);

        error = txn->commit();

        ASSERT_TRUE(!error);
    }

    // Verify values exist in both databases
    {
        const auto [error1, data1] = db1->get(key.data(), key.size());

        ASSERT_TRUE(!error1);
        ASSERT_EQ(from_result(data1), val);

        const auto [error2, data2] = db2->get(key.data(), key.size());

        ASSERT_TRUE(!error2);
        ASSERT_EQ(from_result(data2), val);
    }
}

static void test_transaction_reset_renew()
{
    std::cout << "== test_transaction_reset_renew ==" << std::endl;

    auto env = Environment::instance("test.db", MDB_NOSUBDIR, 0600, 8, 32);
    auto db = env->database("test_reset_renew");

    const auto k = std::string("rr_key");
    const auto v = std::string("rr_val");

    auto error = db->put_key(k, v);

    ASSERT_TRUE(!error);

    // Open readonly, read, reset, renew, read again
    auto txn = db->transaction(true);

    {
        const auto [get_error, data] = txn->get_key(k);

        ASSERT_TRUE(!get_error);
        ASSERT_EQ(from_result(data), v);
    }

    txn->reset();

    error = txn->renew();

    ASSERT_TRUE(!error);

    {
        const auto [get_error, data] = txn->get_key(k);

        ASSERT_TRUE(!get_error);
        ASSERT_EQ(from_result(data), v);
    }
}

// ============================================================
// Cursor tests
// ============================================================

static void test_cursor_iteration()
{
    std::cout << "== test_cursor_iteration ==" << std::endl;

    auto env = Environment::instance("test.db", MDB_NOSUBDIR, 0600, 8, 32);
    auto db = env->database("test_cursor_iter");

    // Insert sorted keys
    for (int i = 0; i < 5; ++i)
    {
        const auto k = std::string("ckey_") + std::to_string(i);
        const auto v = std::string("cval_") + std::to_string(i);

        auto error = db->put_key(k, v);

        ASSERT_TRUE(!error);
    }

    // Forward iteration with cursor
    {
        auto txn = db->transaction(true);
        auto cursor = txn->cursor();

        size_t count = 0;

        auto [error, key, value] = cursor->get(MDB_FIRST);

        while (!error)
        {
            count++;
            std::tie(error, key, value) = cursor->get(MDB_NEXT);
        }

        ASSERT_EQ(count, 5u);
    }

    // MDB_LAST gives the last key
    {
        auto txn = db->transaction(true);
        auto cursor = txn->cursor();

        const auto [error, key, value] = cursor->get(MDB_LAST);

        ASSERT_TRUE(!error);
        ASSERT_EQ(from_result(key), std::string("ckey_4"));
        ASSERT_EQ(from_result(value), std::string("cval_4"));
    }

    // Reverse iteration
    {
        auto txn = db->transaction(true);
        auto cursor = txn->cursor();

        size_t count = 0;

        auto [error, key, value] = cursor->get(MDB_LAST);

        while (!error)
        {
            count++;
            std::tie(error, key, value) = cursor->get(MDB_PREV);
        }

        ASSERT_EQ(count, 5u);
    }
}

static void test_cursor_get_by_key()
{
    std::cout << "== test_cursor_get_by_key ==" << std::endl;

    auto env = Environment::instance("test.db", MDB_NOSUBDIR, 0600, 8, 32);
    auto db = env->database("test_cursor_get_key");

    const auto k = std::string("cg_key");
    const auto v = std::string("cg_val");

    auto error = db->put_key(k, v);

    ASSERT_TRUE(!error);

    auto txn = db->transaction(true);
    auto cursor = txn->cursor();

    const auto [get_error, rkey, rvalue] = cursor->get_key(k, MDB_SET);

    ASSERT_TRUE(!get_error);
    ASSERT_EQ(from_result(rvalue), v);
}

static void test_cursor_put_and_del()
{
    std::cout << "== test_cursor_put_and_del ==" << std::endl;

    auto env = Environment::instance("test.db", MDB_NOSUBDIR, 0600, 8, 32);
    auto db = env->database("test_cursor_put_del");

    auto txn = db->transaction();
    auto cursor = txn->cursor();

    ASSERT_TRUE(!cursor->readonly());

    const auto k = std::string("cpd_key");
    const auto v = std::string("cpd_val");

    // Put via cursor
    auto error = cursor->put_key(k, v);

    ASSERT_TRUE(!error);

    // Position cursor at the key we just inserted
    {
        const auto [get_error, rkey, rvalue] = cursor->get_key(k, MDB_SET);

        ASSERT_TRUE(!get_error);
        ASSERT_EQ(from_result(rvalue), v);
    }

    // Delete via cursor
    error = cursor->del();

    ASSERT_TRUE(!error);

    error = txn->commit();

    ASSERT_TRUE(!error);

    // Verify deleted
    ASSERT_TRUE(!db->exists_key(k));
}

// ============================================================
// Drop test
// ============================================================

static void test_drop()
{
    std::cout << "== test_drop ==" << std::endl;

    auto env = Environment::instance("test.db", MDB_NOSUBDIR, 0600, 8, 32);
    auto db = env->database("test_drop");

    const auto k = std::string("drop_key");
    const auto v = std::string("drop_val");

    auto error = db->put_key(k, v);

    ASSERT_TRUE(!error);
    ASSERT_EQ(db->count(), 1u);

    // Drop (empty) the database
    error = db->drop(false);

    ASSERT_TRUE(!error);
    ASSERT_EQ(db->count(), 0u);
}

// ============================================================
// Error class tests
// ============================================================

static void test_error_class()
{
    std::cout << "== test_error_class ==" << std::endl;

    // SUCCESS is falsy
    Error success(SUCCESS);

    ASSERT_TRUE(!success);
    ASSERT_TRUE(success == SUCCESS);
    ASSERT_TRUE(success != LMDB_ERROR);
    ASSERT_EQ(success.code(), SUCCESS);

    // Error is truthy
    Error err(LMDB_NOTFOUND);

    ASSERT_TRUE(err);
    ASSERT_TRUE(err == LMDB_NOTFOUND);
    ASSERT_TRUE(err != SUCCESS);
    ASSERT_EQ(err.code(), LMDB_NOTFOUND);

    // to_string returns non-empty
    ASSERT_TRUE(!success.to_string().empty());
    ASSERT_TRUE(!err.to_string().empty());

    // Error with source location
    Error located(LMDB_ERROR, 42, "test_file.cpp");

    ASSERT_EQ(located.line(), 42u);
    ASSERT_EQ(located.file_name(), std::string("test_file.cpp"));

    // Error with custom message
    Error custom(LMDB_ERROR, "custom message", 99, "custom.cpp");

    ASSERT_EQ(custom.to_string(), std::string("custom message"));
    ASSERT_EQ(custom.line(), 99u);

    // Equality between two Error objects
    Error a(LMDB_NOTFOUND);
    Error b(LMDB_NOTFOUND);
    Error c(LMDB_ERROR);

    ASSERT_TRUE(a == b);
    ASSERT_TRUE(a != c);
}

// ============================================================
// Main
// ============================================================

int main()
{
    // Environment
    test_environment_version();
    test_environment_singleton();
    test_environment_info_and_stats();
    test_environment_flags();
    test_environment_max_key_size();
    test_environment_max_readers();
    test_environment_open_transactions();
    test_environment_flush();
    test_environment_copy();

    // Database
    test_uncompressed_basic();
    test_template_helpers();
    test_put_overwrite();
    test_get_nonexistent();
    test_del_nonexistent();
    test_database_get_flags();
    test_database_reopen_same_name();

    // Compressed
    test_compressed_roundtrip();
    test_compressed_delete_and_verify();

    // Transaction
    test_transaction_raii_abort();
    test_transaction_explicit_abort();
    test_transaction_readonly();
    test_transaction_id();
    test_transaction_multiple_operations();
    test_multi_database_transaction();
    test_transaction_reset_renew();

    // Cursor
    test_cursor_iteration();
    test_cursor_get_by_key();
    test_cursor_put_and_del();

    // Drop
    test_drop();

    // Error
    test_error_class();

    if (failures > 0)
    {
        std::cerr << "\n" << failures << " test(s) FAILED\n";
        return 1;
    }

    std::cout << "\nAll tests passed\n";
    return 0;
}
