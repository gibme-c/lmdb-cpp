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
#include "lmdb_cpp.hpp"

using namespace LMDB;

int main()
{
    const auto key = std::string("ABCDEFGHIJKLMNOPQRSTUVWXYZABCDEFGHIJKLMNOPQRSTUVWXYZABCDEFGHIJKLMNOPQRSTUVWXYZ");
    const auto val = std::string("ABCDEFGHIJKLMNOPQRSTUVWXYZABCDEFGHIJKLMNOPQRSTUVWXYZABCDEFGHIJKLMNOPQRSTUVWXYZ");

    auto env = Environment::instance("test.db");

    {
        auto db = env->database("test");

        std::cout << "Compressed?: " << db->compressed() << std::endl;

        std::cout << db->count() << std::endl;

        for (size_t i = 0; i < 10; ++i) {
            const auto temp = key + std::to_string(i);
            const auto temp2 = val + std::to_string(i);

            db->put(temp.data(), temp.size(), temp2.data(), temp2.size());
        }

        std::cout << db->count() << std::endl;

        const auto keys = db->list_keys();

        for (const auto &k: keys) {
            const auto temp = std::string(k.begin(), k.end());

            std::cout << "Key: " << temp << std::endl;
        }

        const auto values = db->get_all();

        for (const auto &v: values) {
            const auto temp = std::string(v.begin(), v.end());

            std::cout << "Value: " << temp << std::endl;
        }
    }

    std::cout << std::endl << std::endl;

    {
        auto db = env->database("Test2", true);

        std::cout << "Compressed?: " << db->compressed() << std::endl;

        std::cout << db->count() << std::endl;

        for (size_t i = 0; i < 10; ++i) {
            const auto temp = key + std::to_string(i);
            const auto temp2 = val + std::to_string(i);

            db->put_key(temp, temp2);
        }

        std::cout << db->count() << std::endl;

        const auto keys = db->list_keys();

        for (const auto &k: keys) {
            const auto temp = std::string(k.begin(), k.end());

            std::cout << "Key: " << temp << std::endl;
        }

        const auto values = db->get_all();

        for (const auto &v: values) {
            const auto temp = std::string(v.begin(), v.end());

            std::cout << "Value: " << temp << std::endl;
        }
    }

    env->copy("test2.db");

    std::cout << std::endl << std::endl;

    {
        const auto db1 = env->database("test3", true);

        const auto db2 = env->database("test4", true);

        const auto txn = env->transaction();

        txn->use(db1);

        if (const auto error = txn->put(key.data(), key.size(), val.data(), val.size())) {
            std::cout << "Error putting into db1" << std::endl;

            exit(1);
        }

        txn->use(db2);

        if (const auto error = txn->put(key.data(), key.size(), val.data(), val.size()))
        {
            std::cout << "Error putting into db2" << std::endl;

            exit(1);
        }

        if (const auto error = txn->commit())
        {
            std::cout << "Error committing transaction" << std::endl;

            exit(1);
        }

        if (const auto [error, data] = db1->get(key.data(), key.size()); !error)
        {
            std::cout << "DB1 Value: " << std::string(data.begin(), data.end()) << std::endl;
        } else
        {
            std::cout << "Error getting value" << std::endl;
        }

        if (const auto [error, data] = db2->get(key.data(), key.size()); !error)
        {
            std::cout << "DB2 Value: " << std::string(data.begin(), data.end()) << std::endl;
        } else
        {
            std::cout << "Error getting value" << std::endl;
        }
    }
}
