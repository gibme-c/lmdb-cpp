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

#ifndef LMDB_THREAD_SAFE_MAP_H
#define LMDB_THREAD_SAFE_MAP_H

#include <functional>
#include <map>
#include <mutex>
#include <shared_mutex>

namespace LMDB
{
    template<typename L, typename R> class ThreadSafeMap
    {
      public:
        ThreadSafeMap() = default;

        /**
         * Returns the element at the specified key in the container
         *
         * @param key
         * @return
         */
        R at(L key) const
        {
            std::shared_lock lock(m_mutex);

            return m_container.at(key);
        }

        /**
         * Removes all elements from the container
         */
        void clear()
        {
            std::unique_lock lock(m_mutex);

            m_container.clear();
        }

        /**
         * checks if the container contains element with specific key
         *
         * @param key
         * @return
         */
        bool contains(const L &key) const
        {
            std::shared_lock lock(m_mutex);

            return m_container.count(key) != 0;
        }

        /**
         * loops over the the container and executes the provided function
         * using each element
         *
         * @param func
         */
        void each(const std::function<void(const L &, const R &)> &func) const
        {
            std::shared_lock lock(m_mutex);

            for (const auto &[key, value] : m_container)
            {
                func(key, value);
            }
        }

        /**
         * loops over the the container and executes the provided function
         * using each element (without const)
         *
         * @param func
         */
        void eachref(const std::function<void(L &, R &)> &func)
        {
            std::unique_lock lock(m_mutex);

            for (auto &[key, value] : m_container)
            {
                func(key, value);
            }
        }

        /**
         * Returns whether the container is empty
         *
         * @return
         */
        bool empty() const
        {
            std::shared_lock lock(m_mutex);

            return m_container.empty();
        }

        /**
         * erases elements
         *
         * @param key
         */
        void erase(const L &key)
        {
            std::unique_lock lock(m_mutex);

            m_container.erase(key);
        }

        /**
         * inserts elements
         *
         * @param key
         * @param value
         */
        void insert(const L &key, const R &value)
        {
            std::unique_lock lock(m_mutex);

            m_container.insert({key, value});
        }

        /**
         * inserts elements
         *
         * @param kv
         */
        void insert(const std::tuple<L, R> &kv)
        {
            std::unique_lock lock(m_mutex);

            m_container.insert(kv);
        }

        /**
         * inserts an element or assigns to the current element if the key already exists
         *
         * @param key
         * @param value
         */
        void insert_or_assign(const L &key, const R &value)
        {
            std::unique_lock lock(m_mutex);

            m_container.insert_or_assign(key, value);
        }

        /**
         * inserts an element or assigns to the current element if the key already exists
         *
         * @param kv
         */
        void insert_or_assign(const std::tuple<L, R> &kv)
        {
            std::unique_lock lock(m_mutex);

            const auto [key, value] = kv;

            m_container.insert_or_assign(key, value);
        }

        /**
         * Returns the maximum possible number of elements for the container
         *
         * @return
         */
        size_t max_size() const
        {
            std::shared_lock lock(m_mutex);

            return m_container.max_size();
        }

        /**
         * Returns the size of the container
         *
         * @return
         */
        size_t size() const
        {
            std::shared_lock lock(m_mutex);

            return m_container.size();
        }

      private:
        std::map<L, R> m_container;

        mutable std::shared_mutex m_mutex;
    };
} // namespace LMDB

#endif
