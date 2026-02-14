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
    /**
     * A thin, reader-writer-locked wrapper around std::map.
     *
     * Read operations (at, contains, each, empty, size) take a shared lock so
     * multiple readers can proceed concurrently. Write operations (insert,
     * erase, clear, etc.) take an exclusive lock.
     *
     * Used internally to manage the Environment and Database registries.
     */
    template<typename L, typename R> class ThreadSafeMap
    {
      public:
        ThreadSafeMap() = default;

        /// Returns the value for the given key. Throws std::out_of_range if absent.
        R at(L key) const
        {
            std::shared_lock lock(m_mutex);

            return m_container.at(key);
        }

        /// Removes every element from the map.
        void clear()
        {
            std::unique_lock lock(m_mutex);

            m_container.clear();
        }

        /// Returns true if the map contains an element with the given key.
        bool contains(const L &key) const
        {
            std::shared_lock lock(m_mutex);

            return m_container.count(key) != 0;
        }

        /// Calls func(key, value) for each element (read-only, shared lock).
        void each(const std::function<void(const L &, const R &)> &func) const
        {
            std::shared_lock lock(m_mutex);

            for (const auto &[key, value] : m_container)
            {
                func(key, value);
            }
        }

        /// Calls func(key, value) for each element (mutable references, exclusive lock).
        void eachref(const std::function<void(L &, R &)> &func)
        {
            std::unique_lock lock(m_mutex);

            for (auto &[key, value] : m_container)
            {
                func(key, value);
            }
        }

        /// Returns true when the map has no elements.
        bool empty() const
        {
            std::shared_lock lock(m_mutex);

            return m_container.empty();
        }

        /// Removes the element with the given key (no-op if absent).
        void erase(const L &key)
        {
            std::unique_lock lock(m_mutex);

            m_container.erase(key);
        }

        /**
         * Atomically looks up a key and, if missing, inserts the provided value.
         * Either way, returns the value now associated with that key.
         */
        R find_or_insert(const L &key, const R &value)
        {
            std::unique_lock lock(m_mutex);

            auto it = m_container.find(key);

            if (it != m_container.end())
            {
                return it->second;
            }

            m_container.insert({key, value});

            return value;
        }

        /**
         * Same as find_or_insert, but accepts a factory function that's only
         * called when the key is absent. Handy when constructing the value is
         * expensive and you'd rather not do it unless you have to.
         */
        R find_or_insert(const L &key, const std::function<R()> &factory)
        {
            std::unique_lock lock(m_mutex);

            auto it = m_container.find(key);

            if (it != m_container.end())
            {
                return it->second;
            }

            auto value = factory();

            m_container.insert({key, value});

            return value;
        }

        /// Inserts a key-value pair. Does nothing if the key already exists.
        void insert(const L &key, const R &value)
        {
            std::unique_lock lock(m_mutex);

            m_container.insert({key, value});
        }

        /// Inserts a key-value pair from a tuple. Does nothing if the key already exists.
        void insert(const std::tuple<L, R> &kv)
        {
            std::unique_lock lock(m_mutex);

            m_container.insert(kv);
        }

        /// Inserts a key-value pair, or overwrites the existing value if the key is already present.
        void insert_or_assign(const L &key, const R &value)
        {
            std::unique_lock lock(m_mutex);

            m_container.insert_or_assign(key, value);
        }

        /// Inserts or overwrites from a tuple.
        void insert_or_assign(const std::tuple<L, R> &kv)
        {
            std::unique_lock lock(m_mutex);

            const auto [key, value] = kv;

            m_container.insert_or_assign(key, value);
        }

        /// Returns the theoretical maximum number of elements the map can hold.
        size_t max_size() const
        {
            std::shared_lock lock(m_mutex);

            return m_container.max_size();
        }

        /// Returns how many elements are currently in the map.
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
