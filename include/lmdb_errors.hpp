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

#ifndef LMDBCPP_ERRORS_HPP
#define LMDBCPP_ERRORS_HPP

#include <string>

namespace LMDB
{
    enum ErrorCode
    {
        SUCCESS = 0,

        LMDB_ENV_NOT_OPEN = -40001,
        /**
         * Do not change LMDB values as they map directly to LMDB return codes
         * See: http://www.lmdb.tech/doc/group__errors.html
         */
        LMDB_ERROR = -40000,
        LMDB_EMPTY = -39999,
        LMDB_KEYEXIST = -30799,
        LMDB_NOTFOUND = -30798,
        LMDB_PAGE_NOTFOUND = -30797,
        LMDB_CORRUPTED = -30796,
        LMDB_PANIC = -30795,
        LMDB_VERSION_MISMATCH = -30794,
        LMDB_INVALID = -30793,
        LMDB_MAP_FULL = -30792,
        LMDB_DBS_FULL = -30791,
        LMDB_READERS_FULL = -30790,
        LMDB_TLS_FULL = -30789,
        LMDB_TXN_FULL = -30788,
        LMDB_CURSOR_FULL = -30787,
        LMDB_PAGE_FULL = -30786,
        LMDB_MAP_RESIZED = -30785,
        LMDB_INCOMPATIBLE = -30784,
        LMDB_BAD_RSLOT = -30783,
        LMDB_BAD_TXN = -30782,
        LMDB_BAD_VALSIZE = -30781,
        LMDB_BAD_DBI = -30780
    };

    class Error
    {
      public:
        Error();

        /**
         * Creates an error with the specified code
         *
         * @param code
         * @param line_number
         * @param file_name
         */
        Error(const ErrorCode &code, size_t line_number = 0, std::string file_name = "");

        /**
         * Creates an error with the specified code and a custom error message
         *
         * @param code
         * @param custom_message
         * @param line_number
         * @param file_name
         */
        Error(const ErrorCode &code, std::string custom_message, size_t line_number = 0, std::string file_name = "");

        /**
         * Creates an error with the specified code
         *
         * @param code
         * @param line_number
         * @param file_name
         */
        Error(const int &code, size_t line_number = 0, std::string file_name = "");

        /**
         * Creates an error with the specified code and a custom error message
         *
         * @param code
         * @param custom_message
         * @param line_number
         * @param file_name
         */
        Error(const int &code, std::string custom_message, size_t line_number = 0, std::string file_name = "");

        bool operator==(const ErrorCode &code) const;

        bool operator==(const Error &error) const;

        bool operator!=(const ErrorCode &code) const;

        bool operator!=(const Error &error) const;

        explicit operator bool() const;

        /**
         * Returns the error code
         *
         * @return
         */
        [[nodiscard]] ErrorCode code() const;

        /**
         * Return the filename of the file where the error was created
         *
         * @return
         */
        [[nodiscard]] std::string file_name() const;

        /**
         * Return the line number of the file where the error was created
         *
         * @return
         */
        [[nodiscard]] size_t line() const;

        /**
         * Returns the error message of the instance
         *
         * @return
         */
        [[nodiscard]] std::string to_string() const;

      private:
        ErrorCode m_error_code;

        size_t m_line_number = 0;

        std::string m_file_name;

        std::string m_custom_error_message;
    };
} // namespace LMDB

#endif // LMDBCPP_ERRORS_HPP
