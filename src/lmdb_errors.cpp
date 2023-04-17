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

#include "lmdb_errors.hpp"

#include <sstream>
#include <utility>

namespace LMDB
{
    Error::Error(): m_error_code(SUCCESS) {}

    Error::Error(const ErrorCode &code, size_t line_number, std::string file_name):
        m_error_code(code), m_line_number(line_number), m_file_name(std::move(file_name))
    {
    }

    Error::Error(const ErrorCode &code, std::string custom_message, size_t line_number, std::string file_name):
        m_error_code(code),
        m_custom_error_message(std::move(custom_message)),
        m_line_number(line_number),
        m_file_name(std::move(file_name))
    {
    }

    Error::Error(const int &code, size_t line_number, std::string file_name):
        m_error_code(static_cast<ErrorCode>(code)), m_line_number(line_number), m_file_name(std::move(file_name))
    {
    }

    Error::Error(const int &code, std::string custom_message, size_t line_number, std::string file_name):
        m_error_code(static_cast<ErrorCode>(code)),
        m_custom_error_message(std::move(custom_message)),
        m_line_number(line_number),
        m_file_name(std::move(file_name))
    {
    }

    bool Error::operator==(const LMDB::ErrorCode &code) const
    {
        return code == m_error_code;
    }

    bool Error::operator==(const LMDB::Error &error) const
    {
        return error.code() == m_error_code;
    }

    bool Error::operator!=(const LMDB::ErrorCode &code) const
    {
        return code != m_error_code;
    }

    bool Error::operator!=(const LMDB::Error &error) const
    {
        return error.code() != m_error_code;
    }

    Error::operator bool() const
    {
        return m_error_code != SUCCESS;
    }

    ErrorCode Error::code() const
    {
        return m_error_code;
    }

    std::string Error::file_name() const
    {
        return m_file_name;
    }

    size_t Error::line() const
    {
        return m_line_number;
    }

    std::string Error::to_string() const
    {
        if (!m_custom_error_message.empty())
        {
            return m_custom_error_message;
        }

        switch (m_error_code)
        {
            case SUCCESS:
                return "The operation completed successfully.";
            case LMDB_ERROR:
                return "The LMDB operation failed. Please report this error as this default text should be "
                       "replaced by "
                       "more detailed information.";
            case LMDB_EMPTY:
                return "The LMDB database appears to be empty. The database may be legitimately empty or an "
                       "underlying "
                       "issue persists in the database.";
            case LMDB_ENV_NOT_OPEN:
                return "The LMDB environment has been previously closed or never opened.";
            default:
                return "The error code supplied does not have a default message. Please create one.";
        }
    }
} // namespace LMDB

namespace std
{
    inline std::ostream &operator<<(std::ostream &os, const LMDB::Error &error)
    {
        if (!error.file_name().empty())
        {
            os << error.file_name() << " L#" << error.line() << " ";
        }

        os << "Error #" << std::to_string(error.code()) << ": " << error.to_string();

        return os;
    }
} // namespace std
