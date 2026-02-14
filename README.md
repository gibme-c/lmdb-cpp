# lmdb-cpp

A straightforward C++17 wrapper around [LMDB](https://en.wikipedia.org/wiki/Lightning_Memory-Mapped_Database) that handles the fiddly bits for you -- RAII resource management, thread-safe singletons, automatic map expansion, and optional [Snappy](https://github.com/google/snappy) compression.

## Features

- **RAII everywhere** -- Environments, Databases, Transactions, and Cursors clean up after themselves. Transactions auto-abort if you don't commit.
- **Thread-safe singletons** -- `Environment::instance()` and `Environment::database()` return the same shared pointer for the same path/name, so you can call them from any thread without worrying about duplicates.
- **Automatic map expansion** -- `Database::put()` and `Database::del()` detect `MDB_MAP_FULL` and transparently grow the memory map, then retry.
- **Optional Snappy compression** -- Enable per-database at creation time. Reads decompress automatically, even if the compression setting changes later.
- **Template convenience methods** -- `put_key()`, `get_key()`, `del_key()`, and `exists_key()` work directly with `std::string`, `std::vector`, or anything with `.data()` and `.size()`.

## Quick Start

```cpp
#include <lmdb_cpp.hpp>
#include <iostream>
#include <string>
#include <vector>

int main()
{
    // Open (or reuse) an environment -- singleton per path
    auto env = LMDB::Environment::instance("my.db");

    // Open a named database (singleton per name within the environment)
    auto db = env->database("users");

    // Write a value
    std::string key = "user:42";
    std::string value = "Alice";
    if (auto err = db->put_key(key, value))
    {
        std::cerr << "put failed: " << err.to_string() << "\n";
        return 1;
    }

    // Read it back
    auto [err, data] = db->get_key(key);
    if (err)
    {
        std::cerr << "get failed: " << err.to_string() << "\n";
        return 1;
    }

    std::string result(data.begin(), data.end());
    std::cout << key << " => " << result << "\n";
}
```

### Multi-Database Transactions

When you need atomic writes across multiple databases, use an Environment-level transaction and switch between databases with `use()`:

```cpp
auto env = LMDB::Environment::instance("my.db");
auto db_users    = env->database("users");
auto db_sessions = env->database("sessions");

auto txn = env->transaction();
txn->use(db_users);
txn->put_key(user_id, user_data);
txn->use(db_sessions);
txn->put_key(session_id, session_data);

if (auto err = txn->commit())
    std::cerr << "commit failed: " << err.to_string() << "\n";
```

### Compression

Enable Snappy compression per-database. Values are compressed on write and decompressed on read -- completely transparent:

```cpp
auto db = env->database("logs", true);  // second arg enables compression
db->put_key(key, large_payload);        // stored compressed
auto [err, data] = db->get_key(key);    // returned decompressed
```

### Cursors

Walk through the database in order:

```cpp
auto txn    = db->transaction(true);  // read-only
auto cursor = txn->cursor();

auto [err, key, value] = cursor->get(MDB_FIRST);
while (!err)
{
    // process key/value ...
    std::tie(err, key, value) = cursor->get(MDB_NEXT);
}
```

## Building

This project uses CMake. Dependencies (lmdb, snappy, cppfs) are included as git submodules.

### Cloning

```bash
git clone --recursive https://github.com/gibme-c/lmdb-cpp
```

### As a Submodule Dependency

```bash
git submodule add https://github.com/gibme-c/lmdb-cpp external/lmdb-cpp
git submodule update --init --recursive
```

Then in your `CMakeLists.txt`:

```cmake
add_subdirectory(external/lmdb-cpp)
target_link_libraries(your_target PRIVATE lmdb-cpp)
```

### Build Commands

```bash
# Configure
cmake -B build -DBUILD_TESTS=1

# Build
cmake --build build

# Run tests
ctest --test-dir build --output-on-failure
```

**CMake options:**

| Flag | Default | Description |
|------|---------|-------------|
| `BUILD_TESTS` | `OFF` | Build the test suite |
| `STATIC_LIBC` | `OFF` | Statically link libc |
| `ARCH` | *(unset)* | Target architecture (e.g. `native`) |

## API Overview

| Class | Purpose |
|-------|---------|
| `Environment` | Singleton-per-path manager. Opens/creates the LMDB environment file, owns databases and transactions. |
| `Database` | Named key-value store within an Environment. Convenience methods handle transactions automatically. |
| `Transaction` | RAII transaction wrapper. Auto-aborts on scope exit if not committed. Can span multiple databases. |
| `Cursor` | Positioned iterator for walking key-value pairs in order. |
| `Error` | Lightweight error type with LMDB error code mapping. Converts to `true` on failure for easy `if (auto err = ...)` checks. |

Full API documentation lives in the header files under `include/`.

## License

This wrapper is provided under the [BSD-3-Clause license](https://en.wikipedia.org/wiki/BSD_licenses). See LICENSE for details.

- **LMDB** -- [The OpenLDAP Public License v2.8](http://www.OpenLDAP.org/license.html)
- **cppfs** -- [MIT License](https://en.wikipedia.org/wiki/MIT_License)
- **snappy** -- [BSD-3-Clause](https://en.wikipedia.org/wiki/BSD_licenses)
