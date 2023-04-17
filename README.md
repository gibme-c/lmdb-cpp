# lmdb-cpp: A simple OOP-style wrapper around [lmdb](https://en.wikipedia.org/wiki/Lightning_Memory-Mapped_Database)

This library is designed to be as simple as possible to use; however, we do not guarantee full coverage of the entire
LMDB API. If you're in need of something, please feel free to submit an issue or open a pull request to add the
functionality you need. Otherwise, WYSIWYG.

The following *features* have been baked in:

* Builds with CMake making it easy to add to other projects
* GibHub Actions verifies that it builds on Ubuntu, Windows, and MacOS using various compilers
* The *option* to compress values stored in the database using 
  [snappy compression](https://github.com/google/snappy) at a small performance cost.
* Use of shared pointers and singleton patterns to attempt thread-safety while maintaining LMDB sanity.
* Underlying LMDB instances are closed up as required as the shared pointers are destructed (ie. when the last instance 
  of the shared pointer leaves scope).
* Transactions are **automatically aborted** unless you explicitly commit them.

## Documentation

C++ API documentation can be found in the headers (.h)

### Example Use

```c++
#include <lmdb_cpp.hpp>
#include <iostream>

int main () {
    auto env = LMDB::Environment::instance("sample.db");
    
    auto db = env->database();
    
    const auto key = std::string("akey");
    const auto value = std::vector<bool>(10, true);
    
    db->put(key.data(), key.size(), value.data(), value.size());
    
    const auto [error, temp_value] = db->get(key.data(), key.size());
}
```

### Cloning the Repository

This repository uses submodules, make sure you pull those before doing anything if you are cloning the project.

```bash
git clone --recursive https://github.com/gibme-c/lmdb-cpp
```

### As a dependency

```bash
git submodule add https://github.com/gibme-c/lmdb-cpp external/lmdb
git submodule update --init --recursive
```

## License

This wrapper is provided under the [BSD-3-Clause license](https://en.wikipedia.org/wiki/BSD_licenses) found in LICENSE.

* LMDB is licensed under [The OpenLDAP Public License v2.8](http://www.OpenLDAP.org/license.html)
* cppfs is licensed under the [MIT License](https://en.wikipedia.org/wiki/MIT_License)
* snappy is licensed under the [BSD-3-Clause license](https://en.wikipedia.org/wiki/BSD_licenses)
