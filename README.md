# Speedy DB
A database written in C++ and C which has been started due to me needing a project for my A-Level computer science NEA and thought this would be a great project and challenge.

# Features
- Most features you'd see in modern databases including inserting, deleting and updating records.
- TCP connection which also has custom-made frames which are optimized to benefit from TCP grouping multiple packets which allows for simulataneous query sending.
- Encryption of the TCP connection without TLS using AES256 cipher-block chaining and the diffie hellman key exchange (not secure! diffie hellman algorithm is cryptographically broken).
- JSON querying language which is designed specifically for languages such as JavaScript and Python which have native JSON support.
- Standard column data types including bytes, integers, floats, strings and longs.
- Strings and other long pieces of data are hashed which allows for the same performance as if you were querying a single number.
- Data is stored in a compact manner which does not sacrifice performance allowing for efficient use of storage space.
- Performance of queries without caching and indexing faster than other modern databases.
- Password protection enforced on connecting clients which also has a back-off period to prevent brute-force attacks.
- Handles high traffic very well.

# Upcoming features
- Caching of frequently queried records to reduce the amount of times needed to query the disk.
- Indexing of tables to allow for near O(1) speed of queries regardless of the size of the table.
- Privilege system which allows for accounts to have specific access and privileges for specific tables.
- More data-types.
- ACID compliance.
- Switching to [simdjson](https://github.com/simdjson/simdjson) / [rapidjson](https://github.com/Tencent/rapidjson) (this was made by Tencent in case this worries you) for the JSON parsing library for better performance and memory footprint.

# Supported platforms
- Linux (no windows yet sorry).
- Little-endian byte order CPU (big-endian may work just fine but this has been untested).

# Bugs
- Password provided by the client is sent in the handshake stage which is not encrypted.

# Libraries used
- [nlohmann json](https://github.com/nlohmann/json) for JSON parsing.
- [xxHash](https://github.com/Cyan4973/xxHash) for fast hashing of data.
- [OpenSSL](https://github.com/openssl/openssl) for cryptography.
- Everything else was written by me.

# Command-line parameters
All parameters are specified without dashes (e.g. `./bin password=hello_world`).
- `password=[your_password]` - specifies the password clients must send in order to connect.
- `no-password`- allows you to start an instance without a password (this will allow anyone to connect).

# What this is not
An enterprise-level database which is reliable and can handle very high traffic reliably.
For something like this you need to use a database such as MySQL/Postgres/MariaDB.

# Notes
This was originally meant to be written in C; however I have opted into using C++ as it allows for better looking and maintainable code as well as built-in structures and methods which would be a hassle to implement in C.
I have also opted into using C APIs for most things such as TCP and disk access as they are less complicated and result in more than 2x better performance than equivalent C++ APIs.
I also had to write this entire README twice as I accidentally clicked off when writing it on GitHub.