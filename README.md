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
- Write caching and deferring writes intelligently.
- Indexing of tables to allow for near O(1) speed of queries regardless of the size of the table.
- More data-types.
  - Large data type used for optimised transfers and caching procedures of data in the range of KB-GB.
  - Strings but with fixed lengths which are stored in the efficient and predictable record area for performance when size of a string is known beforehand. 
- ACID compliance.
- New data type - references to other records in same or different table.
- Query planner query which can be used to debug the execution plan of a query.

# Supported platforms
- Linux (no windows yet sorry).
- Little-endian byte order CPU (big-endian may work just fine but this has been untested).

# Bugs
- Password provided by the client is sent in the handshake stage which is not encrypted.

# Libraries used
- [simdjson](https://github.com/simdjson/simdjson) for accelerated JSON parsing.
- [RapidJSON](https://github.com/Tencent/rapidjson) for building of JSON data.
- [xxHash](https://github.com/Cyan4973/xxHash) for fast hashing of data.
- [OpenSSL](https://github.com/openssl/openssl) for cryptography.
- Everything else was written by me.

# Command-line parameters
All parameters are specified without dashes (e.g. `./bin password=hello_world`).
- `password=[your_password]` - specifies the password clients must send in order to connect.
- `no-password`- allows you to start an instance without a password (this will allow anyone to connect).
- `force-encrypted-traffic` - allows you to force connecting clients to connect with encryption enabled.
- `port=[your_port]` - allows you to set the listening port for the database.
- `max-connections=[maximum_connections]` - allows you to set the maximum amount of concurrent connections.

# What this is not
An enterprise-level database which is reliable and can handle very high traffic reliably.
For something like this you need to use a database such as MySQL/Postgres/MariaDB.

# JavaScript example
```js
const db = new Client({
	socket: {
		ip: "127.0.0.1",
		port: 4546
	},
	auth: {
		password: "mydatabaseisthebest"
	},
	cipher: "diffie-hellman-aes256-cbc" // This is the only cipher supported at the moment.
});

await db.connect();

db.on("fatalError", d => {
	console.error("Fatal error from SpeedyDB:", d);
});

// Create table - catch to prevent error if table already exists.
await db.table("users").create({
    name: { type: "string" },
	age: { type: "byte" },
    balance: { type: "float" }
}).catch(() => null);

// Open table - catch to prevent error if table is already open.
await db.table("users").open().catch(() => null);

const users = await db.table("users").findMany({
    // Query conditions - leave empty for all.
    where: {
        // Multiple conditions in a where turns into an AND.
        name: "henry",
        age: { greater_than_equal_to: 30, less_than: 80 }
    },
    // Return only the name and balance of records - remove for all.
    return: ['name', 'balance'],
    // Only return the first 5 records that match the criteria - remove for no limit.
    limit: 5,
    // Advanced queries ahead! None are necessary.
    // Start looking for data only after this condition is satisfied - useful for finding data before/after a certain date.
    seek_where: {},
    // Specify the direction the database searches for records (1 = start-end, -1 = end-start) - useful for ordering data a certain way.
    seek_direction: 1
}); // [{ name: "henry", balance: 21.83 }, { name: "henry", balance: 238.0 }, ...]

// More examples coming soon!
```

# Notes
This was originally meant to be written in C; however I have opted into using C++ as it allows for better looking and maintainable code as well as built-in structures and methods which would be a hassle to implement in C.
I have also opted into using C APIs for most things such as TCP and disk access as they are less complicated and result in more than 2x better performance than equivalent C++ APIs.
I also had to write this entire README twice as I accidentally clicked off when writing it on GitHub.
 
