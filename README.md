**Note:** This is a public mirror. Not much has changed however; issues & pull requests here are still accepted.

# Speedy DB
A database written in C++ and C which has been started due to me needing a project for my A-Level computer science NEA and thought this would be a great project and challenge.

# Features
- Most features you'd see in modern databases including inserting, deleting and updating records.
- TCP connection which also has custom-made frames which are optimized to benefit from TCP grouping multiple packets which allows for simulataneous query sending.
- Encryption of the connection without TLS using AES256 cipher-block chaining and the diffie hellman key exchange with a known safe prime.
- JSON querying language which is designed specifically for languages such as JavaScript and Python which have native JSON support.
- Standard column data types including bytes, integers, floats, strings and longs.
- Strings and other long pieces of data are hashed which allows for the same performance as if you were querying a single number.
- Data is stored in a compact manner which does not sacrifice performance allowing for efficient use of storage space.
- Performance of queries without caching and indexing faster than other modern databases.
- Password protection enforced on connecting clients which also has a back-off period to prevent brute-force attacks.
- Handles high traffic very well.

# Goals / Philosophy
- **Flexibility and choice:** Most of the databases you see lean in a specific direction, they're either overly restrictive with what queries or features they allow for performance reasons (e.g. MySQL, MariaDB), or they are extremely relaxed/flexible in terms of schema and queries where performance is an afterthought (e.g. MongoDB). SpeedyDB wants to provide developers with both. You should be able to pick between strict schemas and strongly typed queries for maximum performance, as well as being able to use the database in more "relaxed/flexible" ways (e.g. querying or modifying data with custom JavaScript sent to the DB). Databases are used in increasingly variable ways, and I think the solution should be that the database should support multiple styles of usage instead of the solution being to "use another database for that". Data tends to be highly volatile, and I don't think the approach of "do one thing well" fits databases particularly.

# Security / Containerization
Since I feel quite guilty about some of the code design decisions I made here, I've created 2 options for locking the database down and limiting the impact of a zero-day attack on the database.

## Option 1: Docker / Podman Container
I've added a `Dockerfile` in `~/c/` which lets you setup a minimal rootless Docker/Podman container running under Alpine Linux, I would heavily recommend running this under Podman which has much better rootless containerization than Docker, nothing that the database does needs root access, and since Docker installations typically run all containers as root, this is a grave security risk.
- To build the image simply `cd` into the `~/c/` directory and run `(docker/podman) build -t speedy-db .`.
- The `docker-compose.yml` file contains a typical setup which you can run using `(docker/podman)-compose up --detach`, which mounts a data directory at `/var/lib/speedy-db` (make sure the directory exists).
  - **Note:** The rootless user runs under UID `1000`, to give the underlying containerized user access to the data directory, allocate the namespace mount for the user running the container in the `/etc/subuid` file (if you are running rootless) and `chown` the data directory to be owned by the `1000` UID offset by the start of your allocated namespace mount (if you have no idea what any of this means, if you paste it into an LLM it will tell you exactly what to do).

## Option 2: AppArmor Profile (MAC)
I've added an AppArmor profile in `~/c/` which is locked down to access only the resources that the database needs and will make the database much more secure should anything happen, AA can completely thwart an otherwise possible attack attempt with a good profile.
- Copy the file in `~/c/production.profile` to `/etc/apparmor.d/`.
- Modify the profile to point to the DB binary (otherwise the profile assumes the DB binary is in `/usr/lib/speedy-db/production`).
- Modify the profile to point to the correct DB data directory (otherwise the profile assumes the data directory is at `/var/lib/speedy-db/`).
- Enable the profile using `aa-enforce /etc/apparmor.d/production.profile`.
- You are done.

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
- Linux X86 (no windows yet, but Node.JS client driver supports practically every environment that Node.JS supports).
  - ARM64 should work fine but there may be compatibility issues with RapidJSON & slight performance drops with xxHash & simdjson. 
- Little-endian byte order CPU (big-endian may work just fine but this has been untested).

# Libraries used
- [simdjson](https://github.com/simdjson/simdjson) for accelerated JSON parsing.
- [RapidJSON](https://github.com/Tencent/rapidjson) for building of JSON data.
- [xxHash](https://github.com/Cyan4973/xxHash) for fast hashing of data.
- [OpenSSL](https://github.com/openssl/openssl) for cryptography (uses system bundle).
- Everything else was written by me.

# Command-line parameters
All parameters are specified without dashes (e.g. `./bin port=4547`).
- `force-encrypted-traffic` - allows you to force connecting clients to connect with encryption enabled.
- `port=[your_port]` - allows you to set the listening port for the database.
- `max-connections=[maximum_connections]` - allows you to set the maximum amount of concurrent connections.
- `data-directory=[path]` - allows you to set a custom data directory location (default `./data`).
- `enable-root-account` - enables the disabled root account with a temporary password which can be used to create initial user accounts.

# What this is not
An enterprise-level database which is reliable and can handle very high traffic reliably.
Despite having a comprehensive fuzzing and testing process in place as well as using the database for my own mail server, nothing replaces a proper test like many different users with different requirements testing the software.
Alongside this, it doesn't (yet) have flexible queries present like in mainstream databases.
For something like this, you should use a database such as MySQL/Postgres/MariaDB.

# JavaScript example
```js
const db = new Client({
	socket: {
		ip: "127.0.0.1",
		port: 4546
	},
	auth: {
    username: "somethingcreative",
		password: "mydatabaseisthebest"
	},
	cipher: "diffie-hellman-aes256-cbc"
});

await db.connect();

db.on("fatalError", d => {
	console.error("Fatal error from SpeedyDB:", d);
});

// Create table - catch to prevent error if table already exists.
await db.table("users").create({
    name: { type: "string" },
	  age: { type: "byte" },
    balance: { type: "float" }, /* FYI floats are safe to use for currency... provided they stay within the float precision range and no arithmetic is done! */
    favourite_number: { type: "long" }
}).catch(() => null);

const users = await db.table("users").findMany({
    // Query conditions - leave empty for all.
    where: {
        // Multiple conditions in a where turns into an AND.
        name: "henry",
        // Range query, age >= 30 but less than 80.
        age: { ">=": 30, "<": 80 },
        // A negation query, return all records if balance != 0 (all advanced query operators support negation via '!').
        balance: { "!==": 0.0 },
        // Contains query, matches columns that are any of the following: 21, 52, 91 or 100.
        favourite_number: { "in": [21, 52, 91, 100] }
    },
    // Return only the name and balance of records - remove for all.
    return: ['name', 'balance'],
    // Only return the first 5 records that match the criteria - remove for no limit.
    limit: 5,
    // Skip 1 matched record before beginning to return records (does not count towards limit).
    offset: 1
}); // [{ name: "henry", balance: 21.83 }, { name: "henry", balance: 238.0 }, ...]

// More examples coming soon!
```

# Notes
Something worth keeping in mind is that when I started this project, this was straight after writing my own operating system kernel which was my first ever exposure to assembly/c/c++/low level code, and so I have learnt a lot of practices that are essential in operating system development but not necessarily helpful when writing modern userspace low-level software, and pairing this with a fundamental misunderstanding of the language and typical practices, you end up with some weird unsafe code, but I am trying my best to rewrite/modern-ise it as I go. 
This was originally meant to be written in C; however I have opted into using C++ as it allows for better looking and maintainable code as well as built-in structures and methods which would be a hassle to implement in C.
I have also opted into using C APIs for most things such as TCP and disk access as they are less complicated and result in more than 2x better performance than equivalent C++ APIs.
I also had to write this entire README twice as I accidentally clicked off when writing it on GitHub.