const util = require("util");
const Net = require("net");
const EventEmitter = require("events");
const sleepAsync = util.promisify(setTimeout);
const crypto = require("crypto");
const fs = require("fs");

function randomInt(min, max) {
	return Math.floor(Math.random() * (max - min + 1)) + min;
}

const keepAlivePacket = new Uint8Array([0, 0, 0, 0]);
const operationMap = { 0: "TableCreate", 1: "Open", 2: "Describe", 3: "Insert", 4: "FindOne", 5: "FindMany", 6: "EraseMany", 7: "UpdateMany", 8: "Close", 9: "Rebuild", 10: "AccountCreate", 11: "AccountDelete", 12: "TableSetPermissions", 13: "TableGetPermissions", 14: "GetAllTableNames", 15: "GetAllAccountNames", 16: "AccountGetPermissions", 17: "NoOperation" };

module.exports = class SpeedDBClient extends EventEmitter {
    constructor(config = { socket: { ip: "127.0.0.1", port: 4546 }, auth: {}, cipher: "", queryLogger: false }) {
        if (!config.socket) config.socket = { ip: "127.0.0.1", port: 4546 };

        super();

        this.port = config.socket.port || 4546;
        this.ip = config.socket.ip || "127.0.0.1";
        this.auth = config.auth;
        this.serverVersion = { minor: 0, major: 0 };
        this.activeQueries = {};

        this.buffer = null;
        this.bufferPosition = 0;
        this.bufferRemainingBytes = 0;

        if (config.cipher) {
            this.cipher = config.cipher;
            this.encrypted = true;
        }

        if (config.queryLogger) {
            this.queryLoggerStream = fs.createWriteStream('query-log.txt', { flags: 'w' });
        }
    }

    _encrypt(buffer) {
        const cipher = crypto.createCipheriv('aes-256-cbc', this.secret, this.client_iv);
        let encrypted = cipher.update(buffer);
        encrypted = Buffer.concat([encrypted, cipher.final()]);

        return encrypted;
    }

    _decrypt(buffer) {
        const cipher = crypto.createDecipheriv('aes-256-cbc', this.secret, this.server_iv);
        let decrypted = cipher.update(buffer);
        decrypted = Buffer.concat([decrypted, cipher.final()]);

        return decrypted;
    }

    account(username) {
        return {
            create: (data) => {
                return this._send_query({ o: 10, d: { username, ...data } });
            },
            
            delete: () => {
                return this._send_query({ o: 11, d: { username } });
            },
            
            get_permissions: () => {
                return this._send_query({ o: 16, d: { username } });
            }
        };
    }

    no_operation() {
        return this._send_query({ o: 17, d: {} });
    }

    get_all_table_names() {
        return this._send_query({ o: 14, d: {} });
    }

    get_all_account_names() {
        return this._send_query({ o: 15, d: {} });
    }

    table(name) {
        return {
            create: (columns) => {
                return this._send_query({ o: 0, d: { columns, name } });
            },

            open: () => {
                return this._send_query({ o: 1, d: { table: name } });
            },

            describe: () => {
                return this._send_query({ o: 2, d: { table: name } });
            },

            insert: (columns) => {
                return this._send_query({ o: 3, d: { table: name, columns } });
            },

            findOne: (data) => {
                return this._send_query({ o: 4, d: { table: name, ...data } });
            },

            findMany: (data) => {
                return this._send_query({ o: 5, d: { table: name, ...data } });
            },

            eraseMany: (data) => {
                return this._send_query({ o: 6, d: { table: name, ...data } });
            },

            updateMany: (data) => {
                return this._send_query({ o: 7, d: { table: name, ...data } });
            },

            close: () => {
                return this._send_query({ o: 8, d: { table: name } });
            },

            rebuild: (data) => {
                return this._send_query({ o: 9, d: { table: name } });
            },

            set_permissions_for: (account, permissions) => {
                return this._send_query({ o: 12, d: { table: name, username: account, permissions } });
            },

            get_permissions_for: (account) => {
                return this._send_query({ o: 13, d: { table: name, username: account } });
            }
        };
    }

    _send_query(data) {
        // console.log("Q DBG", `op=${data.o}`, `data=${JSON.stringify(data.d)}`);
        if (this.queryLoggerStream) {
            this.queryLoggerStream.write(`[${operationMap[data.o]}] ` + JSON.stringify(data).replace(/{/g,'{ ').replace(/}/g,' }').replace(/:/g,': ').replace(/,/g,', ') + "\n");
        }

        // Generate a random unique identifier.
        let nonce;
        do {
            nonce = randomInt(0, 999999999);
        } while (nonce in this.activeQueries);

        // Reserve the nonce value.
        this.activeQueries[nonce] = null;
        data["n"] = nonce;

        return new Promise(async (resolve, reject) => {
            // Transform the data into a compliant format.
            let d = Buffer.from(JSON.stringify(data));

            if (this.encrypted) {
                // Encrypt the data before sending.
                d = this._encrypt(d);
            }

            let packet = new Uint8Array([
                0, 0, 0, 0, ...d, 0
            ]);
            const length = packet.length - 4;

            packet[0] = length & 255;
            packet[1] = (length >> 8) & 255;
            packet[2] = (length >> 16) & 255;
            packet[3] = (length >> 24) & 255;

            const timeout = setTimeout(() => {
                delete this.activeQueries[nonce];
                reject("This query has timed out.");
            }, 15000);

            this.activeQueries[nonce] = { resolve, reject, timeout };

            this.socket.write(packet);
        });
    }

    _on_message(raw_data) {
        let data;

        if (this.encrypted) data = JSON.parse(this._decrypt(raw_data).toString());
        else data = JSON.parse(raw_data.toString());

        // If data has no nonce, it is a global message.
        if (!data.n) {
            // Global errors are always fatal.
            if (data.e) {
                this.emit("fatalError", data.d);
            }
        }

        const query = this.activeQueries[data.n];
        if (!query) return;

        clearTimeout(query.timeout);

        if (data.e) query.reject(data.d.t);
        else query.resolve(data.d);

        delete this.activeQueries[data.n];
    }

    /**
     * 
     * @param {Buffer<ArrayBufferLike>} data 
     */
    _on_data(data) {
        if (!this.buffer) {
            // Read the length of the buffer.
            const length = data[0] | (data[1] << 8) | (data[2] << 16) | (data[3] << 24);

            // If the packet is a keep-alive test, send back a keep-alive message.
            if (length === 0) {
                // Send back keep-alive packet to signify the connection is still alive.
                this.socket.write(keepAlivePacket);
                
                // If packet has more data.
                if (data.length > 4) {
                    this._on_data(data.subarray(4));
                }
                
                return;
            }

            // If the entire packet is present.
            if (data.length >= length + 4) {
                this._on_message(data.subarray(4, length + 3));

                // If there is more data.
                if (data.length != length + 4) {
                    this._on_data(data.subarray(data.length + 4));
                }
            }

            // Save the length.
            this.bufferPosition = 0;
            this.bufferRemainingBytes = length;

            // Allocate a new buffer for the data.
            this.buffer = Buffer.allocUnsafe(length);

            // Continue with rest of data if any (minus header).
            if (data.length > 4) this._on_data(data.subarray(4));
        } else {
            const packetPortion = data.subarray(0, this.bufferRemainingBytes);
            
            // Copy packet portion to buffer.
            packetPortion.copy(this.buffer, this.bufferPosition);

            // Increment buffer position and subtract bytes remaining.
            this.bufferRemainingBytes -= packetPortion.length;
            this.bufferPosition += packetPortion.length;

            // If whole packet has been received.
            if (this.bufferRemainingBytes === 0) {
                // Parse JSON (minus terminator because JS doesn't like those).
                this._on_message(this.buffer.subarray(0, -1));

                // Reset buffer.
                this.buffer = null;

                // If there is still data in the arriving packet.
                if (packetPortion.length !== data.length) {
                    // Process new packet.
                    this._on_data(data.subarray(packetPortion.length));
                }
            }
        }
    }    

    async close() {
        return new Promise(async (resolve, reject) => {
            if (Object.values(this.activeQueries).length) {
                while (true) {
                    await sleepAsync(1);
                    if (Object.values(this.activeQueries).length) break;
                }
            }

            this.socket.end();
            this.socket.destroy();

            if (this.queryLoggerStream) {
                this.queryLoggerStream.end();
            }

            resolve();
        });
    }

    async connect() {
        this.socket = new Net.Socket();
        this.socket.setKeepAlive(true, 5000);

        return new Promise((resolve, reject) => {
            this.socket.once("ready", async () => {
                this.socket.removeAllListeners("error");

                let dh;

                // Send handshake.
                let handshakeData = {
                    version: { major: 8, minor: 0 },
                    options: { error_text: true }
                };

                if (this.encrypted) {
                    handshakeData["cipher"] = {
                        algorithm: this.cipher
                    };
                }
                if (this.auth) handshakeData["auth"] = this.auth;
                
                this.socket.write(JSON.stringify(handshakeData));

                // Wait for handshake confirmation.
                let handshakeConfirmation = await new Promise((resolve) => {
                    this.socket.once("data", d => {
                        resolve(JSON.parse(d));
                    });
                });

                if (handshakeConfirmation.e) {
                    reject(handshakeConfirmation.d.t);
                    return;
                }

                if (this.encrypted) {
                    dh = crypto.createDiffieHellman(Buffer.from(handshakeConfirmation.cipher.prime, "base64"), handshakeConfirmation.cipher.generator);
                    dh.generateKeys();
                    const handshakeData2 = {
                        public_key: dh.getPublicKey("base64")
                    };
                    this.socket.write(JSON.stringify(handshakeData2));

                    const secret = dh.computeSecret(Buffer.from(handshakeConfirmation.cipher.public_key, "base64"), null);
                    console.log("Secret computed as:", secret.subarray(0, 32).toString("base64"));
                    this.secret = secret.subarray(0, 32);
                    this.client_iv = Buffer.from(handshakeConfirmation.cipher.initial_iv, "base64");
                    this.server_iv = Buffer.from(handshakeConfirmation.cipher.initial_iv, "base64");

                    let encryptionConfirmation = await new Promise((resolve) => {
                        this.socket.once("data", d => {
                            resolve(JSON.parse(d));
                        });
                    });

                    if (encryptionConfirmation.e) {
                        reject(encryptionConfirmation.d.t);
                        return;
                    }
                }

                this.serverVersion = handshakeConfirmation.version;

                this.socket.on("data", this._on_data.bind(this));

                resolve();
            });

            this.socket.once("error", e => {
                reject(e);
            });

            this.socket.connect(this.port, this.ip);
        });
    }
};