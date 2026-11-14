export class DISRUPTOR_CRYPTO {
    ser(value: unknown): string {
        if (value instanceof Map) {
            return JSON.stringify({ __type: "Map", value: Array.from(value.entries()).map(([key, val]) => [this.ser(key), this.ser(val)]) });
        }
        if (value instanceof Set) {
            return JSON.stringify({ __type: "Set", value: Array.from(value.values()).map((value) => this.ser(value)) });
        }
        return typeof value === "string" ? value : JSON.stringify(value);
    }
    deser(raw: string | null): any {
        if (raw === null) return null;
        try {
            const parsed = JSON.parse(raw);
            if (parsed?.__type === "Map") {
                return new Map(parsed.value.map(([key, value]: [string, string]) => [this.deser(key), this.deser(value)]));
            }
            if (parsed?.__type === "Set") {
                return new Set(parsed.value.map((value: string) => this.deser(value)));
            }
            return parsed;
        } catch {
            return raw;
        }
    }

    hasNode = typeof process !== "undefined" && !!process.versions?.node;
    _buf(bytes: Uint8Array): Uint8Array<ArrayBuffer> {
        const out = new Uint8Array(new ArrayBuffer(bytes.byteLength)) as Uint8Array<ArrayBuffer>;
        out.set(bytes);
        return out;
    }
    _hasSubtle = typeof globalThis.crypto !== "undefined" && typeof (globalThis.crypto as any).subtle !== "undefined" && typeof (globalThis.crypto as any).getRandomValues === "function";
    _nodeCrypto: any = null;
    _getNodeCrypto() {
        if (this._nodeCrypto) return this._nodeCrypto;
        if (!this.hasNode) return null;
        try {
            this._nodeCrypto = require("node:crypto");
        } catch {
            this._nodeCrypto = null;
        }
        return this._nodeCrypto;
    }
    constructor() {}

    // ── random bytes ────────────────────────────────────────────────────────
    random_bytes(size = 32): Uint8Array {
        if (this._hasSubtle) return globalThis.crypto.getRandomValues(new Uint8Array(size));
        const nc = this._getNodeCrypto();
        if (nc) return new Uint8Array(nc.randomBytes(size));
        // last resort — NOT cryptographically secure, but the bus will refuse
        // to init auth without real randomness, so this never protects anything.
        const out = new Uint8Array(size);
        for (let i = 0; i < size; i++) out[i] = (Math.random() * 256) | 0;
        return out;
    }

    // ── HMAC-SHA256 ─────────────────────────────────────────────────────────
    // Returned as hex so it matches the existing computeHmacSha256 contract.

    // Async version — used when WebCrypto is the only option.
    async hash_hmac_hex_async(secret: string | Uint8Array, data: string | Uint8Array): Promise<string> {
        const keyBytes = typeof secret === "string" ? this.to_utf8_bytes(secret) : secret;
        const dataBytes = typeof data === "string" ? this.to_utf8_bytes(data) : data;

        if (this._hasSubtle) {
            const key = await globalThis.crypto.subtle.importKey(
                "raw",
                this._buf(keyBytes), // ← wrap
                { name: "HMAC", hash: "SHA-256" },
                false,
                ["sign"],
            );
            const sig = await globalThis.crypto.subtle.sign(
                "HMAC",
                key,
                this._buf(dataBytes), // ← wrap
            );
            return this.to_hex(new Uint8Array(sig));
        }

        const nc = this._getNodeCrypto();
        if (nc) {
            const h = nc.createHmac("sha256", Buffer.from(keyBytes));
            h.update(Buffer.from(dataBytes));
            return h.digest("hex");
        }

        return "";
    }

    // Cache used by the sync path when only WebCrypto is available.
    // Populated by hash_hmac_hex_async's callers, read by hash_hmac_hex.
    _hmacCache = new Map<string, string>();

    // ── SHA-256 ─────────────────────────────────────────────────────────────
    async hash_sha256_hex(data: string | Uint8Array): Promise<string> {
        const bytes = typeof data === "string" ? this.to_utf8_bytes(data) : data;

        if (this._hasSubtle) {
            const digest = await globalThis.crypto.subtle.digest(
                "SHA-256",
                this._buf(bytes), // ← wrap
            );
            return this.to_hex(new Uint8Array(digest));
        }

        const nc = this._getNodeCrypto();
        if (nc) {
            const h = nc.createHash("sha256");
            h.update(Buffer.from(bytes));
            return h.digest("hex");
        }

        return "";
    }

    // ── HKDF (derive a session key from a shared secret) ────────────────────
    async derive_hkdf(ikm: Uint8Array, salt: Uint8Array, info: Uint8Array, length = 32): Promise<Uint8Array> {
        if (this._hasSubtle) {
            const key = await globalThis.crypto.subtle.importKey(
                "raw",
                this._buf(ikm), // ← wrap
                "HKDF",
                false,
                ["deriveBits"],
            );
            const bits = await globalThis.crypto.subtle.deriveBits(
                {
                    name: "HKDF",
                    hash: "SHA-256",
                    salt: this._buf(salt), // ← wrap
                    info: this._buf(info), // ← wrap
                },
                key,
                length * 8,
            );
            return new Uint8Array(bits);
        }

        const nc = this._getNodeCrypto();
        if (nc) {
            return new Uint8Array(nc.hkdfSync("sha256", Buffer.from(ikm), Buffer.from(salt), Buffer.from(info), length));
        }

        return new Uint8Array(length);
    }

    // ── AES-GCM (the bus's symmetric p encryption) ────────────────────
    async encrypt_aes_gcm(key: Uint8Array, iv: Uint8Array, data: Uint8Array): Promise<Uint8Array> {
        if (this._hasSubtle) {
            const k = await globalThis.crypto.subtle.importKey(
                "raw",
                this._buf(key), // ← wrap
                "AES-GCM",
                false,
                ["encrypt"],
            );
            const ct = await globalThis.crypto.subtle.encrypt(
                { name: "AES-GCM", iv: this._buf(iv) }, // ← wrap
                k,
                this._buf(data), // ← wrap
            );
            return new Uint8Array(ct);
        }

        const nc = this._getNodeCrypto();
        if (nc) {
            const cipher = nc.createCipheriv("aes-256-gcm", Buffer.from(key), Buffer.from(iv));
            const ct = Buffer.concat([cipher.update(Buffer.from(data)), cipher.final()]);
            const tag = cipher.getAuthTag();
            // append tag, matching WebCrypto's output format
            return new Uint8Array(Buffer.concat([ct, tag]));
        }

        return data;
    }

    async decrypt_aes_gcm(key: Uint8Array, iv: Uint8Array, ct: Uint8Array): Promise<Uint8Array> {
        if (this._hasSubtle) {
            const k = await globalThis.crypto.subtle.importKey(
                "raw",
                this._buf(key), // ← wrap
                "AES-GCM",
                false,
                ["decrypt"],
            );
            const pt = await globalThis.crypto.subtle.decrypt(
                { name: "AES-GCM", iv: this._buf(iv) }, // ← wrap
                k,
                this._buf(ct), // ← wrap
            );
            return new Uint8Array(pt);
        }

        const nc = this._getNodeCrypto();
        if (nc) {
            // split off the 16-byte tag from the end
            const tag = ct.slice(ct.length - 16);
            const body = ct.slice(0, ct.length - 16);
            const decipher = nc.createDecipheriv("aes-256-gcm", Buffer.from(key), Buffer.from(iv));
            decipher.setAuthTag(Buffer.from(tag));
            const pt = Buffer.concat([decipher.update(Buffer.from(body)), decipher.final()]);
            return new Uint8Array(pt);
        }

        return ct;
    }

    // ── Encoding ────────────────────────────────────────────────────────────
    to_utf8_bytes(str: string): Uint8Array {
        if (typeof TextEncoder !== "undefined") return new TextEncoder().encode(str);
        const nc = this._getNodeCrypto();
        if (nc) return new Uint8Array(Buffer.from(str, "utf8"));
        // last resort
        const out = new Uint8Array(str.length);
        for (let i = 0; i < str.length; i++) out[i] = str.charCodeAt(i) & 0xff;
        return out;
    }

    to_hex(data: Uint8Array): string {
        if (typeof Buffer !== "undefined") return Buffer.from(data).toString("hex");
        let out = "";
        for (let i = 0; i < data.length; i++) out += data[i].toString(16).padStart(2, "0");
        return out;
    }

    from_hex(hex: string): Uint8Array {
        if (typeof Buffer !== "undefined") return new Uint8Array(Buffer.from(hex, "hex"));
        const out = new Uint8Array(hex.length / 2);
        for (let i = 0; i < hex.length; i += 2) out[i / 2] = parseInt(hex.slice(i, i + 2), 16);
        return out;
    }

    to_base64(data: Uint8Array): string {
        if (typeof Buffer !== "undefined") return Buffer.from(data).toString("base64");
        let bin = "";
        for (let i = 0; i < data.length; i++) bin += String.fromCharCode(data[i]);
        return btoa(bin);
    }

    from_base64(str: string): Uint8Array {
        if (typeof Buffer !== "undefined") return new Uint8Array(Buffer.from(str, "base64"));
        const bin = atob(str);
        const out = new Uint8Array(bin.length);
        for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
        return out;
    }

    // ── Constants ───────────────────────────────────────────────────────────
    ready(): boolean {
        return this._hasSubtle || this._getNodeCrypto() !== null;
    }

    secure_equal(a: Uint8Array, b: Uint8Array): boolean {
        let diff = a.length ^ b.length;
        const len = Math.max(a.length, b.length);
        for (let i = 0; i < len; i++) diff |= (a[i] ?? 0) ^ (b[i] ?? 0);
        return diff === 0;
    }

    hash_hmac_hex(secret: string | Uint8Array, data: string | Uint8Array): string {
        const nc = this._getNodeCrypto();
        if (!nc) throw new Error("Synchronous HMAC requires Node crypto");
        const key = typeof secret === "string" ? this.to_utf8_bytes(secret) : secret;
        const bytes = typeof data === "string" ? this.to_utf8_bytes(data) : data;
        return nc.createHmac("sha256", Buffer.from(key)).update(Buffer.from(bytes)).digest("hex");
    }

    hash_hmac(secret: string | Uint8Array, data: string | Uint8Array): string {
        return this.hash_hmac_hex(secret, data);
    }

    hash_sha256(data: string | Uint8Array): string {
        const nc = this._getNodeCrypto();
        if (!nc) throw new Error("Synchronous hashing requires Node crypto");
        const bytes = typeof data === "string" ? this.to_utf8_bytes(data) : data;
        return nc.createHash("sha256").update(Buffer.from(bytes)).digest("hex");
    }

    derive_key(password: string, salt: Uint8Array, iterations = 310_000, length = 32): Uint8Array {
        const nc = this._getNodeCrypto();
        if (!nc) throw new Error("Synchronous PBKDF2 requires Node crypto");
        return new Uint8Array(nc.pbkdf2Sync(Buffer.from(this.to_utf8_bytes(password)), Buffer.from(salt), iterations, length, "sha256"));
    }

    symmetric_encrypt(p: string, key: Uint8Array): { ciphertext: string; nonce: string } {
        const nc = this._getNodeCrypto();
        if (!nc) throw new Error("Synchronous encryption requires Node crypto");
        if (key.length !== 32) throw new Error("AES-256-GCM requires a 32-byte key");

        const nonce = nc.randomBytes(12);
        const cipher = nc.createCipheriv("aes-256-gcm", Buffer.from(key), nonce);
        const ciphertext = Buffer.concat([cipher.update(p, "utf8"), cipher.final(), cipher.getAuthTag()]);

        return {
            ciphertext: this.to_base64(new Uint8Array(ciphertext)),
            nonce: this.to_base64(new Uint8Array(nonce)),
        };
    }

    symmetric_decrypt(p: { ciphertext: string; nonce: string }, key: Uint8Array): string {
        const nc = this._getNodeCrypto();
        if (!nc) throw new Error("Synchronous decryption requires Node crypto");
        if (key.length !== 32) throw new Error("AES-256-GCM requires a 32-byte key");

        const ciphertext = this.from_base64(p.ciphertext);
        const nonce = this.from_base64(p.nonce);
        if (nonce.length !== 12 || ciphertext.length < 16) throw new Error("Invalid AES-GCM payload");

        const body = ciphertext.slice(0, -16);
        const tag = ciphertext.slice(-16);
        const decipher = nc.createDecipheriv("aes-256-gcm", Buffer.from(key), Buffer.from(nonce));
        decipher.setAuthTag(Buffer.from(tag));

        return Buffer.concat([decipher.update(Buffer.from(body)), decipher.final()]).toString("utf8");
    }
    generate_key_pair(_type: string): { publicKey: Uint8Array; secretKey: Uint8Array } {
        const nc = this._getNodeCrypto();
        if (!nc) throw new Error("Synchronous key generation requires Node crypto");

        const pair = nc.generateKeyPairSync("ed25519");
        return {
            publicKey: new Uint8Array(pair.publicKey.export({ type: "spki", format: "der" })),
            secretKey: new Uint8Array(pair.privateKey.export({ type: "pkcs8", format: "der" })),
        };
    }

    pqc_generate_KEM_keys(): { publicKey: Uint8Array; secretKey: Uint8Array; algorithm: string } {
        const nc = this._getNodeCrypto();
        if (!nc) throw new Error("PQC key generation requires a supported Node crypto provider");

        const pair = nc.generateKeyPairSync("ml-kem-768");
        return {
            publicKey: new Uint8Array(pair.publicKey.export({ type: "spki", format: "der" })),
            secretKey: new Uint8Array(pair.privateKey.export({ type: "pkcs8", format: "der" })),
            algorithm: "ML-KEM-768",
        };
    }
}

export const BUS_CRYPTO  = new DISRUPTOR_CRYPTO()
