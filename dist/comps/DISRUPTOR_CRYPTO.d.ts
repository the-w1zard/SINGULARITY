export declare class DISRUPTOR_CRYPTO {
    ser(value: unknown): string;
    deser(raw: string | null): any;
    hasNode: boolean;
    _buf(bytes: Uint8Array): Uint8Array<ArrayBuffer>;
    _hasSubtle: boolean;
    _nodeCrypto: any;
    _getNodeCrypto(): any;
    constructor();
    random_bytes(size?: number): Uint8Array;
    hash_hmac_hex_async(secret: string | Uint8Array, data: string | Uint8Array): Promise<string>;
    _hmacCache: Map<string, string>;
    hash_sha256_hex(data: string | Uint8Array): Promise<string>;
    derive_hkdf(ikm: Uint8Array, salt: Uint8Array, info: Uint8Array, length?: number): Promise<Uint8Array>;
    encrypt_aes_gcm(key: Uint8Array, iv: Uint8Array, data: Uint8Array): Promise<Uint8Array>;
    decrypt_aes_gcm(key: Uint8Array, iv: Uint8Array, ct: Uint8Array): Promise<Uint8Array>;
    to_utf8_bytes(str: string): Uint8Array;
    to_hex(data: Uint8Array): string;
    from_hex(hex: string): Uint8Array;
    to_base64(data: Uint8Array): string;
    from_base64(str: string): Uint8Array;
    ready(): boolean;
    secure_equal(a: Uint8Array, b: Uint8Array): boolean;
    hash_hmac_hex(secret: string | Uint8Array, data: string | Uint8Array): string;
    hash_hmac(secret: string | Uint8Array, data: string | Uint8Array): string;
    hash_sha256(data: string | Uint8Array): string;
    derive_key(password: string, salt: Uint8Array, iterations?: number, length?: number): Uint8Array;
    symmetric_encrypt(p: string, key: Uint8Array): {
        ciphertext: string;
        nonce: string;
    };
    symmetric_decrypt(p: {
        ciphertext: string;
        nonce: string;
    }, key: Uint8Array): string;
    generate_key_pair(_type: string): {
        publicKey: Uint8Array;
        secretKey: Uint8Array;
    };
    pqc_generate_KEM_keys(): {
        publicKey: Uint8Array;
        secretKey: Uint8Array;
        algorithm: string;
    };
}
export declare const BUS_CRYPTO: DISRUPTOR_CRYPTO;
//# sourceMappingURL=DISRUPTOR_CRYPTO.d.ts.map