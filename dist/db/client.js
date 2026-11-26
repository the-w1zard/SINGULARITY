import { dark } from "../signals";
import { ENVELOPE_CODEC, PAYLOAD_CODEC } from "./binary";
import { KV_RPC_OP } from "./op";
export class WAL_KVDB_CLIENT {
    nextId = 0;
    timeoutMs = 10_000;
    pending = Object.create(null);
    s;
    m;
    r;
    client;
    constructor(client, s, m, r) {
        this.s = s;
        this.m = m;
        this.r = r;
        this.client = client;
        client.bus.sub.infinity((raw) => [
            this.#handleMessage(raw)
        ], 'WAL_KVDB', s, m, r);
    }
    hot_cache = Object.create(null);
    get = async (key) => {
        const cached = this.hot_cache[key];
        if (cached !== undefined)
            return cached;
        const response = await this.#request(KV_RPC_OP.GET, key, { key, value: null, durable: false, ok: true });
        const value = response.value === null ? undefined : response.value;
        if (value !== undefined)
            this.hot_cache[key] = value;
        return value;
    };
    set = async (key, value, options = {}) => {
        const bytes = typeof value === "string" ? new TextEncoder().encode(value) : value;
        this.hot_cache[key] = bytes;
        await this.#request(KV_RPC_OP.SET, key, { key, value: bytes, durable: options.durable ?? true, ok: true });
    };
    delete = async (key, options = {}) => {
        delete this.hot_cache[key];
        const response = await this.#request(KV_RPC_OP.DELETE, key, { key, value: null, durable: options.durable ?? true, ok: true });
        return response.ok;
    };
    update = async (key, updater) => {
        const previous = await this.get(key);
        const next = updater(previous);
        await this.set(key, next);
        return next;
    };
    has = async (key) => { return (await this.get(key)) !== undefined; };
    #request(op, key, payload) {
        const id = this.client.reg._sid();
        const encodedPayload = PAYLOAD_CODEC.encode_msg(payload);
        const frame = ENVELOPE_CODEC.encode_msg({ op, id, r: "THE_WIZARD_ROOT_REG", m: "SAURON_MASTER_REG", s: "SAURON_SLAVE_REG", k: key, p: encodedPayload, replyTo: null, error: null });
        return new Promise((resolve, reject) => {
            const timer = setTimeout(() => {
                delete this.pending[id];
                reject(new Error(`KV RPC timeout: ${op} ${key}`));
            }, this.timeoutMs);
            this.pending[id] = { resolve: resolve, reject, timer };
            dark.bus.send(frame, 'WAL_KVDB', this.s, this.m, this.r);
        });
    }
    #handleMessage(raw) {
        const message = ENVELOPE_CODEC.decode_msg(raw);
        if (!message || message.replyTo === null)
            return;
        const pending = this.pending[message.replyTo];
        if (!pending)
            return;
        delete this.pending[message.replyTo];
        clearTimeout(pending.timer);
        if (message.error) {
            pending.reject(new Error(message.error.message));
            return;
        }
        const payload = PAYLOAD_CODEC.decode_msg(message.p);
        if (!payload) {
            pending.reject(new Error("Invalid KV payload"));
            return;
        }
        pending.resolve(payload);
    }
}
