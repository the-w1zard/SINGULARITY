import { dark } from "../signals";
import { ENVELOPE_CODEC, PAYLOAD_CODEC } from "./binary";
import { KV_RPC_OP, KV_RPC_OP_VALUE } from "./op";

type PendingRequest = {
	resolve: (value: unknown) => void;
	reject: (error: Error) => void;
	timer: NodeJS.Timeout;
};
export class WAL_KVDB_CLIENT {
    private nextId = 0;
    private readonly timeoutMs = 10_000
    public pending: Record<string, PendingRequest> = Object.create(null);
    public s: S
    public m: S
    public r: S
    public client: A

    constructor(client: A, s: S, m: S, r: S ) {
        this.s = s
        this.m = m
        this.r = r
        this.client = client

        client.bus.sub.infinity((raw) => [
            this.#handleMessage(raw)
        ], 'WAL_KVDB', s, m, r);
    }
  
    public hot_cache: Record<string, unknown> = Object.create(null);
    public get = async <T = unknown>(key: string): Promise<T | undefined> => {
      const cached = this.hot_cache[key];
      if (cached !== undefined) return cached as T;
      const response = await this.#request(KV_RPC_OP.GET, key, { key, value: null, durable: false, ok: true } );
      const value = response.value === null ? undefined : response.value;
      if (value !== undefined) this.hot_cache[key] = value;
      return value as T | undefined;
    };
    public set = async ( key: string, value: Uint8Array | string, options: { durable?: boolean } = {}	): Promise<void> => {
      const bytes = typeof value === "string"? new TextEncoder().encode(value): value;
      this.hot_cache[key] = bytes;
      await this.#request(KV_RPC_OP.SET, key, { key, value: bytes, durable: options.durable ?? true, ok: true } );
    };
    public delete = async (key: string, options: { durable?: boolean } = {}	): Promise<boolean> => {
      delete this.hot_cache[key];
      const response = await this.#request(KV_RPC_OP.DELETE, key, { key, value: null, durable: options.durable ?? true, ok: true } );
      return response.ok;
    };
    public update = async <T = unknown>(key: string, updater: (previous: T | undefined) => T ): Promise<T> => {
      const previous = await this.get<T>(key);
      const next = updater(previous);
      await this.set(key, next as unknown as Uint8Array | string);
      return next;
    };
    public has = async (key: string): Promise<boolean> => {return (await this.get(key)) !== undefined;	};
    #request(op: KV_RPC_OP_VALUE, key: string, payload: { key: string; value: Uint8Array | null; durable: boolean; ok: boolean } ): Promise<{ value: Uint8Array | null; ok: boolean }> {
      const id = this.client.reg._sid()
      const encodedPayload = PAYLOAD_CODEC.encode_msg(payload);
      const frame = ENVELOPE_CODEC.encode_msg({op, id, r: "THE_WIZARD_ROOT_REG", m: "SAURON_MASTER_REG", s: "SAURON_SLAVE_REG", k: key, p: encodedPayload, replyTo: null, error: null });
  
      return new Promise((resolve, reject) => {
        const timer = setTimeout(() => {
          delete this.pending[id];
          reject(new Error(`KV RPC timeout: ${op} ${key}`));
        }, this.timeoutMs);
        this.pending[id] = {resolve: resolve as (value: unknown) => void, reject, timer };
        dark.bus.send(frame, 'WAL_KVDB', this.s, this.m, this.r);
      });
    }
    #handleMessage(raw: Uint8Array): void {
      const message = ENVELOPE_CODEC.decode_msg(raw) as any;
      if (!message || message.replyTo === null) return;
      const pending = this.pending[message.replyTo]
      if (!pending) return;
      delete this.pending[message.replyTo];
      clearTimeout(pending.timer);
      if (message.error) {pending.reject(new Error(message.error.message)); return;	  }
      const payload = PAYLOAD_CODEC.decode_msg(message.p) as any;
      if (!payload) {pending.reject(new Error("Invalid KV payload"));return;}
      pending.resolve(payload);
    }
}
