import { ENVELOPE_CODEC, PAYLOAD_CODEC } from "./binary";
import { KV_RPC_OP, KV_RPC_OP_VALUE } from "./op";

type PendingRequest = {
	resolve: (value: unknown) => void;
	reject: (error: Error) => void;
	timer: NodeJS.Timeout;
};
export class WAL_KVDB_CLIENT {
  private nextId = 0;
  private readonly timeoutMs = 10_000;
  public pending: R<S, PendingRequest> = Object.create(null);
  public s: S;
  public m: S;
  public r: S;
  public client: A;
  public hot_cache: R<S, U> = Object.create(null);
  #queue: Array<() => P<U> | V> = [];
  #draining = false;

  constructor(client: A, s: S, m: S, r: S) {
      this.s = s;
      this.m = m;
      this.r = r;
      this.client = client;
      client.bus.sub.infinity((raw) => { this.#handleMessage(raw); }, 'WAL_KVDB', s, m, r);
  }
  /* ------------------------------------------------------------------ */
  /* USER PATH                                                           */
  /* ------------------------------------------------------------------ */
  public get = <T = U>(key: S) => {
      const cached = this.hot_cache[key];
      if (cached !== undefined) {  return MaybePromise(cached as T);      } 
      else {  return MaybePromise(this.get_worker<T>(key)); }
  };
  public set = (key: S, value: U8 | S, options: { durable?: B } = {}  ): V => {
      this.hot_cache[key] = value;
      this.#enqueue(() => { this.set_worker(key, value, options); });
  };
  public delete = ( key: S, options: { durable?: B } = {}  ): V => {
      delete this.hot_cache[key];
      this.#enqueue(() => {this.delete_worker(key, options);});
  };
  public update = <T = U>(key: S, updater: A  ) => {
      const previous = this.hot_cache[key];

      if (previous !== undefined) {
          const next = typeof updater === "function" ? updater(previous) as T : updater as T;
          this.hot_cache[key] = next;
          this.#enqueue(() => {this.set_worker(key, next as Uint8Array | S, {} ); });

          return MaybePromise(next);
      } else {
          return MaybePromise(
              this.get_worker<T>(key).then((value) => {
                  const next = typeof updater === "function" ? updater(previous) as T : updater as T;
                  this.hot_cache[key] = next;
                  this.set_worker(key, next as U8 | S, {} ).then(() => next);
              })
          );
      }
  };
  public get_worker<T = U>(key: S): P<T | UD> {
      return this.#request(
          KV_RPC_OP.GET,
          key,
          {
              key,
              value: null,
              durable: false,
              ok: true,
          }
      ).then((response) => {
          const value = response.value === null
              ? undefined
              : response.value;

          return value as T | undefined;
      });
  }
  public set_worker( key: S, value: U8 | S, options: { durable?: B } = {}  ): P<V> {
      const bytes = typeof value === 'string'
          ? new TextEncoder().encode(value)
          : value;

      return this.#request(
          KV_RPC_OP.SET,
          key,
          {
              key,
              value: bytes,
              durable: options.durable ?? true,
              ok: true,
          }
      ).then(() => undefined);
  }
  public delete_worker( key: S, options: { durable?: B } = {}  ): P<B> {
      return this.#request(
          KV_RPC_OP.DELETE,
          key,
          {
              key,
              value: null,
              durable: options.durable ?? true,
              ok: true,
          }
      ).then((response) => {
          return response.ok;
      });
  }
  public update_worker<T = U>(key: S, updater: (previous: T | UD) => T  ): P<T> {
      return this.get_worker<T>(key).then((previous) => {
          const next = updater(previous);

          this.hot_cache[key] = next;

          return this.set_worker(
              key,
              next as Uint8Array | string,
              {}
          ).then(() => next);
      });
  }
  public has = (key: S) => {
      const cached = this.hot_cache[key];
      if (cached !== undefined) { return MaybePromise(true); } 
      else { return MaybePromise(this.get_worker(key).then((value) => {  value !== undefined; }) );  }
  };
  public waiters: Record<S, Set<(value: unknown) => void>> = Object.create(null);
  #patternWaiters: Array<{pattern: S; cb: (key: S, value: A | undefined) => void;}> = [];
  #resolveWaiters(key: S, value: A | undefined): void {
      const waiterKey = `__waiters_${key}`;
      const set = this.waiters[waiterKey] as
          | Set<(value: A | undefined) => void>
          | undefined;

      if (!set) return;

      delete this.waiters[waiterKey];

      for (const resolve of set) {
          resolve(value);
      }
  }
  private _emit(key: S, value: A | undefined): void {
      const set = this.waiters[key];

      if (set) {
          for (const cb of set) {
              try {
                  cb(value);
              } catch (error) {
                  this._onError(error);
              }
          }
      }

      for (const watcher of this.#patternWaiters) {
          if (this.#matchesPattern(key, watcher.pattern)) {
              try {
                  watcher.cb(key, value);
              } catch (error) {
                  this._onError(error);
              }
          }
      }
  }
  #matchesPattern(key: S, pattern: S): boolean {
      if (pattern === '*') return true;

      if (pattern.endsWith('*')) {
          return key.startsWith(pattern.slice(0, -1));
      }

      return key === pattern;
  }
  /* ------------------------------------------------------------------ */
  /* WORK QUEUE                                                          */
  /* ------------------------------------------------------------------ */
  #enqueue(task: () => P<U> | V): V {
      this.#queue.push(task);

      void this.#drain();
  }
  async #drain(): P<V> {
      if (this.#draining) return;

      this.#draining = true;

      while (this.#queue.length !== 0) {
          const task = this.#queue.shift()!;

          try {
              await task();
          } catch {
              // The user path must not depend on worker failures.
          }
      }

      this.#draining = false;
  }
  /* ------------------------------------------------------------------ */
  /* RPC                                                                 */
  /* ------------------------------------------------------------------ */
  #request(
      op: KV_RPC_OP_VALUE,
      key: S,
      payload: {
          key: S;
          value: U8 | NU;
          durable: B;
          ok: B;
      }
  ): P<{ value: U8 | NU; ok: B }> {
      const id = this.client.reg._sid();

      const encodedPayload = PAYLOAD_CODEC.encode_msg(payload);

      const frame = ENVELOPE_CODEC.encode_msg({
          op,
          id,
          r: this.r,
          m: this.m,
          s: this.s,
          k: key,
          p: encodedPayload,
          replyTo: null,
          error: null,
      });

      return new Promise((resolve, reject) => {
          const timer = setTimeout(() => {
              delete this.pending[id];

              reject(new Error(`KV RPC timeout: ${op} ${key}`));
          }, this.timeoutMs);

          this.pending[id] = {
              resolve: resolve as (value: unknown) => void,
              reject,
              timer,
          };

          this.client.bus.send(frame, 'WAL_KVDB', this.s, this.m, this.r);
      });
  }
  /* ------------------------------------------------------------------ */
  /* INBOUND                                                             */
  /* ------------------------------------------------------------------ */
  #handleMessage(raw: U8): V {
      const message = ENVELOPE_CODEC.decode_msg(raw) as any;

      if (!message) return;

      const payload = message.p
          ? PAYLOAD_CODEC.decode_msg(message.p) as any
          : null;

      if (message.replyTo !== null && message.replyTo !== undefined) {
          const pending = this.pending[message.replyTo];

          if (pending) {
              delete this.pending[message.replyTo];
              clearTimeout(pending.timer);

              if (message.error) {
                  pending.reject(new Error(
                      typeof message.error === 'string'
                          ? message.error
                          : message.error?.message ?? 'KV RPC failed'
                  ));
              } else {
                  pending.resolve(payload);
              }
          }
      }

      this.#handleHot(message, payload);
  }
  #handleHot(msg: A, payload: A): V {
      if (msg.op === KV_RPC_OP.GET || msg.op === XE.CLIENT_HOT || msg.op === XE.CLIENT_HAS) {
          this.hot_cache[msg.k] = payload.value ?? payload.v;
          this.#resolveWaiters(msg.k, payload.value ?? payload.v);
          this._emit(msg.k, payload.value ?? payload.v);
      }
      else if (msg.op === XE.CLIENT_SET || msg.op === XE.CLIENT_UPDATE || msg.op === XE.CLIENT_RESET ) {
          this.hot_cache[msg.k] = payload.value ?? payload.v;
          this.#resolveWaiters(msg.k, payload.value ?? payload.v);
          this._emit(msg.k, payload.value ?? payload.v);
      }
      else if (msg.op === XE.CLIENT_SET_MANY || msg.op === XE.CLIENT_UPDATE_MANY) {
          for (const entry of payload.entries) {this.hot_cache[entry.k] = entry.v;}
      }
      else if (msg.op === XE.CLIENT_DELETE) {
          delete this.hot_cache[msg.k];
          this.#resolveWaiters(msg.k, undefined);
          this._emit(msg.k, undefined);
      }
      else if (msg.op === XE.CLIENT_DELETE_MANY) {
          for (const key of payload.keys) {
            delete this.hot_cache[msg.k];
            this.#resolveWaiters(msg.k, undefined);
            this._emit(msg.k, undefined);
          }
      }
  }
}
