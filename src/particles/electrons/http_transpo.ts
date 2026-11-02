// electrons/http_transpo.ts
// ═════════════════════════════════════════════════════════════════════════════
//  HTTP_TRANSPORT — send your bus over the web to another IP, before any
//  portal/dashboard exists. Bare HTTP, zero config beyond a port and a token.
//
//  Two halves, one class:
//   • OUTBOUND — send(endpoint, envelope): POST the CBOR-encoded envelope,
//     await the ack frame. Backoff on failure. No bus required to send.
//   • INBOUND  — attach(server, path): accept POSTs, decode, hand to a
//     dispatcher (your bus's send path, or any fn). Ack = CBOR error frame.
//
//  Wire format: BINARY_DISRUPTOR's encode_msg/decode_msg (CBOR + topic-id
//  header). Envelope shape: { rt, ms, sl, key, payload, seq, ts }.
//
//  Works in Node / Bun / Deno. Uses only WHATWG fetch + a pluggable server
//  hook — the runtime's own http server is expected to call .attach().
//
//  Auth: bearer token checked BEFORE dispatch. Untrusted boundary first,
//  then the ELECTRON pipeline (permissions/schema/history) stays authoritative
//  for everything after the token.
// ═════════════════════════════════════════════════════════════════════════════

import type { BinaryCodecLike } from './Soul ELECTRONs Binary Sink';

export interface HttpEnvelope {
    rt: string;
    ms: string;
    sl: string;
    key: string;
    payload: unknown;
    seq?: number;
    ts?: number;
    id?: string;
}

export interface HttpTransportOptions {
    /** Shared bearer token. Checked on every inbound request. */
    token: string;
    /** Outbound: request timeout, ms. */
    timeoutMs?: number;
    /** Outbound: first retry backoff, ms. Doubles, capped. */
    backoffMs?: number;
    maxBackoffMs?: number;
    /** Outbound: max attempts per send. 1 = no retry. */
    maxAttempts?: number;
    /** The CBOR codec (BINARY_DISRUPTOR's encode_msg/decode_msg). */
    codec: BinaryCodecLike;
    /** Called on any send failure after retries, or protocol violation. */
    onError?: (err: unknown) => void;
}

const seqId = (() => {
    let n = 0;
    return (): string => `t${Date.now().toString(36)}-${(n++).toString(36)}`;
})();

export class HTTP_TRANSPORT {
    private readonly token: string;
    private readonly timeoutMs: number;
    private readonly backoffMs: number;
    private readonly maxBackoffMs: number;
    private readonly maxAttempts: number;
    private readonly codec: BinaryCodecLike;
    private readonly onError: (err: unknown) => void;

    /** Inbound dispatcher — set at attach(). Receives decoded envelopes. */
    private dispatch: ((env: HttpEnvelope) => Promise<unknown> | unknown) | null = null;

    constructor(opts: HttpTransportOptions) {
        if (!opts.token) throw new Error('[http-transport] token required — an open bus on the wire is not a bus, it is a hole');
        this.token = opts.token;
        this.timeoutMs = opts.timeoutMs ?? 10_000;
        this.backoffMs = opts.backoffMs ?? 100;
        this.maxBackoffMs = opts.maxBackoffMs ?? 5_000;
        this.maxAttempts = opts.maxAttempts ?? 3;
        this.codec = opts.codec;
        this.onError = opts.onError ?? (() => { /* history is the record; the gate decides display */ });
    }

    // ── OUTBOUND: send an envelope to another IP ─────────────────────────────
    async send(endpoint: string, env: HttpEnvelope): Promise<{ ok: boolean; result?: unknown; error?: string }> {
        const envelope: Required<Pick<HttpEnvelope, 'ts' | 'id'>> & HttpEnvelope = {
            ts: Date.now(),
            id: seqId(),
            ...env,
        };
        const body = this.codec.encode_msg(envelope);

        let attempt = 0;
        let lastErr: unknown = null;

        while (attempt < this.maxAttempts) {
            attempt++;
            try {
                const res = await fetch(endpoint, {
                    method: 'POST',
                    headers: {
                        'content-type': 'application/octet-stream',
                        'authorization': `Bearer ${this.token}`,
                    },
                    body: body as unknown as BodyInit,
                    signal: AbortSignal.timeout(this.timeoutMs),
                });

                if (!res.ok) {
                    lastErr = new Error(`[http-transport] endpoint ${endpoint} responded ${res.status}`);
                    // 4xx = caller's fault, retrying will not help. Stop now.
                    if (res.status >= 400 && res.status < 500) break;
                    continue; // 5xx / network: retry with backoff
                }

                const raw = new Uint8Array(await res.arrayBuffer());
                const decoded = this.codec.decode_msg(raw);
                if (decoded === null) {
                    lastErr = new Error(`[http-transport] endpoint ${endpoint} returned undecodable ack`);
                    continue;
                }
                return decoded as { ok: boolean; result?: unknown; error?: string };
            } catch (err) {
                lastErr = err;
                // network-level failure: retry with backoff
            }

            if (attempt < this.maxAttempts) {
                await new Promise(r => setTimeout(r, Math.min(this.backoffMs * 2 ** (attempt - 1), this.maxBackoffMs)));
            }
        }

        this.onError(lastErr);
        return { ok: false, error: lastErr instanceof Error ? lastErr.message : String(lastErr) };
    }

    // ── INBOUND: accept POSTs from the wire ──────────────────────────────────
    /**
     * Wire this into any runtime's http server. `handler` must:
     *   • match on the path (transport does not route — the host decides)
     *   • read the full request body into a Uint8Array
     *   • call this.receive(bytes) and send back the returned ack frame
     * Node:    http.createServer((req, res) => { if (transport.matches(req)) { ... res.end(ack) } })
     * Bun:     Bun.serve({ fetch: (req) => transport.matchesBun(req) ? new Response(ack) : undefined })
     * Deno:    same shape as Bun via req.arrayBuffer()
     */
    attach(dispatch: (env: HttpEnvelope) => Promise<unknown> | unknown): void {
        this.dispatch = dispatch;
    }

    /** Path/content-type matcher for host servers. */
    matches(pathname: string, method: string): boolean {
        return method === 'POST' && pathname === '/bus';
    }

    /**
     * The one method a host server calls: bytes in → ack bytes out.
     * Auth is checked BEFORE dispatch. Dispatch happens inside try/catch —
     * a throwing handler becomes an error frame, never a crash.
     */
    async receive(bytes: Uint8Array, authHeader: string | null): Promise<Uint8Array> {
        const ack = (ok: boolean, result?: unknown, error?: string): Uint8Array =>
            this.codec.encode_msg({ ok, result, error });

        if (authHeader !== `Bearer ${this.token}`) {
            return ack(false, undefined, 'unauthorized');
        }

        const env = this.codec.decode_msg(bytes);
        if (env === null || typeof env !== 'object' || env === null) {
            return ack(false, undefined, 'undecodable envelope');
        }
        const e = env as Partial<HttpEnvelope>;
        if (typeof e.rt !== 'string' || typeof e.ms !== 'string' || typeof e.sl !== 'string' || typeof e.key !== 'string') {
            return ack(false, undefined, 'malformed envelope: rt/ms/sl/key required');
        }

        if (!this.dispatch) return ack(false, undefined, 'no dispatcher attached');

        try {
            const result = await this.dispatch(env as HttpEnvelope);
            return ack(true, result);
        } catch (err) {
            return ack(false, undefined, err instanceof Error ? err.message : String(err));
        }
    }
}
