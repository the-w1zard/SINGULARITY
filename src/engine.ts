// #endregion 
//                                                                                               [USAGE]
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
/*  ====================================================================================================
     const store = require('./store');

     const unsub = store.subscribe('user', (value, prev, key) => {
      console.log(`${key} changed:`, prev, '->', value);
     });

     store.set('user', 'Alice');  // logs
     store.set('other', 1);       // ignored
     unsub();                     // stop listening

     const unsub = store.subscribe((value, prev, key) => {
      console.log('any change:', key);
    });
  
  
    for await (const value of store.subscribe('token', { immediate: true })) {
      await refreshWith(value);
    if (value === 'stop') break;   // breaking 
	  ============================================================= ⇦✦──⇦✦──✦──✦──← 'pew pew' 🧙 [STORE] */
// #endregion 
//                                                                                               [STORE]
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════


// #region ───────────────────────────────────────────────────────────────────────────────────────────── STORE / MAELSTROM ───
/*  ==========================================================================================================================
     Yaaaaa... I know.
     BUT... Just ignore what you don't need, and when the day comes that you do... its here.

     This now solves 4 problems, almost all of which in the end need the same thing... a set of node modules all neededing
     either the same state, store or rift ie module whether it be locally sourced or not. Each solving the problem, exactly
     the way they need to. The stores problem... is not the same as the rifts, and is not the same as the states.

     Which is why none of them, are entirely collapsed into one another.

     The 4th problem was not mentioned yet, because it basically hitched a free ride in with maelstrom, which is 
     apparations. If your unfamiliar with that... basically, its offensive coding inplace of defensive coding. Instead of 
     dropping a bunch of if (this || that) and all that shit... it gives the fn exactly what it requested, so both typescript
     AND the runtime will just be fucking la-di-da with no thrown errors and simply be happy with life and let the app
     continue running.
    ==========================================================================================================================
     SET / GET
     Store a value                                                                                   STORE.set("user", user)
     Store a lazily-created value                                                  STORE.setLazy("user", () => createUser())
     Retrieve a value                                                                                      STORE.get("user")
     Retrieve with fallback                                                                      STORE.get("user", fallback)
     Check whether a key exists                                                                            STORE.has("user")
     Delete a value                                                                                     STORE.delete("user")
     Remove all values                                                                                         STORE.clear()
     Retrieve all values                                                                                      STORE.getAll()
     Freeze further writes                                                                                    STORE.freeze()

     SUBSCRIPTIONS
     Subscribe to changes on a specific key                                                      STORE.subscribe("user", fn)
     Subscribe to all store changes                                                                      STORE.subscribe(fn)
     Subscribe and receive the current value immediately                    STORE.subscribe("user", fn, { immediate: true })
     Observe changes through an async iterator                                                       STORE.subscribe("user")
     Observe with an immediate initial value                                    STORE.subscribe("user", { immediate: true })
     Unsubscribe all matching listeners                                                            STORE.unsubscribeAll(fns)
     Get subscribers registered for multiple keys                                    STORE.subscribersOf(["user", "config"])

     WAITERS
     Wait for the next matching value                                                               await STORE.wait("user")
     Wait for a change, ignoring the current value                                            await STORE.waitChange("user")
     Wait until a predicate is satisfied                                              await STORE.waitFor("user", predicate)
     Wait with a timeout                                                         await STORE.wait("user", { timeout: 5000 })
     Wait with an AbortSignal                                                           await STORE.wait("user", { signal })

     ATOMIC OPERATIONS
     Set only when a key does not exist                                                      STORE.setIfAbsent("user", user)
     Set only when a key already exists                                                     STORE.setIfPresent("user", user)
     Replace an existing value                                                                   STORE.replace("user", user)
     Update using the previous value                                                            STORE.update("count", fn, 0)
     Increment a numeric value                                                                         STORE.inc("count", 1)
     Register a key without overwriting its value                                                        STORE.touch("user")

     COLLECTION / INSPECTION
     Convert store to a plain object                                                                          STORE.toJSON()
     Retrieve entries                                                                                        STORE.entries()
     Retrieve keys                                                                                              STORE.keys()
     Retrieve values                                                                                          STORE.values()
     Find a value                                                                                      STORE.find(predicate)
     Find a matching key                                                                            STORE.findKey(predicate)
     Group values                                                                                            STORE.group(fn)
     Select specific keys                                                                                   STORE.pick(keys)
     Exclude specific keys                                                                                  STORE.omit(keys)
     Create a snapshot                                                                                      STORE.snapshot()
     Compare stores                                                                                      STORE.equals(other)
     Inspect subscription state                                                                         STORE.snapshotSubs()
     ──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
     STORE.STATE                                                   Scoped state is organized by root → master → slave → key.

     Retrieve a scoped state map                                                                        STORE.state.getMap()
     Retrieve a map using custom channels                                            STORE.state.getMap(slave, master, root)
     Retrieve scoped metadata                                                                      STORE.state.getMetadata()
     Read a state value                                                                                 STORE.state.get(key)
     Read using custom channels                                                                   STORE.state.get(key, opts)
     Write a state value                                                                         STORE.state.set(key, value)
     Write with notification options                                                       STORE.state.set(key, value, opts)
     Subscribe to a state key                                                           STORE.state.subscribe(key, listener)
     Check whether a state key exists                                                                   STORE.state.has(key)
     Delete a state key                                                                              STORE.state.delete(key)
     Retrieve scoped entries                                                                           STORE.state.entries()
     Clear scoped state                                                                                  STORE.state.clear()
     Schedule a key notification                                                                      STORE.state._emit(key)

     PERSISTENCE
     Mark a state key for persistence                                                               STORE.state.persist(key)
     Remove a key from persistence                                                                STORE.state.unpersist(key)
     Clear persistent values                                                                   STORE.state.clearPersistent()

     Notification scheduling is cached and flushed through the Store's
     notification pipeline. State metadata also holds batching, history,
     time-travel, listeners, waiters, contracts, conflicts and scope information.
     ──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
     MAELSTROM

     MODULE RESOLUTION
     Dynamically import an ESM module                                                 await STORE.maelstrom.module("module")
     Resolve a module relative to the current module                                  STORE.maelstrom.key_for("./module.js")
     Synchronously require a module                                                STORE.maelstrom.module.require("node:fs")
     Check whether a module has loaded                                        STORE.maelstrom.module.isLoaded("./module.js")
     Retrieve a loaded module                                                STORE.maelstrom.module.getLoaded("./module.js")
     Extract a named export                                           STORE.maelstrom.module.extract_named(ns, "exportName")
     Retrieve a named export                                                  STORE.maelstrom.module.export_of("exportName")
     Retrieve a named export from a specific module            STORE.maelstrom.module.export_of("exportName", "./module.js")
     Load and resolve a named export                             STORE.maelstrom.module.named_export("module", "exportName")
     Initialize module resolution                                                      STORE.maelstrom.module.init("module")
     Initialize a named export                              STORE.maelstrom.module.init.named_export("module", "exportName")
     Check whether a module exists                                              await STORE.maelstrom.module.check("module")
     Index exports from a loaded module                                  STORE.maelstrom.module.indexExports(rift, key, mod)

     RIFT REGISTRY
     Retrieve or create a Rift                                                         STORE.maelstrom.rift("service.ready")
     Create a Rift directly                                                                      STORE.maelstrom.rift.make()
     Check whether a Rift exists                                                   STORE.maelstrom.rift.has("service.ready")
     Inspect registered Rifts                                                                 STORE.maelstrom.rift.inspect()
     Clear the Rift registry                                                                    STORE.maelstrom.rift.clear()
     Read a Rift value                                                            STORE.maelstrom.rift.read("service.ready")
     Reset a Rift                                                                STORE.maelstrom.rift.reset("service.ready")
     Delete a Rift                                                              STORE.maelstrom.rift.delete("service.ready")
     Wait for a Rift                                                      await STORE.maelstrom.rift("service.ready").wait()
     Guard execution behind a Rift                                           STORE.maelstrom.rift.guard("service.ready", fn)
     Resolve a global Rift                              STORE.maelstrom.rift.resolve.global("service.ready", timeout, value)

     RIFT RESOLUTION
     Retrieve or resolve a module through a Rift                                         STORE.maelstrom.rift.use("service")
     Create a Rift from a supplied value                                       STORE.maelstrom.rift.create("service", value)
     Read a Rift value                                                                  STORE.maelstrom.rift.read("service")
     Resolve an existing or claimed Rift                                      STORE.maelstrom.rift.resolve.global("service")
     Resolve a module with a fallback                      STORE.maelstrom.rift.resolve.global("service", timeout, fallback)
     Probe a named export with a synchs fallback    STORE.maelstrom.rift.probe("service", "./module.js", "export", fallback)
     Probe a named export w/o blocking   STORE.maelstrom.rift.use_if_available("service", "./module.js", "export", fallback)
     Conjure a module through the Rift resolver                                        STORE.maelstrom.rift.conjure(options)
     Conjure without caching                                                  STORE.maelstrom.rift.conjure.uncached(options)
     Retrieve a module Rift                                                       STORE.maelstrom.rift.conjure.get("module")

     RESULTS
     Retrieve all recorded results                                                                   STORE.maelstrom.results
     Retrieve the number of registered Rifts                                                            STORE.maelstrom.size
     Retrieve a specific result                                                       STORE.maelstrom.rift.result("service")
     Filter results by solver                                                     STORE.maelstrom.rift.resultsFrom("solver")
     Subscribe to recorded results                                                         STORE.maelstrom.rift.onResult(fn)
     Record a resolution outcome                                                STORE.maelstrom.record(label, solver, value)

     WAITERS
     Wait for one or more readiness signals                                               STORE.maelstrom.waiter("auth", fn)
     Wait for multiple signals                                                STORE.maelstrom.waiter(["auth", "config"], fn)
     Cancel a multi-signal waiter                                                          STORE.maelstrom.waiter.cancel(id)
     Wait for a Rift through a Promise                STORE.maelstrom.waiter.wait_and_promise(timeout, key, isOpen, current)

     DEFER
     Schedule execution when dependencies become available                                STORE.maelstrom.defer("cache", fn)
     Schedule execution after multiple dependencies                           STORE.maelstrom.defer(["cache", "logger"], fn)
     Provide a dependency                                              STORE.maelstrom.defer.provide("cache", cacheInstance)
     Provide multiple dependencies                                          STORE.maelstrom.defer.provide({ cache, logger })
     Resolve a dependency without a value                                     STORE.maelstrom.defer.resolve("service.ready")
     Invalidate a dependency                                               STORE.maelstrom.defer.invalidate("service.ready")
     Check whether a dependency is available                                              STORE.maelstrom.defer.has("cache")
     Retrieve a dependency                                                                STORE.maelstrom.defer.get("cache")
     Check whether a task's dependencies are ready                                         STORE.maelstrom.defer.ready(task)
     Cancel a pending task                                                                  STORE.maelstrom.defer.cancel(id)
     Inspect pending/running tasks and scheduler statistics                                  STORE.maelstrom.defer.inspect()
     Clear pending tasks                                                                       STORE.maelstrom.defer.clear()
     Drain the scheduler                                                                       STORE.maelstrom.defer.drain()

     ENVIRONMENT DETECTION
     Detect runtime and available modules                                               STORE.maelstrom.detect.environment()
     Detect a specific module                                                  STORE.maelstrom.detect.firstMatch(candidates)
     Detect all available modules                                                     STORE.maelstrom.detect.all(candidates)
     Detect a runtime capability                                          STORE.maelstrom.detect.capability("shared_worker")
     Detect the package manager                                                             STORE.maelstrom.detect.bundler()
     Inspect environment state                                                                 STORE.maelstrom.useEnvState()
     APPARATION
     Create a generic array                                                       STORE.maelstrom.apparation.array<string>()
     Create a generic object                                                     STORE.maelstrom.apparation.object<MyType>()
     Create a generic Map                                                   STORE.maelstrom.apparation.map<string, number>()
     Create a generic Set                                                           STORE.maelstrom.apparation.set<string>()
     Create a generic Promise                                                   STORE.maelstrom.apparation.promise<MyType>()
     Create a generic function                                                     STORE.maelstrom.apparation.func<MyType>()
     Create a generic WeakMap                                           STORE.maelstrom.apparation.weakMap<object, MyType>()
     Create a generic async iterable                                      STORE.maelstrom.apparation.asyncIterable<MyType>()
     Create a generic typed array                                        STORE.maelstrom.apparation.typedArray<Uint8Array>()
     Create a generic value placeholder                                             STORE.maelstrom.apparation.any<MyType>()

     RESET / LOGGING
     Log a MAELSTROM event                                                               STORE.maelstrom.log("AREA", detail)
     Reset MAELSTROM registries, tasks and environment state                                         STORE.maelstrom.reset()
	======================================================================== ⇦✦──⇦✦──✦──✦──← 'pew pew' 🧙 [STORE/MAELSTROM] */
// #endregion ──────────────────────────────────────────────────────────────────────────────────
//                                           TYPE ALIASES (the shorthand table, carried forward)
// #region ─────────────────────────────────────────────────────────────────────────────────────

type MaybePromise<R = any> = R;
function MaybePromise<R = any>(v: R): MaybePromise {
    return v !== undefined ? v as R : Promise.resolve(v) as Promise<R> 
}
// #endregion ──────────────────────────────────────────────────────────────────────────────────
//                                                                        Rift — the local shape
// #region ─────────────────────────────────────────────────────────────────────────────────────
//
// A named slot holding a value that may not have landed yet. In-process. It carries, on
// itself, everything the registry needs from it: the value, the open/claim state, the
// subscriber set (THE one subscription mechanism for this entry), the solver tag of whoever
// produced its value, and — when the entry holds a module — its export index and inflight
// promise. Nothing about a result is stored anywhere else.
function wait_for_promise_in_rift<T>(gate: Promise<T>, timeoutMs: number): Promise<T> {
    let timer!: ReturnType<typeof setTimeout>;
    const timeout = new Promise<T>((_, reject) => {
        timer = setTimeout(() => reject(new Error(`[rift] wait() timed out after ${timeoutMs}m`)), timeoutMs);
    });
    return Promise.race([gate, timeout]).finally(() => clearTimeout(timer));
}
class Rift<T = A> implements RiftLike<T> {
    private opened = false;
    private claimed = false;
    private value: T | undefined;
    private resolveFn!: (v: T) => void;
    private gate!: Promise<T>;
    private listeners: RiftListener<T>[] = [];

    readonly createdAt = Date.now();
    openedAt: number | undefined;

    constructor() { this.makeGate(); }

    private makeGate() { this.gate = new Promise<T>((resolve) => { this.resolveFn = resolve; }); }// CHANGED: default is now "wait forever". Explicit timeout = opt-in rejection.           
    wait(timeoutMs = 0) { // : Promise<T>
        if (!timeoutMs) return this.gate as T;
        else {
            return wait_for_promise_in_rift(this.gate, timeoutMs) as T
        }
    }
    open(value?: T) {
        if (this.opened) return;
        this.opened = true;
        this.openedAt = Date.now();
        this.value = value as T;
        this.resolveFn(value as T);
        for (const l of this.listeners) l(value as T);
    }
    reset() {
        this.opened = false;
        this.claimed = false;
        this.value = undefined;
        this.openedAt = undefined;
        this.makeGate();
    }
    claim(): boolean {
        if (this.claimed) return false;
        this.claimed = true;
        return true;
    }
    onOpen(listener: RiftListener<T>): () => void {
        this.listeners.push(listener);
        return () => { const i = this.listeners.indexOf(listener); if (i !== -1) this.listeners.splice(i, 1); };
    }

    get isOpen() { return this.opened; }
    get isClaimed() { return this.claimed; }
    get current(): T | undefined { return this.value; }
}
// #endregion ──────────────────────────────────────────────────────────────────────────────────
//                                                                                REACTIVE_STORE
// #region ─────────────────────────────────────────────────────────────────────────────────────
// Same contract as Rift, but the slot lives in a shared store: writes are visible to every
// reader of the same label, and onOpen subscriptions fire across all holders. Its reactive
// store is the cloud shape's own backbone — one store, one subscription mechanism.
//                                                                                               [USAGE]
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
/*  ====================================================================================================
     const store = require('./store');

     const unsub = store.subscribe('user', (value, prev, key) => {
      console.log(`${key} changed:`, prev, '->', value);
     });

     store.set('user', 'Alice');  // logs
     store.set('other', 1);       // ignored
     unsub();                     // stop listening

     const unsub = store.subscribe((value, prev, key) => {
      console.log('any change:', key);
    });
  
  
    for await (const value of store.subscribe('token', { immediate: true })) {
      await refreshWith(value);
    if (value === 'stop') break;   // breaking 
	  ============================================================= ⇦✦──⇦✦──✦──✦──← 'pew pew' 🧙 [STORE] */
// #endregion
//                                                                                               [STORE]
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
const { EventEmitter } = require("node:events");

class ENGINE extends EventEmitter {
	//                                                                                           [CONSTANTS]
	// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
	#waiters = new M2<S, SET2<A>>(); // { key, predicate, resolve, reject, timer }
	#data = new M2();
	#subs = new M2(); // key -> Set<fn>   (key-specific)
	#anySubs = new SET2(); // global watchers
	#frozen = false;
	#context = new M2();
	#meta = new M2();
	public r = "_root_k_";
	public m = "_master_k_";
	public s = "_slave_k_";
	// #endregion
	//                                                                                               [STATE]
	// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
	public _globalNotifyCache = new WeakMap<A, Map<A, () => V>>();
	public _notifyCache = new WeakMap<Map<A, Set<(value: A) => V>>, Map<A, () => V>>();
	public state = {
		getMap: <K, V>(slave: A = this.s, master: A = this.m, rk: A = this.r): M2<K, V> => {
			let root = this.#context.get(rk); // (globalThis as any)[rk]  //  ; (globalThis as any)[rk] = root;
			if (!root) {
				root = new M2<A, M2I<A, M2I<A, A>>>();
				this.#context.set(rk, root);
			}
	
			let masterMap = root.get(master);
			if (!masterMap) {
				masterMap = new M2<A, M2I<A, A>>();
				root.set(master, masterMap);
			}
	
			let slaveMap = masterMap.get(slave);
			if (!slaveMap) {
				slaveMap = new M2<K, V>();
				masterMap.set(slave, slaveMap);
			}
			return slaveMap;
		},
		getMetadata:(s = this.s, m = this.m, r = this.r) => {
			let root = this.#meta.get(r); //  (globalThis as A)[r]; // STATE_CONTEXT === "sauron" ?  : null;
			if (!root) {
				root = new M2<A, M2I<A, M2I<A, A>>>();
				(globalThis as A)[r] = root;
			}
	
			let masterMap = root.get(m);
			if (!masterMap) {
				masterMap = new M2<A, M2I<A, A>>();
				root.set(m, masterMap);
			}
	
			let metaMap = masterMap.get(s);
			if (!metaMap) {
				metaMap = {
					_batches: new Set<S | SY>(),
					_batchQueue: new Map<S | SY, A>(),
					_batchOpts: new Map<S | SY, { triggerRender?: B }>(),
					_history: new Map<S | SY, A[]>(),
					_timeTravel: new Map<S | SY, N>(),
					_globalListeners: new Set<(key: S | SY, value: A) => V>(),
					_global_subscribers: new Map<S, Set<() => V>>(),
					_waiters: new Map<S | SY, Set<(value: A) => V>>(),
					_sys: new Map<S, A>(),
					_sealed: new Set<S>(),
					_frozen: new Set<S>(),
					_keyContracts: new Map<S, Set<S>>(),
					_conflicts: new Map<S, A>(),
					_conflictStrategies: new Map<S, "latest" | "fail" | "accumulate">(),
					_causal: new Map<S, A[]>(),
					_scopes: new Map<S, A>(),
					_msgHandlers: new Map<S, Set<(p: A) => V>>(),
					_lastWriteSource: new Map<S, S>(),
					_mainMap: new Map<A, S>(),
					_tupleCache: new Map<S, S>(),
					_weakMap: new WeakMap<O, S>(),
					_boundaries: new WeakMap<O, S>(),
				};
				masterMap.set(s, metaMap);
			}
			return metaMap;
		},
		get: <T = A,>(key: A, opts: A = { sl_reg: this.s, ms_reg: this.m, globalKey: this.r }): T | UD => {
			const { sl_reg, ms_reg, globalKey } = opts;
			const _state = this.getMap(sl_reg, ms_reg, globalKey);
			const _persistent = this.state.getMap<A, A>(`${sl_reg}_PERSISTENT`, ms_reg, globalKey);
			const _persistKeys = this.state.getMap<A, B>(`${sl_reg}_PERSIST_KEYS`, ms_reg, globalKey);

			// recordDependency(key)
			if (_state.has(key)) return MaybePromise(_state.get(key));
			if (_persistKeys.has(key) && _persistent.has(key)) {
				const v = _persistent.get(key);
				_state.set(key, v);
				return MaybePromise(v);
			}
			return undefined;
		},
		set: <T = A,>(key: A, value: T | ((prev: T | UD) => T), opts: { triggerRender?: B; cap?: N } & A = { sl_reg: this.s, ms_reg: this.m, globalKey: this.r, triggerRender: true, cap: 200 }) => {
			const { triggerRender = true, sl_reg, ms_reg, globalKey, cap } = opts;
			const _state = this.state.getMap<A, A>(sl_reg, ms_reg, globalKey);
			const _persistent = this.state.getMap<A, A>(`${sl_reg}_PERSISTENT`, ms_reg, globalKey);
			const _persistKeys = this.state.getMap<A, B>(`${sl_reg}_PERSIST_KEYS`, ms_reg, globalKey);
			const _listeners = this.state.getMap<A, Set<(value: A) => V>>(`${sl_reg}_LISTENERS`, ms_reg, globalKey);
			const prev = _state.get(key);
			const next = typeof value === "function" ? (value as (prev: T | UD) => T)(prev) : value;
			if (Object.is(prev, next)) return;
			// TIME TRAVEL

			if (!this.root.TIME_TRAVEL._time_travel_reverting) {
				const meta = this.state.getMetadata(sl_reg, ms_reg, globalKey);
				let hist = meta._history.get(key);
				if (!hist) {
					hist = [];
					meta._history.set(key, hist);
				}
				const lastActive = hist.findLastIndex((e: A) => !e.undone) ?? -1;
				if (lastActive < hist.length - 1) hist.length = lastActive + 1; // truncate redo tail
				hist.push({ k: (hist[hist.length - 1]?.k ?? 0) + 1, prev, next, ts: Date.now(), undone: false });
				const cap_val = cap ?? 200;
				while (hist.length > cap_val) hist.shift();
			}
			_state.set(key, next);
			if (_persistKeys.has(key)) _persistent.set(key, next);
			if (triggerRender) this.state.scheduleGlobalNotify(key, this.getMetadata(sl_reg, ms_reg, globalKey), opts);
			if (triggerRender) this.state.scheduleNotify(key, _listeners, _state);
			return MaybePromise(next as T);
		},
		subscribe: <T = A,>(key: A, listener: (value: T) => V, opts: A = { sl_reg: this.s, ms_reg: this.m, globalKey: this.r }): (() => V) => {
			const { sl_reg, ms_reg, globalKey } = opts;
			const _listeners = this.state.getMap<A, Set<(value: A) => V>>(`${sl_reg}_LISTENERS`, ms_reg, globalKey);

			if (!_listeners.has(key)) _listeners.set(key, new Set());
			_listeners.get(key)!.add(listener);
			return () => {
				_listeners.get(key)?.delete(listener);
				if (_listeners.get(key)?.size === 0) _listeners.delete(key);
			};
		},
		delete: (key: A, opts: A = { sl_reg: this.s, ms_reg: this.m, globalKey: this.r }): B => {
			const { sl_reg, ms_reg, globalKey } = opts;
			const _state = this.state.getMap<A, A>(sl_reg, ms_reg, globalKey);
			const _persistent = this.state.getMap<A, A>(`${sl_reg}_PERSISTENT`, ms_reg, globalKey);
			const _persistKeys = this.state.getMap<A, B>(`${sl_reg}_PERSIST_KEYS`, ms_reg, globalKey);
			const _listeners = this.state.getMap<A, Set<(value: A) => V>>(`${sl_reg}_LISTENERS`, ms_reg, globalKey);

			const existed = _state.delete(key);
			if (_persistKeys.has(key)) {
				_persistKeys.delete(key);
				_persistent.delete(key);
			}
			_listeners.get(key)?.forEach((l) => l(undefined));
			_listeners.delete(key);
			return MaybePromise(existed);
		},
		entries: <T = A,>(opts: A = { sl_reg: this.s, ms_reg: this.m, globalKey: this.r }): [A, T][] => {
			const { sl_reg, ms_reg, globalKey } = opts;
			const _state = this.state.getMap<A, A>(sl_reg, ms_reg, globalKey);
			return MaybePromise(Array.from(_state.entries()));
		},
		persist: (key: A, opts: A = { sl_reg: this.s, ms_reg: this.m, globalKey: this.r }): V => {
			const { sl_reg, ms_reg, globalKey } = opts;
			const _persistKeys = this.state.getMap<A, B>(`${sl_reg}_PERSIST_KEYS`, ms_reg, globalKey);
			_persistKeys.set(key, true);
		},
		unpersist: (key: A, opts: A = { sl_reg: this.s, ms_reg: this.m, globalKey: this.r }): V => {
			const { sl_reg, ms_reg, globalKey } = opts;
			const _persistent = this.state.getMap<A, A>(`${sl_reg}_PERSISTENT`, ms_reg, globalKey);
			const _persistKeys = this.state.getMap<A, B>(`${sl_reg}_PERSIST_KEYS`, ms_reg, globalKey);
			_persistKeys.delete(key);
			_persistent.delete(key);
		},
		has: (key: A, opts: A = { sl_reg: this.s, ms_reg: this.m, globalKey: this.r }): B => {
			const { sl_reg, ms_reg, globalKey } = opts;
			const _state = this.state.getMap<A, A>(sl_reg, ms_reg, globalKey);
			const _persistent = this.state.getMap<A, A>(`${sl_reg}_PERSISTENT`, ms_reg, globalKey);
			const _persistKeys = this.state.getMap<A, B>(`${sl_reg}_PERSIST_KEYS`, ms_reg, globalKey);

			return _state.has(key) || (_persistKeys.has(key) && _persistent.has(key));
		},
		clear: (opts: A = { sl_reg: this.s, ms_reg: this.m, globalKey: this.r }): V => {
			const { sl_reg, ms_reg, globalKey } = opts;
			const _state = this.state.getMap<A, A>(sl_reg, ms_reg, globalKey);
			const _listeners = this.state.getMap<A, Set<(value: A) => V>>(`${sl_reg}_LISTENERS`, ms_reg, globalKey);
			_state.clear();
			_listeners.clear();
		},
		clearPersistent: (opts: A = { sl_reg: this.s, ms_reg: this.m, globalKey: this.r }): V => {
			const { sl_reg, ms_reg, globalKey } = opts;
			const _persistent = this.state.getMap<A, A>(`${sl_reg}_PERSISTENT`, ms_reg, globalKey);
			const _persistKeys = this.state.getMap<A, B>(`${sl_reg}_PERSIST_KEYS`, ms_reg, globalKey);
			_persistent.clear();
			_persistKeys.clear();
		},
		_emit: (key: S | SY, sl_reg = this.s, ms_reg = this.m, globalKey = this.r): V => {
			const opts = { sl_reg, ms_reg, globalKey };
			const _state = this.state.getMap(sl_reg, ms_reg, globalKey);
			const _listeners = this.state.getMap<A, Set<(value: A) => V>>(`${sl_reg}_LISTENERS`, ms_reg, globalKey);
			const meta = this.state.getMetadata(sl_reg, ms_reg, globalKey);
			this.state.scheduleNotify(key, _listeners, _state);
			this.state.scheduleGlobalNotify(key, meta, opts);
		},
		scheduleNotify: (key: A, _listeners: Map<A, Set<(value: A) => V>>, _state: Map<A, A>): V =>{
			let cache = this._notifyCache.get(_listeners);
			if (!cache) {
				cache = new Map();
				this._notifyCache.set(_listeners, cache);
			}
			let fn = cache.get(key);
			if (!fn) {
				fn = () => {
					cache!.delete(key);
					const val = _state.get(key);
					_listeners.get(key)?.forEach((l) => l(val));
				};
				cache.set(key, fn);
			}
			this.onFlush(fn);
		},
		scheduleGlobalNotify :(key: S | SY, meta: A, opts: A): V => {
			let cache = this._globalNotifyCache.get(meta);
			if (!cache) {
				cache = new Map();
				this._globalNotifyCache.set(meta, cache);
			}
			let fn = cache.get(key);
			if (!fn) {
				fn = () => {
					cache!.delete(key);
					const finalVal = this.store.get(key, opts);
					const waiters = meta._waiters.get(key);
					if (waiters) {
						for (const resolveFn of waiters) {
							try {
								resolveFn(finalVal);
							} catch (err) {
								console.error(`[Sauron Waiter Error]:`, err);
							}
						}
						meta._waiters.delete(key);
					}
					for (const listener of meta._globalListeners) {
						try {
							listener(key, finalVal);
						} catch (err) {
							console.error(`[Sauron Global Listener Error]:`, err);
						}
					}
				};
				cache.set(key, fn);
			}
			this.onFlush(fn);
		},
	};
	// #endregion

	//                                                                                                 [SET]
	// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
	public set(key: A, value: A) {
		if (this.#frozen) throw new Error("Store is frozen");
		const prev = this.#data.swap(key, value); // returns previous, writes new
		this.#notify(key, value, prev);
		return this;
	}
	public setLazy(key: A, make: () => A) {
		const prev = this.#data.g(key);
		const value = make();
		this.#data.s(key, value);
		this.#notify(key, value, prev);
		return this;
	}
	public delete(key: A) {
		if (!this.#data.h(key)) return false;
		const prev = this.#data.take(key);
		this.#notify(key, undefined, prev);
		return true;
	}
	public clear() {
		for (const key of this.#data.toKeys()) this.#notify(key, undefined, this.#data.g(key));
		this.#data.clear();
	}
	// #endregion
	//                                                                                                 [SUB]
	// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
	/*  ====================================================================================================
     subscribe(key, fn)  -> unsubscribe
     subscribe(fn)       -> unsubscribe (all keys)
     subscribe(key, { immediate: true }) -> async iterator
    ============================================================= ⇦✦──⇦✦──✦──✦──← 'pew pew' 🧙 [STORE] */
	public subscribe(fn: (value: A, prev: A, key: S) => V): () => V;
	public subscribe(key: S, fn: (value: A, prev: A, key: S) => V, opts?: { immediate?: B }): () => V;
	public subscribe(key: S, opts?: { immediate?: B }): AsyncGenerator<A, V, U>;
	public subscribe(keyOrFn: S | ((value: A, prev: A, key: S) => V), maybeFn?: ((value: A, prev: A, key: S) => V) | { immediate?: B }, opts: { immediate?: B } = {}): (() => V) | AsyncGenerator<A, V, U> {
		// global watcher
		if (typeof keyOrFn === "function") {
			const fn = keyOrFn;
			this.#anySubs.add(fn);
			return () => {
				this.#anySubs.delete(fn);
			};
		}
		const key = keyOrFn;
		// iterator form: no maybeFn, or second arg is an options object
		if (maybeFn === undefined || (typeof maybeFn === "object" && maybeFn !== null)) {
			const iteratorOpts = (typeof maybeFn === "object" && maybeFn) || opts;
			return this.#asIterator(key, iteratorOpts);
		}

		// callback form
		const fn = maybeFn;
		let set = this.#subs.getOrM(  () => new SET2(), key);
		set.add(fn);
		if (opts.immediate && this.#data.has(key)) {
			queueMicrotask(() => fn(this.#data.get(key), undefined, key));
		}
		// unsubscribe
		return () => {
			const set = this.#subs.g(key);
			if (!set) return;
			set.delete(fn);
			if (set.size === 0) this.#subs.d(key);
		};
	}
	public unsubscribeAll(fns: Iterable<FN>) {
		for (const set of this.#subs.values()) set.deleteAll(fns);
		this.#anySubs.deleteAll(fns);
	}
	public subscribersOf(keys: S[]): A {
		let out = new SET2<FN>();
		for (const key of keys) {
			const set = this.#subs.g(key);
			if (set) out = out.union(set);
		}
		return out;
	}
	async *#asIterator(key: A, { immediate = false } = {}) {
		// buffer events so a slow consumer doesn't lose them
		const queue: A = [];
		let resolve: A;
		let done = false;
		const push = (v: A) => {
			queue.push(v);
			if (resolve) {
				const r = resolve;
				resolve = null;
				r();
			}
		};
		const unsub = this.subscribe(key, (v: A, prev: A) => push({ value: v, prev }));
		const stop = this.subscribe((k: A, v: A, prev: A) => {
			if (k === key) push({ value: v, prev });
		});

		try {
			if (immediate && this.has(key)) yield this.get(key);
			while (!done) {
				if (queue.length === 0) await new Promise((r) => (resolve = r));
				while (queue.length) yield queue.shift().value;
			}
		} finally {
			unsub();
			stop();
		}
	}
	#notify(key: A, value: A, prev: A) {
		const waiting = this.#waiters.get(key);
		if (waiting) {
			for (const w of waiting) {
				if (!w.predicate(value, prev)) continue;
				w.cleanup();
				w.resolve(value);
			}
		}
	}
	// #endregion
	//                                                                                                [GETS]
	// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
	public get<T = A>(key: S): T | UD;
	public get<T = A>(key: S, fallback: T): T;
	public get<T = A>(key: S, fallback?: T): T | UD {
		return this.#data.has(key) ? this.#data.get(key) : fallback;
	}
	public getAll(): A {
		return this.#data.clone();
	}
	public has(key: A) {
		return this.#data.has(key);
	}
	public freeze() {
		this.#frozen = true;
	}
	// #endregion
	//                                                                                                [WAIT]
	// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
	/*  ====================================================================================================
     wait(key, opts?) -> Promise<value>

     opts.predicate(value, prev) -> boolean   // keep waiting until true

     opts.timeout   -> ms, rejects on timeout

     opts.immediate -> resolve now if predicate passes for current value

     opts.signal    -> AbortSignal
    ============================================================= ⇦✦──⇦✦──✦──✦──← 'pew pew' 🧙 [STORE] */
	public wait(key: A, { predicate = () => true, timeout, immediate = true, signal }: A = {}) {
		// Fast path: value already satisfies
		if (immediate && this.#data.has(key) && predicate(this.#data.get(key), undefined)) {
			return Promise.resolve(this.#data.get(key));
		}

		return new Promise((resolve, reject) => {
			const waiter = { key, predicate, resolve, reject, timer: null } as A;

			const cleanup = () => {
				this.#waiters.delete(waiter);
				if (waiter.timer) clearTimeout(waiter.timer);
				if (signal) signal.removeEventListener("abort", onAbort);
			};

			const onAbort = () => {
				cleanup();
				const err = new Error("wait aborted");
				err.name = "AbortError";
				reject(err);
			};

			if (timeout) {
				waiter.timer = setTimeout(() => {
					cleanup();
					const err = new Error(`wait('${key}') timed out after ${timeout}ms`);
					err.name = "TimeoutError";
					reject(err);
				}, timeout).unref();
			}

			if (signal) {
				if (signal.aborted) return onAbort();
				signal.addEventListener("abort", onAbort, { once: true });
			}

			waiter.cleanup = cleanup;
			this.#waiters.getOrM( () => new SET2() as A, key).add(waiter);
		});
	} // Convenience: wait for a *change* (ignore current value)
	public waitChange(key: A, opts = {}) {
		return this.wait(key, { ...opts, immediate: false });
	}
	// Convenience: wait until predicate holds for the current or next value
	public waitFor(key: A, predicate: A, opts = {}) {
		return this.wait(key, { ...opts, predicate });
	}
	// #endregion
	//                                                                                              [ATOMIC]
	// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
	public setIfAbsent(key: A, value: U) {
		if (!this.#data.setIfAbsent(key, value)) return false;
		this.#notify(key, value, undefined);
		return true;
	}
	public setIfPresent(key: A, value: U) {
		const prev = this.#data.g(key);
		if (!this.#data.setIfPresent(key, value)) return false;
		this.#notify(key, value, prev);
		return true;
	}
	public replace(key: A, value: U) {
		return this.setIfPresent(key, value);
	}
	public update(key: A, fn: (old: A, has: B) => A, initial: U) {
		const had = this.#data.h(key);
		const prev = had ? this.#data.g(key) : undefined;
		const next = this.#data.update(key, fn, initial);
		this.#notify(key, next, prev);
		return next;
	}
	public inc(key: A, by = 1) {
		const prev = this.#data.g(key);
		const next = this.#data.inc(key, by);
		this.#notify(key, next, prev);
		return next;
	}
	public touch(key: A) {
		// register interest without overwriting — useful for the "subscribed but not yet written" case
		if (!this.#data.setIfAbsent(key, undefined)) return false;
		this.#notify(key, undefined, undefined);
		return true;
	}
	// #endregion
	//                                                                                               [OTHER]
	// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
	public toJSON() {
		return this.#data.toObject();
	}
	public entries() {
		return this.#data.toArray();
	}
	public keys() {
		return this.#data.toKeys();
	}
	public values() {
		return this.#data.toValues();
	}
	public find(pred: A) {
		return this.#data.find(pred);
	}
	public findKey(pred: A) {
		return this.#data.findKey(pred);
	}
	public group(fn: A) {
		return this.#data.group(fn);
	}
	public pick(keys: A) {
		return this.#data.pick(keys);
	}
	public omit(keys: A) {
		return this.#data.omit(keys);
	}
	public snapshot(): A {
		return this.#data.clone();
	}
	public equals(other: A) {
		return this.#data.equals(other);
	}
	public snapshotSubs() {
		return this.#subs.mapValues((set) => set.clone());
	}
	// #endregion
    //                                                                                                [SLAB]
	// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
     #rifts = new M2<S, RiftLike<A>>();
	 #deferTasks = new M2<N, DeferredTask>();
	 #multiWaiters = new SET2<Waiter>();
	 nextWaiterId = 0;
	// ══ THE ONE SCHEDULER STATE ═════════════════════════════════════════════════════════════════
	// Deferred tasks (not results — the results ARE the registry entries above).
	private nextDeferId = 0;
	private draining = false;
	private drainAgain = false;
	private deferStats: DeferredStats = { registered: 0, executed: 0, completed: 0, failed: 0, cancelled: 0 };
	private autoCloud: B | undefined = undefined;
	private warnedUseBeforeReady = false;
	private ENV_STATE: EnvDetectionState = { moduleLevel: null, platform: null, compilers: [], bundler: null, adapters: [], ready: false };
	private shout = new ORC_SHOUT();
    /** the results view — computed from the ONE registry, stored nowhere */
	public get results(): RO<Array<ResultRecord>> {
		const out: ResultRecord[] = [];
		for (const [label, rift] of this.#rifts) {
			const r = rift as RiftLike<A> & { solver?: S; store?: S };
			if (r.solver === undefined) continue;
			out.push({
				label,
				solver: r.solver as Solver,
				store: (r.store ?? "local") as StoreKind,
				value: rift.current,
				ok: rift.current !== undefined && rift.current !== null,
				openedAt: rift.openedAt ?? rift.createdAt,
			});
		}
		return out;
	}
	public get size(): N { return this.#rifts.size;	}
    // ═══════════════════════════════════════════════════════════════════════════════════════════
	// SOLVER · detect_environment — the cartographer
	// ═══════════════════════════════════════════════════════════════════════════════════════════
	public static readonly MODULE_LEVEL_CANDIDATES = ["typescript", "rollup", "vite", "eslint", "babel", "postcss", "tailwindcss", "@babel/core", "prettier", "stylelint", "sass", "less", "jest", "vitest", "playwright", "@types/vscode"] as const;
	public static readonly PLATFORM_CANDIDATES = ["PROJECT_PLATFORM_KILLER", "react", "preact", "@remix-run/react", "@react-router/react", "next", "nuxt", "svelte", "solid", "vite", "arfa", "vue", "angular", "astro", "gatsby", "dojo", "inferno", "lupine", "ember-source", "backbone", "alpinejs", "lit", "marko", "@builder.io/qwik", "solid-start", "@sveltejs/kit", "meteor", "stencil", "aurelia", "nodiff", "qwik", "velo-type", "webpack"] as const;
	public static readonly COMPILER_CANDIDATES = ["rspack", "webpack", "vite", "rollup", "esbuild", "swc-loader", "babel", "next", "nuxt", "@remix-run/react", "@react-router/react", "svelte", "solid", "vue", "@swc/core", "turbopack", "parcel", "tsup", "unbuild", "esbuild-loader", "@babel/core", "terser", "@angular/core", "@angular/compile"] as const;
	public static readonly BUNDLER_CANDIDATES = ["bun", "pnpm", "yarn", "npm"] as const;
	public static readonly ADAPTER_CANDIDATES = ["websocket_dev_server", "cross_app_ws_server", "terminal_ngin_proxy_register", "terminal_ngin_proxy", "workspace_hub_server", "workspace_hub_client", "cross_app_ws_client", "cse_cross_app_server", "cse_cross_app_client", "shared_worker", "service_worker", "unix_worker", "broadcastr_channel", "websocket", "websocket_server", "websocket_client", "webrtc", "webrtc_server", "webrtc_client", "electron_server", "electron_client", "mqtt", "sse", "named_pipe", "worker_thread", "native_msg_host", "native_msg_client"] as const;
	public static readonly CAPABILITY_CANDIDATES = ["shared_worker", "service_worker", "unix_worker", "broadcastr_channel", "electron_server", "electron_client", "named_pipe", "native_msg_host", "native_msg_client"] as const;
    public maelstrom = (() => {
		const fn = () => {};
		fn.log=(where: S, detail: R = {}, type: "info" | "success" | "warn" | "error" = "info"): void =>{
            const payload: R = { ...detail };
            if (payload.error instanceof Error) payload.error = payload.error.message;
            this.shout.log(`[MAELSTROM][${where}]`, payload, this.shout.t[type] ?? this.shout.t.deep);
        }/** resolve a specifier relative to THIS file, always. Absolute out. */
        fn.key_for=(specifier: S): S => {
            try {
                return (import.meta as any).resolve(specifier) as S;
            } catch {
                try {
                    return new URL(specifier, import.meta.url).href;
                } catch {
                    return specifier;
                }
            }
        }
        fn.normalizeFallback=<F>(fallback: F | (() => F) | undefined, fallbackDefault: () => F): () => F =>{
            if (typeof fallback === "function") return fallback as unknown as () => F;
            if (fallback !== undefined) return () => fallback;
            return fallbackDefault;
        }/** the safe proxy — missing module access is suppressed, never crashed */
        fn.createSafeProxy = (): A =>
            new Proxy(function noop() {}, {
                get: (_, prop) => {
                    if (prop === Symbol.toPrimitive) {
                        this.maelstrom.log("SAFE_PROXY", { note: `Missing module. Suppressed access to: ${String(prop)}` }, "warn");
                        return () => "";
                    }
                    if (prop === "valueOf" || prop === "toString") {
                        this.maelstrom.log("SAFE_PROXY", { note: `Missing module. Suppressed access to: ${String(prop)}` }, "warn");
                        return () => "[MAELSTROM:missing-module]";
                    }
                    if (prop === "then" || prop === Symbol.iterator) return undefined;
                    const noop = () => {};
                    return new Proxy(noop, {
                        get: () => noop,
                        apply: () => this.maelstrom.createSafeProxy(),
                        construct: () => this.maelstrom.createSafeProxy(),
                    });
                },
                apply: () => this.maelstrom.createSafeProxy(),
                construct: () => this.maelstrom.createSafeProxy(),
            });
        fn.toNamespace=(m: A): A=> {
                if (m && typeof m === "object") return m;
                return { default: m };
        }
        fn.blind = (() => {
            const fn = () => {};
            /** dynamic import with no static dependency — works in every module system */
            fn.import = new Function("specifier", "return import(specifier)") as (s: string) => Promise<any>;
            /** require with no static dependency — null where require does not exist (browser/ESM) */
            fn.require = typeof require !== "undefined" ? (new Function("specifier", "return require(specifier)") as (s: string) => A) : null;
            return fn;
        })();
        fn.has = (() => {
            const fn = () => {};
            const blindReq = typeof require !== "undefined" ? (new Function("specifier", "return require(specifier)") as (s: string) => A) : null;
            fn.bun = typeof process !== "undefined" && !!(process as any)?.versions?.bun;
            fn.node = typeof process !== "undefined" && !!(process as any)?.versions?.node;
            /** does a local file exist, resolved from cwd — the has_local probe */
            fn.local = (filePath: S | undefined, module?: S): B => {
                try {
                    if (!this.maelstrom.has.node && !this.maelstrom.has.bun) return false;
                    if (!filePath) {
                        this.maelstrom.log("HAS_LOCAL", { note: "FILE PATH UNDEFINED — DEFINE A PATH", module, type: "SYNC" }, "error");
                        return false;
                    }
                    const path = this.maelstrom.blind.require!("path");
                    const fs = this.maelstrom.blind.require!("fs");
                    const resolved = path.resolve(process.cwd(), filePath);
                    return fs.existsSync(resolved) as B;
                } catch (e: A) {
                    this.maelstrom.log("HAS_LOCAL", { module, type: "SYNC", error: e }, "error");
                    return false;
                }
            };
            fn.require = blindReq !== null;
            return fn;
        })();
        fn.is = (() => {
            const blindReq = typeof require !== "undefined" ? (new Function("specifier", "return require(specifier)") as (s: string) => A) : null;
            const fn = () => {};
            fn.require_available = (): B => {
                return blindReq !== null;
            };
            fn.browser = typeof window !== "undefined";
            fn.VSCode = typeof process !== "undefined" && process.env.VSCODE_PID;
            fn.electron = false;
            fn.unix = false;
            fn.windows = true;
            return fn;
        })();
        // ═══════════════════════════════════════════════════════════════════════════════════════════
        // THE SHARED RESULTS SURFACE — a view onto the ONE registry, not a second map
        // ═══════════════════════════════════════════════════════════════════════════════════════════
        /**
         * Record an outcome. This is the single funnel: it tags the registry entry, notifies the
         * entry's subscribers, advances every waiter on the label, and releases deferred tasks.
         * It writes to the ONE registry — it creates no maps.
         */
        fn.record= <T>(label: S, solver: Solver, value: T | undefined, store: StoreKind = "local"): void =>{
            const rift = this.maelstrom.rift(label) as RiftLike<T> & { solver?: S; store?: S; version: N };
            rift.solver = solver;
            rift.store = store;
            rift.version++;
            const rec: ResultRecord<T> = {
                label,
                solver,
                store,
                value,
                ok: value !== undefined && value !== null,
                openedAt: rift.openedAt ?? Date.now(),
            };
            rift.open(value as T); // the entry itself notifies its own subscribers
            this.maelstrom.rift<ResultRecord>("__results__").reset(); // sentinel re-fires per record (open is idempotent)
            this.maelstrom.rift<ResultRecord>("__results__").open(rec); // machine-level watchers ride the same mechanism
            this.maelstrom.waiter.releaseMulti(label, value);
            this.maelstrom.defer.drain();
        }
        fn.rift = (() => {
            /**
             * get_module: require the specifier → local file read → builtin check → cwd-resolved
             * require → async import as the last resort. Every sync path is tried before anything
             * returns a promise.
             */
            const fn = <T = A,>(name: S): RiftLike<T> => {
                if (!this.#rifts.has(name)) this.#rifts.set(name, this.maelstrom.rift.make<T>());
                return this.#rifts.get(name)! as RiftLike<T>;
            };
            fn.clear = (): V => {
                this.#rifts.clear();
            };
            fn.inspect = (): R => {
                return [...this.#rifts.entries()].map(([name, rift]) => ({
                    name,
                    isOpen: rift.isOpen,
                    isClaimed: rift.isClaimed,
                    createdAt: rift.createdAt,
                    openedAt: rift.openedAt,
                })) as A;
            };
            /** guard — wait for a rift, then run a fn on its unlock value */
            fn.guard = <T, R>(name: string, fn: (unlockValue: T) => R | Promise<R>, timeoutMs?: number) => {
                MaybePromise(this.maelstrom.rift.guard_promise_gate(this.get, name, fn, timeoutMs));
            };
            /** async load with inflight dedupe — same key always resolves to the same promise */
            fn.load = <T = A,>(specifier: S): Promise<T> => {
                const key = this.maelstrom.key_for(specifier);
                const rift = this.maelstrom.rift(`mod:${key}`) as Rift<T> & { inflight?: Promise<A> };
                if (rift.isOpen) return Promise.resolve(rift.current as T);
                if (rift.inflight) return rift.inflight as Promise<T>;
                const p = this.maelstrom.blind
                    .import(key)
                    .then((mod: A) => {
                        rift.open(mod as T);
                        this.maelstrom.module.indexExports(rift, key, mod);
                        rift.inflight = undefined;
                        this.maelstrom.record(`mod:${key}`, "load_module", mod);
                        return mod as T;
                    })
                    .catch((err: A) => {
                        rift.inflight = undefined; // let it retry later
                        throw err;
                    });
                rift.inflight = p as A;
                return p;
            };
            fn.result = <T = A,>(label: S): ResultRecord<T> | undefined => {
                const rift = this.#rifts.get(label) as (RiftLike<A> & { solver?: S; store?: S }) | undefined;
                if (!rift || rift.solver === undefined) return undefined;
                return {
                    label,
                    solver: rift.solver as Solver,
                    store: (rift.store ?? "local") as StoreKind,
                    value: rift.current,
                    ok: rift.current !== undefined && rift.current !== null,
                    openedAt: rift.openedAt ?? rift.createdAt,
                } as ResultRecord<T>;
            };
            fn.resultsFrom = (solver: Solver): ResultRecord[] => {
                return this.results.filter((r) => r.solver === solver);
            }; /** "any result, any solver" — rides the ONE mechanism: the sentinel entry's own subscription */
            fn.onResult = (listener: (r: ResultRecord) => V): (() => V) => {
                return this.maelstrom.rift<ResultRecord>("__results__").onOpen(() => listener(this.maelstrom.rift.result("__results__") as ResultRecord));
            };
            fn.place = <T = A,>(label: S, value: T, store: StoreKind = "local"): V => {
                this.maelstrom.rift<T>(label).open(value);
                this.maelstrom.record(label, "place", value, store);
            };
            fn.get_value = <T = A,>(label: S): T | undefined => {
                return this.maelstrom.rift.read<T>(label);
            };
            // ═══════════════════════════════════════════════════════════════════════════════════════════
            // SOLVER · get / place — the plain registry read and write
            // ═══════════════════════════════════════════════════════════════════════════════════════════
            fn.read = <T = A,>(label: S, store: StoreKind = "local"): T | undefined => {
                const rift = this.maelstrom.rift<T>(label);
                const v = rift.current;
                this.maelstrom.record(label, "get", v as T, store);
                return v as T | undefined;
            };
            fn.clear = (): V => {
                this.#rifts.clear();
            };
            fn.reset = (name: S): V => {
                this.#rifts.get(name)?.reset();
            };
            fn.delete = (name: S): B => {
                return this.#rifts.delete(name);
            };
            // ═══════════════════════════════════════════════════════════════════════════════════════════
            // the bandit. Never throws, always returns a usable envelope.
            // ═══════════════════════════════════════════════════════════════════════════════════════════
            fn.bandit = <T = A,>(label: S, module?: A, wait?: N, store: StoreKind = "local"): A => {
                let v: A;
                try {
                    v = this.maelstrom.rift<T>(label );
                    if (v.isOpen) {
                        // check rift registry, return current
                        try {
                            return MaybePromise({ is_waiting: false, isLoaded: true, get: (v as RiftLike<T>).current as T });
                        } catch (e: A) {
                            this.maelstrom.log("SMASH_N_GRAB", { msg: "[CHECK RIFT REG, RETURN CURRENT][FAILED][MOVING ON...]", error: e }, "error");
                        }
                    }
                    if (!module && wait) {
                        // no module but a wait → wait for it
                        try {
                            this.maelstrom.log("SMASH_N_GRAB", { msg: "[NO MODULE BUT WAIT SUPPLIED — WAITING]" }, "info");
                            const raw = (v as RiftLike<T>).wait(wait) as A;
                            const envelope = (w: A) => MaybePromise({ is_waiting: !w, isLoaded: module ? this.maelstrom.rift.has(module as S) : false, mod: w });
                            if (raw instanceof Promise) {
                                // a rejected wait is a miss, not a crash
                                return MaybePromise(
                                    (raw as Promise<A>).then(
                                        (w) => envelope(w),
                                        (e) => {
                                            this.maelstrom.log("SMASH_N_GRAB", { msg: "[WAIT TIMED OUT — RETURNING MISS]", error: e }, "warn");
                                            return envelope(undefined);
                                        },
                                    ),
                                );
                            }
                            return envelope(raw);
                        } catch (e: A) {
                            this.maelstrom.log("SMASH_N_GRAB", { msg: "[WAIT PATH FAILED][MOVING ON...]", error: e }, "error");
                        }
                    }
                    if (label && module && !wait) {
                        // label + module, no wait → open
                        try {
                            return MaybePromise({ is_waiting: false, isLoaded: true, mod: (v as RiftLike<T>).open(module as T) });
                        } catch (e: A) {
                            // all else fails — nuke it
                            const value = this.maelstrom.module<T>(module as A);
                            this.maelstrom.log("SMASH_N_GRAB", { msg: "try failed, attempting catch gate now...", error: e }, "error");
                            (v as RiftLike<T>).open(value as T);
                            this.maelstrom.record(label, "smash_n_grab", value as T, store);
                            return MaybePromise({ is_waiting: false, isLoaded: true, mod: value });
                        }
                    }
                } catch (e: A) {
                    this.maelstrom.log("SMASH_N_GRAB", { error: e }, "error");
                }
                const ap = (this.maelstrom.apparation.func as <T = A>() => () => T)();
                return MaybePromise({ is_waiting: false, isLoaded: false, mod: ap });
            };
            // ═══════════════════════════════════════════════════════════════════════════════════════════
            // SOLVER · create_rift — the founder
            // ═══════════════════════════════════════════════════════════════════════════════════════════
            fn.create = <T = A,>(label: S, module: T, store: StoreKind = "local"): A => {
                try {
                    if (module) {
                        this.maelstrom.rift<T>(label).open(module);
                        this.maelstrom.record(label, "create_rift", module, store);
                        return MaybePromise(module);
                    }
                } catch (e: A) {
                    const value = this.maelstrom.module<T>(module as A);
                    if (value && !(value instanceof Promise)) {
                        this.maelstrom.rift<T>(label).open(value);
                        this.maelstrom.record(label, "create_rift", value, store);
                        return MaybePromise(value);
                    }
                    this.maelstrom.log("CREATE_RIFT", { error: e, msg: "try failed, attempting catch gate now..." }, "error");
                }
                return undefined;
            };
            // ═══════════════════════════════════════════════════════════════════════════════════════════
            // SOLVER · use_rift — the traveler. The full resolution ladder.
            // ═══════════════════════════════════════════════════════════════════════════════════════════
            fn.use = <T = A,>(label: S, wait?: N, apparation?: T, store: StoreKind = "local"): A => {
                try {
                    const rift = this.maelstrom.rift<T>(label );
    
                    if (rift.isOpen) {
                        this.maelstrom.record(label, "use_rift", rift.current as T, store);
                        return MaybePromise(rift.current as T);
                    }
    
                    if (rift.isClaimed) {
                        try {
                            return MaybePromise(rift.wait(wait) as A);
                        } catch (e: A) {
                            this.maelstrom.log("USE_RIFT", { msg: "try failed...", error: e }, "error");
                        }
                    }
    
                    const value = this.maelstrom.module<T>(label);
                    if (value && !(value instanceof Promise)) {
                        rift.open(value);
                        this.maelstrom.record(label, "use_rift", value as T, store);
                        return MaybePromise(value);
                    }
    
                    this.maelstrom.record(label, "use_rift", apparation, store);
                    return apparation as T;
                } catch (e: A) {
                    this.maelstrom.log("USE_RIFT", { error: e }, "error");
                    return apparation as T;
                }
            };
            fn.resolve = (() => {
                // ═══════════════════════════════════════════════════════════════════════════════════════════
                // SOLVER · resolve_global_rift / resolve_rift_initial — the diplomat and the bootstrap
                // ═══════════════════════════════════════════════════════════════════════════════════════════
                const cn = <T = A,>(name: S, v: T): A => {
                    try {
                        return this.maelstrom.rift.resolve.global<T>(name, 15000, v);
                    } catch (e: A) {
                        this.maelstrom.log("RESOLVE_RIFT_INITIAL", { error: e }, "error");
                        return (e as A)?.error ?? v;
                    }
                };
                cn.global = <T = A,>(name: S, wait: N = 20000, fallback?: T): A => {
                    try {
                        const rift = this.maelstrom.rift<T>(name);
                        if (rift.isOpen) return rift.current as T;
                        if (rift.isClaimed) {
                            try {
                                return MaybePromise(rift.wait(wait) as A);
                            } catch (e: A) {
                                this.maelstrom.log("RESOLVE_GLOBAL_RIFT", { error: e }, "error");
                                return fallback as T;
                            }
                        }
                        return fallback as T;
                    } catch (e: A) {
                        this.maelstrom.log("RESOLVE_GLOBAL_RIFT", { error: e }, "error");
                        return fallback as T;
                    }
                };
                return cn;
            })();
            // ═══════════════════════════════════════════════════════════════════════════════════════════
            // SOLVER · has — the scout. Four probes, one boolean.
            // ═══════════════════════════════════════════════════════════════════════════════════════════
            fn.has = (module: S): B => {
                if (this.#rifts.has(module)) return true;
                if (this.maelstrom.module.isLoaded(module)) return true;
                if (this.maelstrom.rift<A>(module).isOpen) return true;
                try {
                    this.maelstrom.blind.require!(module);
                    return true;
                } catch {
                    try {
                        void this.maelstrom.blind.import(module);
                        return true;
                    } catch {
                        return false;
                    }
                }
            };
            // ═══════════════════════════════════════════════════════════════════════════════════════════
            // THE REGISTRY — every solver reads and writes through here, and nowhere else
            // ═══════════════════════════════════════════════════════════════════════════════════════════
            fn.make = <T = A,>(): RiftLike<T> => {
                return new Rift<T>() as RiftLike<T>;
            }; /** true when a shared store can actually round-trip */
            fn.conjure = (() => {
                // ═══════════════════════════════════════════════════════════════════════════════════════════
                // SOLVER · conjure — the conjurer. The full cached/uncached pair.
                // ═══════════════════════════════════════════════════════════════════════════════════════════
                /** cached conjuration — one registry entry per moduleName, claimed once, waited on forever after */
                const cn = <T = A, F = A>(options: OPTIONAL_IMPORT_OPTIONS<T, F>): A => {
                    const { moduleName, fallback: rawFallback, module = true, cache = true, preferRequire = false, importPromise, ...userEnvImports } = options;
                    const fallback = this.maelstrom.normalizeFallback<F>(rawFallback, this.maelstrom.createSafeProxy as unknown as () => F);
    
                    if (!moduleName || !cache) {
                        return this.maelstrom.rift.conjure.uncached<T, F>({ moduleName, fallback: fallback(), module, preferRequire, importPromise, ...userEnvImports } as OPTIONAL_IMPORT_OPTIONS<T, F>);
                    }
    
                    const rift = this.maelstrom.rift<T | F>(moduleName) as Rift<T | F>;
                    if (rift.isOpen) return rift.wait();
                    if (!rift.claim()) return rift.wait();
    
                    const result = this.maelstrom.rift.conjure.uncached<T, F>({ moduleName, fallback, module, preferRequire, importPromise, ...userEnvImports } as OPTIONAL_IMPORT_OPTIONS<T, F>);
                    const settle = (resolved: A): A => {
                        rift.open(resolved);
                        this.maelstrom.record(moduleName, "conjure", resolved);
                        return resolved;
                    };
                    return MaybePromise(result instanceof Promise ? result.then(settle) : settle(result));
                }; /** conjure a rift for a whole module namespace */
                cn.get = <T = A,>(label: S, cache: B = true, module: B = true): A => {
                    const rift = this.maelstrom.rift<T>(label);
                    if (rift.isOpen) return rift;
                    const raw = this.maelstrom.rift.conjure<T>({ moduleName: label, module, cache });
                    const result = MaybePromise(raw);
                    if (rift.isOpen) {
                        return MaybePromise(rift);
                    } else if (raw) {
                        return MaybePromise(raw);
                    } else {
                        const resolve = (resolved: A): A => {
                            if (resolved === undefined || resolved === null) return resolved;
                            rift.open(resolved);
                            this.maelstrom.record(label, "conjure", resolved);
                            return (resolved as A)?.[label];
                        };
                        return MaybePromise(result instanceof Promise ? result.then(resolve) : resolve(result));
                    }
                }; /** the agnostic auto importer: if it doesn't exist, it doesn't crash */
                cn.uncached = <T = A, F = A>(options: OPTIONAL_IMPORT_OPTIONS<T, F>): A => {
                    const { moduleName, fallback: rawFallback, comp_name, module = true, preferRequire = false, importPromise, ...userEnvImports } = options;
                    const fallback = this.maelstrom.normalizeFallback<F>(rawFallback, this.maelstrom.createSafeProxy as () => F);
                    const forceNode = userEnvImports.force !== undefined;
    
                    const state = {
                        moduleLevel: null,
                        platform: null,
                        compilers: [],
                        bundler: null,
                        adapters: [],
                        ready: false,
                    };
                    const priorityKeys = state ? [...state.adapters, ...state.compilers, state.moduleLevel, state.platform, state.bundler] : [];
    
                    const resolveImportCandidate = async (candidate: any) => {
                        if (!candidate) return null;
                        try {
                            const resolved = typeof candidate.then === "function" ? await candidate : candidate;
                            return resolved ?? null;
                        } catch {
                            return null;
                        }
                    };
    
                    let syncResult: any = undefined;
                    let needsAsync = false;
    
                    if (!module || (importPromise && !moduleName)) {
                        if (importPromise && typeof (importPromise as any).then !== "function") {
                            syncResult = importPromise;
                        } else if (importPromise) {
                            needsAsync = true;
                        } else {
                            for (const value of Object.values(userEnvImports)) {
                                if (value && typeof value === "object" && typeof (value as any).then === "function") {
                                    needsAsync = true;
                                    break;
                                }
                                if (value !== null && value !== undefined) {
                                    syncResult = value;
                                    break;
                                }
                            }
                            if (!needsAsync && syncResult === undefined) {
                                syncResult = fallback();
                            }
                        }
                    } else {
                        for (const key of priorityKeys) {
                            if (key && userEnvImports[key]) {
                                const v = userEnvImports[key];
                                if (v && typeof v === "object" && typeof (v as any).then === "function") {
                                    needsAsync = true;
                                    break;
                                }
                                if (v !== null && v !== undefined) {
                                    syncResult = v;
                                    break;
                                }
                            }
                        }
    
                        if (!needsAsync && syncResult === undefined && userEnvImports.default) {
                            const v = userEnvImports.default;
                            if (v && typeof v === "object" && typeof (v as any).then === "function") {
                                needsAsync = true;
                            } else if (v !== null && v !== undefined) {
                                syncResult = v;
                            }
                        }
    
                        if (!needsAsync && syncResult === undefined && moduleName) {
                            if (forceNode) {
                                try {
                                    const res = typeof forceNode === "function" ? (forceNode as () => F)() : forceNode;
                                    if (res !== null && res !== undefined) syncResult = res;
                                    else needsAsync = true;
                                } catch {
                                    needsAsync = true;
                                }
                            } else if (comp_name) {
                                if (this.maelstrom.blind.require && this.maelstrom.has.require) {
                                    try {
                                        const res = this.maelstrom.blind.require(moduleName);
                                        if (res && res[comp_name] !== undefined) syncResult = res[comp_name];
                                        else needsAsync = true;
                                    } catch {
                                        needsAsync = true;
                                    }
                                } else {
                                    needsAsync = true;
                                }
                            } else if (preferRequire && this.maelstrom.has.require) {
                                try {
                                    const res = this.maelstrom.blind.require!(moduleName);
                                    if (res !== null && res !== undefined) syncResult = res;
                                    else needsAsync = true;
                                } catch {
                                    needsAsync = true;
                                }
                            } else if (!preferRequire && this.maelstrom.blind.require && state && (state.bundler === "bun" || this.maelstrom.is.electron || typeof process !== "undefined") && this.maelstrom.has.require) {
                                try {
                                    const res = this.maelstrom.blind.require!(moduleName);
                                    if (res !== null && res !== undefined) syncResult = res;
                                    else needsAsync = true;
                                } catch {
                                    needsAsync = true;
                                }
                            } else if (this.maelstrom.has.require && this.maelstrom.blind.require) {
                                try {
                                    const res = this.maelstrom.blind.require!(moduleName);
                                    if (res !== null && res !== undefined) syncResult = res;
                                    else needsAsync = true;
                                } catch {
                                    needsAsync = true;
                                }
                            } else {
                                needsAsync = true;
                            }
                        }
    
                        if (!needsAsync && syncResult === undefined) {
                            syncResult = fallback();
                        }
                    }
    
                    // AND BAM... All sync work is now complete... NOW let us throw down a logic gate so that we can meet the other req's
                    // We first attempt to return the sync result... just like how it is defined in MaybePromise, followed by the promise functionality...
                    // this is A HARD FUCKING REQUIREMENT... This is because javascript does not trigger async work TILL it ACTUALLY see the promised code, so it DOES NOT TRIGGER... SIMPLY WALKING OVER AWAIT at the start of the function... the same goes for async... BUT not all runtimes play nicely... so to ensure this happen everywhere without any errors whats so ever... we lock it behind a logic gate as well, because when javascript executes the code... it can see and view everything except what is behind that logic gate. Thus... allowing us to hide the async functionality, that when triggered fucks our execution right up and slows everything down...
    
                    if (syncResult) {
                        return MaybePromise(syncResult);
                    } else {
                        return MaybePromise(
                            (() => {
                                const timeoutMs = 10000;
                                return new Promise<T>((resolve, reject) => {
                                    let timer: ReturnType<typeof setTimeout> | undefined;
                                    if (!module || (importPromise && !moduleName)) {
                                        if (importPromise) {
                                            const resolved = importPromise;
                                            if (resolved !== null && resolved !== undefined) {
                                                if (timer) clearTimeout(timer);
                                                resolve(resolved);
                                                return;
                                            }
                                        }
    
                                        for (const value of Object.values(userEnvImports)) {
                                            if (value && typeof value === "object" && typeof (value as any).then === "function") {
                                                try {
                                                    const result = value;
                                                    if (result !== null && result !== undefined) {
                                                        if (timer) clearTimeout(timer);
                                                        resolve(result);
                                                        return;
                                                    }
                                                } catch {
                                                    continue;
                                                }
                                            }
                                        }
                                    }
    
                                    if (moduleName) {
                                        if (moduleName && comp_name) {
                                            const res = this.maelstrom.blind.import(moduleName) as A;
                                            if (res && res[comp_name] !== undefined) {
                                                resolve(res[comp_name]) as unknown as F;
                                                if (timer) clearTimeout(timer);
                                                return;
                                            }
                                        } else {
                                            const res2 = this.maelstrom.blind.import(moduleName);
                                            if (res2 !== null && res2 !== undefined) {
                                                resolve(res2) as unknown as F;
                                                if (timer) clearTimeout(timer);
                                                return;
                                            }
                                        }
                                    }
                                    if (userEnvImports.default) {
                                        const res = resolveImportCandidate(userEnvImports.default);
                                        if (res !== null) {
                                            resolve(res) as unknown as F;
                                            if (timer) clearTimeout(timer);
                                            return;
                                        }
                                    }
                                    if (timeoutMs) {
                                        timer = setTimeout(() => {
                                            reject(new Error(`[rift] wait() timed out after ${timeoutMs}ms`));
                                        }, timeoutMs);
                                    }
                                });
                            })(),
                        );
                    }
                };
                return cn;
            })(); /**
             * The named-export form of use(). `label` is the rift key — one probe per name, ever.
             * `filePath` resolves relative to this file (the "locally sourced" anchor).
             * Cold: sync fallback. Hot: the real named export, as a plain property read.
             */
            fn.use_if_available = <T = A,>(label: S, filePath: S, comp_name: S, fallback: T, opts?: { store?: StoreKind }): T => {
                const rift = this.maelstrom.rift<T>(label);
    
                if (rift.isOpen) return rift.current as T; // hot path — done, sync
    
                if (rift.claim()) {
                    // one-time fire-and-forget probe
                    void (async () => {
                        try {
                            const mod = await this.maelstrom.blind.import(this.maelstrom.key_for(filePath));
                            const named = mod?.[comp_name] !== undefined ? mod[comp_name] : fallback;
                            rift.open(named);
                            this.maelstrom.record(label, "use_if_available", named, opts?.store ?? "local");
                        } catch {
                            rift.open(fallback);
                            this.maelstrom.record(label, "use_if_available", fallback, opts?.store ?? "local");
                        }
                    })();
                }
    
                return fallback; // cold path — instant
            };
            // SOLVER · probe + use_if_available — the locally sourced named import
            // ═══════════════════════════════════════════════════════════════════════════════════════════
            //
            // Cold path: returns `fallback` synchronously. ZERO await. ZERO promise.
            // One probe per label, ever — every later call is a bare property read.
            // The background import resolves the rift; every subsequent call sees the real value.
            fn.probe = <T = A,>(label: S, filePath: S, comp_name: S, fallback: T): T => {
                return this.maelstrom.rift.use_if_available<T>(label, filePath, comp_name, fallback);
            };
            fn.guard_promise_gate = (get: A, name: string, fn: A, timeoutMs?: number) => {
                return async (): Promise<R> => {
                    const val = await this.maelstrom.rift(name).wait(timeoutMs);
                    return fn(val);
                };
            };
            return fn;
        })();
        // ═══════════════════════════════════════════════════════════════════════════════════════════
        // SOLVER · load_module / require_module / export_of — the porters and the index clerk
        // ═══════════════════════════════════════════════════════════════════════════════════════════
        //
        // The module cache IS the registry: loaded modules live under `mod:` labels, sync-required
        // ones under `req:` labels, with the export index stored ON the entry itself. Inflight
        // dedupe lives on the entry too. No separate maps.
        fn.module = (() => {
            /**
             * get_module: require the specifier → local file read → builtin check → cwd-resolved
             * require → async import as the last resort. Every sync path is tried before anything
             * returns a promise.
             */
            const fn = <T = A,>(module: S, filePath?: S): T | Promise<T> => {
                const br = this.maelstrom.blind.require;
                try {
                    let v: A = null;
    
                    try {
                        v = br!(module);
                        return MaybePromise(v) as T;
                    } catch (e: A) {
                        this.maelstrom.log("GET_MODULE", { module, type: "SYNC", import_used: "blindRequire", error: e }, "error");
                    }
    
                    if (module && this.maelstrom.has.local(filePath, module)) {
                        const fs = br!("fs");
                        const path = br!("path");
                        const resolved = path.resolve(process.cwd(), filePath as S);
                        if (fs.existsSync(resolved)) {
                            v = fs.readFileSync(resolved, "utf8").trim();
                            return MaybePromise(v) as T;
                        }
                    }
    
                    if (this.maelstrom.has.local(filePath)) {
                        const path = br!("path");
                        const resolved = path.resolve(process.cwd(), filePath as S);
                        try {
                            v = br!(resolved);
                            return MaybePromise(v) as T;
                        } catch (e: A) {
                            this.maelstrom.log("GET_MODULE", { module, filePath, type: "SYNC", import_used: "local", error: e }, "error");
                        }
                    }
    
                    if (this.maelstrom.has.node || this.maelstrom.has.bun) {
                        const fs = br!("fs");
                        const { isBuiltin } = br!("node:module");
                        if (isBuiltin(module)) {
                            try {
                                v = br!(module);
                                return MaybePromise(v) as T;
                            } catch (e: A) {
                                this.maelstrom.log("GET_MODULE", { module, type: "SYNC", import_used: "builtin", error: e }, "error");
                            }
                        }
                        try {
                            const resolved = (br as A).resolve(module, { paths: [process.cwd()] });
                            if (fs.existsSync(resolved)) {
                                v = br!(resolved);
                                return MaybePromise(v) as T;
                            }
                        } catch (e: A) {
                            this.maelstrom.log("GET_MODULE", { module, type: "SYNC", import_used: "cwd_resolve", error: e }, "error");
                        }
                    }
    
                    this.maelstrom.log("GET_MODULE", { note: "SYNC PATHS EXHAUSTED — ENTERING ASYNC LAST RESORT", module, type: "SYNC" }, "success");
                    if (module) return Promise.resolve().then(() => this.maelstrom.blind.import(module)) as Promise<T>;
                } catch (e: A) {
                    this.maelstrom.log("GET_MODULE", { module, type: "SYNC", error: e }, "error");
                }
                return undefined as A;
            };
            fn.extract_named = (ns: A, comp_name: S): A => {
                const n = this.maelstrom.toNamespace(ns);
                return n[comp_name] ?? n.default?.[comp_name];
            };
            fn.init = (() => {
                /** init-level conjuration: the module itself, no default candidate, no string leak */
                const rn = (label: S, module: B = true): A => {
                    return this.maelstrom.rift.conjure.uncached({ moduleName: label, module } as OPTIONAL_IMPORT_OPTIONS);
                }; /** conjure a rift for a single named export */
                rn.named_export = (label: S, comp_name: S, cache: B = true, module: B = true): A => {
                    const rift = this.maelstrom.rift(label);
                    if (rift.isOpen) return rift;
                    const raw = this.maelstrom.rift.conjure({ moduleName: label, module, cache, comp_name });
                    const result = MaybePromise(raw);
                    const resolve = (resolved: A): A => {
                        if (resolved === undefined || resolved === null) {
                            this.maelstrom.log("CONJURE_NAMED", { msg: "module_not_found", label }, "warn");
                            return resolved;
                        }
                        if (typeof resolved !== "object" || !(comp_name in resolved)) {
                            this.maelstrom.log("CONJURE_NAMED", { msg: "export_not_found", label, comp_name }, "warn");
                            return resolved;
                        }
                        rift.open(resolved);
                        this.maelstrom.record(label, "conjure", resolved);
                        return (resolved as A)[comp_name];
                    };
                    return MaybePromise(result instanceof Promise ? result.then(resolve) : resolve(result));
                };
                return rn;
            })();
            fn.named_export = <T = A,>(label: S, comp_name: S, module: B = true): A => {
                return this.maelstrom.rift.conjure.uncached<T>({ moduleName: label, module, comp_name } as OPTIONAL_IMPORT_OPTIONS<T>);
            };
            // ═══════════════════════════════════════════════════════════════════════════════════════════
            // SOLVER · check — the verifier. One probe per module, ever. Boolean out.
            // ═══════════════════════════════════════════════════════════════════════════════════════════
            fn.check = async (moduleName: S): Promise<B> => {
                const label = `check:${moduleName}`;
                const rift = this.maelstrom.rift<B>(label) as Rift<B>;
    
                if (rift.claim()) {
                    // check does the ONLY importing anywhere in this flow; open() just records the outcome
                    (async () => {
                        const result = await this.maelstrom.rift.conjure.uncached<A>({ moduleName, fallback: () => null } as OPTIONAL_IMPORT_OPTIONS);
                        const ok = result != null;
                        rift.open(ok);
                        this.maelstrom.record(label, "check", ok);
                    })();
                }
    
                return Promise.resolve(rift.wait()) as Promise<B>;
            };
            fn.isLoaded = (specifier: S): B => {
                return this.maelstrom.rift(`mod:${this.maelstrom.key_for(specifier)}`).isOpen;
            };
            fn.getLoaded = <T = A,>(specifier: S): OPT<T> => {
                return this.maelstrom.rift(`mod:${this.maelstrom.key_for(specifier)}`).current as T | undefined;
            };
            /**
             * Find a named export across every loaded module.
             * Ambiguous finds throw — the index never silently guesses.
             */
            fn.export_of = <T = A,>(name: S, specifier?: S): OPT<T> => {
                if (specifier !== undefined) return (this.maelstrom.module.getLoaded<A>(specifier) as A)?.[name] as T | undefined;
                const sources = new Map<S, A>();
                for (const [label, rift] of this.#rifts) {
                    if (!label.startsWith("mod:") || !rift.isOpen) continue;
                    const idx = (rift as A).exports as Map<S, Map<S, A>>;
                    const found = idx?.get(name);
                    if (found) for (const [k, v] of found) sources.set(k, v);
                }
                if (sources.size === 0) return undefined;
                if (sources.size > 1) throw new Error(`Ambiguous export "${name}" found in: ${[...sources.keys()].join(", ")}`);
                return sources.values().next().value as T | undefined;
            };
            fn.indexExports = (rift: A, key: S, mod: A): void => {
                if (!mod || (typeof mod !== "object" && typeof mod !== "function")) return;
                for (const name of Object.keys(mod)) {
                    let sources = rift.exports.get(name);
                    if (!sources) {
                        sources = new Map<S, A>();
                        rift.exports.set(name, sources);
                    }
                    sources.set(key, (mod as A)[name]);
                }
            }; /** the porter's sync twin — blind require, cached, indexed */
            fn.require = <T = A,>(specifier: S): T => {
                const rift = this.maelstrom.rift(`req:${specifier}`) as Rift<T>;
                if (rift.isOpen) return rift.current as T;
                if (!this.maelstrom.blind.require) {
                    // ESM / browser — no require, no crash
                    this.maelstrom.log("REQUIRE_MODULE", { msg: "require unavailable in this module system — returning undefined", specifier }, "warn");
                    return undefined as T;
                }
                const mod = this.maelstrom.blind.require(specifier) as T;
                rift.open(mod);
                this.maelstrom.module.indexExports(rift, specifier, mod);
                this.maelstrom.record(`req:${specifier}`, "require_module", mod);
                return mod;
            };
            return fn;
        })();
        fn.waiter = (() => {
            // ═══════════════════════════════════════════════════════════════════════════════════════════
            // SOLVER · wait_for / signal — the signal waiter. ONE list.
            // ═══════════════════════════════════════════════════════════════════════════════════════════
            /**
             * register a waiter over one or more signal keys; fires when ALL of them have fired.
             * ONE label → the waiter IS an entry subscription (Rift.onOpen). No parallel list.
             * MANY labels → one row in the multi-waiter table, released by each entry's own
             * subscription firing. Every subscribe call goes through the entry, always.
             */
            const fn = (deps: S | S[], fn: (v?: A) => V): N => {
                const keys = [...new Set(Array.isArray(deps) ? deps : [deps])];
                const id = ++this.nextWaiterId;
    
                if (keys.length === 1) {
                    // single-label → pure subscription
                    const unsubs: (() => V)[] = [];
                    const rift = this.maelstrom.rift<A>(keys[0]);
                    if (rift.isOpen) {
                        fn(rift.current);
                        return id;
                    }
                    unsubs.push(
                        rift.onOpen((v) => {
                            fn(v);
                        }),
                    );
                    return id;
                }
    
                const entry: Waiter = { id, deps: keys, fn, ready: new Set() }; // multi-label → one table row
                this.#multiWaiters.add(entry);
                for (const k of keys) {
                    const rift = this.maelstrom.rift<A>(k);
                    if (rift.isOpen) entry.ready.add(k);
                    else rift.onOpen((v) => this.maelstrom.waiter.releaseMulti(k, v));
                }
                if (entry.ready.size === keys.length) {
                    fn();
                    this.#multiWaiters.delete(entry);
                }
                return id;
            };
            /** advance every multi-label waiter on this label */
            fn.releaseMulti = (k: S, v: A): void => {
                for (const entry of [...this.#multiWaiters]) {
                    if (!entry.deps.includes(k)) continue;
                    entry.ready.add(k);
                    if (entry.ready.size !== entry.deps.length) continue;
                    this.#multiWaiters.delete(entry);
                    try {
                        entry.fn(v);
                    } catch (e: A) {
                        queueMicrotask(() => {
                            throw e;
                        });
                    }
                }
            };
            fn.cancel = (id: N): B => {
                for (const entry of this.#multiWaiters) {
                    if (entry.id !== id) continue;
                    this.#multiWaiters.delete(entry);
                    return true;
                }
                return false;
            }; /** rift-shaped wait on a single label — subscribe first, then re-check isOpen */
            fn.wait_and_promise = <T = A,>(timeoutMs: N, key: S, isOpen: B, current: A): Promise<T> => {
                const rift = this.maelstrom.rift<T>(key);
                return new Promise<T>((resolve, reject) => {
                    // subscribe first, THEN re-check isOpen — closes the gap where the record opens mid-subscribe
                    let timer: ReturnType<typeof setTimeout> | undefined;
                    const unsub = rift.onOpen((v) => {
                        if (timer) clearTimeout(timer);
                        unsub();
                        this.maelstrom.record(key, "wait_for", v as T);
                        resolve(v as T);
                    });
                    if (isOpen) {
                        // opened while we were subscribing
                        if (timer) clearTimeout(timer);
                        unsub();
                        this.maelstrom.record(key, "wait_for", current as T);
                        resolve(current as T);
                        return;
                    }
                    if (timeoutMs) {
                        timer = setTimeout(() => {
                            unsub();
                            reject(new Error(`[rift] wait() timed out after ${timeoutMs}ms`));
                        }, timeoutMs);
                    }
                });
            };
            return fn;
        })();
        // ═══════════════════════════════════════════════════════════════════════════════════════════
        // DEFER — the scheduler. Drain loop, versioned deps, stats, cancellation.
        // ═══════════════════════════════════════════════════════════════════════════════════════════
        //
        // Deferred dependency values are registry entries — `provide` opens a rift under the
        // dependency's label. No separate dependency map. The task table holds tasks only.
        fn.defer = (() => {
            const fn = (deps: S | S[], fn: (...v: A[]) => A): N => {
                const keys = [...new Set(typeof deps === "string" ? [deps] : deps)];
                const id = ++this.nextDeferId;
                this.deferStats.registered++;
                this.#deferTasks.set(id, { id, deps: keys, fn, state: "pending" });
                this.maelstrom.defer.drain();
                return id;
            };
            fn.provide = (k: S | Record<S, A>, v?: A): void => {
                const entries: [S, A][] = typeof k === "string" ? [[k as S, v as A]] : Object.entries(k as Record<S, A>);
                for (const [key, value] of entries) {
                    const rift = this.maelstrom.rift(key) as RiftLike<A> & { version: N };
                    rift.reset(); // replace, don't just first-open
                    rift.open(value);
                    rift.version++;
                    this.maelstrom.record(key, "manual", value);
                }
                this.maelstrom.defer.drain();
            };
            fn.resolve = (k: S): void => {
                this.maelstrom.defer.provide(k, undefined);
            };
            fn.invalidate = (k: S): B => {
                return this.maelstrom.rift.delete(k);
            };
            fn.has = (k: S): B => {
                return this.maelstrom.rift(k).isOpen;
            };
            fn.get = <T = A,>(k: S): OPT<T> => {
                return this.maelstrom.rift(k).current as T | undefined;
            };
            fn.cancel = (id: N): B => {
                const task = this.#deferTasks.get(id);
                if (!task || task.state !== "pending") return false;
                this.#deferTasks.delete(id);
                this.deferStats.cancelled++;
                return true;
            };
            fn.ready = (task: DeferredTask): B => {
                return task.deps.every((k) => this.maelstrom.rift(k).isOpen);
            };
            fn.drain = (): V => {
                if (this.draining) {
                    this.drainAgain = true;
                    return;
                }
                this.draining = true;
                try {
                    do {
                        this.drainAgain = false;
                        for (const task of this.#deferTasks.values()) {
                            if (task.state !== "pending" || !this.maelstrom.defer.ready(task)) continue;
                            task.state = "running"; // claim before execution to prevent duplicate dispatch
                            this.deferStats.executed++;
                            const v = task.deps.map((k) => this.maelstrom.rift(k).current);
                            try {
                                const result = task.fn(...v);
                                if (result != null && typeof (result as A).then === "function") {
                                    Promise.resolve(result)
                                        .then(() => {
                                            this.deferStats.completed++;
                                            this.maelstrom.defer.onSuccess(task.id, task.deps);
                                        })
                                        .catch((e: A) => {
                                            this.deferStats.failed++;
                                            this.maelstrom.defer.reportError(task, e);
                                        })
                                        .finally(() => {
                                            this.#deferTasks.delete(task.id);
                                        });
                                } else {
                                    this.deferStats.completed++;
                                    this.maelstrom.defer.onSuccess(task.id, task.deps);
                                    this.#deferTasks.delete(task.id);
                                }
                            } catch (e: A) {
                                this.deferStats.failed++;
                                this.maelstrom.defer.reportError(task, e);
                                this.#deferTasks.delete(task.id);
                            }
                        }
                    } while (this.drainAgain);
                } finally {
                    this.draining = false;
                }
            };
            fn.reportError = (task: DeferredTask, e: A): void => {
                try {
                    this.maelstrom.defer.onError(e, task.id, task.deps);
                } catch {
                    /* error reporting must not interrupt the scheduler */
                }
            };
            fn.inspect = (): R => {
                const pending = [...this.#deferTasks.values()].filter((x) => x.state === "pending").map((x) => ({ id: x.id, deps: x.deps, missing: x.deps.filter((k) => !this.maelstrom.rift(k).isOpen) }));
                const running = [...this.#deferTasks.values()].filter((x) => x.state === "running").map((x) => ({ id: x.id, deps: x.deps }));
                return {
                    stats: { ...this.deferStats },
                    pending,
                    running,
                } as A;
            };
            fn.clear = (): void => {
                for (const [id, task] of this.#deferTasks) {
                    if (task.state === "pending") {
                        this.#deferTasks.delete(id);
                        this.deferStats.cancelled++;
                    }
                }
            };
            fn.onError = (e: A, id: N, deps: S[]) => {
                this.maelstrom.log("DEFERRED", { msg: "Task failed", id, deps, error: e }, "error");
            };
            fn.onSuccess = (id: N, deps: S[]) => {
                this.maelstrom.log("DEFERRED", { msg: "Task fulfilled", id, deps }, "info");
            };
            return fn;
        })();
        fn. detect = (() => {
            const fn = () => {};
            fn.capability = (name: S): B => {
                const g: A = typeof globalThis !== "undefined" ? globalThis : {};
                switch (name) {
                    case "shared_worker":
                        return typeof g.SharedWorker !== "undefined";
                    case "service_worker":
                        return typeof g.ServiceWorkerGlobalScope !== "undefined" || (typeof g.navigator !== "undefined" && "serviceWorker" in g.navigator);
                    case "unix_worker":
                        return typeof g.process !== "undefined" && !!g.process?.versions?.node;
                    case "broadcastr_channel":
                        return typeof g.BroadcastChannel !== "undefined";
                    case "electron_server":
                    case "electron_client":
                        return typeof g.process !== "undefined" && !!g.process?.versions?.electron;
                    case "named_pipe":
                    case "native_msg_host":
                    case "native_msg_client":
                        return typeof g.process !== "undefined" && !!g.process?.versions?.node;
                    default:
                        return false;
                }
            };
            fn.bundler = (): S | null => {
                const proc: A = typeof process !== "undefined" ? process : null;
                if (!proc || !proc.versions?.node) return null; // .versions check distinguishes real Node from a browser shim
                if (proc.versions?.bun) return "bun";
                const ua: S = proc.env?.npm_config_user_agent ?? "";
                if (ua.startsWith("pnpm")) return "pnpm";
                if (ua.startsWith("yarn")) return "yarn";
                if (ua.startsWith("npm")) return "npm";
                return null;
            };
            fn.firstMatch = async (candidates: readonly S[]): Promise<S | null> => {
                for (const name of candidates) {
                    if (await this.maelstrom.module.check(name)) return name;
                }
                return null;
            };
            fn.all = async (candidates: readonly S[]): Promise<S[]> => {
                const results = await Promise.all(candidates.map(async (name) => ((await this.maelstrom.module.check(name)) ? name : null)));
                return results.filter((r): r is S => r !== null);
            }; /** runs once. Everything downstream just reads the result. */
            fn.environment = (env_lib?: S): void => {
                void (async () => {
                    const bundler = this.maelstrom.detect.bundler();
                    const [moduleLevel, platform, compilers, adapters] = await Promise.all([this.maelstrom.detect.firstMatch(env_lib ? [env_lib] : this.MODULE_LEVEL_CANDIDATES), this.maelstrom.detect.firstMatch(this.PLATFORM_CANDIDATES), this.maelstrom.detect.all(this.COMPILER_CANDIDATES), this.maelstrom.detect.all(this.ADAPTER_CANDIDATES)]);
                    this.ENV_STATE = { moduleLevel, platform, compilers, bundler, adapters, ready: true };
                    this.maelstrom.record("ENV_DETECTION", "detect_environment", this.ENV_STATE);
                })();
            };
            // ═══════════════════════════════════════════════════════════════════════════════════════════
            // what_is_this_item_exactly — the type interrogator, carried whole
            // ═══════════════════════════════════════════════════════════════════════════════════════════
            fn.type = (v: unknown) => {
                // #region ═════════════════════════════════════════════
                // ── axis 2 first: is it async? thenable (promise-like, not necessarily Promise)  ─────────────────────══
                if (v != null && typeof (v as any).then === "function")/*                            */ return "thenable";
                // ─── async iterable ───────────────────────────────────────────────────────────────────────═────────────
                if (v != null && typeof (v as any)[Symbol.asyncIterator] === "function")/*     */ return "async-iterable";
                // ── axis 1: what shape? ────────────────────────────────────────────────────────────────────────────────
                if (v === null)/*                           null (typeof null === 'object', so separate) */ return "null";
                const t = typeof v;/*                                                                                  */;
                if (t !== "object" && t !== "function")/*        primitive: str/num/bool/bigint/sym/undefined */ return t;
                if (typeof v === "function")/*                                              function */ return "function";
                if (ArrayBuffer.isView(v))/*                                     TypedArray or DataView */ return "array";
                if (v instanceof Map)/*                                                                   */ return "map";
                if (v instanceof Set)/*                                                                   */ return "set";
                if (v instanceof Date)/*                                                                 */ return "date";
                if (v instanceof RegExp)/*                                                             */ return "regexp";
                if (v instanceof Error)/*                                                               */ return "error";
                if (typeof (v as any)[Symbol.iterator] === "function")/*                             */ return "iterable";
                if (t === "object")/*                                                     plain record */ return "object";
                // ─── iterable ──────────────────────────────────────────────────────────────────────────────────────────
                if (v != null && typeof (v as any)[Symbol.iterator] === "function")/*                */ return "iterator";
                if (typeof v === "string")/*                                                           */ return "string";
                if (typeof v === "number")/*                                                           */ return "number";
                if (typeof v === "boolean")/*                                                         */ return "boolean";
                if (typeof v === "bigint")/*                                                           */ return "bigint";
                if (typeof v === "symbol")/*                                                           */ return "symbol";
                if (typeof v === "undefined")/*                                                     */ return "undefined";
                // ─── objects by constructor ────────────────────────────────────────────────────────────────────────────
                if (v instanceof Array)/*                                           or Array.isArray(v) */ return "Array";
                if (v instanceof WeakMap)/*                                                           */ return "WeakMap";
                if (v instanceof WeakSet)/*                                                           */ return "WeakSet";
                if (v instanceof Promise)/*                                                           */ return "Promise";
                if (v instanceof ArrayBuffer)/*                                                   */ return "ArrayBuffer";
                // ─── node-specific ─────────────────────────────────────────────────────────────────────────────────────
                if (v instanceof Buffer)/*                                                 Node buffer */ return "Buffer";
                if (v instanceof URL)/*                                                       oWHATWG URL */ return "URL";
                if (v instanceof URLSearchParams)/*                                           */ return "URLSearchParams";
                // ─── plain object / record ─────────────────────────────────────────────────────────────────────────────
                if (typeof v === "object" && v !== null && !Array.isArray(v) && !(v instanceof Map) 
                    && !(v instanceof Set))/*                                                     */ return "plainObject";
                // ───────────────────────────────────────────────────────────────────────────────────────────────────────
            };
            fn.cloudSupport = (): B => {
                const g: A = typeof globalThis !== "undefined" ? globalThis : {};
                return !!(g.window && g.window !== g) || typeof (g as A).document !== "undefined";
            };
            return fn;
        })();
        fn. useEnvState =(): EnvDetectionState & { rt: S; subChannel: S } =>{
            if (!this.ENV_STATE.ready && !this.warnedUseBeforeReady) {
                this.warnedUseBeforeReady = true; // warn once, not spam
                this.maelstrom.log("USE_ENV_STATE", { note: "called before detect_environment() finished — falling back to defaults" }, "warn");
            }
            return { ...this.ENV_STATE, rt: "ENV_DETECTION", subChannel: "[CSE_ENV]" };
        }// ═══════════════════════════════════════════════════════════════════════════════════════════
        // APPARATION — the no-op factory, carried whole
        // ═══════════════════════════════════════════════════════════════════════════════════════════
        fn. apparation = {
            any: <T = A,>(): T => ({}) as T,
            anyGeneric: <T = A,>(): T => ({}) as T,
            anyProp: <T = A,>(v: T): T => v,
            anyProp2: <T = A,>(v: A): T => v as T,
            anyProp3: (v: A): A => v,
            anyAs: (v: S | undefined): A => v as A,
            promise: <T = A,>(): Promise<T> => Promise.resolve({} as T),
            booleanPromise: (): Promise<B> => Promise.resolve(false),
            void: (): void => {},
            func:
                <T = A,>(): (() => T) =>
                () =>
                    ({}) as T,
            funcVoid: (): (() => void) => () => {},
            universal: <T = A,>(): T =>
                new Proxy({} as A, {
                    get: (target, prop) => {
                        if (typeof prop === "symbol") return undefined;
                        const propName = prop as S;
                        if (propName === "then" || propName === "catch" || propName === "finally") return undefined;
                        if ((propName as A) === (Symbol.iterator as A)) return function* () {};
                        if (propName === "toString" || propName === "valueOf") return () => "";
                        return (...args: A[]) => target;
                    },
                    set: () => true,
                    deleteProperty: () => true,
                    ownKeys: () => [],
                    getOwnPropertyDescriptor: () => undefined,
                    apply: (target) => target,
                    construct: (target) => target,
                }) as T,
            universalNoop: <T = A,>(): T =>
                new Proxy({} as A, {
                    get: (target, prop) => {
                        if (typeof prop === "symbol") return undefined;
                        const propName = prop as S;
                        if (propName === "then" || propName === "catch" || propName === "finally") return undefined;
                        if ((propName as A) === (Symbol.iterator as A)) return function* () {};
                        if (propName === "toString" || propName === "valueOf") return () => "";
                        return (...args: A[]) => target;
                    },
                    set: () => true,
                    deleteProperty: () => true,
                    ownKeys: () => [],
                    getOwnPropertyDescriptor: () => undefined,
                    apply: (target) => target,
                    construct: (target) => target,
                }) as T,
            simpleNoop: <T = A,>(): T => ({}) as T,
            noopPromise: <T = A,>(): Promise<T> => Promise.resolve({} as T),
            noopBooleanPromise: (): Promise<B> => Promise.resolve(false),
            noopVoid: (): void => {},
            noopFunction:
                <T = A,>(): (() => T) =>
                () =>
                    ({}) as T,
            thenable: <T = A,>(): PromiseLike<T> => ({ then: (_onF?: A, _onR?: A) => new Promise<T>(() => {}) }) as A,
            asyncIterable: <T = A,>(): AsyncIterable<T> =>
                ({
                    [Symbol.asyncIterator]: async function* () {
                        await new Promise(() => {});
                    },
                }) as A,
            null: (): null => null,
            primitive: <T extends S | N | B | bigint | symbol | undefined>(): T => undefined as T,
            string: (): S => "",
            number: (): N => 0,
            boolean: (): B => false,
            bigint: (): bigint => 0n,
            symbol: (): symbol => Symbol("apparation"),
            undefined: (): undefined => undefined,
            undefinedString: (): S => "undefined",
            function_: (): ((...args: A[]) => A) => () => {},
            typedArray: <T extends ArrayBufferView = Uint8Array>(): T => new Uint8Array() as unknown as T,
            dataView: (): DataView => new DataView(new ArrayBuffer(0)),
            arrayBuffer: (): ArrayBuffer => new ArrayBuffer(0),
            map: <K = A, V = A>(): Map<K, V> => new Map<K, V>(),
            set: <T = A,>(): Set<T> => new Set<T>(),
            date: (): D => new Date(0),
            regexp: (): RegExp => /(?:)/,
            error: (msg = "[Apparation]"): Error => new Error(msg),
            iterable: <T = A,>(): Iterable<T> => [] as A,
            iterator: <T = A,>(): Iterator<T> => [][Symbol.iterator](),
            object: <T extends object = object>(): T => ({}) as T,
            plainObject: <T extends object = Record<S, A>>(): T => ({}) as T,
            array: <T = A,>(): T[] => [] as T[],
            weakMap: <K extends object = object, V = A>(): WeakMap<K, V> => new WeakMap<K, V>(),
            weakSet: <T extends object = object>(): WeakSet<T> => new WeakSet<T>(),
            promiseInstance: <T = A,>(): Promise<T> => Promise.resolve({} as T),
            url: (): URL => new URL("about:blank"),
            urlSearchParams: (): URLSearchParams => new URLSearchParams(),
            buffer: (): A => (typeof Buffer !== "undefined" ? Buffer.alloc(0) : new Uint8Array()) as A,
            _ArrayBuffer: (): Uint8Array => new Uint8Array(),
        } as A;
        fn.reset=(): void =>{
            this.#rifts.clear();
            this.#multiWaiters.clear();
            this.#deferTasks.clear();
            this.deferStats = { registered: 0, executed: 0, completed: 0, failed: 0, cancelled: 0 };
            this.autoCloud = undefined;
            this.ENV_STATE = { moduleLevel: null, platform: null, compilers: [], bundler: null, adapters: [], ready: false };
        }
		return fn;
	})();
	// #endregion
}
export { ENGINE };
export default new ENGINE();
// #endregion
//                                     ORC_SHOUT — the real logger, carried in whole, unmodified
// #region ─────────────────────────────────────────────────────────────────────────────────────
class ORC_SHOUT {
	c = {
		emerald: { 10: "#002c22", 9: "#004f3b", 8: "#016630", 7: "#007a55", 6: "#009966", 5: "#00bc7d" },
		blue: { 10: "", 9: "", 8: "", 7: "", 6: "", 5: "" },
	};
	t = {
		deep: {
			bg: "#020817",
			txt: "#f8fafc",
			size: 12,
			title_size: 14,
			title_txt: "#94a3b8",
			payload_key: "#a4ed2c",
			payload_value: "#f8fafc",
			font: "monospace",
			border: "2px solid #101727",
			borderRadius: "6px",
			padding: "8px 16px",
			svg: "",
		},
		vintage: {
			bg: "#001B29",
			txt: "#e9e1ba",
			size: 12,
			title_size: 14,
			title_txt: "#ada785",
			payload_key: "#29c3a0",
			payload_value: "#fafafa",
			font: "monospace",
			border: "2px solid #112932",
			borderRadius: "6px",
			padding: "8px 16px",
			svg: "",
		},
		error: {
			bg: "#09090B",
			txt: "#e7000b",
			size: 12,
			title_size: 14,
			title_txt: "#9f0712",
			payload_key: "#a4ed2c",
			payload_value: "#f8fafc",
			font: "monospace",
			border: "2px solid #e7000b",
			borderRadius: "6px",
			padding: "8px 16px",
			svg: `<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="#e7000b" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><line x1="12" x2="12" y1="8" y2="12"/><line x1="12" x2="12.01" y1="16" y2="16"/></svg>`,
		},
		warn: {
			bg: "#09090B",
			txt: "#e17100",
			size: 12,
			title_size: 14,
			title_txt: "#973c00",
			payload_key: "#a4ed2c",
			payload_value: "#f8fafc",
			font: "monospace",
			border: "2px solid #e17100",
			borderRadius: "6px",
			padding: "8px 16px",
			svg: `<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="#e17100" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="m21.73 18-8-14a2 2 0 0 0-3.48 0l-8 14A2 2 0 0 0 4 21h16a2 2 0 0 0 1.73-3"/><path d="M12 9v4"/><path d="M12 17h.01"/></svg>`,
		},
		success: {
			bg: "#09090B",
			txt: "#009966",
			size: 12,
			title_size: 14,
			title_txt: "#016630",
			payload_key: "#a4ed2c",
			payload_value: "#f8fafc",
			font: "monospace",
			border: "2px solid #009966",
			borderRadius: "6px",
			padding: "8px 16px",
			svg: `<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="#009966" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21.801 10A10 10 0 1 1 17 3.335"/><path d="m9 11 3 3L22 4"/></svg>`,
		},
		info: {
			bg: "#09090B",
			txt: "#155dfc",
			size: 12,
			title_size: 14,
			title_txt: "#193cb8",
			payload_key: "#a4ed2c",
			payload_value: "#f8fafc",
			font: "monospace",
			border: "2px solid #155dfc",
			borderRadius: "6px",
			padding: "8px 16px",
			svg: `<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="#155dfc" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><line x1="12" x2="12" y1="8" y2="12"/><line x1="12" x2="12.01" y1="16" y2="16"/></svg>`,
		},
	};
	log(msg: string | string[], p?: any, theme = this.t.deep) {
		const isArray = Array.isArray(msg);
		const title = isArray ? msg[0] : msg;
		const paragraphs = isArray ? msg.slice(1) : [];

		const containerStyle = `
              background: ${theme.bg};
              ${theme.border ? `border: ${theme.border};` : ""}
              ${theme.borderRadius ? `border-radius: ${theme.borderRadius};` : ""}
              ${theme.padding ? `padding: ${theme.padding};` : ""}
              font-family: ${theme.font};
              display: block;
              margin: 4px 0;
          `;

		const titleWithIcon = theme?.svg ? `${theme?.svg} ${title}` : title;
		const titleStyle = `
              color: ${theme.title_txt};
              font-size: ${theme.title_size}px;
              font-weight: bold;
              font-family: ${theme.font};
              display: flex;
              align-items: center;
              gap: 8px;
          `;

		const paragraphStyle = `
              color: ${theme.txt};
              font-size: ${theme.size}px;
              font-family: ${theme.font};
              margin-top: 4px;
          `;

		const keyStyle = `
              color: ${theme.payload_key};
              font-weight: bold;
              font-family: ${theme.font};
          `;
		const valueStyle = `
              color: ${theme.payload_value};
              font-family: ${theme.font};
          `;

		let payloadStr = "";
		if (p && typeof p === "object") {
			const entries = Object.entries(p);
			payloadStr = entries.map(([key, value]) => `%c${key}: %c${JSON.stringify(value, null, 2)}`).join("\n");
		}

		const logParts = [`%c${titleWithIcon}`, ...paragraphs.map((p) => `%c${p}`), payloadStr ? `\n${payloadStr}` : ""].filter(Boolean).join("\n");

		const styles = [containerStyle + titleStyle, ...paragraphs.map(() => paragraphStyle), ...(payloadStr ? [keyStyle, valueStyle] : [])];

		console.log(logParts, ...styles);
	}
}
// #endregion ──────────────────────────────────────────────────────────────────────────────────
//                                           TYPE ALIASES (the shorthand table, carried forward)
// #region ─────────────────────────────────────────────────────────────────────────────────────
// THE EXPORT — exactly three classes leave this file, plus the live machine.
// MaybePromise stays outside the class, because that is how that one works.
// Nothing from any prior solution file is imported, referenced, or relied upon.
// ─────────────────────────────────────────────────────────────────────────────────────────────
interface RiftLike<T = A> {
	readonly isOpen: B;
	readonly isClaimed: B;
	readonly current: OPT<T>;
	readonly createdAt: N;
	readonly openedAt: OPT<N>;
	open(v?: T): V;
	reset(): V;
	claim(): B;
	wait(timeoutMs?: N): T | Promise<T>;
	onOpen(listener: (v: T) => V): () => V;
}
interface RiftRecord<T = A> {
	opened: B;
	claimed: B;
	value?: T;
	createdAt: N;
	openedAt?: N;
}

type StoreListener<T = A> = (v: RiftRecord<T>) => void;
type Solver = "get" | "place" | "smash_n_grab" | "create_rift" | "use_rift" | "resolve_global_rift" | "resolve_rift_initial" | "has" | "probe" | "use_if_available" | "conjure" | "check" | "load_module" | "require_module" | "export_of" | "wait_for" | "defer" | "detect_environment" | "manual";

type StoreKind = "local" | "cloud" | "auto";

interface ResultRecord<T = A> {
	label: S;
	solver: Solver;
	store: StoreKind;
	value: T | undefined;
	ok: B;
	openedAt: N;
}

interface OPTIONAL_IMPORT_OPTIONS<T = A, F = A> {
	moduleName?: S;
	fallback?: F | (() => F);
	module?: B; // true → external module lookup; false → internally sourced import
	cache?: B; // default true. false = always re-run, skip the rift entirely.
	preferRequire?: B; // default false. true = require() over import() when possible.
	importPromise?: A; // a raw promise to await directly — for local files, no moduleName lookup.
	force?: F | (() => F); // force a value over any import at all
	comp_name?: S; // extract a single named export
	[key: S]: A; // ...userEnvImports: candidates keyed by detected environment
}

interface EnvDetectionState {
	moduleLevel: S | null;
	platform: S | null;
	compilers: S[]; // full scan
	bundler: S | null;
	adapters: S[]; // full scan
	ready: B;
}
type Waiter = { id: N; deps: S[]; fn: (v?: A) => V; ready: SET<S> };
type DeferredState = "pending" | "running";
type DeferredTask = { id: N; deps: S[]; fn: (...v: A[]) => A; state: DeferredState };
type DeferredStats = { registered: N; executed: N; completed: N; failed: N; cancelled: N };
// #endregion ──────────────────────────────────────────────────────────────────────────────────

// #endregion 
/**
 * 
function safeCall(fn: A, ...args: A) {
	try {
		fn(...args);
	} catch (e: A) {
		process.emitWarning(e);
	}
}



// sauron/transaction.ts

type Undo = () => void;

let activeTx: Undo[] | null = null;

export function beginTx(): Undo[] {
  const journal: Undo[] = [];

  const previous = activeTx;
  activeTx = journal;

  return journal;
}

export function commitTx(): void {
  activeTx = null;
}

export function rollbackTx(journal: Undo[]): void {
  for (let i = journal.length - 1; i >= 0; i--) {
    journal[i]();
  }

  activeTx = null;
}

export function tx<T>(fn: () => T): T {
  const journal = beginTx();

  return batch(() => {
    try {
      const result = fn();
      commitTx();
      return result;
    } catch (error) {
      rollbackTx(journal);
      throw error;
    }
  });
}

// sauron/resource.ts

export interface Resource<T> {
  data: Signal<T | undefined>;
  loading: Signal<boolean>;
  error: Signal<unknown | null>;
  reload(): Promise<void>;
  dispose(): void;
}

export function resource<T>(
  loader: (abort: AbortSignal, previous: T | undefined) => Promise<T>,
  options: {
    scope?: Scope;
    initial?: T;
    revalidate?: Signal<unknown>;
  } = {}
): Resource<T> {
  const scope = options.scope ?? createScope(() => {});
  const data = signal<T | undefined>(options.initial);
  const loading = signal(false);
  const error = signal<unknown | null>(null);

  let generation = 0;

  const load = async (): Promise<void> => {
    const current = ++generation;
    const controller = new AbortController();

    onScopeCleanup(() => controller.abort());

    loading(true);
    error(null);

    try {
      const value = await loader(controller.signal, data.peek());

      // Ignore stale completions.
      if (current !== generation || controller.signal.aborted) return;

      data(value);
    } catch (err) {
      if (current !== generation || controller.signal.aborted) return;

      error(err);
    } finally {
      if (current === generation && !controller.signal.aborted) {
        loading(false);
      }
    }
  };

  // Revalidate automatically when the trigger changes.
  effect(() => {
    options.revalidate?.();
    void load();
  });

  scope.cleanups.add(() => {
    generation++;
    loading(false);
  });

  return {
    data,
    loading,
    error,
    reload: load,
    dispose: () => scope.dispose(),
  };
}

// sauron/timeline.ts

export interface TimelineEntry<T> {
  id: string;
  scopeId: string | null;
  key: SignalKey;
  previous: T;
  next: T;
  source: string;
  cause?: string;
  timestamp: number;
  txId?: string;
}

export class Timeline {
  private entries: TimelineEntry<any>[] = [];
  private index = -1;
  private replaying = false;

  record<T>(entry: Omit<TimelineEntry<T>, 'id'>): void {
    if (this.replaying) return;

    this.entries.splice(this.index + 1);
    this.entries.push({ id: crypto.randomUUID(), ...entry });

    if (this.entries.length > 1000) {
      this.entries.shift();
    } else {
      this.index++;
    }
  }

  back(steps = 1): boolean {
    return this.goto(this.index - steps);
  }

  forward(steps = 1): boolean {
    return this.goto(this.index + steps);
  }

  goto(target: number): boolean {
    if (target < 0 || target >= this.entries.length) return false;
    if (target === this.index) return false;

    const direction = target > this.index ? 1 : -1;

    this.replaying = true;

    try {
      while (this.index !== target) {
        this.index += direction;

        const entry = this.entries[this.index];

        if (direction === 1) {
          entry.key.write?.(entry.next);
        } else {
          entry.key.write?.(entry.previous);
        }
      }

      return true;
    } finally {
      this.replaying = false;
    }
  }

  causalChain(key: SignalKey): TimelineEntry<any>[] {
    return this.entries.filter((entry) => entry.key === key);
  }
}

// sauron/scope.ts

export type Disposer = () => void;

export interface Scope {
  id: string;
  parent: Scope | null;
  disposed: boolean;
  children: Set<Scope>;
  cleanups: Set<Disposer>;
  abort: AbortController;

  run<T>(fn: () => T): T;
  dispose(): void;
}

let activeScope: Scope | null = null;

export function getScope(): Scope | null {
  return activeScope;
}

export function createScope(
  fn: (scope: Scope) => void | Promise<void>,
  parent: Scope | null = activeScope
): Scope {
  const scope: Scope = {
    id: crypto.randomUUID(),
    parent,
    disposed: false,
    children: new Set(),
    cleanups: new Set(),
    abort: new AbortController(),

    run<T>(task: () => T): T {
      const previous = activeScope;
      activeScope = scope;

      try {
        return task();
      } finally {
        activeScope = previous;
      }
    },

    dispose() {
      if (scope.disposed) return;
      scope.disposed = true;

      scope.abort.abort();

      for (const child of [...scope.children]) {
        child.dispose();
      }

      for (const cleanup of [...scope.cleanups].reverse()) {
        try {
          cleanup();
        } catch (error) {
          reportError(error);
        }
      }

      scope.children.clear();
      scope.cleanups.clear();
      parent?.children.delete(scope);
    },
  };

  parent?.children.add(scope);

  const result = scope.run(() => fn(scope));

  // The critical part Solid makes users do manually:
  // preserve ownership across await.
  if (result instanceof Promise) {
    result.catch((error) => reportError(error));
  }

  return scope;
}

export function onScopeCleanup(disposer: Disposer): void {
  if (!activeScope) {
    throw new Error(
      '[Sauron] onScopeCleanup() called outside a scope'
    );
  }

  activeScope.cleanups.add(disposer);
}

export function withScope<T>(scope: Scope, fn: () => T): T {
  return scope.run(fn);
}

 */
