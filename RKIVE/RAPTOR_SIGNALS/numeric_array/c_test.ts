/**
 * PART 1 — CORRECTNESS
 *
 * Exercises BINARY_REACTOR's public contract, plus a few white-box checks on
 * the internal counters that the public surface cannot express (edge cleanup,
 * subscriber counts, capacity growth).
 *
 * Run:  npx tsx "Correctness Test.ts"
 *       npx tsx "Correctness Test.ts" --only=computed
 */

import { BINARY_REACTOR } from "./BINARY_REACTOR";
import { advisory, assert, describe, it, main } from "./Harness";

const reactor = (): BINARY_REACTOR => new BINARY_REACTOR();

// Private state, used only where the behaviour is not otherwise observable.
const raw = (instance: BINARY_REACTOR): any => instance as any;

function collect<T>(): { fn: (value: T) => void; values: T[] } {
    const values: T[] = [];

    return { values, fn: (value: T) => void values.push(value) };
}

describe("atom: value semantics", () => {
    it("get returns the initial value", () => {
        const b = reactor();
        const a = b.atom(41, "answer");

        assert.equal(a.get(), 41);
        assert.equal(a.peek(), 41);
    });

    it("set stores the value and returns it", () => {
        const b = reactor();
        const a = b.atom(0, "counter");

        const returned = a.set(7);

        assert.equal(returned, 7);
        assert.equal(a.get(), 7);
    });

    it("update receives the current value and stores the result", () => {
        const b = reactor();
        const a = b.atom(2, "counter");
        const seen: number[] = [];

        a.update((value) => {
            seen.push(value);
            return value * 10;
        });

        assert.deepEqual(seen, [2]);
        assert.equal(a.get(), 20);
    });

    it("keeps two atoms on the same address in sync", () => {
        const b = reactor();
        const first = b.atom(1, "shared");
        const second = b.atom(999, "shared");

        assert.equal(first.id, second.id);
        assert.equal(second.get(), 1, "second atom must not reset the cell");

        first.set(5);

        assert.equal(second.get(), 5);
    });

    it("holds object payloads by reference", () => {
        const b = reactor();
        const payload = { name: "initial" };
        const a = b.atom(payload, "object");

        assert.equal(a.get(), payload);
        assert.equal(a.get(), a.get());

        payload.name = "mutated";
        assert.equal(a.get().name, "mutated");
    });

    it("supports symbol payloads and symbol keys", () => {
        const b = reactor();
        const symbolValue = Symbol("value");
        const symbolKey = Symbol("key");
        const a = b.atom<symbol>(symbolValue, symbolKey);

        assert.equal(a.get(), symbolValue);

        const other = b.atom(1, symbolKey);

        assert.equal(other.id, a.id);
    });

    it("exposes a version that advances only on real changes", () => {
        const b = reactor();
        const a = b.atom(0, "versioned");

        const start = a.version;
        a.set(1);
        const afterSet = a.version;
        a.set(1);
        const afterSameSet = a.version;

        assert.ok(afterSet > start, "version must advance on change");
        assert.equal(afterSameSet, afterSet, "same value must not advance version");
    });

    it("uses Object.is for change detection", () => {
        const b = reactor();
        const nan = b.atom(NaN, "nan");

        const before = nan.version;
        nan.set(NaN);

        assert.equal(nan.version, before, "NaN → NaN is not a change");

        const zero = b.atom(0, "zero");

        const zeroBefore = zero.version;
        zero.set(-0);

        assert.equal(zero.version, zeroBefore + 1, "0 → -0 is a change");
    });
});

describe("atom: four-tier addressing", () => {
    it("isolates cells that differ in any tier", () => {
        const b = reactor();

        const a = b.atom("a", "k", "s1", "m1", "r1");
        const b1 = b.atom("b", "k", "s2", "m1", "r1");
        const c = b.atom("c", "k", "s1", "m2", "r1");
        const d = b.atom("d", "k", "s1", "m1", "r2");
        const e = b.atom("e", "k2", "s1", "m1", "r1");

        const ids = new Set([a.id, b1.id, c.id, d.id, e.id]);

        assert.equal(ids.size, 5);

        a.set("a2");

        assert.equal(b1.get(), "b");
        assert.equal(c.get(), "c");
        assert.equal(d.get(), "d");
        assert.equal(e.get(), "e");
    });

    it("treats 1 and \"1\" as different keys", () => {
        const b = reactor();
        const numeric = b.atom("numeric", 1);
        const textual = b.atom("textual", "1");

        assert.notEqual(numeric.id, textual.id);
        assert.equal(numeric.get(), "numeric");
        assert.equal(textual.get(), "textual");
    });

    it("keeps symbols with identical descriptions apart", () => {
        const b = reactor();
        const first = Symbol("same");
        const second = Symbol("same");

        const a = b.atom(1, first);
        const c = b.atom(2, second);

        assert.notEqual(a.id, c.id);
        assert.equal(a.get(), 1);
        assert.equal(c.get(), 2);
    });

    it("applies the documented default tiers", () => {
        const b = reactor();
        const implicit = b.atom(1, "k");
        const explicit = b.atom(1, "k", "__SLAVE__", "__MASTER__", "__ROOT__");

        assert.equal(implicit.id, explicit.id);
    });
});

describe("subscribe", () => {
    it("reports previous and next value", () => {
        const b = reactor();
        const a = b.atom("first", "s");
        const calls: Array<[string, string]> = [];

        a.subscribe((previous, value) => void calls.push([previous, value]));

        a.set("second");
        a.set("third");

        assert.deepEqual(calls, [
            ["first", "second"],
            ["second", "third"]
        ]);
    });

    it("stops after dispose", () => {
        const b = reactor();
        const a = b.atom(0, "s");
        const sink = collect<number>();

        const dispose = a.subscribe(sink.fn);

        a.set(1);
        dispose();
        a.set(2);

        assert.deepEqual(sink.values, [1]);
    });

    it("disposes each listener independently", () => {
        const b = reactor();
        const a = b.atom(0, "s");
        const left = collect<number>();
        const right = collect<number>();

        const disposeLeft = a.subscribe(left.fn);
        a.subscribe(right.fn);

        a.set(1);
        disposeLeft();
        a.set(2);

        assert.deepEqual(left.values, [1]);
        assert.deepEqual(right.values, [1, 2]);
    });

    it("is silent when the value does not change", () => {
        const b = reactor();
        const a = b.atom(1, "s");
        const sink = collect<number>();

        a.subscribe(sink.fn);
        a.set(1);

        assert.deepEqual(sink.values, []);
    });

    it("tolerates disposing twice", () => {
        const b = reactor();
        const a = b.atom(0, "s");
        const sink = collect<number>();

        const dispose = a.subscribe(sink.fn);

        dispose();
        dispose();
        a.set(1);

        assert.deepEqual(sink.values, []);
    });

    it("can write another cell from a listener", () => {
        const b = reactor();
        const source = b.atom(0, "source");
        const mirror = b.atom(0, "mirror");
        const sink = collect<number>();

        mirror.subscribe(sink.fn);
        source.subscribe((_previous, value) => void mirror.set(value * 2));

        source.set(3);

        assert.equal(mirror.get(), 6);
        assert.deepEqual(sink.values, [6]);
    });

    advisory("fires per write even inside a batch (effects coalesce, listeners do not)", () => {
        const b = reactor();
        const a = b.atom(0, "s");
        const sink = collect<number>();

        a.subscribe(sink.fn);
        b.batch(() => {
            a.set(1);
            a.set(2);
            a.set(3);
        });

        assert.deepEqual(sink.values, [1, 2, 3]);
    });
});

describe("effect", () => {
    it("runs immediately", () => {
        const b = reactor();
        let runs = 0;

        b.effect(() => void runs++);

        assert.equal(runs, 1);
    });

    it("re-runs when a dependency changes", () => {
        const b = reactor();
        const a = b.atom(0, "a");
        const seen: number[] = [];

        b.effect(() => void seen.push(a.get()));

        a.set(1);
        a.set(2);

        assert.deepEqual(seen, [0, 1, 2]);
    });

    it("ignores unrelated cells", () => {
        const b = reactor();
        const a = b.atom(0, "a");
        const other = b.atom(0, "other");
        let runs = 0;

        b.effect(() => {
            runs++;
            a.get();
        });

        other.set(1);
        other.set(2);

        assert.equal(runs, 1);
    });

    it("does not re-run when a dependency is set to the same value", () => {
        const b = reactor();
        const a = b.atom(3, "a");
        let runs = 0;

        b.effect(() => {
            runs++;
            a.get();
        });

        a.set(3);

        assert.equal(runs, 1);
    });

    it("does not re-run after dispose", () => {
        const b = reactor();
        const a = b.atom(0, "a");
        let runs = 0;

        const dispose = b.effect(() => {
            runs++;
            a.get();
        });

        a.set(1);
        dispose();
        a.set(2);

        assert.equal(runs, 2);
    });

    it("releases its dependencies on dispose", () => {
        const b = reactor();
        const a = b.atom(0, "a");
        const c = b.atom(0, "c");

        const dispose = b.effect(() => {
            a.get();
            c.get();
        });

        assert.equal(raw(b).subscriberCount[a.id], 1);
        assert.equal(raw(b).subscriberCount[c.id], 1);

        dispose();

        assert.equal(raw(b).subscriberCount[a.id], 0);
        assert.equal(raw(b).subscriberCount[c.id], 0);
    });

    it("tracks only what the last run read", () => {
        const b = reactor();
        const flag = b.atom(true, "flag");
        const left = b.atom(1, "left");
        const right = b.atom(2, "right");
        let runs = 0;

        b.effect(() => {
            runs++;
            flag.get() ? left.get() : right.get();
        });

        assert.equal(raw(b).subscriberCount[right.id], 0);

        flag.set(false);

        assert.equal(runs, 2);
        assert.equal(raw(b).subscriberCount[left.id], 0);
        assert.equal(raw(b).subscriberCount[right.id], 1);
    });

    it("does not track peek", () => {
        const b = reactor();
        const a = b.atom(0, "a");
        let runs = 0;

        b.effect(() => {
            runs++;
            a.peek();
        });

        a.set(1);

        assert.equal(runs, 1);
    });

    it("cascades writes between effects", () => {
        const b = reactor();
        const source = b.atom(0, "source");
        const derived = b.atom(0, "derived");
        const seen: number[] = [];

        b.effect(() => void derived.set(source.get() + 10));
        b.effect(() => void seen.push(derived.get()));

        source.set(1);
        source.set(2);

        assert.deepEqual(seen, [10, 11, 12]);
    });

    it("can dispose itself from inside its own run", () => {
        const b = reactor();
        const a = b.atom(0, "a");
        let runs = 0;
        let dispose: () => void = () => undefined;

        dispose = b.effect(() => {
            runs++;

            if (a.get() === 2) dispose();
        });

        a.set(1);
        a.set(2);
        a.set(3);

        assert.equal(runs, 3);
    });
});

describe("batch", () => {
    it("returns the callback result", () => {
        const b = reactor();

        assert.equal(b.batch(() => 42), 42);
    });

    it("coalesces many writes into one effect run", () => {
        const b = reactor();
        const a = b.atom(0, "a");
        const seen: number[] = [];

        b.effect(() => void seen.push(a.get()));

        b.batch(() => {
            a.set(1);
            a.set(2);
            a.set(3);
        });

        assert.deepEqual(seen, [0, 3]);
    });

    it("does not flush inside the batch", () => {
        const b = reactor();
        const a = b.atom(0, "a");
        let runs = 0;
        let runsInsideBatch = -1;

        b.effect(() => {
            runs++;
            a.get();
        });

        b.batch(() => {
            a.set(1);
            runsInsideBatch = runs;
        });

        assert.equal(runsInsideBatch, 1, "effect must not run mid-batch");
        assert.equal(runs, 2);
    });

    it("flushes once for nested batches", () => {
        const b = reactor();
        const a = b.atom(0, "a");
        const seen: number[] = [];

        b.effect(() => void seen.push(a.get()));

        b.batch(() => {
            a.set(1);
            b.batch(() => {
                a.set(2);
            });
            a.set(3);
        });

        assert.deepEqual(seen, [0, 3]);
    });

    it("resets depth when the callback throws", () => {
        const b = reactor();
        const a = b.atom(0, "a");
        const seen: number[] = [];

        b.effect(() => void seen.push(a.get()));

        assert.throws(() =>
            b.batch(() => {
                a.set(1);
                throw new Error("boom");
            })
        );

        assert.equal(raw(b).batchDepth, 0);

        b.batch(() => a.set(2));

        // The throwing batch still flushes what it queued, then normal batching resumes.
        assert.deepEqual(seen, [0, 1, 2]);
    });
});

describe("computed", () => {
    it("is lazy until read", () => {
        const b = reactor();
        const a = b.atom(2, "a");
        let calls = 0;

        b.computed(() => {
            calls++;
            return a.get() * 2;
        }, "double");

        assert.equal(calls, 0);
    });

    it("memoises repeated reads", () => {
        const b = reactor();
        const a = b.atom(2, "a");
        let calls = 0;

        const double = b.computed(() => {
            calls++;
            return a.get() * 2;
        }, "double");

        assert.equal(double.get(), 4);
        assert.equal(double.get(), 4);
        assert.equal(double.peek(), 4);
        assert.equal(calls, 1);
    });

    it("recomputes once per dependency change", () => {
        const b = reactor();
        const a = b.atom(1, "a");
        let calls = 0;

        const double = b.computed(() => {
            calls++;
            return a.get() * 2;
        }, "double");

        assert.equal(double.get(), 2);
        a.set(5);
        assert.equal(double.get(), 10);
        assert.equal(double.get(), 10);
        assert.equal(calls, 2);
    });

    it("recomputes lazily, not on write", () => {
        const b = reactor();
        const a = b.atom(1, "a");
        let calls = 0;

        const double = b.computed(() => {
            calls++;
            return a.get() * 2;
        }, "double");

        double.get();
        a.set(2);
        a.set(3);
        a.set(4);

        assert.equal(calls, 1, "writes must not force recomputation");

        assert.equal(double.get(), 8);
        assert.equal(calls, 2);
    });

    it("chains through several levels", () => {
        const b = reactor();
        const source = b.atom(1, "source");
        const plusOne = b.computed(() => source.get() + 1, "plusOne");
        const timesTwo = b.computed(() => plusOne.get() * 2, "timesTwo");
        const minusThree = b.computed(() => timesTwo.get() - 3, "minusThree");

        assert.equal(minusThree.get(), 1);

        source.set(4);

        assert.equal(minusThree.get(), 7);
    });

    it("recomputes each level once per source change", () => {
        const b = reactor();
        const source = b.atom(1, "source");
        let level1 = 0;
        let level2 = 0;
        let level3 = 0;

        const a = b.computed(() => {
            level1++;
            return source.get() + 1;
        }, "l1");

        const c = b.computed(() => {
            level2++;
            return a.get() + 1;
        }, "l2");

        const d = b.computed(() => {
            level3++;
            return c.get() + 1;
        }, "l3");

        assert.equal(d.get(), 4);
        source.set(2);
        assert.equal(d.get(), 5);

        assert.deepEqual([level1, level2, level3], [2, 2, 2]);
    });

    it("evaluates a diamond once", () => {
        const b = reactor();
        const root = b.atom(1, "root");
        let calls = 0;

        const left = b.computed(() => root.get() + 1, "left");
        const right = b.computed(() => root.get() * 10, "right");
        const join = b.computed(() => {
            calls++;
            return left.get() + right.get();
        }, "join");

        assert.equal(join.get(), 12);
        root.set(2);

        assert.equal(join.get(), 23);
        assert.equal(calls, 2);
    });

    it("suppresses downstream work when the result is unchanged", () => {
        const b = reactor();
        const source = b.atom(0, "source");
        let computedCalls = 0;
        let effectRuns = 0;

        const positive = b.computed(() => {
            computedCalls++;
            return source.get() > 0;
        }, "positive");

        b.effect(() => {
            effectRuns++;
            positive.get();
        });

        source.set(1);
        source.set(5);
        source.set(9);

        assert.equal(computedCalls, 4);
        assert.equal(effectRuns, 2, "effect must not run while the boolean stays true");
    });

    it("notifies effects that read it", () => {
        const b = reactor();
        const source = b.atom(1, "source");
        const seen: number[] = [];
        const double = b.computed(() => source.get() * 2, "double");

        b.effect(() => void seen.push(double.get()));

        source.set(2);
        source.set(3);

        assert.deepEqual(seen, [2, 4, 6]);
    });

    it("drops dependencies it no longer reads", () => {
        const b = reactor();
        const flag = b.atom(true, "flag");
        const left = b.atom(1, "left");
        const right = b.atom(10, "right");
        let calls = 0;

        const chosen = b.computed(() => {
            calls++;
            return flag.get() ? left.get() : right.get();
        }, "chosen");

        assert.equal(chosen.get(), 1);
        assert.equal(calls, 1);

        right.set(11);
        assert.equal(chosen.get(), 1, "unread branch must not invalidate");
        assert.equal(calls, 1);

        flag.set(false);
        assert.equal(chosen.get(), 11);
        assert.equal(calls, 2);

        assert.equal(raw(b).subscriberCount[left.id], 0, "abandoned branch must be released");

        left.set(99);
        assert.equal(chosen.get(), 11);
        assert.equal(calls, 2, "abandoned branch must not recompute");
    });

    it("refuses a second computed on the same address", () => {
        const b = reactor();

        b.computed(() => 1, "dup");

        assert.throws(() => b.computed(() => 2, "dup"));
    });

    it("propagates errors and recovers afterwards", () => {
        const b = reactor();
        const a = b.atom(1, "a");
        const guarded = b.computed(() => {
            if (a.get() < 0) throw new Error("negative");
            return a.get();
        }, "guarded");

        assert.equal(guarded.get(), 1);

        a.set(-1);
        assert.throws(() => guarded.get(), /negative/);

        a.set(4);
        assert.equal(guarded.get(), 4);
    });

    it("is not evaluated eagerly by batch", () => {
        const b = reactor();
        const a = b.atom(1, "a");
        let calls = 0;

        const double = b.computed(() => {
            calls++;
            return a.get() * 2;
        }, "double");

        b.batch(() => a.set(2));

        assert.equal(calls, 0);
        assert.equal(double.get(), 4);
        assert.equal(calls, 1);
    });
});

describe("scheduler", () => {
    it("keeps queue and dirty stacks balanced after a normal flush", () => {
        const b = reactor();
        const a = b.atom(0, "a");

        b.effect(() => void a.get());
        b.effect(() => void a.get());

        b.batch(() => {
            a.set(1);
            a.set(2);
        });

        assert.equal(raw(b).queueHead, 0);
        assert.equal(raw(b).queueTail, 0);
        assert.equal(raw(b).flushing, false);
        assert.equal(raw(b).currentExecution, 0);
    });

    it("runs every subscriber of a cell exactly once per change", () => {
        const b = reactor();
        const a = b.atom(0, "a");
        const counts = new Array(8).fill(0);

        counts.forEach((_unused, index) => {
            b.effect(() => {
                counts[index]++;
                a.get();
            });
        });

        a.set(1);

        assert.deepEqual(counts, new Array(8).fill(2));
    });

    it("handles a cascade deeper than the initial queue size", () => {
        const b = reactor();
        const cells = Array.from({ length: 40 }, (_unused, index) =>
            b.atom(0, `cell${index}`)
        );
        const seen: number[] = [];

        // Each effect mirrors cell[i] into cell[i + 1].
        for (let i = 0; i < cells.length - 1; i++) {
            const source = cells[i];
            const target = cells[i + 1];

            b.effect(() => void target.set(source.get() + 1));
        }

        b.effect(() => void seen.push(cells[cells.length - 1].get()));
        seen.length = 0;

        cells[0].set(10);

        assert.equal(seen[seen.length - 1], cells.length - 1 + 10);
        assert.equal(cells[cells.length - 1].get(), cells.length - 1 + 10);
    });

    advisory("an effect that throws does not stall the rest of the queue", () => {
        const b = reactor();
        const a = b.atom(0, "a");
        let throwingRuns = 0;
        let steadyRuns = 0;

        b.effect(() => {
            throwingRuns++;

            if (a.get() === 1) throw new Error("effect failed");
        });

        b.effect(() => {
            steadyRuns++;
            a.get();
        });

        assert.throws(() => a.set(1));

        a.set(2);

        assert.equal(throwingRuns, 3);
        assert.equal(steadyRuns, 3);
    });

    advisory("an effect that throws stays subscribed", () => {
        const b = reactor();
        const a = b.atom(0, "a");
        let runs = 0;

        b.effect(() => {
            runs++;
            a.get();

            if (runs === 2) throw new Error("first rerun failed");
        });

        assert.throws(() => a.set(1));
        a.set(2);

        assert.equal(runs, 3);
    });
});

describe("capacity and growth", () => {
    it("handles more than 1,000 cells", () => {
        const b = reactor();
        const atoms = Array.from({ length: 1200 }, (_unused, index) =>
            b.atom(index, `bulk${index}`)
        );

        assert.equal(atoms[1199].get(), 1199);
        assert.equal(new Set(atoms.map((atom) => atom.id)).size, 1200);

        atoms[600].set(-600);

        assert.equal(atoms[600].get(), -600);
        assert.equal(atoms[601].get(), 601);
    });

    it("handles 300 effects", () => {
        const b = reactor();
        const a = b.atom(0, "a");
        const counts = new Array(300).fill(0);

        counts.forEach((_unused, index) => {
            b.effect(() => {
                counts[index]++;
                a.get();
            });
        });

        a.set(1);

        assert.deepEqual(counts, new Array(300).fill(2));
    });

    it("handles 300 subscribers on a single cell", () => {
        const b = reactor();
        const a = b.atom(0, "a");
        let total = 0;

        for (let index = 0; index < 300; index++) {
            a.subscribe(() => void total++);
        }

        a.set(1);
        a.set(2);

        assert.equal(total, 600);
    });

    it("handles a batch that queues 300 effects", () => {
        const b = reactor();
        const atoms = Array.from({ length: 300 }, (_unused, index) =>
            b.atom(0, `queued${index}`)
        );
        let runs = 0;

        for (const atom of atoms) {
            b.effect(() => {
                runs++;
                atom.get();
            });
        }

        runs = 0;

        b.batch(() => {
            for (const atom of atoms) atom.set(1);
        });

        assert.equal(runs, 300);
    });

    advisory("handles 300 computeds", () => {
        const b = reactor();
        const inputs = Array.from({ length: 300 }, (_unused, index) =>
            b.atom(index, `input${index}`)
        );

        const outputs = inputs.map((input, index) =>
            b.computed(() => input.get() * 2, `output${index}`)
        );

        assert.equal(outputs[299].get(), 598);

        inputs[150].set(1);
        assert.equal(outputs[150].get(), 2);
    });

    it("reuses freed effect slots", () => {
        const b = reactor();
        const a = b.atom(0, "a");

        for (let index = 0; index < 400; index++) {
            const dispose = b.effect(() => void a.get());
            dispose();
        }

        let runs = 0;

        b.effect(() => {
            runs++;
            a.get();
        });

        a.set(1);

        assert.equal(runs, 2);
        assert.ok(raw(b).execCount <= 401);
    });

    it("keeps subscriber counts exact after churn", () => {
        const b = reactor();
        const a = b.atom(0, "a");
        const disposers = Array.from({ length: 50 }, () =>
            b.effect(() => void a.get())
        );

        assert.equal(raw(b).subscriberCount[a.id], 50);

        disposers.forEach((dispose) => dispose());

        assert.equal(raw(b).subscriberCount[a.id], 0);
        assert.equal(raw(b).execDeps[1], 0);
    });
});

describe("value handles", () => {
    it("returns the newest payload, not a stale one", () => {
        const b = reactor();
        const a = b.atom<unknown>(undefined, "a");

        for (let index = 0; index < 5000; index++) {
            a.set({ index });
        }

        assert.deepEqual(a.get(), { index: 4999 });
    });

    advisory("does not accumulate a handle per write", () => {
        const b = reactor();
        const a = b.atom(0, "a");

        const before = raw(b).objectValues.length;

        for (let index = 0; index < 10000; index++) {
            a.set(index);
        }

        const growth = raw(b).objectValues.length - before;

        assert.ok(
            growth < 100,
            `objectValues grew by ${growth} handles for 10000 writes`
        );
    });
});

main(() => undefined);
