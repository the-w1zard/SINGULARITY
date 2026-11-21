import { getActiveSub, setActiveSub, getBatchDepth, startBatch, endBatch, isSignal, isComputed, isEffect, isEffectScope, signal, computed, effect, effectScope, trigger, createStore } from "./class";


if (FIGHT === "yoda") {
    const yoda = new YODA();

    // ================================================================
    // BUS: DIRECT DISPATCH
    // ================================================================

    // 1. Hot send: direct handler invocation
    let sendValue = 0;
    yoda.sub.hot((p: number) => {
        sendValue = p;
    }, "send.hot" as S);

    let sendWrite = 0;
    bench("YODA.send.hot", () => {
        sendWrite ^= 1;
        yoda.send.hot(sendWrite, "send.hot" as S);
    });

    // 2. Cold send: channel lookup + message construction
    let coldSendValue = 0;
    yoda.sub((p: number) => {
        coldSendValue = p;
    }, "send.cold" as S);

    let coldSendWrite = 0;
    bench("YODA.send.cold", () => {
        coldSendWrite ^= 1;
        yoda.send(coldSendWrite, "send.cold" as S);
    });

    // 3. Hot subscription registration and disposal
    const subHandler = (_p: number) => {};
    bench("YODA.sub.hot", () => {
        const dispose = yoda.sub.hot(subHandler, "sub.hot" as S);
        dispose();
    });

    // 4. Cold subscription registration and disposal
    bench("YODA.sub.cold", () => {
        const dispose = yoda.sub(subHandler, "sub.cold" as S);
        dispose();
    });

    // 5. Hot stream listener registration and disposal
    const streamHandler = (_msg: SIG_BUS_Message) => {};
    bench("YODA.stream.hot", () => {
        const dispose = (yoda.stream as A).hot(streamHandler, "stream.hot" as S);
        dispose();
    });

    // 6. Stream dispatch: one listener
    const streamOne = "stream.one" as S;
    const disposeStreamOne = (yoda.stream as A).hot(streamHandler, streamOne);

    let streamWrite = 0;
    bench("YODA.stream.dispatch.1", () => {
        streamWrite ^= 1;
        yoda.sendStream(streamWrite, streamOne);
    });




    // 12. Duplex construction
    bench("YODA.duplex.create", () => {
        yoda.duplex("duplex.create" as S);
    });

    // 13. Duplex hot send
    const duplex = (yoda.duplex as A).hot("duplex.hot" as S);

    let duplexWrite = 0;
    bench("YODA.duplex.hot.send", () => {
        duplexWrite ^= 1;
        duplex.send(duplexWrite);
    });

    // 14. Duplex hot subscription
    bench("YODA.duplex.hot.sub", () => {
        const dispose = duplex.sub(subHandler);
        dispose();
    });

    // ================================================================
    // SIGNAL
    // ================================================================

    // 15. Signal read
    yoda.signal.hot("signal.read" as S, 1);

    bench("YODA.signal.hot.read", () => {
        yoda.signal.hot("signal.read" as S);
    });

    // 16. Signal write
    let signalWrite = 0;
    bench("YODA.signal.hot.write", () => {
        signalWrite ^= 1;
        yoda.signal.hot("signal.write" as S, signalWrite);
    });

    // 17. Signal read + write
    let signalRW = 0;
    bench("YODA.signal.hot.read.write", () => {
        yoda.signal.hot("signal.rw" as S);
        signalRW ^= 1;
        yoda.signal.hot("signal.rw" as S, signalRW);
    });

    // 18. Atom getter
    const atom = yoda.atom<number>("atom.read" as S);
    atom.value = 1;

    bench("YODA.atom.read", () => {
        const value = atom.value;
        void value;
    });

    // 19. Atom setter
    let atomWrite = 0;
    bench("YODA.atom.write", () => {
        atomWrite ^= 1;
        atom.value = atomWrite;
    });

    // ================================================================
    // STORE: READ PATHS
    // ================================================================

    // 20. Hot get
    yoda.set.hot(123, "get.hot" as S);

    bench("YODA.get.hot", () => {
        yoda.get.hot("get.hot" as S);
    });

    // 21. Cold get
    bench("YODA.get.cold", () => {
        yoda.get("get.hot" as S);
    });

    // 22. Has
    bench("YODA.has.hot", () => {
        yoda.has.hot("get.hot" as S);
    });

    // 23. Peek
    bench("YODA.peek.hot", () => {
        yoda.peek.hot("get.hot" as S);
    });

    // 24. Create: existing key
    bench("YODA.create.existing", () => {
        yoda.create("get.hot" as S, 999);
    });

    // 25. Get many: 16 keys
    const getMany16 = Array.from(
        { length: 16 },
        (_, i) => `getMany.16.${i}` as S
    );

    for (let i = 0; i < 16; i++) {
        yoda.set.hot(i, getMany16[i]);
    }

    bench("YODA.getMany.16", () => {
        yoda.getMany(getMany16);
    });


    // 29. Entries
    bench("YODA.entries", () => {
        yoda.entries();
    });

    // ================================================================
    // STORE: WRITE PATHS
    // ================================================================

    // 30. Hot set
    let setWrite = 0;
    bench("YODA.set.hot", () => {
        setWrite ^= 1;
        yoda.set.hot(setWrite, "set.hot" as S);
    });

    // 31. Cold set
    let coldSetWrite = 0;
    bench("YODA.set.cold", () => {
        coldSetWrite ^= 1;
        yoda.set(coldSetWrite, "set.cold" as S);
    });

    // 32. Swap
    let swapWrite = 0;
    bench("YODA.swap", () => {
        swapWrite ^= 1;
        yoda.swap(swapWrite, "swap" as S);
    });

    // 33. Edit
    yoda.set.hot(0, "edit" as S);

    bench("YODA.edit", () => {
        yoda.edit<number>("edit" as S, old => old + 1);
    });

    // 34. Delete: absent key
    bench("YODA.delete.miss", () => {
        yoda.delete.hot("delete.absent" as S);
    });

    // 35. Delete: existing key + recreation
    let deleteWrite = 0;
    bench("YODA.delete.hit-cycle", () => {
        yoda.set.hot(deleteWrite, "delete.cycle" as S);
        yoda.delete.hot("delete.cycle" as S);
        deleteWrite++;
    });

    // 36. Set many: 2 keys
    const setMany2: [S, A][] = [
        ["setMany.2.a" as S, 0],
        ["setMany.2.b" as S, 0],
    ];

    let setMany2Write = 0;
    bench("YODA.setMany.2", () => {
        setMany2Write ^= 1;
        setMany2[0][1] = setMany2Write;
        setMany2[1][1] = setMany2Write;
        yoda.setMany(setMany2);
    });

    // 37. Set many: 16 keys
    const setMany16: [S, A][] = Array.from(
        { length: 16 },
        (_, i) => [`setMany.16.${i}` as S, 0]
    );

    let setMany16Write = 0;
    bench("YODA.setMany.16", () => {
        setMany16Write ^= 1;
        for (const entry of setMany16) {
            entry[1] = setMany16Write;
        }
        yoda.setMany(setMany16);
    });



    // 39. Delete many: 16 keys
    const deleteMany16 = Array.from(
        { length: 16 },
        (_, i) => `deleteMany.16.${i}` as S
    );

    let deleteMany16Write = 0;
    bench("YODA.deleteMany.16", () => {
        for (const key of deleteMany16) {
            yoda.set.hot(deleteMany16Write, key);
        }
        yoda.deleteMany(deleteMany16);
        deleteMany16Write++;
    });

    // ================================================================
    // RAW ACCESS
    // ================================================================

    // 41. Raw write
    let rawWrite = 0;
    bench("YODA.write", () => {
        rawWrite ^= 1;
        yoda.write("raw.write" as S, rawWrite);
    });

    // 42. Raw read
    yoda.write("raw.read" as S, 123);

    bench("YODA.read", () => {
        yoda.read("raw.read" as S);
    });

    // 43. Raw write + read
    let rawRW = 0;
    bench("YODA.write.read", () => {
        rawRW ^= 1;
        yoda.write("raw.rw" as S, rawRW);
        yoda.read("raw.rw" as S);
    });

    // ================================================================
    // SUBSCRIPTIONS AND NOTIFICATIONS
    // ================================================================

    // 44. Store write with one key-specific stream subscriber
    const storeSubKey = "store.sub.one" as S;
    const disposeStoreSub = (yoda.stream as A).hot(
        (_event: M2Event<S, A>) => {},
        storeSubKey
    );

    let storeSubWrite = 0;
    bench("YODA.store.notify.1", () => {
        storeSubWrite ^= 1;
        yoda.set.hot(storeSubWrite, storeSubKey);
    });

    // 45. Store write with 16 key-specific subscribers
    const storeSub16 = "store.sub.16" as S;
    const disposeStoreSub16: (() => V)[] = [];

    for (let i = 0; i < 16; i++) {
        disposeStoreSub16.push(
            (yoda.stream as A).hot(
                (_event: M2Event<S, A>) => {},
                storeSub16
            )
        );
    }

    let storeSub16Write = 0;
    bench("YODA.store.notify.16", () => {
        storeSub16Write ^= 1;
        yoda.set.hot(storeSub16Write, storeSub16);
    });

    // 46. Global subscription registration and disposal
    bench("YODA.subAll.register.dispose", () => {
        const dispose = yoda.subAll(() => {});
        dispose();
    });

    // 47. Global subscription notification
    const disposeGlobal = yoda.subAll(() => {});

    let globalWrite = 0;
    bench("YODA.subAll.notify", () => {
        globalWrite ^= 1;
        yoda.set.hot(globalWrite, "global.notify" as S);
    });

    // 48. Multi-key subscription registration and disposal
    const subManyKeys = [
        "subMany.a" as S,
        "subMany.b" as S,
        "subMany.c" as S,
        "subMany.d" as S,
    ];

    bench("YODA.subMany.register.dispose", () => {
        const dispose = yoda.subMany(subManyKeys, () => {});
        dispose();
    });

    // 49. Multi-key notification
    const disposeSubMany = yoda.subMany(
        subManyKeys,
        (_event: M2Event<S, A>) => {}
    );

    let subManyWrite = 0;
    bench("YODA.subMany.notify", () => {
        subManyWrite ^= 1;
        yoda.set.hot(subManyWrite, subManyKeys[0]);
    });

    // 50. Effect-like bus subscription fan-out across 16 keys
    const subMany16 = Array.from(
        { length: 16 },
        (_, i) => `subMany16.${i}` as S
    );

    const disposeSubMany16 = yoda.subMany(
        subMany16,
        (_event: M2Event<S, A>) => {}
    );

    let subMany16Write = 0;
    bench("YODA.subMany.notify.16", () => {
        subMany16Write ^= 1;
        yoda.set.hot(subMany16Write, subMany16[0]);
    });

    // ================================================================
    // BATCHING
    // ================================================================

    // 51. Delete many: one batch of 16
    // (Included above as a separate bulk operation.)

    // 52. Set many: 2 writes in one batch
    // (Included above as a separate bulk operation.)

    // 53. Set many: 16 writes in one batch
    // (Included above as a separate bulk operation.)

    // ================================================================
    // KEEP SUBSCRIPTIONS ALIVE DURING MEASUREMENTS
    // ================================================================

    void sendValue;
    void coldSendValue;
    void streamWrite;
    void duplexWrite;
    void signalWrite;
    void signalRW;
    void atomWrite;
    void setWrite;
    void coldSetWrite;
    void swapWrite;
    void deleteWrite;
    void setMany2Write;
    void setMany16Write;
    void deleteMany16Write;
    void rawWrite;
    void rawRW;
    void storeSubWrite;
    void storeSub16Write;
    void globalWrite;
    void subManyWrite;
    void subMany16Write;

    void disposeStreamOne;
    void disposeStoreSub;
    void disposeStoreSub16;
    void disposeGlobal;
    void disposeSubMany;
    void disposeSubMany16;
}
else if (FIGHT === "vader") {
	const sauron = new vader_signalsbus(r, m, s);

	// ================================================================
	// SIGNAL: READ PATHS
	// ================================================================
	const readSignal = vader_signal<number>(1);
	bench("vader.signal.read", () => {readSignal();	});
	bench("vader.signal.peek", () => {readSignal.peek(); });
	// ================================================================
	// SIGNAL: WRITE PATHS
	// ================================================================
	const writeSignal = vader_signal<number>(0);
	let writeValue = 0;
	bench("vader.signal.write", () => {writeValue ^= 1;writeSignal(writeValue); });
	const unchangedSignal = vader_signal<number>(1);
	bench("vader.signal.write.unchanged", () => {unchangedSignal(1); });
	const readWriteSignal = vader_signal<number>(0);
	let readWriteValue = 0;
	bench("vader.signal.read.write", () => {readWriteSignal(); readWriteValue ^= 1; readWriteSignal(readWriteValue); 	});
	// ================================================================
	// SIGNAL: NOTIFICATION
	// ================================================================
	const notifySignal = vader_signal<number>(0);
	let notifyRuns = 0;
	const disposeNotify = vader_effect(() => {		notifySignal();		notifyRuns++;	});
	let notifyValue = 0;
	bench("vader.signal.notify.1", () => {		notifyValue ^= 1;		notifySignal(notifyValue);	});
	const notify16Signal = vader_signal<number>(0);
	let notify16Runs = 0;
	const disposeNotify16: (() => V)[] = [];
	for (let i = 0; i < 16; i++) {
		disposeNotify16.push(
			effect(() => {
				notify16Signal();
				notify16Runs++;
			})
		);
	}
	let notify16Value = 0;
	bench("vader.signal.notify.16", () => {		notify16Value ^= 1;		notify16Signal(notify16Value);	});
	// ================================================================
	// COMPUTED: READ PATHS
	// ================================================================
	const computedSource = vader_signal<number>(1);
	const computedValue = vader_computed(() => computedSource() * 2);
	computedValue();
	bench("vader.computed.read.cached", () => {computedValue();	});
	const computedColdSource = vader_signal<number>(1);
	const computedColdValue = vader_computed(() => computedColdSource() * 2);
	bench("vader.computed.read.first", () => {computedColdValue();	});
	// ================================================================
	// COMPUTED: INVALIDATION AND RECOMPUTATION
	// ================================================================
	const invalidationSource = vader_signal<number>(0);
	const invalidationComputed = vader_computed(() => invalidationSource() + 1);
	invalidationComputed();
	let invalidationValue = 0;
	bench("vader.computed.invalidate", () => {		invalidationValue++;		invalidationSource(invalidationValue);	});
	const recomputeSource = vader_signal<number>(0);
	let recomputeRuns = 0;
	const recomputeComputed = vader_computed(() => {		recomputeRuns++;		return recomputeSource() * 2;	});
	recomputeComputed();
	let recomputeValue = 0;
	bench("vader.computed.recompute", () => {		recomputeValue++;		recomputeSource(recomputeValue);		recomputeComputed();	});
	// ================================================================
	// COMPUTED: CHAIN DEPTH
	// ================================================================
	const chain8Source = vader_signal<number>(0);
	let chain8 = vader_computed(() => chain8Source() + 1);
	for (let i = 1; i < 8; i++) {
		const previous = chain8;
		chain8 = vader_computed(() => previous() + 1);
	}
	chain8();
	let chain8Value = 0;
	bench("vader.computed.chain.8", () => {		chain8Value++;		chain8Source(chain8Value);		chain8();	});
	const chain32Source = vader_signal<number>(0);
	let chain32 = vader_computed(() => chain32Source() + 1);
	for (let i = 1; i < 32; i++) {		const previous = chain32;		chain32 = vader_computed(() => previous() + 1);	}
	chain32();
	let chain32Value = 0;
	bench("vader.computed.chain.32", () => {		chain32Value++;		chain32Source(chain32Value);		chain32();	});
	// ================================================================
	// EFFECT: CREATION AND DISPOSAL
	// ================================================================
	const effectCreateSource = vader_signal<number>(0);
	bench("vader.effect.create.dispose", () => {		const dispose = effect(() => {			effectCreateSource();		});		dispose();	});
	// ================================================================
	// EFFECT: RERUN
	// ================================================================
	const effectRerunSource = vader_signal<number>(0);
	let effectReruns = 0;
	const disposeEffectRerun = vader_effect(() => {		effectRerunSource();		effectReruns++;	});
	let effectRerunValue = 0;
	bench("vader.effect.rerun", () => {		effectRerunValue++;		effectRerunSource(effectRerunValue);	});
	// ================================================================
	// EFFECT: CLEANUP
	// ================================================================
	const cleanupSource = vader_signal<number>(0);
	let cleanupRuns = 0;
	const disposeCleanup = vader_effect(() => {		cleanupSource();		return () => {			cleanupRuns++;		};	});
	let cleanupValue = 0;
	bench("vader.effect.cleanup.rerun", () => {		cleanupValue++;		cleanupSource(cleanupValue);	});
	// ================================================================
	// DEPENDENCIES: FAN-IN
	// ================================================================
	const fanIn16 = Array.from({ length: 16 }, () => vader_signal<number>(0));
	let fanIn16Runs = 0;
	const disposeFanIn16 = vader_effect(() => {
		let total = 0;
		for (let i = 0; i < fanIn16.length; i++) {			total += fanIn16[i]();		}
		fanIn16Runs++;
		return total;
	});
	let fanIn16Value = 0;
	bench("vader.dependencies.fan-in.16", () => {		fanIn16Value ^= 1;		fanIn16[0](fanIn16Value);	});
	// ================================================================
	// DEPENDENCIES: CONDITIONAL TRACKING
	// ================================================================
	const condition = vader_signal<boolean>(true);
	const conditionalA = vader_signal<number>(0);
	const conditionalB = vader_signal<number>(0);
	let conditionalRuns = 0;
	const disposeConditional = vader_effect(() => {		conditionalRuns++;		return condition() ? conditionalA() : conditionalB();	});
	let conditionalValue = 0;
	bench("vader.dependencies.conditional.switch", () => {		conditionalValue ^= 1;		condition(!!conditionalValue);	});
	// ================================================================
	// BATCHING
	// ================================================================
	const batchSource = vader_signal<number>(0);
	let batchRuns = 0;
	const disposeBatch = vader_effect(() => {		batchSource();		batchRuns++;	});
	let batchValue = 0;
	bench("vader.batch.2", () => {
		batchValue++;
		vader_batch(() => {
			batchSource(batchValue);
			batchSource(batchValue + 1);
		});
	});
	const batch16Sources = Array.from({ length: 16 }, () => vader_signal<number>(0));
	let batch16Runs = 0;
	const disposeBatch16 = vader_effect(() => {
		let total = 0;
		for (let i = 0; i < batch16Sources.length; i++) {			total += batch16Sources[i]();
		}

		batch16Runs++;
		return total;
	});
	let batch16Value = 0;
	bench("vader.batch.16", () => {
		batch16Value++;
		vader_batch(() => {
			for (let i = 0; i < batch16Sources.length; i++) {
				batch16Sources[i](batch16Value);
			}
		});
	});
	// ================================================================
	// GRAPH: DIAMOND
	// ================================================================
	const diamondSource = vader_signal<number>(0);
	const diamondLeft = vader_computed(() => diamondSource() + 1);
	const diamondRight = vader_computed(() => diamondSource() + 2);
	const diamondJoin = vader_computed(() => diamondLeft() + diamondRight());
	let diamondRuns = 0;
	const disposeDiamond = vader_effect(() => {		diamondRuns++;		diamondJoin();	});
	let diamondValue = 0;
	bench("vader.graph.diamond", () => {		diamondValue++;		diamondSource(diamondValue);	});
	// ================================================================
	// GRAPH: BRANCHING
	// ================================================================
	const branchSource = vader_signal<number>(0);
	const branches = Array.from(
		{ length: 16 },
		(_, i) => vader_computed(() => branchSource() + i)
	);
	const branchJoin = vader_computed(() => {
		let total = 0;
		for (let i = 0; i < branches.length; i++) {			total += branches[i]();		}
		return total;
	});
	let branchRuns = 0;
	const disposeBranch = vader_effect(() => { branchRuns++;		branchJoin();	});
	let branchValue = 0;
	bench("vader.graph.branch.16", () => { branchValue++;		branchSource(branchValue);	});
	// ================================================================
	// LIFECYCLE: DISPOSAL
	// ================================================================
	const lifecycleSource = vader_signal<number>(0);
	let lifecycleRuns = 0;
	const lifecycleDispose = vader_effect(() => { lifecycleSource(); lifecycleRuns++; });
	bench("vader.effect.dispose", () => { const dispose = vader_effect(() => { lifecycleSource(); }); dispose();	});
	// ================================================================
	// BUS: DIRECT DISPATCH
	// ================================================================
	let sendValue = 0;
	sauron.sub(k, (p: U) => { sendValue = p as number;	});
	let sendWrite = 0;
	bench("vader.bus.send", () => { sendWrite ^= 1; sauron.send(sendWrite, k);	});
	let hotSendValue = 0;
	sauron.sub("send.hot" as S, (p: U) => { hotSendValue = p as number;	});
	let hotSendWrite = 0;
	bench("vader.bus.hotSend", () => { hotSendWrite ^= 1; sauron.hotSend(hotSendWrite, "send.hot" as S); });
	// ================================================================
	// BUS: STREAM DISPATCH
	// ================================================================
	const streamHandler = (_msg: U) => {};
	const streamOne = "stream.one" as S;
	const disposeStreamOne = sauron.stream(streamOne, streamHandler);
	let streamWrite = 0;
	const streamMessage = { r, m, s, k: streamOne, p: 0, };
	bench("vader.bus.stream.publish.1", () => {
		streamWrite ^= 1;
		streamMessage.p = streamWrite;
		sauron.hotPublish(streamMessage);
	});
	const stream16 = "stream.16" as S;
	const disposeStream16: (() => V)[] = [];
	for (let i = 0; i < 16; i++) { disposeStream16.push(sauron.stream(stream16, streamHandler)); }
	let stream16Write = 0;
	const stream16Message = {r, m, s, k: stream16, p: 0, };
	bench("vader.bus.stream.publish.16", () => {
		stream16Write ^= 1;
		stream16Message.p = stream16Write;
		sauron.hotPublish(stream16Message);
	});
	// ================================================================
	// BUS: STREAM REGISTRATION AND DISPOSAL
	// ================================================================
	bench("vader.bus.stream.register.dispose", () => { const dispose = sauron.stream("stream.lifecycle" as S, streamHandler); dispose(); });
	// ================================================================
	// BUS: SUBSCRIPTION REGISTRATION AND DISPOSAL
	// ================================================================
	const subHandler = (_p: U) => {};
	bench("vader.bus.sub.register.dispose", () => { const dispose = sauron.sub("sub.lifecycle" as S, subHandler); dispose(); });
	// ================================================================
	// BUS: DUPLEX
	// ================================================================
	const duplex = sauron.duplex("duplex" as S);
	let duplexValue = 0;
	bench("vader.bus.duplex.send", () => { duplexValue ^= 1; duplex.send(duplexValue);	});
	bench("vader.bus.duplex.sub.register.dispose", () => { const dispose = duplex.sub(subHandler); dispose(); });
	// ================================================================
	// KEEP ALL LIVE GRAPH NODES AND SUBSCRIPTIONS ALIVE
	// ================================================================
	void notifyRuns;
	void notify16Runs;
	void recomputeRuns;
	void effectReruns;
	void cleanupRuns;
	void fanIn16Runs;
	void conditionalRuns;
	void batchRuns;
	void batch16Runs;
	void diamondRuns;
	void branchRuns;
	void lifecycleRuns;

	void sendValue;
	void hotSendValue;
	void streamWrite;
	void stream16Write;
	void duplexValue;

	void disposeNotify;
	void disposeNotify16;
	void disposeEffectRerun;
	void disposeCleanup;
	void disposeFanIn16;
	void disposeConditional;
	void disposeBatch;
	void disposeBatch16;
	void disposeDiamond;
	void disposeBranch;
	void lifecycleDispose;

	void disposeStreamOne;
	void disposeStream16;
}

else if (FIGHT === "the_force2") {
	const sauron1 = new f_bus();
	const sauron = sauron1.bus;
	const store = sauron1.store;

	// ================================================================
	// SIGNAL: READ
	// ================================================================

	const readSignal = signal(1);

	bench("the_force.signal.read", () => {
		readSignal();
	});

	bench("the_force.signal.peek", () => {
		store.peek("readSignal");
	});

	// ================================================================
	// SIGNAL: WRITE
	// ================================================================

	const writeSignal = signal<number>(0);
	let writeValue = 0;

	bench("the_force.signal.write", () => {
		writeValue ^= 1;
		writeSignal(writeValue);
	});

	const unchangedSignal = signal<number>(1);

	bench("the_force.signal.write.unchanged", () => {
		unchangedSignal(1);
	});

	const readWriteSignal = signal<number>(0);
	let readWriteValue = 0;

	bench("the_force.signal.read.write", () => {
		readWriteSignal();
		readWriteValue ^= 1;
		readWriteSignal(readWriteValue);
	});

	// ================================================================
	// COMPUTED: CACHED READ
	// ================================================================

	const cachedSource = signal<number>(1);
	const cachedComputed = computed(() => cachedSource() * 2);

	cachedComputed();

	bench("the_force.computed.read.cached", () => {
		cachedComputed();
	});

	// ================================================================
	// COMPUTED: FIRST EVALUATION
	// ================================================================

	const firstSource = signal<number>(1);
	const firstComputed = computed(() => firstSource() * 2);

	bench("the_force.f_computed.read.first", () => {
		firstComputed();
	});

	// ================================================================
	// COMPUTED: INVALIDATION
	// ================================================================

	const invalidationSource = signal<number>(0);
	const invalidationComputed = computed(() => invalidationSource() + 1);

	invalidationComputed();

	let invalidationValue = 0;

	bench("the_force.f_computed.invalidate", () => {
		invalidationValue++;
		invalidationSource(invalidationValue);
	});

	// ================================================================
	// COMPUTED: RECOMPUTATION
	// ================================================================

	const recomputeSource = signal<number>(0);
	let recomputeRuns = 0;

	const recomputeComputed = computed(() => {
		recomputeRuns++;
		return recomputeSource() * 2;
	});

	recomputeComputed();

	let recomputeValue = 0;

	bench("the_force.f_computed.recompute", () => {
		recomputeValue++;
		recomputeSource(recomputeValue);
		recomputeComputed();
	});

	// ================================================================
	// COMPUTED: CHAIN DEPTH
	// ================================================================

	const chain8Source = signal<number>(0);
	let chain8 = computed(() => chain8Source() + 1);

	for (let i = 1; i < 8; i++) {
		const previous = chain8;
		chain8 = computed(() => previous() + 1);
	}

	chain8();

	let chain8Value = 0;

	bench("the_force.f_computed.chain.8", () => {
		chain8Value++;
		chain8Source(chain8Value);
		chain8();
	});

	const chain32Source = signal<number>(0);
	let chain32 = computed(() => chain32Source() + 1);

	for (let i = 1; i < 32; i++) {
		const previous = chain32;
		chain32 = computed(() => previous() + 1);
	}

	chain32();

	let chain32Value = 0;

	bench("the_force.f_computed.chain.32", () => {
		chain32Value++;
		chain32Source(chain32Value);
		chain32();
	});

	// ================================================================
	// EFFECT: CREATION
	// ================================================================

	const effectCreateSource = signal<number>(0);

	bench("the_force.effect.create", () => {
		const dispose = effect(() => {});
		dispose();
	});

	// ================================================================
	// EFFECT: DISPOSAL
	// ================================================================

	const effectDisposeSource = signal<number>(0);

	bench("the_force.effect.dispose", () => {
		const dispose = effect(() => {
			effectDisposeSource();
		});
		dispose();
	});

	// ================================================================
	// EFFECT: CREATION WITH DEPENDENCY
	// ================================================================

	const effectDependencySource = signal<number>(0);

	bench("the_force.effect.create.dependency", () => {
		const dispose = effect(() => {
			effectDependencySource();
		});
		dispose();
	});

	// ================================================================
	// EFFECT: CLEANUP
	// ================================================================

	const cleanupSource = signal<number>(0);
	let cleanupRuns = 0;

	const disposeCleanup = effect(() => {
		cleanupSource();

		return () => {
			cleanupRuns++;
		};
	});

	let cleanupValue = 0;

	bench("the_force.effect.cleanup", () => {
		cleanupValue++;
		f_batch(() => {
			cleanupSource(cleanupValue);
		});
	});

	// ================================================================
	// EFFECT: RERUN
	// ================================================================

	const rerunSource = signal<number>(0);
	let rerunRuns = 0;

	const disposeRerun = effect(() => {
		rerunSource();
		rerunRuns++;
	});

	let rerunValue = 0;

	bench("the_force.effect.rerun", () => {
		rerunValue++;
		f_batch(() => {
			rerunSource(rerunValue);
		});
	});

	// ================================================================
	// DEPENDENCIES: FAN-IN
	// ================================================================

	const fanIn16 = Array.from({ length: 16 }, () => signal<number>(0));
	let fanIn16Runs = 0;

	const disposeFanIn16 = effect(() => {
		let total = 0;
	
		for (let i = 0; i < fanIn16.length; i++) {
			total += fanIn16[i]();
		}
	
		fanIn16Runs++;
		void total;
	});

	let fanIn16Value = 0;

	bench("the_force.dependencies.fan-in.16", () => {
		fanIn16Value ^= 1;
		f_batch(() => fanIn16[0](fanIn16Value));
	});

	// ================================================================
	// DEPENDENCIES: FAN-OUT
	// ================================================================

	const fanOut16Source = signal<number>(0);
	let fanOut16Runs = 0;

	const disposeFanOut16: (() => void)[] = [];

	for (let i = 0; i < 16; i++) {
		disposeFanOut16.push(
			effect(() => {
				fanOut16Source();
				fanOut16Runs++;
			})
		);
	}

	let fanOut16Value = 0;

	bench("the_force.dependencies.fan-out.16", () => {
		fanOut16Value ^= 1;
		f_batch(() => fanOut16Source(fanOut16Value));
	});

	// ================================================================
	// DEPENDENCIES: CONDITIONAL TRACKING
	// ================================================================

	const condition = signal<boolean>(true);
	const conditionalA = signal<number>(0);
	const conditionalB = signal<number>(0);
	let conditionalRuns = 0;

	const disposeConditional = effect(() => {
		conditionalRuns++;

		if (condition()) {
			conditionalA();
		} else {
			conditionalB();
		}
	});

	let conditionalValue = 0;

	bench("the_force.dependencies.conditional.switch", () => {
		conditionalValue ^= 1;
		f_batch(() => condition(!!conditionalValue));
	});

	// ================================================================
	// BATCHING: ONE SIGNAL, TWO WRITES
	// ================================================================

	const batch2Source = signal<number>(0);
	let batch2Runs = 0;

	const disposeBatch2 = effect(() => {
		batch2Source();
		batch2Runs++;
	});

	let batch2Value = 0;

	bench("the_force.f_batch.2", () => {
		batch2Value++;
		f_batch(() => {
			batch2Source(batch2Value);
			batch2Source(batch2Value + 1);
		});
	});

	// ================================================================
	// BATCHING: 16 SIGNALS
	// ================================================================

	const batch16Sources = Array.from({ length: 16 }, () => signal<number>(0));
	let batch16Runs = 0;

	const disposeBatch16 = effect(() => {
		let total = 0;

		for (let i = 0; i < batch16Sources.length; i++) {
			total += batch16Sources[i]();
		}

		batch16Runs++;
		void total;
	});

	let batch16Value = 0;

	bench("the_force.f_batch.16", () => {
		batch16Value++;
		f_batch(() => {
			for (let i = 0; i < batch16Sources.length; i++) {
				batch16Sources[i](batch16Value);
			}
		});
	});

	// ================================================================
	// GRAPH: DIAMOND
	// ================================================================

	const diamondSource = signal<number>(0);
	const diamondLeft = computed(() => diamondSource() + 1);
	const diamondRight = computed(() => diamondSource() + 2);
	const diamondJoin = computed(() => diamondLeft() + diamondRight());

	let diamondRuns = 0;

	const disposeDiamond = effect(() => {
		diamondRuns++;
		diamondJoin();
	});

	let diamondValue = 0;

	bench("the_force.graph.diamond", () => {
		diamondValue++;
		f_batch(() => diamondSource(diamondValue));
	});

	// ================================================================
	// GRAPH: BRANCHING
	// ================================================================

	const branchSource = signal<number>(0);
	const branches = Array.from(
		{ length: 16 },
		(_, i) => computed(() => branchSource() + i)
	);

	const branchJoin = computed(() => {
		let total = 0;

		for (let i = 0; i < branches.length; i++) {
			total += branches[i]();
		}

		return total;
	});

	let branchRuns = 0;

	const disposeBranch = effect(() => {
		branchRuns++;
		branchJoin();
	});

	let branchValue = 0;

	bench("the_force.graph.branch.16", () => {
		branchValue++;
		f_batch(() => branchSource(branchValue));
	});

	// ================================================================
	// BUS: DIRECT SEND
	// ================================================================

	let sendValue = 0;

	sauron.sub( (p: U) => {
		sendValue = p as number;
	},k,);

	let sendWrite = 0;

	bench("the_force.bus.send", () => {
		sendWrite ^= 1;
		sauron.send(sendWrite, k);
	});

	let hotSendValue = 0;

	sauron.sub((p: U) => {
		hotSendValue = p as number;
	}, "send.hot" as S, );

	let hotSendWrite = 0;

	bench("the_force.bus.hotSend", () => {
		hotSendWrite ^= 1;
		sauron.hotSend(hotSendWrite, "send.hot" as S);
	});

	// ================================================================
	// BUS: STREAM PUBLISH
	// ================================================================

		const streamHandler = (_message: ChannelMessage) => {};

		const stream = sauron.stream();

		const streamOne = "stream.one" as S;
		const disposeStreamOne = stream(streamHandler, streamOne, s);

		let streamWrite = 0;

		const streamMessage = {
			r,
			m,
			s,
			k: streamOne,
			p: 0,
		};

		bench("the_force.bus.stream.publish.1", () => {
			streamWrite ^= 1;
			streamMessage.p = streamWrite;
			sauron.hotPublish(streamMessage);
		});

		const stream16 = "stream.16" as S;
		const disposeStream16: (() => void)[] = [];

		for (let i = 0; i < 16; i++) {
			disposeStream16.push(stream(streamHandler, stream16, s));
		}

		let stream16Write = 0;

		const stream16Message = {
			r,
			m,
			s,
			k: stream16,
			p: 0,
		};

		bench("the_force.bus.stream.publish.16", () => {
			stream16Write ^= 1;
			stream16Message.p = stream16Write;
			sauron.hotPublish(stream16Message);
		});

	// ================================================================
	// BUS: REGISTRATION AND DISPOSAL
	// ================================================================
	
	// ================================================================
	// BUS: REGISTRATION AND DISPOSAL
	// ================================================================
	
	const subHandler = (_payload: U) => {};
	
	bench("the_force.bus.sub.register.dispose", () => {
		const dispose = sauron.sub(
			subHandler,
			"sub.lifecycle" as S,
			s,
			m,
			r
		);
		dispose();
	});
	
	bench("the_force.bus.stream.register.dispose", () => {
		const dispose = sauron.stream()(
			streamHandler,
			"stream.lifecycle" as S,
			s,
			m,
			r
		);
		dispose();
	});

	// ================================================================
	// BUS: DUPLEX
	// ================================================================

	const duplex = sauron.duplex("duplex" as S);

	let duplexValue = 0;

	bench("the_force.bus.duplex.send", () => {
		duplexValue ^= 1;
		duplex.send(duplexValue);
	});

	bench("the_force.bus.duplex.sub.register.dispose", () => {
		const dispose = duplex.sub(subHandler);
		dispose();
	});

	bench("the_force.bus.duplex.stream.register.dispose", () => {
		const dispose = duplex.stream(streamHandler);
		dispose();
	});

	// ================================================================
	// KEEP LIVE REFERENCES
	// ================================================================

	void recomputeRuns;
	void cleanupRuns;
	void rerunRuns;
	void fanIn16Runs;
	void fanOut16Runs;
	void conditionalRuns;
	void batch2Runs;
	void batch16Runs;
	void diamondRuns;
	void branchRuns;

	void sendValue;
	void hotSendValue;
	void streamWrite;
	void stream16Write;
	void duplexValue;

	void disposeCleanup;
	void disposeRerun;
	void disposeFanIn16;
	void disposeFanOut16;
	void disposeConditional;
	void disposeBatch2;
	void disposeBatch16;
	void disposeDiamond;
	void disposeBranch;

	void disposeStreamOne;
	void disposeStream16;
}
else if (FIGHT === "the_force3") {
    const sauron1 = new f_bus();
    const sauron = sauron1.bus;
    const store = sauron1.store;

    // SIGNAL: READ
    const readSignal = f_sig<number>(1);
    bench("the_force.signal.read", () => { readSignal(); });

    store.set("signal.peek", 1);
    bench("the_force.signal.peek", () => { store.peek("signal.peek"); });

    // SIGNAL: WRITE
    const writeSignal = f_sig<number>(0);
    let writeValue = 0;
    bench("the_force.signal.write", () => {
        writeValue ^= 1;
        writeSignal(writeValue);
    });

    const unchangedSignal = f_sig<number>(1);
    bench("the_force.signal.write.unchanged", () => { unchangedSignal(1); });

    const readWriteSignal = f_sig<number>(0);
    let readWriteValue = 0;
    bench("the_force.signal.read.write", () => {
        readWriteSignal();
        readWriteValue ^= 1;
        readWriteSignal(readWriteValue);
    });

    // COMPUTED: CACHED READ
    const cachedSource = f_sig<number>(1);
    const cachedComputed = f_computed(() => cachedSource() * 2);
    cachedComputed();
    bench("the_force.computed.read.cached", () => { cachedComputed(); });

    // COMPUTED: FIRST EVALUATION
    const firstSource = f_sig<number>(1);
    const firstComputed = f_computed(() => firstSource() * 2);
    bench("the_force.f_computed.read.first", () => { firstComputed(); });

    // COMPUTED: INVALIDATION
    const invalidationSource = f_sig<number>(0);
    const invalidationComputed = f_computed(() => invalidationSource() + 1);
    invalidationComputed();
    let invalidationValue = 0;
    bench("the_force.f_computed.invalidate", () => {
        invalidationValue++;
        invalidationSource(invalidationValue);
    });

    // COMPUTED: RECOMPUTATION
    const recomputeSource = f_sig<number>(0);
    let recomputeRuns = 0;
    const recomputeComputed = f_computed(() => {
        recomputeRuns++;
        return recomputeSource() * 2;
    });
    recomputeComputed();
    let recomputeValue = 0;
    bench("the_force.f_computed.recompute", () => {
        recomputeValue++;
        recomputeSource(recomputeValue);
        recomputeComputed();
    });

    // COMPUTED: CHAIN DEPTH
    const chain8Source = f_sig<number>(0);
    let chain8 = f_computed(() => chain8Source() + 1);
    for (let i = 1; i < 8; i++) {
        const previous = chain8;
        chain8 = f_computed(() => previous() + 1);
    }
    chain8();
    let chain8Value = 0;
    bench("the_force.f_computed.chain.8", () => {
        chain8Value++;
        chain8Source(chain8Value);
        chain8();
    });

    const chain32Source = f_sig<number>(0);
    let chain32 = f_computed(() => chain32Source() + 1);
    for (let i = 1; i < 32; i++) {
        const previous = chain32;
        chain32 = f_computed(() => previous() + 1);
    }
    chain32();
    let chain32Value = 0;
    bench("the_force.f_computed.chain.32", () => {
        chain32Value++;
        chain32Source(chain32Value);
        chain32();
    });

    // EFFECT: CREATION
    const effectCreateSource = f_sig<number>(0);
    bench("the_force.effect.create", () => {
        const dispose = effect(() => {
            effectCreateSource();
        });
        dispose();
    });

    // EFFECT: DISPOSAL
    const effectDisposeSource = f_sig<number>(0);
    bench("the_force.effect.dispose", () => {
        const dispose = effect(() => {
            effectDisposeSource();
        });
        dispose();
    });

    // EFFECT: CREATION WITH DEPENDENCY
    const effectDependencySource = f_sig<number>(0);
    bench("the_force.effect.create.dependency", () => {
        const dispose = effect(() => {
            effectDependencySource();
        });
        dispose();
    });

    // EFFECT: CLEANUP
    const cleanupSource = f_sig<number>(0);
    let cleanupRuns = 0;
    const disposeCleanup = effect(() => {
        cleanupSource();
        return () => { cleanupRuns++; };
    });
    let cleanupValue = 0;
    bench("the_force.effect.cleanup", () => {
        cleanupValue++;
        f_batch(() => { cleanupSource(cleanupValue); });
    });

    // EFFECT: RERUN
    const rerunSource = f_sig<number>(0);
    let rerunRuns = 0;
    const disposeRerun = effect(() => {
        rerunSource();
        rerunRuns++;
    });
    let rerunValue = 0;
    bench("the_force.effect.rerun", () => {
        rerunValue++;
        f_batch(() => { rerunSource(rerunValue); });
    });

    // DEPENDENCIES: FAN-IN
    const fanIn16 = Array.from({ length: 16 }, () => f_sig<number>(0));
    let fanIn16Runs = 0;
    const disposeFanIn16 = effect(() => {
        let total = 0;
        for (let i = 0; i < fanIn16.length; i++) {
            total += fanIn16[i]();
        }
        fanIn16Runs++;
        void total;
    });
    let fanIn16Value = 0;
    bench("the_force.dependencies.fan-in.16", () => {
        fanIn16Value ^= 1;
        f_batch(() => fanIn16[0](fanIn16Value));
    });

    // DEPENDENCIES: FAN-OUT
    const fanOut16Source = f_sig<number>(0);
    let fanOut16Runs = 0;
    const disposeFanOut16: (() => V)[] = [];
    for (let i = 0; i < 16; i++) {
        disposeFanOut16.push(effect(() => {
            fanOut16Source();
            fanOut16Runs++;
        }));
    }
    let fanOut16Value = 0;
    bench("the_force.dependencies.fan-out.16", () => {
        fanOut16Value ^= 1;
        f_batch(() => fanOut16Source(fanOut16Value));
    });

    // DEPENDENCIES: CONDITIONAL TRACKING
    const condition = f_sig<boolean>(true);
    const conditionalA = f_sig<number>(0);
    const conditionalB = f_sig<number>(0);
    let conditionalRuns = 0;
    const disposeConditional = effect(() => {
        conditionalRuns++;
        if (condition()) {
            conditionalA();
        } else {
            conditionalB();
        }
    });
    let conditionalValue = 0;
    bench("the_force.dependencies.conditional.switch", () => {
        conditionalValue ^= 1;
        f_batch(() => condition(!!conditionalValue));
    });

    // BATCHING: ONE SIGNAL, TWO WRITES
    const batch2Source = f_sig<number>(0);
    let batch2Runs = 0;
    const disposeBatch2 = effect(() => {
        batch2Source();
        batch2Runs++;
    });
    let batch2Value = 0;
    bench("the_force.f_batch.2", () => {
        batch2Value++;
        f_batch(() => {
            batch2Source(batch2Value);
            batch2Source(batch2Value + 1);
        });
    });

    // BATCHING: 16 SIGNALS
    const batch16Sources = Array.from({ length: 16 }, () => f_sig<number>(0));
    let batch16Runs = 0;
    const disposeBatch16 = effect(() => {
        let total = 0;
        for (let i = 0; i < batch16Sources.length; i++) {
            total += batch16Sources[i]();
        }
        batch16Runs++;
        void total;
    });
    let batch16Value = 0;
    bench("the_force.f_batch.16", () => {
        batch16Value++;
        f_batch(() => {
            for (let i = 0; i < batch16Sources.length; i++) {
                batch16Sources[i](batch16Value);
            }
        });
    });

    // GRAPH: DIAMOND
    const diamondSource = f_sig<number>(0);
    const diamondLeft = f_computed(() => diamondSource() + 1);
    const diamondRight = f_computed(() => diamondSource() + 2);
    const diamondJoin = f_computed(() => diamondLeft() + diamondRight());
    let diamondRuns = 0;
    const disposeDiamond = effect(() => {
        diamondRuns++;
        diamondJoin();
    });
    let diamondValue = 0;
    bench("the_force.graph.diamond", () => {
        diamondValue++;
        f_batch(() => diamondSource(diamondValue));
    });

    // GRAPH: BRANCHING
    const branchSource = f_sig<number>(0);
    const branches = Array.from(
        { length: 16 },
        (_, i) => f_computed(() => branchSource() + i)
    );
    const branchJoin = f_computed(() => {
        let total = 0;
        for (let i = 0; i < branches.length; i++) {
            total += branches[i]();
        }
        return total;
    });
    let branchRuns = 0;
    const disposeBranch = effect(() => {
        branchRuns++;
        branchJoin();
    });
    let branchValue = 0;
    bench("the_force.graph.branch.16", () => {
        branchValue++;
        f_batch(() => branchSource(branchValue));
    });

    // BUS: DIRECT SEND
    let sendValue = 0;
    sauron.sub((p: U) => { sendValue = p as number; }, k, s, m, r);
    let sendWrite = 0;
    bench("the_force.bus.send", () => {
        sendWrite ^= 1;
        sauron.send(sendWrite, k);
    });

    let hotSendValue = 0;
    sauron.sub((p: U) => { hotSendValue = p as number; }, "send.hot" as S, s, m, r);
    let hotSendWrite = 0;
    bench("the_force.bus.hotSend", () => {
        hotSendWrite ^= 1;
        sauron.hotSend(hotSendWrite, "send.hot" as S);
    });

	// BUS: STREAM PUBLISH
	const stream = sauron.stream.bind(sauron)();
	const streamHandler = (_message: ChannelMessage) => {};
	const streamOne = "stream.one" as S;
	const disposeStreamOne = stream(streamHandler, streamOne, s, m, r);
	
	let streamWrite = 0;
	const streamMessage = { r, m, s, k: streamOne, p: 0 };
	bench("the_force.bus.stream.publish.1", () => {
		streamWrite ^= 1;
		streamMessage.p = streamWrite;
		sauron.hotPublish(streamMessage);
	});
	
	const stream16 = "stream.16" as S;
	const disposeStream16: (() => V)[] = [];
	for (let i = 0; i < 16; i++) {
		disposeStream16.push(stream(streamHandler, stream16, s, m, r));
	}
	let stream16Write = 0;
	const stream16Message = { r, m, s, k: stream16, p: 0 };
	bench("the_force.bus.stream.publish.16", () => {
		stream16Write ^= 1;
		stream16Message.p = stream16Write;
		sauron.hotPublish(stream16Message);
	});
	
	// BUS: REGISTRATION AND DISPOSAL
	const subHandler = (_payload: U) => {};
	bench("the_force.bus.sub.register.dispose", () => {
		const dispose = sauron.sub(subHandler, "sub.lifecycle" as S, s, m, r);
		dispose();
	});
	bench("the_force.bus.stream.register.dispose", () => {
		const dispose = stream(streamHandler, "stream.lifecycle" as S, s, m, r);
dispose();
	});

    // BUS: REGISTRATION AND DISPOSAL
    bench("the_force.bus.sub.register.dispose", () => {
        const dispose = sauron.sub(subHandler, "sub.lifecycle" as S, s, m, r);
        dispose();
    });
    bench("the_force.bus.stream.register.dispose", () => {
        const dispose = stream(streamHandler, "stream.lifecycle" as S, s, m, r);
        dispose();
    });

    // BUS: DUPLEX
    const duplex = sauron.duplex("duplex" as S);
    let duplexValue = 0;
    bench("the_force.bus.duplex.send", () => {
        duplexValue ^= 1;
        duplex.send(duplexValue);
    });
    bench("the_force.bus.duplex.sub.register.dispose", () => {
        const dispose = duplex.sub(subHandler);
        dispose();
    });
    bench("the_force.bus.duplex.stream.register.dispose", () => {
        const dispose = duplex.stream(streamHandler);
        dispose();
    });

    // KEEP LIVE REFERENCES
    void recomputeRuns;
    void cleanupRuns;
    void rerunRuns;
    void fanIn16Runs;
    void fanOut16Runs;
    void conditionalRuns;
    void batch2Runs;
    void batch16Runs;
    void diamondRuns;
    void branchRuns;
    void sendValue;
    void hotSendValue;
    void streamWrite;
    void stream16Write;
    void duplexValue;
    void disposeCleanup;
    void disposeRerun;
    void disposeFanIn16;
    void disposeFanOut16;
    void disposeConditional;
    void disposeBatch2;
    void disposeBatch16;
    void disposeDiamond;
    void disposeBranch;
    void disposeStreamOne;
    void disposeStream16;
}
else if (FIGHT === "the_force") {
    const sauron1 = new f_bus();
    const sauron = sauron1.bus;
    const store = sauron1.store;

    // SIGNAL: READ
    const readSignal = f_sig<number>(1);
    bench("the_force.signal.read", () => { readSignal(); });

    // FIX #1: signal.peek was calling store.peek() — a keyed-store lookup,
    // not the signal's own .peek(). Now it actually reads readSignal.peek(),
    // which hits Signal's `fn.peek = () => node.value` closure directly.
    bench("the_force.signal.peek", () => { readSignal.peek(); });

    // SIGNAL: WRITE
    const writeSignal = f_sig<number>(0);
    let writeValue = 0;
    bench("the_force.signal.write", () => {
        writeValue ^= 1;
        writeSignal(writeValue);
    });

    const unchangedSignal = f_sig<number>(1);
    bench("the_force.signal.write.unchanged", () => { unchangedSignal(1); });

    const readWriteSignal = f_sig<number>(0);
    let readWriteValue = 0;
    bench("the_force.signal.read.write", () => {
        readWriteSignal();
        readWriteValue ^= 1;
        readWriteSignal(readWriteValue);
    });

    // COMPUTED: CACHED READ
    const cachedSource = f_sig<number>(1);
    const cachedComputed = f_computed(() => cachedSource() * 2);
    cachedComputed();
    bench("the_force.computed.read.cached", () => { cachedComputed(); });

    // COMPUTED: FIRST EVALUATION
    const firstSource = f_sig<number>(1);
    const firstComputed = f_computed(() => firstSource() * 2);
    bench("the_force.f_computed.read.first", () => { firstComputed(); });

    // COMPUTED: INVALIDATION
    const invalidationSource = f_sig<number>(0);
    const invalidationComputed = f_computed(() => invalidationSource() + 1);
    invalidationComputed();
    let invalidationValue = 0;
    bench("the_force.f_computed.invalidate", () => {
        invalidationValue++;
        invalidationSource(invalidationValue);
    });

    // COMPUTED: RECOMPUTATION
    const recomputeSource = f_sig<number>(0);
    let recomputeRuns = 0;
    const recomputeComputed = f_computed(() => {
        recomputeRuns++;
        return recomputeSource() * 2;
    });
    recomputeComputed();
    let recomputeValue = 0;
    bench("the_force.f_computed.recompute", () => {
        recomputeValue++;
        recomputeSource(recomputeValue);
        recomputeComputed();
    });

    // COMPUTED: CHAIN DEPTH
    const chain8Source = f_sig<number>(0);
    let chain8 = f_computed(() => chain8Source() + 1);
    for (let i = 1; i < 8; i++) {
        const previous = chain8;
        chain8 = f_computed(() => previous() + 1);
    }
    chain8();
    let chain8Value = 0;
    bench("the_force.f_computed.chain.8", () => {
        chain8Value++;
        chain8Source(chain8Value);
        chain8();
    });

    const chain32Source = f_sig<number>(0);
    let chain32 = f_computed(() => chain32Source() + 1);
    for (let i = 1; i < 32; i++) {
        const previous = chain32;
        chain32 = f_computed(() => previous() + 1);
    }
    chain32();
    let chain32Value = 0;
    bench("the_force.f_computed.chain.32", () => {
        chain32Value++;
        chain32Source(chain32Value);
        chain32();
    });

    // FIX #2: f_effect.create / f_effect.dispose / f_effect.create.dependency were
    // all the same benchmark (create + read one signal + dispose, same shape,
    // different variable names) run three times under three labels. Rewritten
    // so each one isolates something actually different:

    // f_effect.create — pure node allocation/teardown cost, NO tracked dependency.
    // An empty f_effect body means no link() call ever happens, so this measures
    // only the EffectNode object + queue bookkeeping, nothing else.
    bench("the_force.f_effect.create", () => {
        const dispose = f_effect(() => { /* no reads: zero dependency edges */ });
        dispose();
    });

    // f_effect.create.dependency — same create+dispose shape, but now the body
    // reads one signal, so it pays the extra cost of one link() call during
    // creation and one unlink() during clearSources() at dispose. The delta
    // between this and f_effect.create above IS the cost of one dependency edge.
    const effectDependencySource = f_sig<number>(0);
    bench("the_force.f_effect.create.dependency", () => {
        const dispose = f_effect(() => { effectDependencySource(); });
        dispose();
    });

    // f_effect.dispose — isolates disposal alone, with creation cost paid outside
    // the timed loop. A pool of pre-built effects (each with one dependency,
    // to match the shape above) is drained one per call; refilled in bulk
    // whenever it runs low so the steady-state average isn't creation cost in
    // disguise. (A refill iteration will spike slower — expected and fine for
    // Average; just don't read too much into a single Peak sample here.)
    const disposeSource = f_sig<number>(0);
    let disposePool: Array<() => void> = [];
    const DISPOSE_POOL_SIZE = 10_000;
    const refillDisposePool = () => {
        for (let i = 0; i < DISPOSE_POOL_SIZE; i++) {
            disposePool.push(f_effect(() => { disposeSource(); }));
        }
    };
    refillDisposePool();
    bench("the_force.f_effect.dispose", () => {
        if (disposePool.length === 0) refillDisposePool();
        const dispose = disposePool.pop()!;
        dispose();
    });

    // EFFECT: CLEANUP
    const cleanupSource = f_sig<number>(0);
    let cleanupRuns = 0;
    const disposeCleanup = f_effect(() => {
        cleanupSource();
        return () => { cleanupRuns++; };
    });
    let cleanupValue = 0;
    bench("the_force.f_effect.cleanup", () => {
        cleanupValue++;
        f_batch(() => { cleanupSource(cleanupValue); });
    });

    // EFFECT: RERUN
    const rerunSource = f_sig<number>(0);
    let rerunRuns = 0;
    const disposeRerun = f_effect(() => {
        rerunSource();
        rerunRuns++;
    });
    let rerunValue = 0;
    bench("the_force.f_effect.rerun", () => {
        rerunValue++;
        f_batch(() => { rerunSource(rerunValue); });
    });

    // DEPENDENCIES: FAN-IN
    const fanIn16 = Array.from({ length: 16 }, () => f_sig<number>(0));
    let fanIn16Runs = 0;
    const disposeFanIn16 = f_effect(() => {
        let total = 0;
        for (let i = 0; i < fanIn16.length; i++) {
            total += fanIn16[i]();
        }
        fanIn16Runs++;
        void total;
    });
    let fanIn16Value = 0;
    bench("the_force.dependencies.fan-in.16", () => {
        fanIn16Value ^= 1;
        f_batch(() => fanIn16[0](fanIn16Value));
    });

    // DEPENDENCIES: FAN-OUT
    const fanOut16Source = f_sig<number>(0);
    let fanOut16Runs = 0;
    const disposeFanOut16: (() => V)[] = [];
    for (let i = 0; i < 16; i++) {
        disposeFanOut16.push(f_effect(() => {
            fanOut16Source();
            fanOut16Runs++;
        }));
    }
    let fanOut16Value = 0;
    bench("the_force.dependencies.fan-out.16", () => {
        fanOut16Value ^= 1;
        f_batch(() => fanOut16Source(fanOut16Value));
    });

    // DEPENDENCIES: CONDITIONAL TRACKING
    const condition = f_sig<boolean>(true);
    const conditionalA = f_sig<number>(0);
    const conditionalB = f_sig<number>(0);
    let conditionalRuns = 0;
    const disposeConditional = f_effect(() => {
        conditionalRuns++;
        if (condition()) {
            conditionalA();
        } else {
            conditionalB();
        }
    });
    let conditionalValue = 0;
    bench("the_force.dependencies.conditional.switch", () => {
        conditionalValue ^= 1;
        f_batch(() => condition(!!conditionalValue));
    });

    // BATCHING: ONE SIGNAL, TWO WRITES
    const batch2Source = f_sig<number>(0);
    let batch2Runs = 0;
    const disposeBatch2 = f_effect(() => {
        batch2Source();
        batch2Runs++;
    });
    let batch2Value = 0;
    bench("the_force.f_batch.2", () => {
        batch2Value++;
        f_batch(() => {
            batch2Source(batch2Value);
            batch2Source(batch2Value + 1);
        });
    });

    // BATCHING: 16 SIGNALS
    const batch16Sources = Array.from({ length: 16 }, () => f_sig<number>(0));
    let batch16Runs = 0;
    const disposeBatch16 = f_effect(() => {
        let total = 0;
        for (let i = 0; i < batch16Sources.length; i++) {
            total += batch16Sources[i]();
        }
        batch16Runs++;
        void total;
    });
    let batch16Value = 0;
    bench("the_force.f_batch.16", () => {
        batch16Value++;
        f_batch(() => {
            for (let i = 0; i < batch16Sources.length; i++) {
                batch16Sources[i](batch16Value);
            }
        });
    });

    // GRAPH: DIAMOND
    const diamondSource = f_sig<number>(0);
    const diamondLeft = f_computed(() => diamondSource() + 1);
    const diamondRight = f_computed(() => diamondSource() + 2);
    const diamondJoin = f_computed(() => diamondLeft() + diamondRight());
    let diamondRuns = 0;
    const disposeDiamond = f_effect(() => {
        diamondRuns++;
        diamondJoin();
    });
    let diamondValue = 0;
    bench("the_force.graph.diamond", () => {
        diamondValue++;
        f_batch(() => diamondSource(diamondValue));
    });

    // GRAPH: BRANCHING
    const branchSource = f_sig<number>(0);
    const branches = Array.from(
        { length: 16 },
        (_, i) => f_computed(() => branchSource() + i)
    );
    const branchJoin = f_computed(() => {
        let total = 0;
        for (let i = 0; i < branches.length; i++) {
            total += branches[i]();
        }
        return total;
    });
    let branchRuns = 0;
    const disposeBranch = f_effect(() => {
        branchRuns++;
        branchJoin();
    });
    let branchValue = 0;
    bench("the_force.graph.branch.16", () => {
        branchValue++;
        f_batch(() => branchSource(branchValue));
    });

    // BUS: DIRECT SEND
    let sendValue = 0;
    sauron.sub((p: U) => { sendValue = p as number; }, k, s, m, r);
    let sendWrite = 0;
    bench("the_force.bus.send", () => {
        sendWrite ^= 1;
        sauron.send(sendWrite, k);
    });

    let hotSendValue = 0;
    sauron.sub((p: U) => { hotSendValue = p as number; }, "send.hot" as S, s, m, r);
    let hotSendWrite = 0;
    bench("the_force.bus.hotSend", () => {
        hotSendWrite ^= 1;
        sauron.hotSend(hotSendWrite, "send.hot" as S);
    });

	// BUS: STREAM PUBLISH
	const stream = sauron.stream.bind(sauron)();
	const streamHandler = (_message: ChannelMessage) => {};
	const streamOne = "stream.one" as S;
	const disposeStreamOne = stream(streamHandler, streamOne, s, m, r);

	let streamWrite = 0;
	const streamMessage = { r, m, s, k: streamOne, p: 0 };
	bench("the_force.bus.stream.publish.1", () => {
		streamWrite ^= 1;
		streamMessage.p = streamWrite;
		sauron.hotPublish(streamMessage);
	});

	const stream16 = "stream.16" as S;
	const disposeStream16: (() => V)[] = [];
	for (let i = 0; i < 16; i++) {
		disposeStream16.push(stream(streamHandler, stream16, s, m, r));
	}
	let stream16Write = 0;
	const stream16Message = { r, m, s, k: stream16, p: 0 };
	bench("the_force.bus.stream.publish.16", () => {
		stream16Write ^= 1;
		stream16Message.p = stream16Write;
		sauron.hotPublish(stream16Message);
	});

	// =====================================================================
// ADD THIS inside the `else if (FIGHT === "the_force")` block,
// right after the existing BUS: DUPLEX section and before
// "// KEEP LIVE REFERENCES". Uses the same `sauron1`, `store` that
// are already declared at the top of the block:
//
//   const sauron1 = new f_bus();
//   const sauron = sauron1.bus;
//   const store = sauron1.store;
//
// Test names mirror ALIEN2_STORAGE / ALIEN3_STORAGE so results line
// up directly in the comparison tables.
// =====================================================================

// -------------------- STORE (mirrors ALIEN2_STORAGE) --------------------

store.set("store.get.hot" as any, 1);
bench("the_force.store.get.hot", () => {
    store.get("store.get.hot" as any);
});

bench("the_force.store.get.missing", () => {
    store.get("store.get.missing.__absent" as any);
});

store.set("store.has.existing" as any, 1);
bench("the_force.store.has.existing", () => {
    store.has("store.has.existing" as any);
});

bench("the_force.store.has.missing", () => {
    store.has("store.has.missing.__absent" as any);
});

let storeSetChangedValue = 0;
bench("the_force.store.set.changed", () => {
    storeSetChangedValue ^= 1;
    store.set("store.set.changed" as any, storeSetChangedValue);
});

store.set("store.set.same" as any, 1);
bench("the_force.store.set.same", () => {
    store.set("store.set.same" as any, 1);
});

let storeSetNotifyValue = 0;
let storeSetNotifyRuns = 0;
const disposeStoreSetNotify = store.sub("store.set.notify" as any, () => { storeSetNotifyRuns++; });
bench("the_force.store.set.notify", () => {
    storeSetNotifyValue ^= 1;
    store.set("store.set.notify" as any, storeSetNotifyValue);
});

// FAN-OUT: one key, N subscribers, one write notifies all of them
let storeFanout8Runs = 0;
const disposeStoreFanout8: (() => void)[] = [];
for (let i = 0; i < 8; i++) {
    disposeStoreFanout8.push(store.sub("store.fanout.8" as any, () => { storeFanout8Runs++; }));
}
let storeFanout8Value = 0;
bench("the_force.store.fanout.8", () => {
    storeFanout8Value ^= 1;
    store.set("store.fanout.8" as any, storeFanout8Value);
});



// FAN-IN: N source keys collapsed into one derived key via derive()
const storeFanIn8Sources = Array.from({ length: 8 }, (_, i) => `store.fanin.8.src.${i}`);
for (const k of storeFanIn8Sources) store.set(k as any, 0);
const disposeStoreFanIn8 = store.derive(
    "store.fanin.8.out" as any,
    storeFanIn8Sources as any,
    (...vals: number[]) => vals.reduce((a, b) => a + b, 0),
);
let storeFanIn8Value = 0;
bench("the_force.store.fanin.8", () => {
    storeFanIn8Value ^= 1;
    store.set(storeFanIn8Sources[0] as any, storeFanIn8Value);
});


// getMany.8
const storeGetMany8Keys = Array.from({ length: 8 }, (_, i) => `store.getMany.8.${i}`);
for (const k of storeGetMany8Keys) store.set(k as any, 1);
bench("the_force.store.getMany.8", () => {
    store.getMany(storeGetMany8Keys as any);
});

// delete + recreate, same key, every iteration
store.set("store.delete.recreate" as any, 1);
bench("the_force.store.delete.recreate", () => {
    store.delete("store.delete.recreate" as any);
    store.create("store.delete.recreate" as any, 1);
});

// batch of 8 individual writes inside one batch() call
const storeBatchWrites8Keys = Array.from({ length: 8 }, (_, i) => `store.batch.writes.8.${i}`);
for (const k of storeBatchWrites8Keys) store.set(k as any, 0);
let storeBatchWrites8Value = 0;
bench("the_force.store.batch.writes.8", () => {
    storeBatchWrites8Value++;
    f_batch(() => {
        for (const k of storeBatchWrites8Keys) store.set(k as any, storeBatchWrites8Value);
    });
});

// -------------------- PERSISTED (mirrors ALIEN3_STORAGE) --------------------
// Uses store.persist() to mark each slot persisted, matching ALIEN3's
// "storage-backed signal" semantics rather than the plain in-memory store above.

store.persist("persisted.get.hot" as any, 1);
bench("the_force.persisted.get.hot", () => {
    store.get("persisted.get.hot" as any);
});

bench("the_force.persisted.get.missing", () => {
    store.get("persisted.get.missing.__absent" as any);
});

let persistedSetChangedValue = 0;
store.persist("persisted.set.changed" as any, 0);
bench("the_force.persisted.set.changed", () => {
    persistedSetChangedValue ^= 1;
    store.set("persisted.set.changed" as any, persistedSetChangedValue);
});

store.persist("persisted.set.same" as any, 1);
bench("the_force.persisted.set.same", () => {
    store.set("persisted.set.same" as any, 1);
});

let persistedSetNotifyValue = 0;
let persistedSetNotifyRuns = 0;
store.persist("persisted.set.notify" as any, 0);
const disposePersistedSetNotify = store.sub("persisted.set.notify" as any, () => { persistedSetNotifyRuns++; });
bench("the_force.persisted.set.notify", () => {
    persistedSetNotifyValue ^= 1;
    store.set("persisted.set.notify" as any, persistedSetNotifyValue);
});

let persistedFanout8Runs = 0;
store.persist("persisted.fanout.8" as any, 0);
const disposePersistedFanout8: (() => void)[] = [];
for (let i = 0; i < 8; i++) {
    disposePersistedFanout8.push(store.sub("persisted.fanout.8" as any, () => { persistedFanout8Runs++; }));
}
let persistedFanout8Value = 0;
bench("the_force.persisted.fanout.8", () => {
    persistedFanout8Value ^= 1;
    store.set("persisted.fanout.8" as any, persistedFanout8Value);
});


const persistedFanIn8Sources = Array.from({ length: 8 }, (_, i) => `persisted.fanin.8.src.${i}`);
for (const k of persistedFanIn8Sources) store.persist(k as any, 0);
const disposePersistedFanIn8 = store.derive(
    "persisted.fanin.8.out" as any,
    persistedFanIn8Sources as any,
    (...vals: number[]) => vals.reduce((a, b) => a + b, 0),
);
let persistedFanIn8Value = 0;
bench("the_force.persisted.fanin.8", () => {
    persistedFanIn8Value ^= 1;
    store.set(persistedFanIn8Sources[0] as any, persistedFanIn8Value);
});


// computed.read.clean — reading a derive() output whose sources haven't changed
const persistedComputedSource = "persisted.computed.src";
store.persist(persistedComputedSource as any, 1);
const disposePersistedComputed = store.derive(
    "persisted.computed.read.clean" as any,
    [persistedComputedSource] as any,
    (v: number) => v * 2,
);
bench("the_force.persisted.computed.read.clean", () => {
    store.get("persisted.computed.read.clean" as any);
});

// computed.recompute — source changes every iteration, forcing derive's fn to rerun
let persistedRecomputeValue = 0;
const persistedRecomputeSource = "persisted.recompute.src";
store.persist(persistedRecomputeSource as any, 0);
const disposePersistedRecompute = store.derive(
    "persisted.computed.recompute" as any,
    [persistedRecomputeSource] as any,
    (v: number) => v * 2,
);
bench("the_force.persisted.computed.recompute", () => {
    persistedRecomputeValue++;
    store.set(persistedRecomputeSource as any, persistedRecomputeValue);
});

// chain.1 / chain.8 — derive() chained N levels deep
let persistedChain1Value = 0;
const persistedChain1Source = "persisted.chain.1.src";
store.persist(persistedChain1Source as any, 0);
const disposePersistedChain1 = store.derive(
    "persisted.chain.1.out" as any,
    [persistedChain1Source] as any,
    (v: number) => v + 1,
);
bench("the_force.persisted.chain.1", () => {
    persistedChain1Value++;
    store.set(persistedChain1Source as any, persistedChain1Value);
});

let persistedChain8Value = 0;
const persistedChain8Source = "persisted.chain.8.src.0";
store.persist(persistedChain8Source as any, 0);
const disposePersistedChain8: (() => void)[] = [];
for (let i = 1; i <= 8; i++) {
    const outKey = `persisted.chain.8.src.${i}`;
    const inKey = `persisted.chain.8.src.${i - 1}`;
    disposePersistedChain8.push(
        store.derive(outKey as any, [inKey] as any, (v: number) => v + 1),
    );
}
bench("the_force.persisted.chain.8", () => {
    persistedChain8Value++;
    store.set(persistedChain8Source as any, persistedChain8Value);
});

// batch of 8 persisted writes inside one batch() call
const persistedBatchWrites8Keys = Array.from({ length: 8 }, (_, i) => `persisted.batch.writes.8.${i}`);
for (const k of persistedBatchWrites8Keys) store.persist(k as any, 0);
let persistedBatchWrites8Value = 0;
bench("the_force.persisted.batch.writes.8", () => {
    persistedBatchWrites8Value++;
    f_batch(() => {
        for (const k of persistedBatchWrites8Keys) store.set(k as any, persistedBatchWrites8Value);
    });
});

// effect.rerun against a persisted slot, via store.sub (the store's own
// listener path, not f_effect — this is the "real UI binding" shape:
// a component subscribed to a persisted value re-rendering on write)
let persistedEffectRerunRuns = 0;
store.persist("persisted.effect.rerun" as any, 0);
const disposePersistedEffectRerun = store.sub("persisted.effect.rerun" as any, () => { persistedEffectRerunRuns++; });
let persistedEffectRerunValue = 0;
bench("the_force.persisted.effect.rerun", () => {
    persistedEffectRerunValue++;
    store.set("persisted.effect.rerun" as any, persistedEffectRerunValue);
});

// effect.create.dispose against a persisted slot via store.sub register+dispose
bench("the_force.persisted.effect.create.dispose", () => {
    const dispose = store.sub("persisted.effect.create.dispose" as any, () => {});
    dispose();
});

// -------------------- KEEP LIVE REFERENCES (append to existing void block) --------------------
void storeSetNotifyRuns;
void storeFanout8Runs;
void persistedSetNotifyRuns;
void persistedFanout8Runs;
void persistedEffectRerunRuns;
void disposeStoreSetNotify;
void disposeStoreFanout8;
void disposeStoreFanIn8;
void disposePersistedSetNotify;
void disposePersistedFanout8;
void disposePersistedFanIn8;
void disposePersistedComputed;
void disposePersistedRecompute;
void disposePersistedChain1;
void disposePersistedChain8;
void disposePersistedEffectRerun;


	// BUS: REGISTRATION AND DISPOSAL
	// FIX #3: this block was duplicated verbatim right below itself in the
	// original file (same two benches, same comment, copy-pasted). Deleted
	// the second copy — that's exactly why "bus.sub.register.dispose" showed
	// up twice in your last run output.
	const subHandler = (_payload: U) => {};
	bench("the_force.bus.sub.register.dispose", () => {
		const dispose = sauron.sub(subHandler, "sub.lifecycle" as S, s, m, r);
		dispose();
	});
	bench("the_force.bus.stream.register.dispose", () => {
		const dispose = stream(streamHandler, "stream.lifecycle" as S, s, m, r);
		dispose();
	});

    // BUS: DUPLEX
    const duplex = sauron.duplex("duplex" as S);
    let duplexValue = 0;
    bench("the_force.bus.duplex.send", () => {
        duplexValue ^= 1;
        duplex.send(duplexValue);
    });
    bench("the_force.bus.duplex.sub.register.dispose", () => {
        const dispose = duplex.sub(subHandler);
        dispose();
    });
    bench("the_force.bus.duplex.stream.register.dispose", () => {
        const dispose = duplex.stream(streamHandler);
        dispose();
    });

    // KEEP LIVE REFERENCES
    void recomputeRuns;
    void cleanupRuns;
    void rerunRuns;
    void fanIn16Runs;
    void fanOut16Runs;
    void conditionalRuns;
    void batch2Runs;
    void batch16Runs;
    void diamondRuns;
    void branchRuns;
    void sendValue;
    void hotSendValue;
    void streamWrite;
    void stream16Write;
    void duplexValue;
    void disposeCleanup;
    void disposeRerun;
    void disposeFanIn16;
    void disposeFanOut16;
    void disposeConditional;
    void disposeBatch2;
    void disposeBatch16;
    void disposeDiamond;
    void disposeBranch;
    void disposeStreamOne;
    void disposeStream16;
}
else if (FIGHT === "SKYWALKER") {
	const sauron = new SKYWALKER("SAURON_BENCH", {
		broadcastChannel: false,
	});

	// ================================================================
	// STORE: DIRECT READ
	// ================================================================

	const readKey = "store.read" as S;
	sauron.hotSet(readKey, 1);

	bench("SKYWALKER.store.hotGet", () => {
		sauron.hotGet(readKey);
	});

	bench("SKYWALKER.store.get", () => {
		sauron.get(readKey);
	});

	bench("SKYWALKER.store.peek", () => {
		sauron.peek(readKey);
	});

	bench("SKYWALKER.store.read", () => {
		sauron.read(readKey);
	});

	bench("SKYWALKER.store.has", () => {
		sauron.has(readKey);
	});

	// ================================================================
	// STORE: DIRECT WRITE
	// ================================================================

	const writeKey = "store.write" as S;
	let writeValue = 0;

	bench("SKYWALKER.store.hotSet", () => {
		writeValue ^= 1;
		sauron.hotSet(writeKey, writeValue);
	});

	const setKey = "store.set" as S;
	let setValue = 0;

	bench("SKYWALKER.store.set", () => {
		setValue ^= 1;
		sauron.set(setKey, setValue);
	});

	const writeMethodKey = "store.write.method" as S;
	let writeMethodValue = 0;

	bench("SKYWALKER.store.write", () => {
		writeMethodValue ^= 1;
		sauron.write(writeMethodKey, writeMethodValue);
	});

	const editKey = "store.edit" as S;
	let editValue = 0;

	bench("SKYWALKER.store.edit", () => {
		editValue ^= 1;
		sauron.edit(editKey, editValue);
	});

	const unchangedKey = "store.unchanged" as S;
	sauron.hotSet(unchangedKey, 1);

	bench("SKYWALKER.store.hotSet.unchanged", () => {
		sauron.hotSet(unchangedKey, 1);
	});

	const swapKey = "store.swap" as S;
	let swapValue = 0;

	bench("SKYWALKER.store.swap", () => {
		swapValue ^= 1;
		sauron.swap(swapKey, swapValue);
	});

	// ================================================================
	// STORE: CREATE
	// ================================================================

	const createKey = "store.create" as S;

	bench("SKYWALKER.store.create.existing", () => {
		sauron.create(createKey, 1);
	});

	let createSequence = 0;

	bench("SKYWALKER.store.create.new", () => {
		sauron.create(
			"store.create." + (++createSequence),
			createSequence
		);
	});

	// ================================================================
	// STORE: DELETE
	// ================================================================

	const deleteKey = "store.delete" as S;
	let deleteValue = 0;

	bench("SKYWALKER.store.delete", () => {
		sauron.hotSet(deleteKey, ++deleteValue);
		sauron.delete(deleteKey);
	});

	bench("SKYWALKER.store.delete.missing", () => {
		sauron.delete("store.delete.missing");
	});

	// ================================================================
	// STORE: SIGNAL READ
	// ================================================================

	const signalKey = "signal.read" as S;
	const readSignal = sauron.signal<number>(signalKey, 1);

	bench("SKYWALKER.signal.read", () => {
		readSignal();
	});

	bench("SKYWALKER.signal.peek", () => {
		readSignal.peek();
	});

	bench("SKYWALKER.signal.value.get", () => {
		readSignal.value;
	});

	bench("SKYWALKER.signal.current.get", () => {
		readSignal.current;
	});

	bench("SKYWALKER.signal.crt.get", () => {
		readSignal.crt;
	});

	// ================================================================
	// SIGNAL: WRITE
	// ================================================================

	let signalWriteValue = 0;

	bench("SKYWALKER.signal.write", () => {
		signalWriteValue ^= 1;
		readSignal(signalWriteValue);
	});

	let signalPropertyValue = 0;

	bench("SKYWALKER.signal.value.set", () => {
		signalPropertyValue ^= 1;
		readSignal.value = signalPropertyValue;
	});

	let signalCurrentValue = 0;

	bench("SKYWALKER.signal.current.set", () => {
		signalCurrentValue ^= 1;
		readSignal.current = signalCurrentValue;
	});

	let signalCrtValue = 0;

	bench("SKYWALKER.signal.crt.set", () => {
		signalCrtValue ^= 1;
		readSignal.crt = signalCrtValue;
	});

	let signalSetValue = 0;

	bench("SKYWALKER.signal.set.set", () => {
		signalSetValue ^= 1;
		readSignal.set = signalSetValue;
	});

	// ================================================================
	// SIGNAL: MANAGEMENT
	// ================================================================

	bench("SKYWALKER.signal.has", () => {
		readSignal.has();
	});

	bench("SKYWALKER.signal.delete", () => {
		readSignal.delete();
		sauron.hotSet(signalKey, 1);
	});

	// ================================================================
	// ATOM: READ
	// ================================================================

	const atomKey = "atom.read" as S;
	const readAtom = sauron.atom<number>(atomKey, 1);

	bench("SKYWALKER.atom.value.get", () => {
		readAtom.value;
	});

	bench("SKYWALKER.atom.current.get", () => {
		readAtom.current;
	});

	bench("SKYWALKER.atom.crt.get", () => {
		readAtom.crt;
	});

	bench("SKYWALKER.atom.peek", () => {
		readAtom.peek();
	});

	// ================================================================
	// ATOM: WRITE
	// ================================================================

	let atomValue = 0;

	bench("SKYWALKER.atom.value.set", () => {
		atomValue ^= 1;
		readAtom.value = atomValue;
	});

	let atomCurrentValue = 0;

	bench("SKYWALKER.atom.current.set", () => {
		atomCurrentValue ^= 1;
		readAtom.current = atomCurrentValue;
	});

	let atomCrtValue = 0;

	bench("SKYWALKER.atom.crt.set", () => {
		atomCrtValue ^= 1;
		readAtom.crt = atomCrtValue;
	});

	let atomSetValue = 0;

	bench("SKYWALKER.atom.set.set", () => {
		atomSetValue ^= 1;
		readAtom.set = atomSetValue;
	});

	// ================================================================
	// LISTENERS: REGISTRATION / DISPOSAL
	// ================================================================

	const listenerKey = "listener.lifecycle" as S;
	const listener = (_value: number) => {};

	bench("SKYWALKER.listener.register.dispose", () => {
		const dispose = sauron.hotSub(listenerKey, listener);
		dispose();
	});

	bench("SKYWALKER.listener.sub.register.dispose", () => {
		const dispose = sauron.sub(listenerKey, listener);
		dispose();
	});

	bench("SKYWALKER.signal.sub.register.dispose", () => {
		const dispose = readSignal.sub(listener);
		dispose();
	});

	bench("SKYWALKER.atom.sub.register.dispose", () => {
		const dispose = readAtom.sub(listener);
		dispose();
	});

	// ================================================================
	// LISTENERS: NOTIFICATION
	// ================================================================

	const notifyKey = "listener.notify" as S;
	let notifyRuns = 0;

	const disposeNotify = sauron.hotSub(notifyKey, () => {
		notifyRuns++;
	});

	let notifyValue = 0;

	bench("SKYWALKER.listener.notify.1", () => {
		notifyValue ^= 1;
		sauron.hotSet(notifyKey, notifyValue);
	});

	// ================================================================
	// LISTENERS: FAN-OUT
	// ================================================================

	const fanOut16Key = "listener.fanout.16" as S;
	let fanOut16Runs = 0;
	const fanOut16Disposers: Unsubscribe[] = [];

	for (let i = 0; i < 16; i++) {
		fanOut16Disposers.push(
			sauron.hotSub(fanOut16Key, () => {
				fanOut16Runs++;
			})
		);
	}

	let fanOut16Value = 0;

	bench("SKYWALKER.listener.fanout.16", () => {
		fanOut16Value ^= 1;
		sauron.hotSet(fanOut16Key, fanOut16Value);
	});




	// ================================================================
	// GLOBAL LISTENERS
	// ================================================================

	let globalRuns = 0;

	const disposeGlobal = sauron.subAll(() => {
		globalRuns++;
	});

	let globalValue = 0;

	bench("SKYWALKER.listener.global.1", () => {
		globalValue ^= 1;
		sauron.hotSet("listener.global" as S, globalValue);
	});

	// ================================================================
	// GLOBAL LISTENER FAN-OUT
	// ================================================================

	const global16Disposers: Unsubscribe[] = [];
	let global16Runs = 0;

	for (let i = 0; i < 16; i++) {
		global16Disposers.push(
			sauron.subAll(() => {
				global16Runs++;
			})
		);
	}

	let global16Value = 0;

	bench("SKYWALKER.listener.global.fanout.16", () => {
		global16Value ^= 1;
		sauron.hotSet("listener.global.16" as S, global16Value);
	});

	// ================================================================
	// DOM BINDINGS
	// ================================================================

	const domKey = "dom.binding" as S;
	let domValue = 0;

	const domTarget = {
		textContent: "",
	};

	const disposeDOM = sauron.dom(
		domKey,
		domTarget as unknown as Node,
		(value, node) => {
			(node as unknown as { textContent: string }).textContent =
				String(value);
		}
	);

	bench("SKYWALKER.dom.notify.1", () => {
		domValue ^= 1;
		sauron.hotSet(domKey, domValue);
	});

	// ================================================================
	// DOM BINDING REGISTRATION / DISPOSAL
	// ================================================================

	const domLifecycleKey = "dom.lifecycle" as S;

	bench("SKYWALKER.dom.register.dispose", () => {
		const dispose = sauron.dom(
			domLifecycleKey,
			domTarget as unknown as Node,
			(value, node) => {
				(node as unknown as { textContent: string }).textContent =
					String(value);
			}
		);

		dispose();
	});

	// ================================================================
	// BATCH: TWO WRITES, ONE KEY
	// ================================================================

	const batch2Key = "batch.2" as S;
	let batch2Value = 0;
	let batch2Runs = 0;

	const disposeBatch2 = sauron.hotSub(batch2Key, () => {
		batch2Runs++;
	});

	bench("SKYWALKER.batch.2", () => {
		batch2Value++;

		sauron.batch(() => {
			sauron.hotSet(batch2Key, batch2Value);
			sauron.hotSet(batch2Key, batch2Value + 1);
		});
	});

	// ================================================================
	// BATCH: 16 DISTINCT KEYS
	// ================================================================

	const batch16Keys = Array.from(
		{ length: 16 },
		(_, i) => `batch.16.${i}`
	);

	let batch16Value = 0;
	let batch16Runs = 0;

	const batch16Disposers = batch16Keys.map((key) =>
		sauron.hotSub(key, () => {
			batch16Runs++;
		})
	);

	bench("SKYWALKER.batch.16", () => {
		batch16Value++;

		sauron.batch(() => {
			for (let i = 0; i < batch16Keys.length; i++) {
				sauron.hotSet(batch16Keys[i], batch16Value);
			}
		});
	});


	// ================================================================
	// COLLECTIONS: GET MANY
	// ================================================================

	const manyKeys = Array.from({ length: 64 }, (_, i) => `many.${i}`);

	for (let i = 0; i < manyKeys.length; i++) {
		sauron.hotSet(manyKeys[i], i);
	}

	bench("SKYWALKER.store.getMany.64", () => {
		sauron.getMany(manyKeys);
	});

	// ================================================================
	// COLLECTIONS: SET MANY
	// ================================================================

	const setManyEntries: Record<S, number> = Object.create(null);

	for (let i = 0; i < 64; i++) {
		setManyEntries[`setMany.${i}`] = 0;
	}

	let setManyValue = 0;

	bench("SKYWALKER.store.setMany.64", () => {
		setManyValue ^= 1;

		for (const key in setManyEntries) {
			setManyEntries[key] = setManyValue;
		}

		sauron.setMany(setManyEntries);
	});

	// ================================================================
	// COLLECTIONS: DELETE MANY
	// ================================================================

	const deleteManyKeys = Array.from(
		{ length: 64 },
		(_, i) => `deleteMany.${i}`
	);

	let deleteManyValue = 0;

	bench("SKYWALKER.store.deleteMany.64", () => {
		deleteManyValue++;

		for (let i = 0; i < deleteManyKeys.length; i++) {
			sauron.hotSet(deleteManyKeys[i], deleteManyValue);
		}

		sauron.deleteMany(deleteManyKeys);
	});

	// ================================================================
	// COLLECTIONS: ENTRIES / ALL
	// ================================================================

	bench("SKYWALKER.store.entries", () => {
		sauron.entries();
	});

	bench("SKYWALKER.store.getAll", () => {
		sauron.getAll();
	});

	// ================================================================
	// COLLECTIONS: PREFIX
	// ================================================================

	bench("SKYWALKER.store.getByPrefix", () => {
		sauron.getByPrefix("many.");
	});

	// ================================================================
	// BUS: SUBSCRIBE / SEND
	// ================================================================

	const busKey = "bus.send" as S;
	let busValue = 0;

	const disposeBus = sauron.subHandler(busKey, (payload: number) => {
		busValue = payload;
	});

	let busWrite = 0;

	bench("SKYWALKER.bus.send", () => {
		busWrite ^= 1;
		sauron.send(busWrite, busKey);
	});

	bench("SKYWALKER.bus.hotSend", () => {
		busWrite ^= 1;
		sauron.hotSend(busWrite, busKey);
	});

	// ================================================================
	// BUS: STREAM PUBLISH
	// ================================================================

	const streamKey = "bus.stream" as S;
	let streamRuns = 0;

	const disposeStream = sauron.stream(streamKey, () => {
		streamRuns++;
	});

	const streamMessage = {
		r: "THE_WIZARD_ROOT_REG",
		m: "SAURON_MASTER_REG",
		s: sauron.name,
		k: streamKey,
		p: 0,
	};

	let streamValue = 0;

	bench("SKYWALKER.bus.stream.publish.1", () => {
		streamValue ^= 1;
		streamMessage.p = streamValue;

		const channel = sauron["_channels"][streamKey];

		if (channel?.push) {
			for (const id in channel.push) {
				channel.push[id](streamMessage);
			}
		}
	});

	// ================================================================
	// BUS: STREAM FAN-OUT
	// ================================================================

	const stream16Key = "bus.stream.16" as S;
	let stream16Runs = 0;
	const stream16Disposers: Unsubscribe[] = [];

	for (let i = 0; i < 16; i++) {
		stream16Disposers.push(
			sauron.stream(stream16Key, () => {
				stream16Runs++;
			})
		);
	}

	const stream16Message = {
		r: "THE_WIZARD_ROOT_REG",
		m: "SAURON_MASTER_REG",
		s: sauron.name,
		k: stream16Key,
		p: 0,
	};

	let stream16Value = 0;

	bench("SKYWALKER.bus.stream.publish.16", () => {
		stream16Value ^= 1;
		stream16Message.p = stream16Value;

		const channel = sauron["_channels"][stream16Key];

		if (channel?.push) {
			for (const id in channel.push) {
				channel.push[id](stream16Message);
			}
		}
	});

	const stream64Key = "bus.stream.64" as S;
	let stream64Runs = 0;
	const stream64Disposers: Unsubscribe[] = [];

	for (let i = 0; i < 64; i++) {
		stream64Disposers.push(
			sauron.stream(stream64Key, () => {
				stream64Runs++;
			})
		);
	}

	const stream64Message = {
		r: "THE_WIZARD_ROOT_REG",
		m: "SAURON_MASTER_REG",
		s: sauron.name,
		k: stream64Key,
		p: 0,
	};

	let stream64Value = 0;

	bench("SKYWALKER.bus.stream.publish.64", () => {
		stream64Value ^= 1;
		stream64Message.p = stream64Value;

		const channel = sauron["_channels"][stream64Key];

		if (channel?.push) {
			for (const id in channel.push) {
				channel.push[id](stream64Message);
			}
		}
	});

	// ================================================================
	// BUS: DUPLEX
	// ================================================================

	const duplex = sauron.duplex("bus.duplex" as S);

	let duplexValue = 0;

	bench("SKYWALKER.bus.duplex.send", () => {
		duplexValue ^= 1;
		duplex.send(duplexValue);
	});

	bench("SKYWALKER.bus.duplex.sub.register.dispose", () => {
		const dispose = duplex.sub(() => {});
		dispose();
	});

	bench("SKYWALKER.bus.duplex.stream.register.dispose", () => {
		const dispose = duplex.stream(() => {});
		dispose();
	});

	// ================================================================
	// WAITERS
	// ================================================================

	const waitKey = "waiter" as S;

	bench("SKYWALKER.waitFor", () => {
		const promise = sauron.waitFor(waitKey);
		sauron.hotSet(waitKey, 1);
		void promise;
		sauron.delete(waitKey);
	});

	// ================================================================
	// KEEP LIVE REFERENCES
	// ================================================================

	void notifyRuns;
	void fanOut16Runs;
	void globalRuns;
	void global16Runs;
	void domValue;

	void batch2Value;
	void batch16Value;
	void batch16Runs;

	void busValue;
	void streamRuns;
	void stream16Runs;
	void stream64Runs;
	void duplexValue;

	void disposeNotify;
	void fanOut16Disposers;
	void disposeGlobal;
	void global16Disposers;
	void disposeDOM;

	void disposeBatch2;
	void batch16Disposers;

	void disposeBus;
	void disposeStream;
	void stream16Disposers;
	void stream64Disposers;
}
else if (FIGHT === "SITH") {
	const store = new SITH({
		channel: false,
		storage: null,
		onError: () => {},
	});

	const readKey = "read";
	const writeKey = "write";
	const notifyKey = "notify";
	const fanoutKey = "fanout";
	const globalKey = "global";
	const prefixKey = "prefix:item";
	const deriveSource = "derive:source";
	const deriveOutput = "derive:output";

	store.set(readKey, 1);
	store.set(writeKey, 0);
	store.set(notifyKey, 0);
	store.set(fanoutKey, 0);
	store.set(globalKey, 0);
	store.set(prefixKey, 0);
	store.set(deriveSource, 0);
	store.set(deriveOutput, 0);

	const sig = store.signal("handle:sig", 0);
	const atom = store.atom("handle:atom", 0);

	const readSig = store.signal("handle:read", 1);
	const readAtom = store.atom("handle:read-atom", 1);

	const writeSig = store.signal("handle:write", 0);
	const writeAtom = store.atom("handle:write-atom", 0);

	const valueSig = store.signal("handle:value", 0);
	const valueAtom = store.atom("handle:value-atom", 0);

	let toggle = 0;
	let sigToggle = 0;
	let atomToggle = 0;
	let valueToggle = 0;

	// ───────────────────────────── reads ─────────────────────────────

	bench("SLOT_STORE.get", () => {
		store.get(readKey);
	});

	bench("SLOT_STORE.peek", () => {
		store.peek(readKey);
	});

	bench("SLOT_STORE.read", () => {
		store.read(readKey);
	});

	bench("SLOT_STORE.hot.read", () => {
		store.hot(readKey);
	});

	bench("SLOT_STORE.getOr", () => {
		store.getOr(readKey, 0);
	});

	bench("SLOT_STORE.has", () => {
		store.has(readKey);
	});

	bench("SLOT_STORE.Sig.get", () => {
		readSig.get();
	});

	bench("SLOT_STORE.Sig.peek", () => {
		readSig.peek();
	});

	bench("SLOT_STORE.Sig.value", () => {
		readSig.value;
	});

	bench("SLOT_STORE.Atom.get", () => {
		readAtom.get();
	});

	bench("SLOT_STORE.Atom.peek", () => {
		readAtom.peek();
	});

	bench("SLOT_STORE.Atom.value", () => {
		readAtom.value;
	});

	bench("SLOT_STORE.Atom.current", () => {
		readAtom.current;
	});

	bench("SLOT_STORE.Atom.crt", () => {
		readAtom.crt;
	});

	bench("SLOT_STORE.Atom.set.read", () => {
		readAtom.set;
	});

	// ───────────────────────────── writes ─────────────────────────────

	bench("SLOT_STORE.set", () => {
		store.set(writeKey, ++toggle);
	});

	bench("SLOT_STORE.write", () => {
		store.write(writeKey, ++toggle);
	});

	bench("SLOT_STORE.hot.write", () => {
		store.hot(writeKey, ++toggle);
	});

	bench("SLOT_STORE.setLocal", () => {
		store.setLocal(writeKey, ++toggle);
	});

	bench("SLOT_STORE.Sig.set", () => {
		writeSig.set(++sigToggle);
	});

	bench("SLOT_STORE.Sig.value.write", () => {
		writeSig.value = ++sigToggle;
	});

	bench("SLOT_STORE.Sig.update", () => {
		writeSig.update((v) => v + 1);
	});

	bench("SLOT_STORE.Atom.value.write", () => {
		writeAtom.value = ++atomToggle;
	});

	bench("SLOT_STORE.Atom.current.write", () => {
		writeAtom.current = ++atomToggle;
	});

	bench("SLOT_STORE.Atom.crt.write", () => {
		writeAtom.crt = ++atomToggle;
	});

	bench("SLOT_STORE.Atom.set.write", () => {
		writeAtom.set = ++atomToggle;
	});

	bench("SLOT_STORE.Atom.update", () => {
		writeAtom.update((v) => v + 1);
	});

	bench("SLOT_STORE.Sig.has", () => {
		sig.has();
	});

	bench("SLOT_STORE.Atom.has", () => {
		store.has("key");
	});	

	// ───────────────────────────── equality ─────────────────────────────

	const equalKey = "equal";
	store.set(equalKey, 1);
	const equalSig = store.signal("equal:handle", 1);

	bench("SLOT_STORE.set.same", () => {
		store.set(equalKey, 1);
	});

	bench("SLOT_STORE.Sig.set.same", () => {
		equalSig.set(1);
	});

	bench("SLOT_STORE.set.NaN", () => {
		store.set("equal:nan", Number.NaN);
	});

	// ───────────────────────────── listeners ─────────────────────────────

	let notifyCount = 0;

	const offNotify = store.sub(notifyKey, () => {
		notifyCount++;
	});

	bench("SLOT_STORE.notify.1", () => {
		store.set(notifyKey, ++toggle);
	});

	const fanoutOffs: (() => void)[] = [];
	let fanoutCount = 0;

	for (let i = 0; i < 16; i++) {
		fanoutOffs.push(
			store.sub(fanoutKey, () => {
				fanoutCount++;
			}),
		);
	}



	// ───────────────────────────── global listeners ─────────────────────────────

	let globalCount = 0;

	const offGlobal = store.subAll(() => {
		globalCount++;
	});

	bench("SLOT_STORE.global.1", () => {
		store.set(globalKey, ++toggle);
	});

	const globalOffs: (() => void)[] = [];

	for (let i = 0; i < 16; i++) {
		globalOffs.push(
			store.subAll(() => {
				globalCount++;
			}),
		);
	}

	bench("SLOT_STORE.global.17", () => {
		store.set(globalKey, ++toggle);
	});

	// ───────────────────────────── prefix listeners ─────────────────────────────

	let prefixCount = 0;

	const offPrefix = store.subPrefix("prefix:", () => {
		prefixCount++;
	});

	bench("SLOT_STORE.prefix.match", () => {
		store.set(prefixKey, ++toggle);
	});

	bench("SLOT_STORE.prefix.nonmatch", () => {
		store.set(globalKey, ++toggle);
	});




	// ───────────────────────────── batching ─────────────────────────────

	const batchSameKey = "batch:same";
	const batchDistinct: string[] = [];

	for (let i = 0; i < 16; i++) {
		batchDistinct.push(`batch:distinct:${i}`);
		store.set(`batch:distinct:${i}`, 0);
	}

	let batchSameNotifications = 0;
	let batchDistinctNotifications = 0;

	const offBatchSame = store.sub(batchSameKey, () => {
		batchSameNotifications++;
	});

	const offBatchDistinct = store.subAll((key) => {
		if (key.startsWith("batch:distinct:")) batchDistinctNotifications++;
	});

	let batchValue = 0;

	bench("SLOT_STORE.batch.same.2", () => {
		store.batch(() => {
			store.set(batchSameKey, ++batchValue);
			store.set(batchSameKey, ++batchValue);
		});
	});

	bench("SLOT_STORE.batch.same.8", () => {
		store.batch(() => {
			for (let i = 0; i < 8; i++) {
				store.set(batchSameKey, ++batchValue);
			}
		});
	});


	// ───────────────────────────── multi-key operations ─────────────────────────────

	const manyKeys = new Array<string>(16);
	const manyObject: Record<string, number> = Object.create(null);
	const manyMap = new Map<string, number>();

	for (let i = 0; i < 16; i++) {
		const k = `many:${i}`;
		manyKeys[i] = k;
		store.set(k, 0);
		manyObject[k] = 0;
		manyMap.set(k, 0);
	}

	let manyValue = 0;



	bench("SLOT_STORE.entries", () => {
		store.entries();
	});

	bench("SLOT_STORE.keys", () => {
		store.keys();
	});

	bench("SLOT_STORE.values", () => {
		store.values();
	});

	bench("SLOT_STORE.getAll", () => {
		store.getAll();
	});

	bench("SLOT_STORE.getByPrefix", () => {
		store.getByPrefix("many:");
	});

	bench("SLOT_STORE.keysByPrefix", () => {
		store.keysByPrefix("many:");
	});

	// ───────────────────────────── create / update / swap ─────────────────────────────

	store.set("create:existing", 1);

	bench("SLOT_STORE.create.existing", () => {
		store.create("create:existing", 2);
	});

	let createValue = 0;

	bench("SLOT_STORE.create.existing.factory", () => {
		store.create("create:existing", () => ++createValue);
	});

	bench("SLOT_STORE.update", () => {
		store.update("update", (v) => (v ?? 0) + 1);
	});

	bench("SLOT_STORE.swap", () => {
		store.swap("swap", ++toggle);
	});

	bench("SLOT_STORE.edit", () => {
		store.edit("edit", (v) => {
			v.count++;
		});
	});

	const editObject = { count: 0 };
	store.set("edit", editObject);

	bench("SLOT_STORE.edit.object", () => {
		store.edit("edit", (v) => {
			v.count++;
		});
	});

	bench("SLOT_STORE.touch", () => {
		store.touch("edit");
	});

	// ───────────────────────────── derive ─────────────────────────────

	let deriveRuns = 0;

	const offDerive = store.derive(
		deriveOutput,
		[deriveSource],
		(value: number) => {
			deriveRuns++;
			return value * 2;
		},
	);

	bench("SLOT_STORE.derive.source.write", () => {
		store.set(deriveSource, ++toggle);
	});

	bench("SLOT_STORE.derive.output.read", () => {
		store.get(deriveOutput);
	});

	// ───────────────────────────── waitFor ─────────────────────────────

	store.set("wait:ready", 1);

	bench("SLOT_STORE.waitFor.resolved", () => {
		void store.waitFor("wait:ready");
	});

	bench("SLOT_STORE.waitFor.resolved.predicate", () => {
		void store.waitFor("wait:ready", (v) => v === 1);
	});

	// ───────────────────────────── delete ─────────────────────────────

	bench("SLOT_STORE.delete.missing", () => {
		store.delete("delete:missing");
	});

	store.set("delete:existing", 1);

	bench("SLOT_STORE.delete.recreate", () => {
		store.delete("delete:existing");
		store.set("delete:existing", 1);
	});

	bench("SLOT_STORE.deleteMany.16", () => {
		for (let i = 0; i < 16; i++) {
			store.set(manyKeys[i], ++manyValue);
		}
		store.deleteMany(manyKeys);
	});

	bench("SLOT_STORE.deleteByPrefix.16", () => {
		for (let i = 0; i < 16; i++) {
			store.set(`delete:prefix:${i}`, ++manyValue);
		}
		store.deleteByPrefix("delete:prefix:");
	});

	// ───────────────────────────── persistence ─────────────────────────────

	const persistenceStorage = {
		data: Object.create(null) as Record<string, string>,
		getItem(k: string) {
			return this.data[k] ?? null;
		},
		setItem(k: string, v: string) {
			this.data[k] = v;
		},
		removeItem(k: string) {
			delete this.data[k];
		},
	};

	const persistentStore = new SITH({
		channel: false,
		storage: persistenceStorage,
		onError: () => {},
	});

	persistentStore.set("persist", 0);
	persistentStore.persist("persist");

	let persistValue = 0;

	bench("SLOT_STORE.persist.set", () => {
		persistentStore.set("persist", ++persistValue);
	});

	bench("SLOT_STORE.persist.flush", () => {
		persistentStore.flush();
	});

	// ───────────────────────────── cleanup / retained handles ─────────────────────────────

	void notifyCount;
	void fanoutCount;
	void globalCount;
	void prefixCount;
	void batchSameNotifications;
	void batchDistinctNotifications;
	void deriveRuns;
	void valueToggle;

	void offNotify;
	void offGlobal;
	void offPrefix;
	void offBatchSame;
	void offBatchDistinct;
	void offDerive;

	for (let i = 0; i < fanoutOffs.length; i++) void fanoutOffs[i];
	for (let i = 0; i < globalOffs.length; i++) void globalOffs[i];

	void sig;
	void atom;
	void valueSig;
	void valueAtom;
	void persistentStore;
	void store;
}
else if (FIGHT === "SIDIUS") {
	const r = "bench-root";
	const m = "bench-master";
	const s = "bench-slave";

	const make = (k: string) => new SIDIUS(r, m, s, k);

	let n = 0;

	// ───────────────────────────── reads ─────────────────────────────

	{
		const store = make("get");
		store.create("value", 1);

		bench("SIDIUS · get · existing cell", () => {
			store.get("value");
		});
	}

	{
		const store = make("peek");
		store.create("value", 1);

		bench("SIDIUS · peek · existing cell", () => {
			store.peek("value");
		});
	}

	{
		const store = make("has");
		store.create("value", 1);

		bench("SIDIUS · has · existing cell", () => {
			store.has("value");
		});
	}

	{
		const store = make("has-missing");

		bench("SIDIUS · has · missing cell", () => {
			store.has("missing");
		});
	}

	{
		const store = make("has-undefined");
		store.create("value", undefined);

		bench("SIDIUS · has · undefined value", () => {
			store.has("value");
		});
	}

	{
		const store = make("get-many");
		const keys = Array.from({ length: 16 }, (_, i) => `k${i}`);

		for (let i = 0; i < keys.length; i++) {
			store.create(keys[i], i);
		}

		bench("SIDIUS · getMany · 16 keys + result allocation", () => {
			store.getMany(keys);
		});
	}


	// ───────────────────────────── writes ─────────────────────────────

	{
		const store = make("set");
		store.create("value", 0);

		bench("SIDIUS · set · changed value", () => {
			store.set("value", ++n);
		});
	}

	{
		const store = make("set-same");
		store.create("value", 1);

		bench("SIDIUS · set · same value", () => {
			store.set("value", 1);
		});
	}

	{
		const store = make("write");
		store.create("value", 0);

		bench("SIDIUS · write · changed value", () => {
			store.write("value", ++n);
		});
	}

	{
		const store = make("swap");
		store.create("value", 0);

		bench("SIDIUS · swap · changed value", () => {
			store.swap("value", () => ++n);
		});
	}

	{
		const store = make("edit");
		store.create("value", 0);

		bench("SIDIUS · edit · changed value", () => {
			store.edit("value", () => ++n);
		});
	}

	{
		const store = make("set-many-8");
		const keys = Array.from({ length: 8 }, (_, i) => `k${i}`);
		const values = Array.from({ length: 8 }, () => 0);

		for (let i = 0; i < keys.length; i++) {
			store.create(keys[i], 0);
		}

		bench("SIDIUS · setMany · 8 individual writes", () => {
			for (let i = 0; i < values.length; i++) values[i] = ++n;
			store.setMany(keys, values);
		});
	}


	// ───────────────────────────── handles ─────────────────────────────

	{
		const store = make("signal-value");
		const signal = store.signal("value", 0);

		bench("SIDIUS · signal.value · read", () => {
			const value = signal.value;
		});
	}

	{
		const store = make("signal-current");
		const signal = store.signal("value", 0);

		bench("SIDIUS · signal.current · read", () => {
			const value = signal.current;
		});
	}

	{
		const store = make("signal-peek");
		const signal = store.signal("value", 0);

		bench("SIDIUS · signal.peek · read", () => {
			const value = signal.peek;
		});
	}

	{
		const store = make("signal-set");
		const signal = store.signal("value", 0);

		bench("SIDIUS · signal.set · changed value", () => {
			signal.set(++n);
		});
	}

	{
		const store = make("signal-update");
		const signal = store.signal("value", 0);

		bench("SIDIUS · signal.update · changed value", () => {
			signal.update(() => ++n);
		});
	}

	{
		const store = make("atom-value");
		const atom = store.atom("value", 0);

		bench("SIDIUS · atom.value · read", () => {
			const value = atom.value;
		});
	}

	// ───────────────────────────── subscriptions ─────────────────────────────

	{
		const store = make("sub-0");
		store.create("value", 0);
		let value = 0;

		bench("SIDIUS · set · zero subscribers", () => {
			store.set("value", ++value);
		});
	}

	{
		const store = make("sub-1");
		store.create("value", 0);
		store.sub("value", () => {});
		let value = 0;

		bench("SIDIUS · set · 1 subscriber", () => {
			store.set("value", ++value);
		});
	}

	{
		const store = make("sub-16");
		store.create("value", 0);

		for (let i = 0; i < 16; i++) store.sub("value", () => {});

		let value = 0;

		bench("SIDIUS · set · 16 subscribers", () => {
			store.set("value", ++value);
		});
	}



	{
		const store = make("subMany-8");
		const keys = Array.from({ length: 8 }, (_, i) => `k${i}`);

		for (let i = 0; i < keys.length; i++) {
			store.create(keys[i], 0);
		}

		const off = store.subMany(keys, () => {});
		let value = 0;

		bench("SIDIUS · setMany · 8 subMany listeners", () => {
			const next = ++value;
			for (let i = 0; i < keys.length; i++) {
				store.set(keys[i], next);
			}
		});

		off();
	}

	{
		const store = make("subAll-16");
		const keys = Array.from({ length: 16 }, (_, i) => `k${i}`);

		for (let i = 0; i < keys.length; i++) {
			store.create(keys[i], 0);
		}

		const off = store.subAll(() => {});
		let value = 0;

		bench("SIDIUS · set · subAll snapshot · 16 keys", () => {
			store.set(keys[value & 15], ++value);
		});

		off();
	}

	{
		const store = make("sub-register");
		store.create("value", 0);

		bench("SIDIUS · sub · register + dispose", () => {
			const off = store.sub("value", () => {});
			off();
		});
	}

	// ───────────────────────────── effects ─────────────────────────────

	{
		const store = make("effect-no-deps");

		bench("SIDIUS · effect · create + initial run + dispose · no deps", () => {
			const off = store.effect(() => {});
			off();
		});
	}

	{
		const store = make("effect-one-dep");
		store.create("value", 0);

		bench("SIDIUS · effect · create + initial tracked read + dispose", () => {
			const off = store.effect(() => {
				store.get("value");
			});
			off();
		});
	}

	{
		const store = make("effect-1-write");
		store.create("value", 0);

		store.effect(() => {
			store.get("value");
		});

		let value = 0;

		bench("SIDIUS · effect · 1 dependency · write + rerun", () => {
			store.set("value", ++value);
		});
	}

	{
		const store = make("effect-8-write");
		const keys = Array.from({ length: 8 }, (_, i) => `k${i}`);

		for (let i = 0; i < keys.length; i++) store.create(keys[i], 0);

		store.effect(() => {
			for (let i = 0; i < keys.length; i++) store.get(keys[i]);
		});

		let value = 0;

		bench("SIDIUS · effect · 8 dependencies · write + rerun", () => {
			store.set(keys[value & 7], ++value);
		});
	}


	{
		const store = make("effect-accumulation");
		store.create("value", 0);

		store.effect(() => {
			store.get("value");
		});

		let value = 0;

		bench("SIDIUS · effect · repeated reruns · inactive subscription accumulation", () => {
			store.set("value", ++value);
		});
	}

	// ───────────────────────────── lifecycle ─────────────────────────────

	{
		const store = make("create");
		let value = 0;

		bench("SIDIUS · create · new cell", () => {
			store.create(`k${++value}`, value);
		});
	}

	{
		const store = make("delete-create");
		let value = 0;
		store.create("value", 0);

		bench("SIDIUS · delete + recreate · same key", () => {
			store.delete("value");
			store.create("value", ++value);
		});
	}

	{
		const store = make("clear");
		const keys = Array.from({ length: 16 }, (_, i) => `k${i}`);
		let value = 0;

		bench("SIDIUS · clear · 16 cells + recreate setup", () => {
			for (let i = 0; i < keys.length; i++) {
				store.create(keys[i], ++value);
			}

			store.clear();
		});
	}

	// ───────────────────────────── iteration ─────────────────────────────

	{
		const store = make("entries");
		const keys = Array.from({ length: 16 }, (_, i) => `k${i}`);

		for (let i = 0; i < keys.length; i++) store.create(keys[i], i);

		bench("SIDIUS · entries · iterate 16 cells", () => {
			for (const entry of store.entries()) {
				void entry;
			}
		});
	}


	// ───────────────────────────── four-tier addressing ─────────────────────────────

	{
		const store = make("address-default");
		store.create("value", 1);

		bench("SIDIUS · get · default four-tier address", () => {
			store.get("value");
		});
	}

	{
		const store = make("address-explicit");
		store.create("value", 1, r, m, s);

		bench("SIDIUS · get · explicit four-tier address", () => {
			store.get("value", r, m, s);
		});
	}

	{
		const store = make("address-variable");
		const roots = ["root-a", "root-b"];
		const masters = ["master-a", "master-b"];
		const slaves = ["slave-a", "slave-b"];

		for (let i = 0; i < 2; i++) {
			store.create("value", i, roots[i], masters[i], slaves[i]);
		}

		let index = 0;

		bench("SIDIUS · get · varying four-tier addresses", () => {
			const i = index++ & 1;
			store.get("value", roots[i], masters[i], slaves[i]);
		});
	}
}
else if (FIGHT === "PALPATINE") {
	const r = "bench-root";
	const m = "bench-master";
	const s = "bench-slave";

	const make = (k: string) => new PALPATINE(r, m, s, k);

	let n = 0;

	// ───────────────────────────── reads ─────────────────────────────

	{
		const store = make("get");
		store.create("value", 1);

		bench("PALPATINE · get · existing cell", () => {
			store.get("value");
		});
	}

	{
		const store = make("peek");
		store.create("value", 1);

		bench("PALPATINE · peek · existing cell", () => {
			store.peek("value");
		});
	}

	{
		const store = make("has");
		store.create("value", 1);

		bench("PALPATINE · has · existing cell", () => {
			store.has("value");
		});
	}

	{
		const store = make("has-missing");

		bench("PALPATINE · has · missing cell", () => {
			store.has("missing");
		});
	}

	{
		const store = make("has-undefined");
		store.create("value", undefined);

		bench("PALPATINE · has · undefined value", () => {
			store.has("value");
		});
	}

	{
		const store = make("get-many");
		const keys = Array.from({ length: 16 }, (_, i) => `k${i}`);

		for (let i = 0; i < keys.length; i++) store.create(keys[i], i);

		bench("PALPATINE · getMany · 16 keys + result allocation", () => {
			store.getMany(keys);
		});
	}


	// ───────────────────────────── writes ─────────────────────────────

	{
		const store = make("set");
		store.create("value", 0);
		let value = 0;

		bench("PALPATINE · set · changed value", () => {
			store.set("value", ++value);
		});
	}

	{
		const store = make("set-same");
		store.create("value", 1);

		bench("PALPATINE · set · same value", () => {
			store.set("value", 1);
		});
	}

	{
		const store = make("write");
		store.create("value", 0);
		let value = 0;

		bench("PALPATINE · write · changed value", () => {
			store.write("value", ++value);
		});
	}

	{
		const store = make("swap");
		store.create("value", 0);
		let value = 0;

		bench("PALPATINE · swap · changed value", () => {
			store.swap("value", () => ++value);
		});
	}

	{
		const store = make("edit");
		store.create("value", 0);
		let value = 0;

		bench("PALPATINE · edit · changed value", () => {
			store.edit("value", () => ++value);
		});
	}

	{
		const store = make("set-many-8");
		const keys = Array.from({ length: 8 }, (_, i) => `k${i}`);
		const values = Array.from({ length: 8 }, () => 0);

		for (let i = 0; i < keys.length; i++) store.create(keys[i], 0);

		bench("PALPATINE · setMany · 8 individual writes", () => {
			for (let i = 0; i < values.length; i++) values[i] = ++n;
			store.setMany(keys, values);
		});
	}


	// ───────────────────────────── handles ─────────────────────────────

	{
		const store = make("signal-value");
		const signal = store.signal("value", 0);

		bench("PALPATINE · signal.value · read", () => {
			const value = signal.value;
		});
	}

	{
		const store = make("signal-current");
		const signal = store.signal("value", 0);

		bench("PALPATINE · signal.current · read", () => {
			const value = signal.current;
		});
	}

	{
		const store = make("signal-peek");
		const signal = store.signal("value", 0);

		bench("PALPATINE · signal.peek · read", () => {
			const value = signal.peek;
		});
	}

	{
		const store = make("signal-set");
		const signal = store.signal("value", 0);
		let value = 0;

		bench("PALPATINE · signal.set · changed value", () => {
			signal.set(++value);
		});
	}

	{
		const store = make("signal-update");
		const signal = store.signal("value", 0);
		let value = 0;

		bench("PALPATINE · signal.update · changed value", () => {
			signal.update(() => ++value);
		});
	}

	{
		const store = make("atom-value");
		const atom = store.atom("value", 0);

		bench("PALPATINE · atom.value · read", () => {
			const value = atom.value;
		});
	}

	// ───────────────────────────── subscriptions ─────────────────────────────

	{
		const store = make("sub-0");
		store.create("value", 0);
		let value = 0;

		bench("PALPATINE · set · zero subscribers", () => {
			store.set("value", ++value);
		});
	}

	{
		const store = make("sub-1");
		store.create("value", 0);
		store.sub("value", () => {});
		let value = 0;

		bench("PALPATINE · set · 1 subscriber", () => {
			store.set("value", ++value);
		});
	}

	{
		const store = make("sub-16");
		store.create("value", 0);

		for (let i = 0; i < 16; i++) store.sub("value", () => {});

		let value = 0;

		bench("PALPATINE · set · 16 subscribers", () => {
			store.set("value", ++value);
		});
	}


	{
		const store = make("subMany-8");
		const keys = Array.from({ length: 8 }, (_, i) => `k${i}`);

		for (let i = 0; i < keys.length; i++) store.create(keys[i], 0);

		store.subMany(keys, () => {});
		let value = 0;

		bench("PALPATINE · setMany · 8 subMany listeners", () => {
			const next = ++value;

			for (let i = 0; i < keys.length; i++) {
				store.set(keys[i], next);
			}
		});
	}

	{
		const store = make("subAll-16");
		const keys = Array.from({ length: 16 }, (_, i) => `k${i}`);

		for (let i = 0; i < keys.length; i++) store.create(keys[i], 0);

		store.subAll(() => {});
		let value = 0;

		bench("PALPATINE · set · subAll snapshot · 16 keys", () => {
			store.set(keys[value & 15], ++value);
		});
	}

	{
		const store = make("sub-register");
		store.create("value", 0);

		bench("PALPATINE · sub · register + dispose", () => {
			const off = store.sub("value", () => {});
			off();
		});
	}

	// ───────────────────────────── effects ─────────────────────────────

	{
		const store = make("effect-no-deps");

		bench("PALPATINE · effect · create + initial run + dispose · no deps", () => {
			const off = store.effect(() => {});
			off();
		});
	}

	{
		const store = make("effect-one-dep");
		store.create("value", 0);

		bench("PALPATINE · effect · create + initial tracked read + dispose", () => {
			const off = store.effect(() => {
				store.get("value");
			});
			off();
		});
	}

	{
		const store = make("effect-1-write");
		store.create("value", 0);

		store.effect(() => {
			store.get("value");
		});

		let value = 0;

		bench("PALPATINE · effect · 1 dependency · write + rerun", () => {
			store.set("value", ++value);
		});
	}

	{
		const store = make("effect-8-write");
		const keys = Array.from({ length: 8 }, (_, i) => `k${i}`);

		for (let i = 0; i < keys.length; i++) store.create(keys[i], 0);

		store.effect(() => {
			for (let i = 0; i < keys.length; i++) store.get(keys[i]);
		});

		let value = 0;

		bench("PALPATINE · effect · 8 dependencies · write + rerun", () => {
			store.set(keys[value & 7], ++value);
		});
	}



	// ───────────────────────────── lifecycle ─────────────────────────────

	{
		const store = make("create");
		let value = 0;

		bench("PALPATINE · create · new cell", () => {
			store.create(`k${++value}`, value);
		});
	}

	{
		const store = make("delete");
		store.create("value", 0);

		bench("PALPATINE · delete · existing cell", () => {
			store.delete("value");
			store.create("value", 0);
		});
	}

	{
		const store = make("clear");
		const keys = Array.from({ length: 16 }, (_, i) => `k${i}`);
		let value = 0;

		bench("PALPATINE · clear · 16 cells + recreate setup", () => {
			for (let i = 0; i < keys.length; i++) {
				store.create(keys[i], ++value);
			}

			store.clear();
		});
	}

	// ───────────────────────────── iteration ─────────────────────────────

	{
		const store = make("entries");
		const keys = Array.from({ length: 16 }, (_, i) => `k${i}`);

		for (let i = 0; i < keys.length; i++) store.create(keys[i], i);

		bench("PALPATINE · entries · iterate 16 cells", () => {
			for (const entry of store.entries()) {
				void entry;
			}
		});
	}


	// ───────────────────────────── four-tier addressing ─────────────────────────────

	{
		const store = make("address-default");
		store.create("value", 1);

		bench("PALPATINE · get · default four-tier address", () => {
			store.get("value");
		});
	}

	{
		const store = make("address-explicit");
		store.create("value", 1, r, m, s);

		bench("PALPATINE · get · explicit four-tier address", () => {
			store.get("value", r, m, s);
		});
	}

	{
		const store = make("address-variable");
		const roots = ["root-a", "root-b"];
		const masters = ["master-a", "master-b"];
		const slaves = ["slave-a", "slave-b"];

		for (let i = 0; i < 2; i++) {
			store.create("value", i, roots[i], masters[i], slaves[i]);
		}

		let index = 0;

		bench("PALPATINE · get · varying four-tier addresses", () => {
			const i = index++ & 1;
			store.get("value", roots[i], masters[i], slaves[i]);
		});
	}
}
else if (FIGHT === "JEDI") {
	const bus = new SIGNALS();
	const jedi = new JEDI<number>(bus, {
		r: "JEDI_BENCH_ROOT",
		m: "JEDI_BENCH_MASTER",
		s: "JEDI_BENCH_SLAVE",
		k: "JEDI_BENCH",
	});

	let n = 0;

	// READ

	jedi.set("read", 1);
	bench("JEDI.get", () => {
		jedi.get("read");
	});

	bench("JEDI.get.hot", () => {
		jedi.get.hot("read");
	});

	bench("JEDI.read", () => {
		jedi.read("read");
	});

	bench("JEDI.peek", () => {
		jedi.peek("read");
	});

	bench("JEDI.has", () => {
		jedi.has("read");
	});

	bench("JEDI.get.missing", () => {
		jedi.get("missing");
	});

	bench("JEDI.getMany.8", () => {
		jedi.getMany(["read", "r1", "r2", "r3", "r4", "r5", "r6", "r7"]);
	});


	// WRITE

	bench("JEDI.set", () => {
		jedi.set("write", ++n);
	});

	bench("JEDI.set.same", () => {
		jedi.set("same", 1);
	});

	bench("JEDI.write", () => {
		jedi.write("write", ++n);
	});

	bench("JEDI.swap", () => {
		jedi.swap("swap", ++n);
	});

	bench("JEDI.edit", () => {
		jedi.edit("edit", (v) => (v ?? 0) + 1);
	});

	bench("JEDI.set.functional", () => {
		jedi.set("functional", (v) => (v ?? 0) + 1);
	});

	bench("JEDI.setMany.8", () => {
		const v = ++n;
		jedi.setMany({
			a0: v, a1: v, a2: v, a3: v,
			a4: v, a5: v, a6: v, a7: v,
		});
	});

	// CREATE / DELETE

	bench("JEDI.create.existing", () => {
		jedi.create("read", 1);
	});




	// SUBSCRIPTIONS

	jedi.set("sub", 0);
	let subSink = 0;

	const sub1 = jedi.sub("sub", (value) => {
		subSink += value;
	});

	bench("JEDI.set.sub.1", () => {
		jedi.set("sub", ++n);
	});

	sub1();

	const sub8Keys = Array.from({ length: 8 }, (_, i) => `sub8-${i}`);
	for (const key of sub8Keys) jedi.set(key, 0);

	const sub8 = jedi.subMany(sub8Keys, (value) => {
		subSink += value;
	});

	bench("JEDI.set.subMany.8", () => {
		const v = ++n;
		for (const key of sub8Keys) jedi.set(key, v);
	});

	sub8();

	const sub64Keys = Array.from({ length: 64 }, (_, i) => `sub64-${i}`);
	for (const key of sub64Keys) jedi.set(key, 0);

	const sub64 = jedi.subMany(sub64Keys, (value) => {
		subSink += value;
	});


	jedi.set("sub-register", 0);

	bench("JEDI.sub.register-dispose", () => {
		jedi.sub("sub-register", () => {})();
	});

	// SIGNAL

	const signal = jedi.signal<number>("signal", { initial: 0 });

	bench("JEDI.signal.get", () => {
		signal.get();
	});

	bench("JEDI.signal.peek", () => {
		signal.peek();
	});

	bench("JEDI.signal.set", () => {
		signal.set(++n);
	});

	bench("JEDI.signal.set.same", () => {
		signal.set(signal.peek());
	});

	bench("JEDI.signal.update", () => {
		signal.update((v) => v + 1);
	});

	// ATOM

	const atom = jedi.atom<number>("atom", { initial: 0 });

	bench("JEDI.atom.get", () => {
		atom.get();
	});

	bench("JEDI.atom.peek", () => {
		atom.peek();
	});

	bench("JEDI.atom.value", () => {
		const value = atom.value;
		void value;
	});

	bench("JEDI.atom.value.set", () => {
		atom.value = ++n;
	});

	bench("JEDI.atom.update", () => {
		atom.update((v) => v + 1);
	});

	// COLLECTIONS

	const collection8: Record<string, number> = Object.create(null);
	for (let i = 0; i < 8; i++) collection8[`c${i}`] = i;


	jedi.setMany(collection8);

	bench("JEDI.setMany.8", () => {
		const v = ++n;
		jedi.setMany({
			c0: v, c1: v, c2: v, c3: v,
			c4: v, c5: v, c6: v, c7: v,
		});
	});



	bench("JEDI.getAll", () => {
		jedi.getAll();
	});

	bench("JEDI.getByPrefix", () => {
		jedi.getByPrefix("c");
	});

	bench("JEDI.entries", () => {
		for (const entry of jedi.entries()) {
			subSink += entry[1];
		}
	});

	// ADDRESSING

	const address = new JEDI<number>(bus, {
		r: "JEDI_ADDRESS_ROOT",
		m: "JEDI_ADDRESS_MASTER",
		s: "JEDI_ADDRESS_SLAVE",
		k: "JEDI_ADDRESS",
	});

	address.set("address", 1);

	bench("JEDI.address.default", () => {
		jedi.set("address", ++n);
	});

	bench("JEDI.address.explicit", () => {
		address.set("address", ++n);
	});

	// CLEANUP / LIFECYCLE

	const clearKeys = Array.from({ length: 16 }, (_, i) => `clear-${i}`);
	for (const key of clearKeys) jedi.set(key, 1);

	bench("JEDI.clear.16", () => {
		for (const key of clearKeys) jedi.set(key, ++n);
		jedi.clear();
		for (const key of clearKeys) jedi.set(key, 1);
	});

	void subSink;
}
else if (FIGHT === "alien") {
	// 1. Signal read
	const read = signal(1);

	bench("ALIEN.signal.read", () => {
		read();
	});

	// 2. Signal write
	const write = signal(0);
	let writeValue = 0;

	bench("ALIEN.signal.write", () => {
		writeValue ^= 1;
		write(writeValue);
	});

	// 3. Signal same-value write
	const sameWrite = signal(1);

	bench("ALIEN.signal.write.same", () => {
		sameWrite(1);
	});

	// 4. Signal write + effect notification
	const notifySource = signal(0);
	let notifyRuns = 0;

	const disposeNotify = effect(() => {
		notifySource();
		notifyRuns++;
	});

	let notifyWrite = 0;

	bench("ALIEN.signal.write.notify", () => {
		notifyWrite ^= 1;
		notifySource(notifyWrite);
	});

	// 5. Fan-out: one signal, 1 dependent effect
	const fanout1Source = signal(0);
	let fanout1Runs = 0;

	const disposeFanout1 = effect(() => {
		fanout1Source();
		fanout1Runs++;
	});

	let fanout1Value = 0;

	bench("ALIEN.signal.fanout.1", () => {
		fanout1Value ^= 1;
		fanout1Source(fanout1Value);
	});

	// 6. Fan-out: one signal, 8 dependent effects
	const fanout8Source = signal(0);
	let fanout8Runs = 0;
	const disposeFanout8: (() => void)[] = [];

	for (let i = 0; i < 8; i++) {
		disposeFanout8.push(effect(() => {
			fanout8Source();
			fanout8Runs++;
		}));
	}

	let fanout8Value = 0;

	bench("ALIEN.signal.fanout.8", () => {
		fanout8Value ^= 1;
		fanout8Source(fanout8Value);
	});

	// 7. Fan-out: one signal, 16 dependent effects
	const fanout16Source = signal(0);
	let fanout16Runs = 0;
	const disposeFanout16: (() => void)[] = [];

	for (let i = 0; i < 16; i++) {
		disposeFanout16.push(effect(() => {
			fanout16Source();
			fanout16Runs++;
		}));
	}

	let fanout16Value = 0;

	bench("ALIEN.signal.fanout.16", () => {
		fanout16Value ^= 1;
		fanout16Source(fanout16Value);
	});




	// 10. Fan-in: one effect, 1 dependent signal
	const fanin1Sources = [signal(0)];
	let fanin1Runs = 0;

	const disposeFanin1 = effect(() => {
		fanin1Sources[0]();
		fanin1Runs++;
	});

	let fanin1Value = 0;

	bench("ALIEN.signal.fanin.1", () => {
		fanin1Value ^= 1;
		fanin1Sources[0](fanin1Value);
	});

	// 11. Fan-in: one effect, 8 dependent signals
	const fanin8Sources = Array.from({ length: 8 }, () => signal(0));
	let fanin8Runs = 0;

	const disposeFanin8 = effect(() => {
		for (let i = 0; i < fanin8Sources.length; i++) {
			fanin8Sources[i]();
		}
		fanin8Runs++;
	});

	let fanin8Value = 0;
	let fanin8Index = 0;

	bench("ALIEN.signal.fanin.8", () => {
		fanin8Value ^= 1;
		fanin8Sources[fanin8Index++ & 7](fanin8Value);
	});

	// 12. Fan-in: one effect, 16 dependent signals
	const fanin16Sources = Array.from({ length: 16 }, () => signal(0));
	let fanin16Runs = 0;

	const disposeFanin16 = effect(() => {
		for (let i = 0; i < fanin16Sources.length; i++) {
			fanin16Sources[i]();
		}
		fanin16Runs++;
	});

	let fanin16Value = 0;
	let fanin16Index = 0;

	bench("ALIEN.signal.fanin.16", () => {
		fanin16Value ^= 1;
		fanin16Sources[fanin16Index++ & 15](fanin16Value);
	});



	// 14. Fan-in: one effect, all 16 signals written
	const faninAllSources = Array.from({ length: 16 }, () => signal(0));
	let faninAllRuns = 0;

	const disposeFaninAll = effect(() => {
		for (let i = 0; i < faninAllSources.length; i++) {
			faninAllSources[i]();
		}
		faninAllRuns++;
	});

	let faninAllValue = 0;

	bench("ALIEN.signal.fanin.16.all-writes", () => {
		faninAllValue ^= 1;
		for (let i = 0; i < faninAllSources.length; i++) {
			faninAllSources[i](faninAllValue);
		}
	});

	// 15. Computed creation
	bench("ALIEN.computed.create", () => {
		computed(() => 1 + 1);
	});

	// 16. Computed read without recomputation
	const cleanSource = signal(1);
	const cleanComputed = computed(() => cleanSource() * 2);
	cleanComputed();

	bench("ALIEN.computed.read.clean", () => {
		cleanComputed();
	});

	// 17. Computed invalidation only
	const invalidateSource = signal(0);
	const invalidateComputed = computed(() => invalidateSource() * 2);
	invalidateComputed();

	let invalidateWrite = 0;

	bench("ALIEN.computed.invalidate", () => {
		invalidateWrite ^= 1;
		invalidateSource(invalidateWrite);
	});

	// 18. Computed invalidation and recomputation
	const computedSource = signal(0);
	const computedValue = computed(() => computedSource() * 2);
	computedValue();

	let computedWrite = 0;

	bench("ALIEN.computed.recompute", () => {
		computedWrite ^= 1;
		computedSource(computedWrite);
		computedValue();
	});

	// 19. Computed with 8 source dependencies
	const computed8Sources = Array.from({ length: 8 }, () => signal(1));
	const computed8 = computed(() => {
		let value = 0;
		for (let i = 0; i < computed8Sources.length; i++) {
			value += computed8Sources[i]();
		}
		return value;
	});

	computed8();

	let computed8Write = 0;

	bench("ALIEN.computed.dependencies.8", () => {
		computed8Write ^= 1;
		computed8Sources[0](computed8Write);
		computed8();
	});

	// 20. Computed with 16 source dependencies
	const computed16Sources = Array.from({ length: 16 }, () => signal(1));
	const computed16 = computed(() => {
		let value = 0;
		for (let i = 0; i < computed16Sources.length; i++) {
			value += computed16Sources[i]();
		}
		return value;
	});

	computed16();

	let computed16Write = 0;

	bench("ALIEN.computed.dependencies.16", () => {
		computed16Write ^= 1;
		computed16Sources[0](computed16Write);
		computed16();
	});



	// 22. Dependency chain: source -> 1 computed node
	const chain1Source = signal(0);
	const chain1 = computed(() => chain1Source() + 1);
	chain1();

	let chain1Write = 0;

	bench("ALIEN.computed.chain.1", () => {
		chain1Write ^= 1;
		chain1Source(chain1Write);
		chain1();
	});

	// 23. Dependency chain: source -> 8 computed nodes
	const chain8Source = signal(0);
	let chain8 = computed(() => chain8Source() + 1);

	for (let i = 1; i < 8; i++) {
		const previous = chain8;
		chain8 = computed(() => previous() + 1);
	}

	chain8();

	let chain8Write = 0;

	bench("ALIEN.computed.chain.8", () => {
		chain8Write ^= 1;
		chain8Source(chain8Write);
		chain8();
	});

	// 24. Dependency chain: source -> 16 computed nodes
	const chain16Source = signal(0);
	let chain16 = computed(() => chain16Source() + 1);

	for (let i = 1; i < 16; i++) {
		const previous = chain16;
		chain16 = computed(() => previous() + 1);
	}

	chain16();

	let chain16Write = 0;

	bench("ALIEN.computed.chain.16", () => {
		chain16Write ^= 1;
		chain16Source(chain16Write);
		chain16();
	});

	// 26. Batched writes with one dependent effect: 2 signals
	const batch2A = signal(0);
	const batch2B = signal(0);
	let batch2Runs = 0;

	const disposeBatch2 = effect(() => {
		batch2A();
		batch2B();
		batch2Runs++;
	});

	let batch2Value = 0;

	bench("ALIEN.batch.two-writes", () => {
		batch2Value ^= 1;
		startBatch();
		try {
			batch2A(batch2Value);
			batch2B(batch2Value);
		} finally {
			endBatch();
		}
	});

	// 27. Batched writes with one dependent effect: 8 signals
	const batch8Sources = Array.from({ length: 8 }, () => signal(0));
	let batch8Runs = 0;

	const disposeBatch8 = effect(() => {
		for (let i = 0; i < batch8Sources.length; i++) {
			batch8Sources[i]();
		}
		batch8Runs++;
	});

	let batch8Value = 0;

	bench("ALIEN.batch.writes.8", () => {
		batch8Value ^= 1;
		startBatch();
		try {
			for (let i = 0; i < batch8Sources.length; i++) {
				batch8Sources[i](batch8Value);
			}
		} finally {
			endBatch();
		}
	});

	// 28. Batched writes with one dependent effect: 16 signals
	const batch16Sources = Array.from({ length: 16 }, () => signal(0));
	let batch16Runs = 0;

	const disposeBatch16 = effect(() => {
		for (let i = 0; i < batch16Sources.length; i++) {
			batch16Sources[i]();
		}
		batch16Runs++;
	});

	let batch16Value = 0;

	bench("ALIEN.batch.writes.16", () => {
		batch16Value ^= 1;
		startBatch();
		try {
			for (let i = 0; i < batch16Sources.length; i++) {
				batch16Sources[i](batch16Value);
			}
		} finally {
			endBatch();
		}
	});

	// 29. Unbatched writes with one dependent effect: 2 signals
	const unbatched2A = signal(0);
	const unbatched2B = signal(0);
	let unbatched2Runs = 0;

	const disposeUnbatched2 = effect(() => {
		unbatched2A();
		unbatched2B();
		unbatched2Runs++;
	});

	let unbatched2Value = 0;

	bench("ALIEN.unbatched.two-writes", () => {
		unbatched2Value ^= 1;
		unbatched2A(unbatched2Value);
		unbatched2B(unbatched2Value);
	});

	// 30. Effect creation without dependencies
	bench("ALIEN.effect.create.empty", () => {
		const dispose = effect(() => {});
		dispose();
	});

	// 31. Effect creation, dependency tracking, and disposal: 1 dependency
	const lifecycle1Source = signal(0);

	bench("ALIEN.effect.create.dispose.1", () => {
		const dispose = effect(() => {
			lifecycle1Source();
		});
		dispose();
	});

	// 32. Effect creation, dependency tracking, and disposal: 8 dependencies
	const lifecycle8Sources = Array.from({ length: 8 }, () => signal(0));

	bench("ALIEN.effect.create.dispose.8", () => {
		const dispose = effect(() => {
			for (let i = 0; i < lifecycle8Sources.length; i++) {
				lifecycle8Sources[i]();
			}
		});
		dispose();
	});


	// 34. Effect rerun
	const rerunSource = signal(0);
	let rerunRuns = 0;

	const disposeRerun = effect(() => {
		rerunSource();
		rerunRuns++;
	});

	let rerunValue = 0;

	bench("ALIEN.effect.rerun", () => {
		rerunValue ^= 1;
		rerunSource(rerunValue);
	});

	// 35. Effect rerun with cleanup
	const cleanupSource = signal(0);
	let cleanupRuns = 0;
	let cleanupCalls = 0;

	const disposeCleanup = effect(() => {
		cleanupSource();
		cleanupRuns++;

		return () => {
			cleanupCalls++;
		};
	});

	let cleanupValue = 0;

	bench("ALIEN.effect.cleanup.rerun", () => {
		cleanupValue ^= 1;
		cleanupSource(cleanupValue);
	});

	// 36. Effect rerun with 8 dependencies
	const rerun8Sources = Array.from({ length: 8 }, () => signal(0));
	let rerun8Runs = 0;

	const disposeRerun8 = effect(() => {
		for (let i = 0; i < rerun8Sources.length; i++) {
			rerun8Sources[i]();
		}
		rerun8Runs++;
	});

	let rerun8Value = 0;

	bench("ALIEN.effect.rerun.8", () => {
		rerun8Value ^= 1;
		rerun8Sources[0](rerun8Value);
	});


	// 38. Effect disposal
	const disposalSource = signal(0);
	const disposeTarget = effect(() => {
		disposalSource();
	});

	bench("ALIEN.effect.dispose", () => {
		const dispose = effect(() => {
			disposalSource();
		});
		dispose();
	});

	// Keep effects alive throughout their respective measurements.
	void disposeNotify;
	void disposeFanout1;
	void disposeFanout8;
	void disposeFanout16;
	void disposeFanin1;
	void disposeFanin8;
	void disposeFanin16;
	void disposeFaninAll;
	void disposeBatch2;
	void disposeBatch8;
	void disposeBatch16;
	void disposeUnbatched2;
	void disposeRerun;
	void disposeCleanup;
	void disposeRerun8;
	void disposeTarget;

	void notifyRuns;
	void fanout1Runs;
	void fanout8Runs;
	void fanout16Runs;
	void fanin1Runs;
	void fanin8Runs;
	void fanin16Runs;
	void faninAllRuns;
	void batch2Runs;
	void batch8Runs;
	void batch16Runs;
	void unbatched2Runs;
	void rerunRuns;
	void cleanupRuns;
	void cleanupCalls;
	void rerun8Runs;
}
else if (FIGHT === "preact") {
	// 1. Signal read
	const read = preact_signal(1);

	bench("PREACT_SIGNALS.signal.read", () => {
		read.value;
	});

	// 2. Signal write
	const write = preact_signal(0);
	let writeValue = 0;

	bench("PREACT_SIGNALS.signal.write", () => {
		writeValue ^= 1;
		write.value = writeValue;
	});

	// 3. Signal same-value write
	const sameWrite = preact_signal(1);

	bench("PREACT_SIGNALS.signal.write.same", () => {
		sameWrite.value = 1;
	});

	// 4. Signal write + effect notification
	const notifySource = preact_signal(0);
	let notifyRuns = 0;

	const disposeNotify = preact_effect(() => {
		notifySource.value;
		notifyRuns++;
	});

	let notifyValue = 0;

	bench("PREACT_SIGNALS.signal.write.notify", () => {
		notifyValue ^= 1;
		notifySource.value = notifyValue;
	});

	// 5. Fan-out: one signal, 1 dependent effect
	const fanout1Source = preact_signal(0);
	let fanout1Runs = 0;

	const disposeFanout1 = preact_effect(() => {
		fanout1Source.value;
		fanout1Runs++;
	});

	let fanout1Value = 0;

	bench("PREACT_SIGNALS.signal.fanout.1", () => {
		fanout1Value ^= 1;
		fanout1Source.value = fanout1Value;
	});

	// 6. Fan-out: one signal, 8 dependent effects
	const fanout8Source = preact_signal(0);
	let fanout8Runs = 0;
	const disposeFanout8: (() => void)[] = [];

	for (let i = 0; i < 8; i++) {
		disposeFanout8.push(preact_effect(() => {
			fanout8Source.value;
			fanout8Runs++;
		}));
	}

	let fanout8Value = 0;

	bench("PREACT_SIGNALS.signal.fanout.8", () => {
		fanout8Value ^= 1;
		fanout8Source.value = fanout8Value;
	});

	// 7. Fan-out: one signal, 16 dependent effects
	const fanout16Source = preact_signal(0);
	let fanout16Runs = 0;
	const disposeFanout16: (() => void)[] = [];

	for (let i = 0; i < 16; i++) {
		disposeFanout16.push(preact_effect(() => {
			fanout16Source.value;
			fanout16Runs++;
		}));
	}

	let fanout16Value = 0;

	bench("PREACT_SIGNALS.signal.fanout.16", () => {
		fanout16Value ^= 1;
		fanout16Source.value = fanout16Value;
	});

	// 8. Fan-out: one signal, 64 dependent effects
	const fanout64Source = preact_signal(0);
	let fanout64Runs = 0;
	const disposeFanout64: (() => void)[] = [];

	for (let i = 0; i < 64; i++) {
		disposeFanout64.push(preact_effect(() => {
			fanout64Source.value;
			fanout64Runs++;
		}));
	}

	let fanout64Value = 0;

	bench("PREACT_SIGNALS.signal.fanout.64", () => {
		fanout64Value ^= 1;
		fanout64Source.value = fanout64Value;
	});



	// 10. Fan-in: one effect, 1 dependent signal
	const fanin1Sources = [preact_signal(0)];
	let fanin1Runs = 0;

	const disposeFanin1 = preact_effect(() => {
		fanin1Sources[0].value;
		fanin1Runs++;
	});

	let fanin1Value = 0;

	bench("PREACT_SIGNALS.signal.fanin.1", () => {
		fanin1Value ^= 1;
		fanin1Sources[0].value = fanin1Value;
	});

	// 11. Fan-in: one effect, 8 dependent signals
	const fanin8Sources = Array.from({ length: 8 }, () => preact_signal(0));
	let fanin8Runs = 0;

	const disposeFanin8 = preact_effect(() => {
		for (let i = 0; i < fanin8Sources.length; i++) {
			fanin8Sources[i].value;
		}
		fanin8Runs++;
	});

	let fanin8Value = 0;
	let fanin8Index = 0;

	bench("PREACT_SIGNALS.signal.fanin.8", () => {
		fanin8Value ^= 1;
		fanin8Sources[fanin8Index++ & 7].value = fanin8Value;
	});

	// 12. Fan-in: one effect, 16 dependent signals
	const fanin16Sources = Array.from({ length: 16 }, () => preact_signal(0));
	let fanin16Runs = 0;

	const disposeFanin16 = preact_effect(() => {
		for (let i = 0; i < fanin16Sources.length; i++) {
			fanin16Sources[i].value;
		}
		fanin16Runs++;
	});

	let fanin16Value = 0;
	let fanin16Index = 0;

	bench("PREACT_SIGNALS.signal.fanin.16", () => {
		fanin16Value ^= 1;
		fanin16Sources[fanin16Index++ & 15].value = fanin16Value;
	});



	// 14. Fan-in: one effect, all 16 signals written
	const faninAllSources = Array.from({ length: 16 }, () => preact_signal(0));
	let faninAllRuns = 0;

	const disposeFaninAll = preact_effect(() => {
		for (let i = 0; i < faninAllSources.length; i++) {
			faninAllSources[i].value;
		}
		faninAllRuns++;
	});

	let faninAllValue = 0;

	bench("PREACT_SIGNALS.signal.fanin.16.all-writes", () => {
		faninAllValue ^= 1;

		for (let i = 0; i < faninAllSources.length; i++) {
			faninAllSources[i].value = faninAllValue;
		}
	});

	// 15. Computed read without recomputation
	const cleanSource = preact_signal(1);
	const cleanComputed = preact_computed(() => cleanSource.value * 2);
	cleanComputed.value;

	bench("PREACT_SIGNALS.computed.read.clean", () => {
		cleanComputed.value;
	});

	// 16. Computed invalidation only
	const invalidateSource = preact_signal(0);
	const invalidateComputed = preact_computed(() => invalidateSource.value * 2);
	invalidateComputed.value;

	let invalidateValue = 0;

	bench("PREACT_SIGNALS.computed.invalidate", () => {
		invalidateValue ^= 1;
		invalidateSource.value = invalidateValue;
	});

	// 17. Computed invalidation and recomputation
	const computedSource = preact_signal(0);
	const computedValue = preact_computed(() => computedSource.value * 2);
	computedValue.value;

	let computedWrite = 0;

	bench("PREACT_SIGNALS.computed.recompute", () => {
		computedWrite ^= 1;
		computedSource.value = computedWrite;
		computedValue.value;
	});

	// 18. Computed with 8 source dependencies
	const computed8Sources = Array.from({ length: 8 }, () => preact_signal(1));
	const computed8 = preact_computed(() => {
		let value = 0;

		for (let i = 0; i < computed8Sources.length; i++) {
			value += computed8Sources[i].value;
		}

		return value;
	});

	computed8.value;

	let computed8Write = 0;

	bench("PREACT_SIGNALS.computed.dependencies.8", () => {
		computed8Write ^= 1;
		computed8Sources[0].value = computed8Write;
		computed8.value;
	});

	// 19. Computed with 16 source dependencies
	const computed16Sources = Array.from({ length: 16 }, () => preact_signal(1));
	const computed16 = preact_computed(() => {
		let value = 0;

		for (let i = 0; i < computed16Sources.length; i++) {
			value += computed16Sources[i].value;
		}

		return value;
	});

	computed16.value;

	let computed16Write = 0;

	bench("PREACT_SIGNALS.computed.dependencies.16", () => {
		computed16Write ^= 1;
		computed16Sources[0].value = computed16Write;
		computed16.value;
	});

	// 20. Computed with 64 source dependencies
	const computed64Sources = Array.from({ length: 64 }, () => preact_signal(1));
	const computed64 = preact_computed(() => {
		let value = 0;

		for (let i = 0; i < computed64Sources.length; i++) {
			value += computed64Sources[i].value;
		}

		return value;
	});

	computed64.value;

	let computed64Write = 0;

	bench("PREACT_SIGNALS.computed.dependencies.64", () => {
		computed64Write ^= 1;
		computed64Sources[0].value = computed64Write;
		computed64.value;
	});

	// 21. Dependency chain: source -> 1 computed node
	const chain1Source = preact_signal(0);
	const chain1 = preact_computed(() => chain1Source.value + 1);
	chain1.value;

	let chain1Write = 0;

	bench("PREACT_SIGNALS.computed.chain.1", () => {
		chain1Write ^= 1;
		chain1Source.value = chain1Write;
		chain1.value;
	});

	// 22. Dependency chain: source -> 8 computed nodes
	const chainSource = preact_signal(0);
	let chainValue = preact_computed(() => chainSource.value + 1);

	for (let i = 1; i < 8; i++) {
		const previous = chainValue;
		chainValue = preact_computed(() => previous.value + 1);
	}

	chainValue.value;

	let chainWrite = 0;

	bench("PREACT_SIGNALS.computed.chain.8", () => {
		chainWrite ^= 1;
		chainSource.value = chainWrite;
		chainValue.value;
	});

	// 23. Dependency chain: source -> 16 computed nodes
	const chain16Source = preact_signal(0);
	let chain16Value = preact_computed(() => chain16Source.value + 1);

	for (let i = 1; i < 16; i++) {
		const previous = chain16Value;
		chain16Value = preact_computed(() => previous.value + 1);
	}

	chain16Value.value;

	let chain16Write = 0;

	bench("PREACT_SIGNALS.computed.chain.16", () => {
		chain16Write ^= 1;
		chain16Source.value = chain16Write;
		chain16Value.value;
	});

	// 24. Dependency chain: source -> 64 computed nodes
	const chain64Source = preact_signal(0);
	let chain64Value = preact_computed(() => chain64Source.value + 1);

	for (let i = 1; i < 64; i++) {
		const previous = chain64Value;
		chain64Value = preact_computed(() => previous.value + 1);
	}

	chain64Value.value;

	let chain64Write = 0;

	bench("PREACT_SIGNALS.computed.chain.64", () => {
		chain64Write ^= 1;
		chain64Source.value = chain64Write;
		chain64Value.value;
	});

	// 25. Batched writes with one dependent effect: 2 signals
	const batch2A = preact_signal(0);
	const batch2B = preact_signal(0);
	let batch2Runs = 0;

	const disposeBatch2 = preact_effect(() => {
		batch2A.value;
		batch2B.value;
		batch2Runs++;
	});

	let batch2Value = 0;

	bench("PREACT_SIGNALS.batch.two-writes", () => {
		batch2Value ^= 1;

		preact_batch(() => {
			batch2A.value = batch2Value;
			batch2B.value = batch2Value;
		});
	});

	// 26. Batched writes with one dependent effect: 8 signals
	const batch8Sources = Array.from({ length: 8 }, () => preact_signal(0));
	let batch8Runs = 0;

	const disposeBatch8 = preact_effect(() => {
		for (let i = 0; i < batch8Sources.length; i++) {
			batch8Sources[i].value;
		}
		batch8Runs++;
	});

	let batch8Value = 0;

	bench("PREACT_SIGNALS.batch.writes.8", () => {
		batch8Value ^= 1;

		preact_batch(() => {
			for (let i = 0; i < batch8Sources.length; i++) {
				batch8Sources[i].value = batch8Value;
			}
		});
	});

	// 27. Batched writes with one dependent effect: 16 signals
	const batch16Sources = Array.from({ length: 16 }, () => preact_signal(0));
	let batch16Runs = 0;

	const disposeBatch16 = preact_effect(() => {
		for (let i = 0; i < batch16Sources.length; i++) {
			batch16Sources[i].value;
		}
		batch16Runs++;
	});

	let batch16Value = 0;

	bench("PREACT_SIGNALS.batch.writes.16", () => {
		batch16Value ^= 1;

		preact_batch(() => {
			for (let i = 0; i < batch16Sources.length; i++) {
				batch16Sources[i].value = batch16Value;
			}
		});
	});

	// 28. Unbatched writes with one dependent effect: 2 signals
	const unbatched2A = preact_signal(0);
	const unbatched2B = preact_signal(0);
	let unbatched2Runs = 0;

	const disposeUnbatched2 = preact_effect(() => {
		unbatched2A.value;
		unbatched2B.value;
		unbatched2Runs++;
	});

	let unbatched2Value = 0;

	bench("PREACT_SIGNALS.unbatched.two-writes", () => {
		unbatched2Value ^= 1;
		unbatched2A.value = unbatched2Value;
		unbatched2B.value = unbatched2Value;
	});

	// 29. Effect creation without dependencies
	bench("PREACT_SIGNALS.effect.create.dispose.empty", () => {
		const dispose = preact_effect(() => {});
		dispose();
	});

	// 30. Effect creation, dependency tracking, and disposal: 1 dependency
	const lifecycle1Source = preact_signal(0);

	bench("PREACT_SIGNALS.effect.create.dispose.1", () => {
		const dispose = preact_effect(() => {
			lifecycle1Source.value;
		});

		dispose();
	});

	// 31. Effect creation, dependency tracking, and disposal: 8 dependencies
	const lifecycle8Sources = Array.from({ length: 8 }, () => preact_signal(0));

	bench("PREACT_SIGNALS.effect.create.dispose.8", () => {
		const dispose = preact_effect(() => {
			for (let i = 0; i < lifecycle8Sources.length; i++) {
				lifecycle8Sources[i].value;
			}
		});

		dispose();
	});

	// 32. Effect creation, dependency tracking, and disposal: 64 dependencies
	const lifecycle64Sources = Array.from({ length: 64 }, () => preact_signal(0));

	bench("PREACT_SIGNALS.effect.create.dispose.64", () => {
		const dispose = preact_effect(() => {
			for (let i = 0; i < lifecycle64Sources.length; i++) {
				lifecycle64Sources[i].value;
			}
		});

		dispose();
	});

	// 33. Effect rerun
	const rerunSource = preact_signal(0);
	let rerunRuns = 0;

	const disposeRerun = preact_effect(() => {
		rerunSource.value;
		rerunRuns++;
	});

	let rerunValue = 0;

	bench("PREACT_SIGNALS.effect.rerun", () => {
		rerunValue ^= 1;
		rerunSource.value = rerunValue;
	});

	// 34. Effect rerun with cleanup
	const cleanupSource = preact_signal(0);
	let cleanupRuns = 0;
	let cleanupCalls = 0;

	const disposeCleanup = preact_effect(() => {
		cleanupSource.value;
		cleanupRuns++;

		return () => {
			cleanupCalls++;
		};
	});

	let cleanupValue = 0;

	bench("PREACT_SIGNALS.effect.cleanup.rerun", () => {
		cleanupValue ^= 1;
		cleanupSource.value = cleanupValue;
	});

	// 35. Effect rerun with 8 dependencies
	const rerun8Sources = Array.from({ length: 8 }, () => preact_signal(0));
	let rerun8Runs = 0;

	const disposeRerun8 = preact_effect(() => {
		for (let i = 0; i < rerun8Sources.length; i++) {
			rerun8Sources[i].value;
		}
		rerun8Runs++;
	});

	let rerun8Value = 0;

	bench("PREACT_SIGNALS.effect.rerun.8", () => {
		rerun8Value ^= 1;
		rerun8Sources[0].value = rerun8Value;
	});

	// 36. Effect rerun with 64 dependencies
	const rerun64Sources = Array.from({ length: 64 }, () => preact_signal(0));
	let rerun64Runs = 0;

	const disposeRerun64 = preact_effect(() => {
		for (let i = 0; i < rerun64Sources.length; i++) {
			rerun64Sources[i].value;
		}
		rerun64Runs++;
	});

	let rerun64Value = 0;

	bench("PREACT_SIGNALS.effect.rerun.64", () => {
		rerun64Value ^= 1;
		rerun64Sources[0].value = rerun64Value;
	});

	// 37. Effect disposal
	const disposalSource = preact_signal(0);
	const disposeTarget = preact_effect(() => {
		disposalSource.value;
	});

	bench("PREACT_SIGNALS.effect.dispose", () => {
		const dispose = preact_effect(() => {
			disposalSource.value;
		});

		dispose();
	});

	void preact_untracked;

	void disposeNotify;
	void disposeFanout1;
	void disposeFanout8;
	void disposeFanout16;
	void disposeFanout64;
	void disposeFanin1;
	void disposeFanin8;
	void disposeFanin16;
	void disposeFaninAll;
	void disposeBatch2;
	void disposeBatch8;
	void disposeBatch16;
	void disposeUnbatched2;
	void disposeRerun;
	void disposeCleanup;
	void disposeRerun8;
	void disposeRerun64;
	void disposeTarget;

	void notifyRuns;
	void fanout1Runs;
	void fanout8Runs;
	void fanout16Runs;
	void fanout64Runs;
	void fanin1Runs;
	void fanin8Runs;
	void fanin16Runs;
	void faninAllRuns;
	void batch2Runs;
	void batch8Runs;
	void batch16Runs;
	void unbatched2Runs;
	void rerunRuns;
	void cleanupRuns;
	void cleanupCalls;
	void rerun8Runs;
	void rerun64Runs;
}
else if (FIGHT === "alien2") {
	// Real-world store: keyed cells, reactive per key.
	const store = createStore();
	const HOT = "hot";            // the key every dev has a hot read on
	store.set(HOT, 0);
	store.set("cold", 0);

	// ---------- reads ----------

	bench("ALIEN.store.get.hot", () => {
		store.get(HOT);
	});

	bench("ALIEN.store.get.missing", () => {
		store.get("nope");
	});

	bench("ALIEN.store.has.existing", () => {
		store.has(HOT);
	});

	bench("ALIEN.store.has.missing", () => {
		store.has("nope");
	});

	// ---------- writes ----------

	let writeValue = 0;
	bench("ALIEN.store.set.changed", () => {
		writeValue ^= 1;
		store.set(HOT, writeValue);
	});

	bench("ALIEN.store.set.same", () => {
		store.set(HOT, writeValue); // same value already there
	});

	// ---------- notify through the store (the common case) ----------

	let notifyRuns = 0;
	const disposeNotify = effect(() => {
		store.get(HOT);
		notifyRuns++;
	});

	let notifyWrite = 0;
	bench("ALIEN.store.set.notify", () => {
		notifyWrite ^= 1;
		store.set(HOT, notifyWrite);
	});

	// ---------- fanout via store subscribers (real shape) ----------

	const disposeFanout8: (() => void)[] = [];
	for (let i = 0; i < 8; i++) {
		disposeFanout8.push(effect(() => { store.get(HOT); }));
	}
	let fanout8Value = 0;
	bench("ALIEN.store.fanout.8", () => {
		fanout8Value ^= 1;
		store.set(HOT, fanout8Value);
	});

	const disposeFanout16: (() => void)[] = [];
	for (let i = 0; i < 16; i++) {
		disposeFanout16.push(effect(() => { store.get(HOT); }));
	}
	let fanout16Value = 0;
	bench("ALIEN.store.fanout.16", () => {
		fanout16Value ^= 1;
		store.set(HOT, fanout16Value);
	});

	// ---------- fan-in via store keys (computed over keys) ----------

	const KEYS8 = Array.from({ length: 8 }, (_, i) => `k${i}`);
	for (const k of KEYS8) store.set(k, 0);
	const fanIn8 = computed(() => {
		let v = 0;
		for (const k of KEYS8) v += (store.get<number>(k) ?? 0);
		return v;
	});
	fanIn8();

	let fanin8Value = 0;
	let fanin8Index = 0;
	bench("ALIEN.store.fanin.8", () => {
		fanin8Value ^= 1;
		store.set(KEYS8[fanin8Index++ & 7], fanin8Value);
		fanIn8();
	});

	const KEYS16 = Array.from({ length: 16 }, (_, i) => `j${i}`);
	for (const k of KEYS16) store.set(k, 0);
	const fanIn16 = computed(() => {
		let v = 0;
		for (const k of KEYS16) v += (store.get<number>(k) ?? 0);
		return v;
	});
	fanIn16();

	let fanin16Value = 0;
	let fanin16Index = 0;
	bench("ALIEN.store.fanin.16", () => {
		fanin16Value ^= 1;
		store.set(KEYS16[fanin16Index++ & 15], fanin16Value);
		fanIn16();
	});

	// all-writes: the real pathological case (many keys change, one computed reads all)
	let faninAllValue = 0;
	bench("ALIEN.store.fanin.16.all-writes", () => {
		faninAllValue ^= 1;
		for (const k of KEYS16) store.set(k, faninAllValue);
		fanIn16();
	});

	// ---------- getMany: the actual "read a slice" path ----------

	bench("ALIEN.store.getMany.8", () => {
		let v = 0;
		for (const k of KEYS8) v += (store.get<number>(k) ?? 0);
		return v;
	});

	// ---------- delete / lifecycle (the real cliff) ----------

	let delCounter = 0;
	bench("ALIEN.store.delete.recreate", () => {
		const k = `tmp${delCounter++ & 7}`;
		store.set(k, 1);
		store.delete(k);
	});

	// ---------- computed over store (real derived state) ----------

	const derived = computed(() => (store.get<number>(HOT) ?? 0) * 2);
	derived();
	bench("ALIEN.computed.read.clean", () => {
		derived();
	});

	let derivedWrite = 0;
	bench("ALIEN.computed.recompute", () => {
		derivedWrite ^= 1;
		store.set(HOT, derivedWrite);
		derived();
	});

	// ---------- chain over store keys (real: form -> validate -> derived) ----------

	const chain1 = computed(() => (store.get<number>(HOT) ?? 0) + 1);
	chain1();
	let chain1Write = 0;
	bench("ALIEN.computed.chain.1", () => {
		chain1Write ^= 1;
		store.set(HOT, chain1Write);
		chain1();
	});

	let chain = computed(() => (store.get<number>(HOT) ?? 0) + 1);
	for (let i = 1; i < 8; i++) {
		const prev = chain;
		chain = computed(() => prev() + 1);
	}
	chain();
	let chain8Write = 0;
	bench("ALIEN.computed.chain.8", () => {
		chain8Write ^= 1;
		store.set(HOT, chain8Write);
		chain();
	});

	// ---------- batching over store (real: multi-key commit) ----------

	const BATCH8 = Array.from({ length: 8 }, (_, i) => `b${i}`);
	for (const k of BATCH8) store.set(k, 0);
	const batch8Sum = computed(() => {
		let v = 0;
		for (const k of BATCH8) v += (store.get<number>(k) ?? 0);
		return v;
	});
	batch8Sum();
	let batch8Value = 0;
	bench("ALIEN.store.batch.writes.8", () => {
		batch8Value ^= 1;
		startBatch();
		try {
			for (const k of BATCH8) store.set(k, batch8Value);
		} finally {
			endBatch();
		}
	});

	// ---------- effects over store (real: subscribe to a key) ----------

	let rerunRuns = 0;
	const disposeRerun = effect(() => {
		store.get(HOT);
		rerunRuns++;
	});
	let rerunValue = 0;
	bench("ALIEN.effect.rerun.via-store", () => {
		rerunValue ^= 1;
		store.set(HOT, rerunValue);
	});

	bench("ALIEN.effect.create.dispose.via-store", () => {
		const dispose = effect(() => { store.get(HOT); });
		dispose();
	});

	// Keep alive
	void disposeNotify;
	void disposeFanout8;
	void disposeFanout16;
	void disposeRerun;
	void notifyRuns;
	void fanout8Value;
	void fanout16Value;
	void fanin8Value;
	void fanin16Value;
	void faninAllValue;
	void derivedWrite;
	void chain1Write;
	void chain8Write;
	void batch8Value;
	void rerunRuns;
	void rerunValue;
}
else if (FIGHT === "preact2") {
	// Real-world persisted storage, using the actual package devs install.
	// @kamod-ch/signals wraps @preact/signals and adds localStorage/session/etc.

	const HOT = "hot";
	// memory driver = pure reactive overhead, no I/O
	const hotSig = persistedSignal(HOT, 0, { storage: "memory" });

	// ---------- reads ----------
	bench("PREACT.persisted.get.hot", () => {
		hotSig.value;
	});

	bench("PREACT.persisted.get.missing", () => {
		persistedSignal("nope", undefined, { storage: "memory" }).value;
	});

	// ---------- writes ----------
	let writeValue = 0;
	bench("PREACT.persisted.set.changed", () => {
		writeValue ^= 1;
		hotSig.value = writeValue;
	});

	bench("PREACT.persisted.set.same", () => {
		hotSig.value = writeValue;
	});

	// ---------- notify ----------
	let notifyRuns = 0;
	const disposeNotify = preact_effect(() => {
		hotSig.value;
		notifyRuns++;
	});

	let notifyValue = 0;
	bench("PREACT.persisted.set.notify", () => {
		notifyValue ^= 1;
		hotSig.value = notifyValue;
	});

	// ---------- fanout (8 / 16 subscribers on one persisted key) ----------
	const disposeFanout8: (() => void)[] = [];
	for (let i = 0; i < 8; i++) {
		disposeFanout8.push(preact_effect(() => { hotSig.value; }));
	}
	let fanout8Value = 0;
	bench("PREACT.persisted.fanout.8", () => {
		fanout8Value ^= 1;
		hotSig.value = fanout8Value;
	});

	const disposeFanout16: (() => void)[] = [];
	for (let i = 0; i < 16; i++) {
		disposeFanout16.push(preact_effect(() => { hotSig.value; }));
	}
	let fanout16Value = 0;
	bench("PREACT.persisted.fanout.16", () => {
		fanout16Value ^= 1;
		hotSig.value = fanout16Value;
	});

	// ---------- fan-in (computed over 8 / 16 persisted keys) ----------
	const KEYS8 = Array.from({ length: 8 }, (_, i) => `k${i}`);
	const sig8 = KEYS8.map((k) => persistedSignal(k, 0, { storage: "memory" }));
	const fanIn8 = preact_computed(() => {
		let v = 0;
		for (const s of sig8) v += s.value;
		return v;
	});
	fanIn8.value;

	let fanin8Value = 0;
	let fanin8Index = 0;
	bench("PREACT.persisted.fanin.8", () => {
		fanin8Value ^= 1;
		sig8[fanin8Index++ & 7].value = fanin8Value;
		fanIn8.value;
	});

	const KEYS16 = Array.from({ length: 16 }, (_, i) => `j${i}`);
	const sig16 = KEYS16.map((k) => persistedSignal(k, 0, { storage: "memory" }));
	const fanIn16 = preact_computed(() => {
		let v = 0;
		for (const s of sig16) v += s.value;
		return v;
	});
	fanIn16.value;

	let fanin16Value = 0;
	let fanin16Index = 0;
	bench("PREACT.persisted.fanin.16", () => {
		fanin16Value ^= 1;
		sig16[fanin16Index++ & 15].value = fanin16Value;
		fanIn16.value;
	});

	let faninAllValue = 0;
	bench("PREACT.persisted.fanin.16.all-writes", () => {
		faninAllValue ^= 1;
		for (const s of sig16) s.value = faninAllValue;
		fanIn16.value;
	});

	// ---------- computed over persisted ----------
	const derived = preact_computed(() => hotSig.value * 2);
	derived.value;

	bench("PREACT.persisted.computed.read.clean", () => {
		derived.value;
	});

	let derivedWrite = 0;
	bench("PREACT.persisted.computed.recompute", () => {
		derivedWrite ^= 1;
		hotSig.value = derivedWrite;
		derived.value;
	});

	// ---------- chain over persisted ----------
	const chain1 = preact_computed(() => hotSig.value + 1);
	chain1.value;
	let chain1Write = 0;
	bench("PREACT.persisted.chain.1", () => {
		chain1Write ^= 1;
		hotSig.value = chain1Write;
		chain1.value;
	});

	let chain = preact_computed(() => hotSig.value + 1);
	for (let i = 1; i < 8; i++) {
		const prev = chain;
		chain = preact_computed(() => prev.value + 1);
	}
	chain.value;
	let chain8Write = 0;
	bench("PREACT.persisted.chain.8", () => {
		chain8Write ^= 1;
		hotSig.value = chain8Write;
		chain.value;
	});

	// ---------- batch over persisted ----------
	const BATCH8 = Array.from({ length: 8 }, (_, i) => `b${i}`);
	const batchSigs = BATCH8.map((k) => persistedSignal(k, 0, { storage: "memory" }));
	const batchSum = preact_computed(() => {
		let v = 0;
		for (const s of batchSigs) v += s.value;
		return v;
	});
	batchSum.value;
	let batch8Value = 0;
	bench("PREACT.persisted.batch.writes.8", () => {
		batch8Value ^= 1;
		preact_batch(() => {
			for (const s of batchSigs) s.value = batch8Value;
		});
	});

	// ---------- effects over persisted ----------
	let rerunRuns = 0;
	const disposeRerun = preact_effect(() => {
		hotSig.value;
		rerunRuns++;
	});
	let rerunValue = 0;
	bench("PREACT.persisted.effect.rerun", () => {
		rerunValue ^= 1;
		hotSig.value = rerunValue;
	});

	bench("PREACT.persisted.effect.create.dispose", () => {
		const dispose = preact_effect(() => { hotSig.value; });
		dispose();
	});

	// keep alive
	void disposeNotify;
	void disposeFanout8;
	void disposeFanout16;
	void disposeRerun;
	void notifyRuns;
	void fanout8Value;
	void fanout16Value;
	void fanin8Value;
	void fanin16Value;
	void faninAllValue;
	void derivedWrite;
	void chain1Write;
	void chain8Write;
	void batch8Value;
	void rerunRuns;
	void rerunValue;
}
else if (FIGHT === "alien3") {
	// Mimic @kamod-ch/signals' persistedSignal semantics, backed by Alien.
	// Same key + same storage -> same underlying signal (global identity).
	const HOT = "hot";
	// memory driver = pure reactive overhead, no I/O
	const hotSig = persistedSignal(HOT, 0, { storage: "memory" });

	// ---------- reads ----------
	bench("ALIEN.persisted.get.hot", () => {
		hotSig.value;
	});

	bench("ALIEN.persisted.get.missing", () => {
		persistedSignal("nope", undefined).value;
	});

	// ---------- writes ----------
	let writeValue = 0;
	bench("ALIEN.persisted.set.changed", () => {
		writeValue ^= 1;
		hotSig.value = writeValue;
	});

	bench("ALIEN.persisted.set.same", () => {
		hotSig.value = writeValue;
	});

	// ---------- notify ----------
	let notifyRuns = 0;
	const disposeNotify = effect(() => {
		hotSig.value;
		notifyRuns++;
	});

	let notifyValue = 0;
	bench("ALIEN.persisted.set.notify", () => {
		notifyValue ^= 1;
		hotSig.value = notifyValue;
	});

	// ---------- fanout ----------
	const disposeFanout8: (() => void)[] = [];
	for (let i = 0; i < 8; i++) {
		disposeFanout8.push(effect(() => { hotSig.value; }));
	}
	let fanout8Value = 0;
	bench("ALIEN.persisted.fanout.8", () => {
		fanout8Value ^= 1;
		hotSig.value = fanout8Value;
	});

	const disposeFanout16: (() => void)[] = [];
	for (let i = 0; i < 16; i++) {
		disposeFanout16.push(effect(() => { hotSig.value; }));
	}
	let fanout16Value = 0;
	bench("ALIEN.persisted.fanout.16", () => {
		fanout16Value ^= 1;
		hotSig.value = fanout16Value;
	});

	// ---------- fan-in ----------
	const KEYS8 = Array.from({ length: 8 }, (_, i) => `k${i}`);
	const sig8 = KEYS8.map((k) => persistedSignal(k, 0));
	const fanIn8 = computed(() => {
		let v = 0;
		for (const s of sig8) v += s.value;
		return v;
	});
	fanIn8();

	let fanin8Value = 0;
	let fanin8Index = 0;
	bench("ALIEN.persisted.fanin.8", () => {
		fanin8Value ^= 1;
		sig8[fanin8Index++ & 7].value = fanin8Value;
		fanIn8();
	});

	const KEYS16 = Array.from({ length: 16 }, (_, i) => `j${i}`);
	const sig16 = KEYS16.map((k) => persistedSignal(k, 0));
	const fanIn16 = computed(() => {
		let v = 0;
		for (const s of sig16) v += s.value;
		return v;
	});
	fanIn16();

	let fanin16Value = 0;
	let fanin16Index = 0;
	bench("ALIEN.persisted.fanin.16", () => {
		fanin16Value ^= 1;
		sig16[fanin16Index++ & 15].value = fanin16Value;
		fanIn16();
	});

	let faninAllValue = 0;
	bench("ALIEN.persisted.fanin.16.all-writes", () => {
		faninAllValue ^= 1;
		for (const s of sig16) s.value = faninAllValue;
		fanIn16();
	});

	// ---------- computed ----------
	const derived = computed(() => hotSig.value * 2);
	derived();

	bench("ALIEN.persisted.computed.read.clean", () => {
		derived();
	});

	let derivedWrite = 0;
	bench("ALIEN.persisted.computed.recompute", () => {
		derivedWrite ^= 1;
		hotSig.value = derivedWrite;
		derived();
	});

	// ---------- chain ----------
	const chain1 = computed(() => hotSig.value + 1);
	chain1();
	let chain1Write = 0;
	bench("ALIEN.persisted.chain.1", () => {
		chain1Write ^= 1;
		hotSig.value = chain1Write;
		chain1();
	});

	let chain = computed(() => hotSig.value + 1);
	for (let i = 1; i < 8; i++) {
		const prev = chain;
		chain = computed(() => prev() + 1);
	}
	chain();
	let chain8Write = 0;
	bench("ALIEN.persisted.chain.8", () => {
		chain8Write ^= 1;
		hotSig.value = chain8Write;
		chain();
	});

	// ---------- batch ----------
	const BATCH8 = Array.from({ length: 8 }, (_, i) => `b${i}`);
	const batchSigs = BATCH8.map((k) => persistedSignal(k, 0));
	const batchSum = computed(() => {
		let v = 0;
		for (const s of batchSigs) v += s.value;
		return v;
	});
	batchSum();
	let batch8Value = 0;
	bench("ALIEN.persisted.batch.writes.8", () => {
		batch8Value ^= 1;
		startBatch();
		try {
			for (const s of batchSigs) s.value = batch8Value;
		} finally {
			endBatch();
		}
	});

	// ---------- effects ----------
	let rerunRuns = 0;
	const disposeRerun = effect(() => {
		hotSig.value;
		rerunRuns++;
	});
	let rerunValue = 0;
	bench("ALIEN.persisted.effect.rerun", () => {
		rerunValue ^= 1;
		hotSig.value = rerunValue;
	});

	bench("ALIEN.persisted.effect.create.dispose", () => {
		const dispose = effect(() => { hotSig.value; });
		dispose();
	});

	// keep alive
	void disposeNotify;
	void disposeFanout8;
	void disposeFanout16;
	void disposeRerun;
	void notifyRuns;
	void fanout8Value;
	void fanout16Value;
	void fanin8Value;
	void fanin16Value;
	void faninAllValue;
	void derivedWrite;
	void chain1Write;
	void chain8Write;
	void batch8Value;
	void rerunRuns;
	void rerunValue;
}

if (FIGHT === "REACTIVE_SYSTEM") {
	/* ------------------------------------------------------------------ *
	 * SIGNAL
	 * ------------------------------------------------------------------ */

	const readSignal = signal(1);

	bench("REACTIVE_SYSTEM.signal.get", () => {
		readSignal();
	});

	let writeValue = 0;

	bench("REACTIVE_SYSTEM.signal.set", () => {
		readSignal(++writeValue);
	});

	bench("REACTIVE_SYSTEM.signal.get.set", () => {
		readSignal();
		readSignal(++writeValue);
	});

	/* ------------------------------------------------------------------ *
	 * COMPUTED
	 * ------------------------------------------------------------------ */

	const computedSource = signal(1);
	const cachedComputed = computed(() => computedSource() * 2);

	cachedComputed();

	bench("REACTIVE_SYSTEM.computed.get.cached", () => {
		cachedComputed();
	});

	bench("REACTIVE_SYSTEM.computed.invalidate", () => {
		computedSource(++writeValue);
	});

	bench("REACTIVE_SYSTEM.computed.recompute", () => {
		computedSource(++writeValue);
		cachedComputed();
	});

	const chainSource = signal(1);
	const chain1 = computed(() => chainSource() + 1);
	const chain2 = computed(() => chain1() + 1);
	const chain3 = computed(() => chain2() + 1);

	chain3();

	bench("REACTIVE_SYSTEM.computed.chain.cached", () => {
		chain3();
	});

	bench("REACTIVE_SYSTEM.computed.chain.recompute", () => {
		chainSource(++writeValue);
		chain3();
	});

	/* ------------------------------------------------------------------ *
	 * EFFECT
	 * ------------------------------------------------------------------ */

	let effectValue = 0;

	bench("REACTIVE_SYSTEM.effect.create-dispose", () => {
		const dispose = effect(() => {
			effectValue++;
		});

		dispose();
	});

	const notifySignal = signal(0);
	let notifyCount = 0;

	const disposeNotify = effect(() => {
		notifySignal();
		notifyCount++;
	});

	bench("REACTIVE_SYSTEM.effect.notify", () => {
		notifySignal(++writeValue);
	});

	disposeNotify();

	const cleanupSignal = signal(0);
	let cleanupCount = 0;

	const disposeCleanup = effect(() => {
		cleanupSignal();

		return () => {
			cleanupCount++;
		};
	});

	bench("REACTIVE_SYSTEM.effect.cleanup-rerun", () => {
		cleanupSignal(++writeValue);
	});

	disposeCleanup();

	/* ------------------------------------------------------------------ *
	 * DEPENDENCY TRACKING
	 * ------------------------------------------------------------------ */

	const dependencySource = signal(1);
	let dependencyResult = 0;

	const disposeDependency = effect(() => {
		dependencyResult = dependencySource();
	});

	bench("REACTIVE_SYSTEM.dependency.track", () => {
		dependencySource(++writeValue);
	});

	disposeDependency();

	/* ------------------------------------------------------------------ *
	 * FAN-OUT
	 * ------------------------------------------------------------------ */

	const fanout16 = signal(0);
	const fanout16Disposers: (() => void)[] = [];

	for (let i = 0; i < 16; i++) {
		fanout16Disposers.push(
			effect(() => {
				fanout16();
			}),
		);
	}

	bench("REACTIVE_SYSTEM.fanout.16", () => {
		fanout16(++writeValue);
	});

	for (const dispose of fanout16Disposers) dispose();

	const fanout64 = signal(0);
	const fanout64Disposers: (() => void)[] = [];

	for (let i = 0; i < 64; i++) {
		fanout64Disposers.push(
			effect(() => {
				fanout64();
			}),
		);
	}

	bench("REACTIVE_SYSTEM.fanout.64", () => {
		fanout64(++writeValue);
	});

	for (const dispose of fanout64Disposers) dispose();

	/* ------------------------------------------------------------------ *
	 * BATCHING
	 * ------------------------------------------------------------------ */

	const batchA = signal(0);
	const batchB = signal(0);
	const batchC = signal(0);

	bench("REACTIVE_SYSTEM.batch.3-writes", () => {
		startBatch();

		batchA(++writeValue);
		batchB(++writeValue);
		batchC(++writeValue);

		endBatch();
	});

	const batchSource = signal(0);
	let batchEffectCount = 0;

	const disposeBatch = effect(() => {
		batchSource();
		batchEffectCount++;
	});

	bench("REACTIVE_SYSTEM.batch.effect.3-writes", () => {
		startBatch();

		batchSource(++writeValue);
		batchSource(++writeValue);
		batchSource(++writeValue);

		endBatch();
	});

	disposeBatch();

	/* ------------------------------------------------------------------ *
	 * GRAPH
	 * ------------------------------------------------------------------ */

	const diamondSource = signal(1);
	const diamondLeft = computed(() => diamondSource() + 1);
	const diamondRight = computed(() => diamondSource() + 2);
	const diamondResult = computed(
		() => diamondLeft() + diamondRight(),
	);

	diamondResult();

	bench("REACTIVE_SYSTEM.graph.diamond.cached", () => {
		diamondResult();
	});

	bench("REACTIVE_SYSTEM.graph.diamond.recompute", () => {
		diamondSource(++writeValue);
		diamondResult();
	});

	const branchSource = signal(1);

	const branches = Array.from(
		{ length: 16 },
		(_, i) => computed(() => branchSource() + i),
	);

	const branchResult = computed(
		() => branches.reduce((total, branch) => total + branch(), 0),
	);

	branchResult();

	bench("REACTIVE_SYSTEM.graph.branch.16.cached", () => {
		branchResult();
	});

	bench("REACTIVE_SYSTEM.graph.branch.16.recompute", () => {
		branchSource(++writeValue);
		branchResult();
	});

	/* ------------------------------------------------------------------ *
	 * KEEP FIXTURES LIVE
	 * ------------------------------------------------------------------ */

	void effectValue;
	void cleanupCount;
	void notifyCount;
	void dependencyResult;
	void batchEffectCount;
}
