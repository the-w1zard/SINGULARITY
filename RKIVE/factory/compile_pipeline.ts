export function compilePipeline<C>(    behaviours: BEHAVIOUR<C>[]): PIPELINE<C> {
    const byHook = new Map<HOOK, BEHAVIOUR<C>[]>();

    for (const b of behaviours) {
        let list = byHook.get(b.hook);
        if (!list) byHook.set(b.hook, list = []);
        list.push(b);
    }

    for (const list of byHook.values()) {
        list.sort((a, b) => (a.order ?? 100) - (b.order ?? 100));
    }

    return {
        run(hook, ctx, terminal) {
            const list = byHook.get(hook);
            if (!list || list.length === 0) return terminal();

            let i = list.length;

            const next = (): V => {
                if (i === 0) return terminal();
                const b = list[--i];
                return b.run(ctx, next);
            };

            return next();
        }
    };
}
