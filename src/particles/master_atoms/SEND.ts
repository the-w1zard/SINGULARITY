import { ATOM_CLASS, ATOM_KEY, BC, GATE_ELECTRON, REGISTRY_PROTON, SEALED_ERROR_ELECTRON, SUBSCRIPTION_ELECTRON } from "../contract_factory/CONTROL_ROOM";

export class SEND_ATOM<T> extends ATOM<MQ_STATE<T>> {
    private pipeline?: PIPELINE<MQ_CTX<T>>;

    send(message: T, signal?: SIGNAL): B {
        const ctx: MQ_CTX<T> = {
            atom: this,
            message,
            signal,
            meta: undefined
        };

        return this.dispatch("beforeSend", ctx, () => {
            this.deliver(ctx);
            return true;
        });
    }

    private dispatch(hook: HOOK,ctx: MQ_CTX<T>,terminal: () => V): V {
        const p = this.pipeline ??= this.compile();
        return p.run(hook, ctx, terminal);
    }
    private deliver(ctx: MQ_CTX<T>): V {
        // bare transport / queue / subscriber fanout
    }
    private compile(): PIPELINE<MQ_CTX<T>> {
        const r = this.registry;
        const behaviours: BEHAVIOUR<MQ_CTX<T>>[] = [];

        if (r !== null) {
            for (const shard of r.shards()) {
                if ("behaviours" in shard) {
                    behaviours.push(...(shard as any).behaviours());
                }
            }
        }

        return compilePipeline(behaviours);
    }
}
