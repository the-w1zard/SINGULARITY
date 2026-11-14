// electrons/governer.ts


export class GOVERNER_ELECTRON {
    private running = false
    private timer: ReturnType<typeof setTimeout> | null = null
    public ticks = 0 // for tests: proves it goes quiet when idle
    this.VERTEX: VERTEX
    constructor(VERTEX: VERTEX) { this.VERTEX = VERTEX }

    public start(): void {
        if (this.running) return // hot path: queue.push calls this on every write
        this.running = true
        this.schedule()
    }
    public stop(): void {
        this.running = false
        if (this.timer) { clearTimeout(this.timer); this.timer = null }
    }
    private schedule(): void {
        this.timer = setTimeout(this.tick, 0)
        ;(this.timer as any).unref?.()
    }
    private tick = (): void => {
        this.timer = null
        if (!this.running) return
        this.ticks++
        const h = this.h
        try {
            if (VERTEX.queue.length) VERTEX.queue.flushToBinary()
            if (VERTEX.engine.dirty) VERTEX.engine.pass()
        } catch (e) { console.error('[governor] tick failed', e) }
        if (VERTEX.queue.length || VERTEX.engine.dirty) this.schedule()
        else this.running = false // nothing to do: no timer until the next write calls start()
    }
}
