import { SERVER_KVDB } from "./db";
const db = new SERVER_KVDB("__SAURON__", "SAURON_SLAVE_REG", "SAURON_MASTER_REG", "THE_WIZARD_ROOT_REG", {
    directory: "./bench-data",
    durability: "group",
    walBatchBytes: 1024 * 1024,
    walBatchRecords: 8192,
    walFlushIntervalMs: 1,
    walHighWaterBytes: 64 * 1024 * 1024,
    checkpointIntervalMs: 10_000
});
await db.open();
const value = new Uint8Array(128).fill(65);
const operations = 10_000_000;
const started = process.hrtime.bigint();
await db.set(`key`, value);
for (let index = 0; index < operations; index++) {
    db.hot(`key`);
}
const elapsedNs = Number(process.hrtime.bigint() - started);
const operationsPerSecond = operations / (elapsedNs / 1e9);
console.log({
    operations,
    elapsedSeconds: elapsedNs / 1e9,
    operationsPerSecond,
    millionOperationsPerSecond: operationsPerSecond / 1e6,
    stats: db.stats
});
await db.close();
