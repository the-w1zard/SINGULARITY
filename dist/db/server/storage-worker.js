import { parentPort } from "node:worker_threads";
import { open, rename, truncate } from "node:fs/promises";
import { PAGE_SIZE } from "./types";
let dataFile;
let walFile;
let compactFile;
let walSize = 0;
function send(response) {
    parentPort.postMessage(response);
}
function fail(id, error) {
    send({
        kind: "error",
        id,
        error: error instanceof Error ? error.message : String(error)
    });
}
async function readPage(pageNumber, dataFile) {
    const page = new Uint8Array(PAGE_SIZE);
    const buffer = Buffer.from(page.buffer, page.byteOffset, page.byteLength);
    const result = await dataFile.read(buffer, 0, PAGE_SIZE, pageNumber * PAGE_SIZE);
    if (result.bytesRead !== PAGE_SIZE) {
        throw new Error(`Short read: page ${pageNumber}, read ${result.bytesRead} bytes`);
    }
    return page;
}
parentPort.on("message", async (request) => {
    try {
        switch (request.kind) {
            case "init": {
                dataFile = await open(request.dataPath, "r+");
                walFile = await open(request.walPath, "r+");
                const walStat = await walFile.stat();
                walSize = Number(walStat.size);
                send({
                    kind: "initOk",
                    id: request.id
                });
                break;
            }
            case "readPages": {
                if (!dataFile) {
                    throw new Error("Data file is not open");
                }
                const pages = [];
                for (const pageNumber of request.pageNumbers) {
                    pages.push(await readPage(pageNumber, dataFile));
                }
                send({
                    kind: "readPagesOk",
                    id: request.id,
                    pages
                });
                break;
            }
            case "appendWal": {
                if (!walFile) {
                    throw new Error("WAL file is not open");
                }
                await walFile.write(request.bytes, 0, request.bytes.byteLength, walSize);
                await walFile.sync();
                walSize += request.bytes.byteLength;
                send({
                    kind: "walOk",
                    id: request.id,
                    batchId: request.batchId,
                    walSize
                });
                break;
            }
            case "checkpoint": {
                if (!dataFile) {
                    throw new Error("Data file is not open");
                }
                for (const page of request.pages) {
                    await dataFile.write(page.buffer, 0, page.buffer.byteLength, page.pageNumber * PAGE_SIZE);
                }
                await dataFile.sync();
                send({
                    kind: "checkpointOk",
                    id: request.id
                });
                break;
            }
            case "compactBegin": {
                compactFile = await open(request.compactPath, "w+");
                send({
                    kind: "compactOk",
                    id: request.id
                });
                break;
            }
            case "compactWrite": {
                if (!compactFile) {
                    throw new Error("Compact file is not open");
                }
                for (const page of request.pages) {
                    await compactFile.write(page.buffer, 0, page.buffer.byteLength, page.pageNumber * PAGE_SIZE);
                }
                send({
                    kind: "compactOk",
                    id: request.id
                });
                break;
            }
            case "compactCommit": {
                if (!compactFile) {
                    throw new Error("Compact file is not open");
                }
                await compactFile.sync();
                await compactFile.close();
                compactFile = undefined;
                await rename(request.compactPath, request.dataPath);
                if (dataFile) {
                    await dataFile.close();
                }
                dataFile = await open(request.dataPath, "r+");
                await truncate(request.walPath, 0);
                if (walFile) {
                    await walFile.close();
                }
                walFile = await open(request.walPath, "r+");
                walSize = 0;
                send({
                    kind: "compactOk",
                    id: request.id
                });
                break;
            }
            case "close": {
                if (dataFile) {
                    await dataFile.sync();
                    await dataFile.close();
                    dataFile = undefined;
                }
                if (walFile) {
                    await walFile.sync();
                    await walFile.close();
                    walFile = undefined;
                }
                send({
                    kind: "closeOk",
                    id: request.id
                });
                break;
            }
        }
    }
    catch (error) {
        fail(request.id, error);
    }
});
