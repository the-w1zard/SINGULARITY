import { writeBigUint64, writeUint32, readUint32, readBigUint64, crc32 } from "./binary";
import { MANIFEST_VERSION, WAL_VERSION, PAGE_PAYLOAD_CAPACITY, PAGE_SIZE, PAGE_RECORD_HEAD, PAGE_RECORD_CONTINUATION, NO_PAGE, PAGE_HEADER_SIZE, PAGE_FREE } from "./types";
//                                                                                                                    MANIFEST
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
const MANIFEST_MAGIC = 0x464b5631; // FKV1
const MANIFEST_SIZE = 24;
export interface Manifest {
	checkpointSequence: bigint;
	pageCount: number;
}
export function encodeManifest(manifest: Manifest): Uint8Array {
	const bytes = new Uint8Array(MANIFEST_SIZE);
	const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);

	view.setUint32(0, MANIFEST_MAGIC, true);
	view.setUint32(4, MANIFEST_VERSION, true);
	writeBigUint64(bytes, 8, manifest.checkpointSequence);
	writeUint32(bytes, 16, manifest.pageCount);
	writeUint32(bytes, 20, crc32(bytes, 0, 20));

	return bytes;
}
export function decodeManifest(bytes: Uint8Array): Manifest | undefined {
	if (bytes.length < MANIFEST_SIZE) return undefined;

	const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);

	if (view.getUint32(0, true) !== MANIFEST_MAGIC) {
		return undefined;
	}

	if (view.getUint32(4, true) !== MANIFEST_VERSION) {
		return undefined;
	}

	const storedChecksum = readUint32(bytes, 20);

	if (storedChecksum !== crc32(bytes, 0, 20)) {
		return undefined;
	}

	return {
		checkpointSequence: readBigUint64(bytes, 8),
		pageCount: readUint32(bytes, 16),
	};
}
// #endregion
//                                                                                                                         WAL
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
export interface WalRecord {
	sequence: bigint;
	operation: number;
	key: Uint8Array;
	value: Uint8Array;
}
export function encodeWalRecord(record: WalRecord): Uint8Array {
	// Single allocation: the frame is written in place, no body staging buffer.
	const bodyLength = 1 + 1 + 8 + 4 + 4 + record.key.length + record.value.length;

	const frame = new Uint8Array(4 + bodyLength + 4);

	const view = new DataView(frame.buffer, frame.byteOffset, frame.byteLength);

	view.setUint32(0, bodyLength, true);

	let offset = 4;

	view.setUint8(offset, WAL_VERSION);
	offset += 1;

	view.setUint8(offset, record.operation);
	offset += 1;

	view.setBigUint64(offset, record.sequence, true);
	offset += 8;

	view.setUint32(offset, record.key.length, true);
	offset += 4;

	view.setUint32(offset, record.value.length, true);
	offset += 4;

	frame.set(record.key, offset);
	offset += record.key.length;

	frame.set(record.value, offset);

	view.setUint32(4 + bodyLength, crc32(frame, 4, 4 + bodyLength), true);

	return frame;
}
export interface DecodedWalResult {
	records: WalRecord[];
	validBytes: number;
}
export function decodeWalRecords(bytes: Uint8Array, minimumSequence: bigint): DecodedWalResult {
	const records: WalRecord[] = [];
	let offset = 0;

	while (offset + 4 <= bytes.length) {
		const bodyLength = readUint32(bytes, offset);

		const frameEnd = offset + 4 + bodyLength + 4;

		if (bodyLength < 18 || frameEnd > bytes.length) {
			break;
		}

		const storedChecksum = readUint32(bytes, offset + 4 + bodyLength);

		const actualChecksum = crc32(bytes, offset + 4, offset + 4 + bodyLength);

		if (storedChecksum !== actualChecksum) {
			break;
		}

		const body = bytes.subarray(offset + 4, offset + 4 + bodyLength);

		const bodyView = new DataView(body.buffer, body.byteOffset, body.byteLength);

		const version = bodyView.getUint8(0);

		if (version !== WAL_VERSION) {
			break;
		}

		const operation = bodyView.getUint8(1);
		const sequence = bodyView.getBigUint64(2, true);
		const keyLength = bodyView.getUint32(10, true);
		const valueLength = bodyView.getUint32(14, true);

		if (sequence <= minimumSequence) {
			offset = frameEnd;
			continue;
		}

		const keyStart = 18;
		const valueStart = keyStart + keyLength;
		const valueEnd = valueStart + valueLength;

		if (valueEnd > body.length) {
			break;
		}

		records.push({
			sequence,
			operation,
			key: body.subarray(keyStart, valueStart),
			value: body.subarray(valueStart, valueEnd),
		});

		offset = frameEnd;
	}

	return {
		records,
		validBytes: offset,
	};
}
// #endregion
//                                                                                                                       PAGES
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
export interface RecordChain {
	pageNumbers: number[];
	pages: Uint8Array[];
}
export function encodeRecordChain(key: Uint8Array, value: Uint8Array, allocatePage: () => number): RecordChain {
	// No payload staging buffer: key/value bytes are written straight into the
	// destination pages. payload layout is [12-byte header][key][value].
	const keyLength = key.length;
	const valueLength = value.length;
	const payloadLength = 12 + keyLength + valueLength;

	const pageCount = Math.max(1, Math.ceil(payloadLength / PAGE_PAYLOAD_CAPACITY));

	const pageNumbers: number[] = [];
	const pages: Uint8Array[] = [];
	const valueChecksum = crc32(value);
	const keyEndInPayload = 12 + keyLength;

	for (let index = 0; index < pageCount; index++) {
		const pageNumber = allocatePage();
		const page = new Uint8Array(PAGE_SIZE);
		const view = new DataView(page.buffer, page.byteOffset, page.byteLength);

		const payloadStart = index * PAGE_PAYLOAD_CAPACITY;
		const payloadEnd = Math.min(payloadLength, payloadStart + PAGE_PAYLOAD_CAPACITY);

		const used = payloadEnd - payloadStart;

		pageNumbers.push(pageNumber);
		pages.push(page);

		view.setUint32(0, index === 0 ? PAGE_RECORD_HEAD : PAGE_RECORD_CONTINUATION, true);

		view.setUint32(4, NO_PAGE, true); // next, patched below once known
		view.setUint32(8, used, true);

		let dataOffset = PAGE_HEADER_SIZE;
		let dataStart = payloadStart;

		if (index === 0) {
			view.setUint32(dataOffset, keyLength, true);
			view.setUint32(dataOffset + 4, valueLength, true);
			view.setUint32(dataOffset + 8, valueChecksum, true);
			dataOffset += 12;
			dataStart = 12;
		}

		if (dataStart < keyEndInPayload) {
			const copyEnd = Math.min(payloadEnd, keyEndInPayload);
			page.set(key.subarray(dataStart - 12, copyEnd - 12), dataOffset);
			dataOffset += copyEnd - dataStart;
			dataStart = copyEnd;
		}

		if (dataStart < payloadEnd) {
			page.set(value.subarray(dataStart - keyEndInPayload, payloadEnd - keyEndInPayload), dataOffset);
		}

		view.setUint32(12, crc32(page, PAGE_HEADER_SIZE, PAGE_HEADER_SIZE + used), true);
	}

	// Patching the next pointer does not affect the checksum: crc covers
	// [PAGE_HEADER_SIZE, ...), never the 16-byte header.
	for (let index = 0; index < pageCount - 1; index++) {
		const page = pages[index]!;
		new DataView(page.buffer, page.byteOffset, page.byteLength).setUint32(4, pageNumbers[index + 1]!, true);
	}

	return {
		pageNumbers,
		pages,
	};
}
export interface DecodedRecord {
	key: Uint8Array;
	value: Uint8Array;
}
export function decodeRecordChain(readPage: (pageNumber: number) => Uint8Array | undefined, headPageNumber: number): DecodedRecord | undefined {
	const chunks: Uint8Array[] = [];
	let pageNumber: number | undefined = headPageNumber;
	let isHead = true;

	while (pageNumber !== undefined && pageNumber !== NO_PAGE) {
		const page = readPage(pageNumber);

		if (!page) return undefined;

		const view = new DataView(page.buffer, page.byteOffset, page.byteLength);

		const state = view.getUint32(0, true);
		const nextPage = view.getUint32(4, true);
		const used = view.getUint32(8, true);
		const checksum = view.getUint32(12, true);

		const expectedState = isHead ? PAGE_RECORD_HEAD : PAGE_RECORD_CONTINUATION;

		if (state !== expectedState || used > PAGE_PAYLOAD_CAPACITY || checksum !== crc32(page, PAGE_HEADER_SIZE, PAGE_HEADER_SIZE + used)) {
			return undefined;
		}

		chunks.push(page.subarray(PAGE_HEADER_SIZE, PAGE_HEADER_SIZE + used));

		pageNumber = nextPage === NO_PAGE ? undefined : nextPage;
		isHead = false;
	}

	if (chunks.length === 0) return undefined;

	const payloadLength = chunks.reduce((total, chunk) => total + chunk.length, 0);

	const payload = new Uint8Array(payloadLength);
	let offset = 0;

	for (const chunk of chunks) {
		payload.set(chunk, offset);
		offset += chunk.length;
	}

	if (payload.length < 12) return undefined;

	const payloadView = new DataView(payload.buffer, payload.byteOffset, payload.byteLength);

	const keyLength = payloadView.getUint32(0, true);
	const valueLength = payloadView.getUint32(4, true);
	const valueChecksum = payloadView.getUint32(8, true);

	const key = payload.subarray(12, 12 + keyLength);
	const value = payload.subarray(12 + keyLength, 12 + keyLength + valueLength);

	if (value.length !== valueLength) return undefined;
	if (crc32(value) !== valueChecksum) return undefined;

	return {
		key,
		value,
	};
}
// A free page is a constant: one shared, never-mutated buffer for every
// freed page instead of a fresh zeroed PAGE_SIZE allocation per page.
const FREE_PAGE: Uint8Array = (() => {
	const page = new Uint8Array(PAGE_SIZE);
	new DataView(page.buffer, page.byteOffset, page.byteLength).setUint32(0, PAGE_FREE, true);
	return page;
})();
export function encodeFreePage(): Uint8Array {
	return FREE_PAGE;
}

// #endregion
