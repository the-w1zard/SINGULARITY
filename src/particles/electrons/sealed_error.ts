// electrons/sealed_error.ts


export class SEALED_ERROR_ELECTRON extends Error {
	constructor(key: ATOM_KEY) {
		super(`cell "${String(key)}" is sealed`);
	}
}
