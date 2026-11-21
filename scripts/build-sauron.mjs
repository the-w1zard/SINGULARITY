import { createRequire } from "node:module";
import path from "node:path";
import { spawnSync } from "node:child_process";

const require = createRequire(import.meta.url);

const ts6Entry = require.resolve("@typescript/typescript6");
const ts6Lib = path.join(path.dirname(ts6Entry), "lib", "typescript.js");

const result = spawnSync(
  process.execPath,
  [
    path.join(
      path.dirname(require.resolve("ts-patch/package.json")),
      "lib",
      "bin.js"
    ),
    ...process.argv.slice(2),
  ],
  {
    stdio: "inherit",
    env: {
      ...process.env,
      TSP_COMPILER_TS_PATH: ts6Lib,
    },
  }
);

process.exit(result.status ?? 1);
