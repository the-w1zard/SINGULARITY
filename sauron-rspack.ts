// sauron-rspack.ts
import * as parser from '@babel/parser';
import traverse from '@babel/traverse';
import * as t from '@babel/types';
import { createHash } from 'crypto';
import path from 'path';
import fs from 'fs';
import os from 'os';

const DEBUG = true;

function shouldDebug(filePath:A) {
  // if (!DEBUG) return false;
  // return DEBUG_FILES.some(f => filePath.replace(/\\/g, '/').includes(f));
  return DEBUG
}

function sauronTransform(code:A, filePath:A) {
  const dbg = shouldDebug(filePath);

  let ast;
  try {
    ast = parser.parse(code, {
      sourceType: 'module',
      plugins: ['typescript', 'jsx', 'decorators-legacy'],
      tokens: true
    });
  } catch (e:A) {
    console.error(`[sauron] PARSE ERROR in ${filePath}:`, e.message);
    return code;
  }

  const idCounts = new Map();
  const insertions:A = [];
  const insertionPositions = new Set();
  let globalCallSiteCounter = 0;

  traverse(ast, {
    CallExpression(path) {
      const node = path.node;
      let isMatch = false;
      let hookName = '';

      if (t.isIdentifier(node.callee) && /^use[A-Z]/.test(node.callee.name)) {
        isMatch = true;
        hookName = node.callee.name;
      } else if (
        t.isMemberExpression(node.callee) &&
        t.isIdentifier(node.callee.object) &&
        t.isIdentifier(node.callee.property) &&
        /^use[A-Z]/.test(node.callee.property.name)
      ) {
        isMatch = true;
        hookName = node.callee.property.name;
      }

      if (!isMatch) return;
      const lastArg = node.arguments[node.arguments.length - 1];
      if (t.isStringLiteral(lastArg) && /^__sauron_/.test(lastArg.value)) { return; }

      const insertPos = node.end - 1;
      if (code[insertPos] !== ')') { console.error(`[sauron] INVARIANT VIOLATION: expected ')' at ${insertPos} for ${hookName}`); return; }

      if (dbg) {
        const line = node.loc?.start.line || 0;
        const col = node.loc?.start.column || 0;
        const charAtPos = insertPos !== -1 ? JSON.stringify(code[insertPos]) : 'NOT FOUND';
        const before = insertPos !== -1 ? code.slice(Math.max(0, insertPos - 40), insertPos).replace(/\n/g, '↵') : '';
        const after = insertPos !== -1 ? code.slice(insertPos, Math.min(code.length, insertPos + 20)).replace(/\n/g, '↵') : '';
        console.log(`[sauron]   ${hookName} @ line ${line}:${col} | node.end=${node.end} insertPos=${insertPos} char=${charAtPos}`);
        console.log(`[sauron]     context: ...${before}|HERE|${after}...`);
        if (insertionPositions.has(insertPos)) { console.log(`[sauron]     SKIPPED — position already used`); }
      }

      if (insertPos === -1) return;
      if (insertionPositions.has(insertPos)) return;
      insertionPositions.add(insertPos);

      const line = node.loc?.start.line || 0;
      const baseHash = createHash('sha1').update(`${filePath}:${hookName}:${line}`).digest('hex').slice(0, 8);
      const count = idCounts.get(baseHash) || 0;
      idCounts.set(baseHash, count + 1);
      // const id = `__sauron_${baseHash}_${count}__`;
      const id = `__sauron_${hookName}_${baseHash}_${count}__${globalCallSiteCounter++}__`;

      const hasArgs = node.arguments.length > 0;
      insertions.push({ pos: insertPos, text: `${hasArgs ? ', ' : ''}"${id}"` });

      if (dbg) { console.log(`[sauron]     INSERTING: ${hasArgs ? ', ' : ''}"${id}"`); }
    }
  });
  if (dbg) { console.log(`[sauron]   total insertions: ${insertions.length}`); }
  if (insertions.length === 0) return code;
  if (dbg) {
    const outPath = path.join(os.tmpdir(), path.basename(filePath) + '.sauron-out.tsx');
    fs.writeFileSync(outPath, result_preview(code, insertions));
    console.log(`[sauron]   preview written to: ${outPath}`);
  }
  function result_preview(src:A, ins:A) {
    const sorted = [...ins].sort((a, b) => b.pos - a.pos);
    let r = src;
    for (const { pos, text } of sorted) { r = r.slice(0, pos) + text + r.slice(pos); }
    return r;
  }
  insertions.sort((a:A, b:A) => b.pos - a.pos);
  let result = code;
  for (const { pos, text } of insertions) { result = result.slice(0, pos) + text + result.slice(pos); }
  return result;
}

export default function sauronLoader(this: any, source: string): string {
  const filePath: string = this.resourcePath;

  // Same guards your Vite plugin had
  if (filePath.startsWith('\0')) return source;
  const cleanId = filePath.split('?')[0];
  if (cleanId.includes('node_modules') || cleanId.includes('.pnpm')) return source;
  if (cleanId.includes('dist')) return source;
  if (cleanId.includes('archive')) return source;
  if (cleanId.includes('.vscode')) return source;
  if (cleanId.endsWith('.d.ts')) return source;
  if (!/\.(js|jsx|ts|tsx)$/.test(cleanId)) return source;
  if (!/\buse[A-Z][a-zA-Z]*\s*\(/.test(source)) return source;

  try {
    return sauronTransform(source, cleanId);
  } catch (e: any) {
    console.error(`[sauron] TRANSFORM ERROR in ${cleanId}:`, e.message);
    return source;
  }
}
