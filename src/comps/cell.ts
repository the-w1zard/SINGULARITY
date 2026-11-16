//                                    CELL  
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
import * as path from 'path'
import { parse as parseTS } from '@typescript-eslint/typescript-estree'
import { Registry } from "@the-w1zard/the-registry";


export function boxContains(b: RangeBox, key: string): boolean {
  const m = KEY_RE.exec(key)
  if (!m || (m[1] ?? undefined) !== b.sheet) return false
  const c = colToIndex(m[2]), r = +m[3]
  return c >= b.c1 && c <= b.c2 && r >= b.r1 && r <= b.r2
}
export function analyzeDependencies(node: ASTNode, cells = new Set<string>(), ranges: RangeBox[] = []): { cells: Set<string>; ranges: RangeBox[] } {
  switch (node.type) {
    case "CellRef":
      cells.add(cellKey(node.col.toUpperCase(), node.row, node.sheet)); break
    case "Range": {
      const a = colToIndex(node.start.col.toUpperCase()), b = colToIndex(node.end.col.toUpperCase())
      ranges.push({ sheet: node.sheet, c1: Math.min(a, b), c2: Math.max(a, b), r1: Math.min(node.start.row, node.end.row), r2: Math.max(node.start.row, node.end.row) })
      break
    }
    case "ColRange": {
      const a = colToIndex(node.start.toUpperCase()), b = colToIndex(node.end.toUpperCase())
      ranges.push({ sheet: node.sheet, c1: Math.min(a, b), c2: Math.max(a, b), r1: 1, r2: MAX_ROW })
      break
    }
    case "BinOp": analyzeDependencies(node.left, cells, ranges); analyzeDependencies(node.right, cells, ranges); break
    case "UnaryOp": analyzeDependencies(node.operand, cells, ranges); break
    case "FunctionCall": for (const arg of node.args) analyzeDependencies(arg, cells, ranges); break
    case "ArrayLiteral": for (const row of node.rows) for (const item of row) analyzeDependencies(item, cells, ranges); break
  }
  return { cells, ranges }
}
export function directDependents(graph: SpreadsheetGraphState, key: string): Set<string> {
  const out = new Set(graph.dependents.get(key) ?? [])
  for (const [target, boxes] of graph.ranges) if (boxes.some(b => boxContains(b, key))) out.add(target)
  return out
}
export function removeDependencies(graph: SpreadsheetGraphState, target: string) {
  for (const source of graph.deps.get(target) ?? []) {
    const downstream = graph.dependents.get(source)
    if (!downstream) continue
    downstream.delete(target)
    if (downstream.size === 0) graph.dependents.delete(source)
  }
  graph.deps.delete(target)
  graph.ranges.delete(target)
}
export function registerDependencies(graph: SpreadsheetGraphState, target: string, dependencies: Set<string>, ranges: RangeBox[] = []) {
  graph.deps.set(target, dependencies)
  for (const source of dependencies) {
    if (!graph.dependents.has(source)) graph.dependents.set(source, new Set())
    graph.dependents.get(source)!.add(target)
  }
  if (ranges.length) graph.ranges.set(target, ranges)
}
export function collectAffected(graph: SpreadsheetGraphState, changed: string): Set<string> {
  const affected = new Set<string>()
  const queue = [changed]
  for (let i = 0; i < queue.length; i++) {
    for (const target of directDependents(graph, queue[i])) {
      if (affected.has(target)) continue
      affected.add(target)
      queue.push(target)
    }
  }
  return affected
}
export function cloneGraph(graph: SpreadsheetGraphState | undefined): SpreadsheetGraphState {
  const cells = new Map<string, SpreadsheetCell>()
  for (const [key, cell] of graph?.cells ?? []) cells.set(key, { ...cell })
  const deps = new Map<string, Set<string>>()
  for (const [key, values] of graph?.deps ?? []) deps.set(key, new Set(values))
  const dependents = new Map<string, Set<string>>()
  for (const [key, values] of graph?.dependents ?? []) dependents.set(key, new Set(values))
  const ranges = new Map<string, RangeBox[]>()
  for (const [key, boxes] of graph?.ranges ?? []) ranges.set(key, boxes.map(b => ({ ...b })))
  return { cells, computed: new Map(graph?.computed ?? []), deps, dependents, ranges }
}
function stageCell(graph: SpreadsheetGraphState, key: string, raw: unknown, defaultSheet?: string) {
  removeDependencies(graph, key)
  const cell: SpreadsheetCell = { raw }
  if (isFormula(raw)) {
    try {
      const ast = qualifyAst(parseFormula(raw), defaultSheet)
      cell.ast = ast
      cell.volatile = isVolatileAst(ast)
      const { cells, ranges } = analyzeDependencies(ast)
      registerDependencies(graph, key, cells, ranges)
    } catch {
      // unparseable: keep raw, no ast, evaluates to #ERROR!
    }
  }
  graph.cells.set(key, cell)
}
export function recalculateDirty(graph: SpreadsheetGraphState, seeds: Iterable<string>, forced?: Iterable<string>): Set<string> {
  const seedSet = new Set(seeds)
  const dirty = new Set<string>(seedSet)
  for (const s of seedSet) for (const a of collectAffected(graph, s)) dirty.add(a)

  // edges between dirty cells only; a self-reference is an edge, so it never reaches in-degree 0 and becomes #CYCLE!
  const indegree = new Map<string, number>()
  for (const k of dirty) indegree.set(k, 0)
  const edges = new Map<string, string[]>()
  for (const u of dirty) {
    const out = [...directDependents(graph, u)].filter(t => dirty.has(t))
    edges.set(u, out)
    for (const t of out) indegree.set(t, indegree.get(t)! + 1)
  }

  const queue: string[] = []
  for (const [k, n] of indegree) if (n === 0) queue.push(k)

  const changed = new Set<string>(forced ?? seedSet)
  const done = new Set<string>()

  for (let i = 0; i < queue.length; i++) {
    const key = queue[i]
    const before = visible(graph, key)
    evaluateCell(graph, key)
    done.add(key)
    if (!sameValue(before, visible(graph, key))) changed.add(key)
    for (const t of edges.get(key) ?? []) {
      const left = indegree.get(t)! - 1
      indegree.set(t, left)
      if (left === 0) queue.push(t)
    }
  }

  for (const key of dirty) {
    if (done.has(key)) continue
    const cell = graph.cells.get(key)
    graph.computed.set(key, "#CYCLE!")
    if (cell) cell.computed = "#CYCLE!"
    changed.add(key)
  }
  return changed
}
const splitSections = (fmt: string) => {
  const out: string[] = []; let cur = "", q = false
  for (let i = 0; i < fmt.length; i++) {
    const ch = fmt[i]
    if (ch === "\\" && i + 1 < fmt.length) { cur += ch + fmt[++i]; continue }
    if (ch === '"') q = !q
    if (ch === ";" && !q) { out.push(cur); cur = ""; continue }
    cur += ch
  }
  return [...out, cur]
}
const unquote = (s: string) => s.replace(/"([^"]*)"/g, "$1").replace(/\\(.)/g, "$1").replace(/_./g, " ").replace(/\*./g, "")
const bare = (s: string) => s.replace(/"[^"]*"|\\.|_.|\*./g, "")
export function formatText(value: unknown, fmt: string): string {
  if (typeof value !== "number") return toStr(value)
  if (fmt.toLowerCase() === "general") return String(value)
  const parts = splitSections(fmt)
  let f = parts[0], v = value, neg = value < 0
  if (neg && parts.length > 1) { f = parts[1]; v = -value; neg = false }
  else if (value === 0 && parts.length > 2) f = parts[2]
  const b = bare(f)
  if (b === "@") return toStr(value)
  if (/[ymdhs]/i.test(b) && !/[0#?]/.test(b)) return formatDate(v, f)
  return (neg ? "-" : "") + formatNumber(Math.abs(v), f)
}
function fmtPlain(v: number, pat: string): string {
  let [ip, dp = ""] = pat.split(".")
  const scale = ip.match(/,+$/)?.[0].length ?? 0
  ip = ip.replace(/,+$/, "")
  const thousands = ip.includes(","); ip = ip.replace(/,/g, "")
  const dec = dp.replace(/[^0#?]/g, "").length
  let [i, d = ""] = round(v / 1000 ** scale, dec).toFixed(dec).split(".")
  const minInt = (ip.match(/0/g) ?? []).length
  i = i.padStart(minInt, "0")
  if (!minInt && /^0+$/.test(i)) i = ""
  const minDec = (dp.match(/0/g) ?? []).length
  while (d.length > minDec && d.endsWith("0")) d = d.slice(0, -1)
  if (thousands) i = i.replace(/\B(?=(\d{3})+(?!\d))/g, ",")
  return d ? `${i}.${d}` : i
}
function formatNumber(v: number, f: string): string {
  const masked = f.replace(/"[^"]*"|\\./g, s => "\u0001".repeat(s.length))
  v *= 100 ** (masked.match(/%/g) ?? []).length
  const m = /[0#?][0#?,.]*(?:[Ee][+-]0+)?/.exec(masked)
  if (!m) return unquote(f)
  const prefix = unquote(f.slice(0, m.index)), suffix = unquote(f.slice(m.index + m[0].length))
  const sci = /^(.*?)[Ee]([+-])(0+)$/.exec(m[0])
  if (!sci) return prefix + fmtPlain(v, m[0]) + suffix
  const [, mant, sign, ex] = sci
  let exp = v === 0 ? 0 : Math.floor(Math.log10(v)), mv = v / 10 ** exp
  const dec = (mant.split(".")[1] ?? "").replace(/[^0#?]/g, "").length
  if (round(mv, dec) >= 10) { exp++; mv /= 10 }
  return prefix + fmtPlain(mv, mant) + "E" + (exp < 0 ? "-" : sign === "+" ? "+" : "") + String(Math.abs(exp)).padStart(ex.length, "0") + suffix
}
const MONTHS = ["January","February","March","April","May","June","July","August","September","October","November","December"]
const DAYS = ["Sunday","Monday","Tuesday","Wednesday","Thursday","Friday","Saturday"]
const DT = /^(yyyy|yy|mmmm|mmm|mm|m|dddd|ddd|dd|d|hh|h|ss|s|AM\/PM|A\/P)$/i
function formatDate(v: number, f: string): string {
  const d = fromSerial(v), ampm = /AM\/PM|A\/P/i.test(bare(f)), h12 = d.getHours() % 12 || 12
  const toks: { t: string; lit: boolean }[] = []
  f.split(/("[^"]*")/).forEach((seg, i) => {
    if (i % 2) { toks.push({ t: seg.slice(1, -1), lit: true }); return }
    for (const p of seg.split(/(yyyy|yy|mmmm|mmm|mm|m|dddd|ddd|dd|d|hh|h|ss|s|AM\/PM|A\/P)/i)) if (p) toks.push({ t: p, lit: !DT.test(p) })
  })
  return toks.map((tk, i) => {
    if (tk.lit) return unquote(tk.t)
    const k = tk.t.toLowerCase()
    if (k === "m" || k === "mm") {
      const prev = toks.slice(0, i).reverse().find(x => !x.lit), next = toks.slice(i + 1).find(x => !x.lit)
      const n = /^h/i.test(prev?.t ?? "") || /^s/i.test(next?.t ?? "") ? d.getMinutes() : d.getMonth() + 1
      return k === "mm" ? String(n).padStart(2, "0") : String(n)
    }
    switch (k) {
      case "yyyy": return String(d.getFullYear())
      case "yy": return String(d.getFullYear() % 100).padStart(2, "0")
      case "mmmm": return MONTHS[d.getMonth()]
      case "mmm": return MONTHS[d.getMonth()].slice(0, 3)
      case "dddd": return DAYS[d.getDay()]
      case "ddd": return DAYS[d.getDay()].slice(0, 3)
      case "dd": return String(d.getDate()).padStart(2, "0")
      case "d": return String(d.getDate())
      case "hh": return String(ampm ? h12 : d.getHours()).padStart(2, "0")
      case "h": return String(ampm ? h12 : d.getHours())
      case "ss": return String(d.getSeconds()).padStart(2, "0")
      case "s": return String(d.getSeconds())
      case "am/pm": return d.getHours() < 12 ? "AM" : "PM"
      case "a/p": return d.getHours() < 12 ? "A" : "P"
    }
    return tk.t
  }).join("")
}
const EPOCH_UTC = Date.UTC(1899, 11, 30)
export function dateSerial(d: Date): number {
  const days = (Date.UTC(d.getFullYear(), d.getMonth(), d.getDate()) - EPOCH_UTC) / 86400000
  return days + (d.getHours() * 3600 + d.getMinutes() * 60 + d.getSeconds() + d.getMilliseconds() / 1000) / 86400
}
export function fromSerial(n: number): Date {
  const whole = Math.floor(n), ms = Math.round((n - whole) * 86400000)
  const d = new Date(1899, 11, 30 + whole)
  return new Date(d.getFullYear(), d.getMonth(), d.getDate(), 0, 0, 0, ms)
}
const isLeap = (y: number) => (y % 4 === 0 && y % 100 !== 0) || y % 400 === 0
function days360(s: Date, e: Date, european: boolean): number {
  let sd = s.getDate(), ed = e.getDate()
  if (european) { if (sd === 31) sd = 30; if (ed === 31) ed = 30 }
  else {
    const lastFeb = (d: Date) => d.getMonth() === 1 && d.getDate() === new Date(d.getFullYear(), 2, 0).getDate()
    const sl = lastFeb(s), el = lastFeb(e)
    if (sl && el) ed = 30
    if (sl) sd = 30
    if (ed === 31 && sd >= 30) ed = 30
    if (sd === 31) sd = 30
  }
  return (e.getFullYear() - s.getFullYear()) * 360 + (e.getMonth() - s.getMonth()) * 30 + (ed - sd)
}
const newton = (f: (x: number) => number, x0: number, tol = 1e-10, max = 100): number | undefined => {
  let x = x0
  for (let i = 0; i < max; i++) {
    const y = f(x); if (Math.abs(y) < tol) return x
    const h = Math.max(Math.abs(x), 1) * 1e-6
    const d = (f(x + h) - f(x - h)) / (2 * h)
    const nx = x - y / d
    if (!Number.isFinite(d) || d === 0 || !Number.isFinite(nx)) return undefined
    if (Math.abs(nx - x) < tol) return nx
    x = nx
  }
  return undefined
}
const sortedNums = (v: unknown) => (flattenRange(v).filter(x => typeof x === "number") as number[]).sort((x, y) => x - y)
const pctInc = (n: number[], p: number) => {
  if (!n.length || p < 0 || p > 1) return "#NUM!"
  const i = p * (n.length - 1), lo = Math.floor(i), hi = Math.ceil(i)
  return n[lo] + (n[hi] - n[lo]) * (i - lo)                 // old version multiplied the next value instead of the gap
}
const VOLATILE_FNS = new Set(["RAND", "RANDBETWEEN", "RANDARRAY", "NOW", "TODAY", "OFFSET", "INDIRECT"])
export function isVolatileAst(n: ASTNode): boolean {
  switch (n.type) {
    case "FunctionCall": return VOLATILE_FNS.has(n.name) || n.args.some(isVolatileAst)
    case "BinOp": return isVolatileAst(n.left) || isVolatileAst(n.right)
    case "UnaryOp": return isVolatileAst(n.operand)
    case "ArrayLiteral": return n.rows.some(r => r.some(isVolatileAst))
    default: return false
  }
}
const sameValue = (a: unknown, b: unknown) =>
  Object.is(a, b) || (Array.isArray(a) && Array.isArray(b) && JSON.stringify(a) === JSON.stringify(b))
type RefBox = { sheet?: string; c1: number; r1: number; c2: number; r2: number }
const keyOfBox = (b: RefBox) => cellKey(indexToCol(b.c1), b.r1, b.sheet)
const selfBox = (ctx: EvalContext): RefBox | undefined => {
  if (!ctx.self) return undefined
  const s = parseCellRef(ctx.self), c = colToIndex(s.col)
  return { sheet: s.sheet, c1: c, r1: s.row, c2: c, r2: s.row }
}
const refFromText = (text: unknown, ctx: EvalContext): RefBox | undefined => {
  const m = String(text).trim().replace(/\$/g, "").match(/^(?:'?([^'!]+)'?!)?([A-Za-z]+)(\d+)(?::([A-Za-z]+)(\d+))?$/)
  if (!m) return undefined
  const c1 = colToIndex(m[2].toUpperCase()), r1 = +m[3]
  const c2 = m[4] ? colToIndex(m[4].toUpperCase()) : c1, r2 = m[5] ? +m[5] : r1
  return { sheet: m[1] ?? ctx.sheet, c1: Math.min(c1, c2), r1: Math.min(r1, r2), c2: Math.max(c1, c2), r2: Math.max(r1, r2) }
}
const offsetBox = (b: RefBox, args: unknown[]): RefBox | undefined => {
  const [dr, dc, h, w] = args.map(x => (x === undefined ? undefined : toNum(x)))
  const height = h ?? b.r2 - b.r1 + 1, width = w ?? b.c2 - b.c1 + 1
  if (height < 1 || width < 1) return undefined
  const r1 = b.r1 + (dr ?? 0), c1 = b.c1 + (dc ?? 0)
  return { sheet: b.sheet, r1, c1, r2: r1 + height - 1, c2: c1 + width - 1 }
}
const refOf = (n: ASTNode, ctx: EvalContext, ev: (n: ASTNode) => unknown): RefBox | undefined => {
  if (n.type === "CellRef") { const c = colToIndex(n.col.toUpperCase()); return { sheet: n.sheet ?? ctx.sheet, c1: c, r1: n.row, c2: c, r2: n.row } }
  if (n.type === "Range") {
    const a = colToIndex(n.start.col.toUpperCase()), b = colToIndex(n.end.col.toUpperCase())
    return { sheet: n.sheet ?? ctx.sheet, c1: Math.min(a, b), c2: Math.max(a, b), r1: Math.min(n.start.row, n.end.row), r2: Math.max(n.start.row, n.end.row) }
  }
  if (n.type === "FunctionCall" && n.name === "INDIRECT") return refFromText(ev(n.args[0]), ctx)
  if (n.type === "FunctionCall" && n.name === "OFFSET") {
    const base = refOf(n.args[0], ctx, ev)
    return base && offsetBox(base, n.args.slice(1).map(x => ev(x)))
  }
  return undefined
}
const boxValue = (b: RefBox, ctx: EvalContext) => {
  if (b.c1 < 0 || b.r1 < 1) return "#REF!"
  const g = expandRange({ sheet: b.sheet, start: { col: indexToCol(b.c1), row: b.r1 }, end: { col: indexToCol(b.c2), row: b.r2 } }, ctx)
  return g.length === 1 && g[0].length === 1 ? g[0][0] : g
}
export const SPECIAL_FORMS: Record<string, SpecialForm> = {
  IF: (n, _c, ev) => { const c = ev(n[0]); if (isErr(c)) return c; return toBool(c) ? ev(n[1]) : n[2] ? ev(n[2]) : false },

  LAMBDA: (n, ctx) => {
    const params = n.slice(0, -1).map(nameOf)
    if (!n.length || params.some(p => !p)) return "#VALUE!"
    return { lambda: true, params: params as string[], body: n[n.length - 1], scope: ctx.scope ? new Map(ctx.scope) : undefined } as Lambda
  },
  LET: (n, ctx, ev) => {
    if (n.length < 3 || n.length % 2 === 0) return "#VALUE!"
    const scope = new Map(ctx.scope ?? []), c = { ...ctx, scope }
    for (let i = 0; i < n.length - 1; i += 2) {
      const name = nameOf(n[i]); if (!name) return "#VALUE!"
      scope.set(name, ev(n[i + 1], c))                          // earlier names visible to later ones
    }
    return ev(n[n.length - 1], c)
  },

  INDIRECT: (n, ctx, ev) => { const b = refOf({ type: "FunctionCall", name: "INDIRECT", args: n }, ctx, ev); return b ? boxValue(b, ctx) : "#REF!" },
  OFFSET:   (n, ctx, ev) => { const b = refOf({ type: "FunctionCall", name: "OFFSET", args: n }, ctx, ev); return b ? boxValue(b, ctx) : "#REF!" },

  ROW: (n, ctx, ev) => {
    const b = n.length ? refOf(n[0], ctx, ev) : selfBox(ctx); if (!b) return "#VALUE!"
    const h = b.r2 - b.r1 + 1
    return h === 1 ? b.r1 : Array.from({ length: h }, (_, i) => [b.r1 + i])
  },
  COLUMN: (n, ctx, ev) => {
    const b = n.length ? refOf(n[0], ctx, ev) : selfBox(ctx); if (!b) return "#VALUE!"
    const w = b.c2 - b.c1 + 1
    return w === 1 ? b.c1 + 1 : [Array.from({ length: w }, (_, i) => b.c1 + 1 + i)]
  },
  ANCHORARRAY: (n, ctx, ev) => { const b = refOf(n[0], ctx, ev); if (!b) return "#VALUE!"; const v = resolveCell(keyOfBox(b), ctx); return Array.isArray(v) ? v : "#REF!" },
  ISREF: (n, ctx, ev) => !!refOf(n[0], ctx, ev),
  ISFORMULA: (n, ctx, ev) => { const b = refOf(n[0], ctx, ev); return b ? isFormula(cellAt(ctx, keyOfBox(b))?.raw) : "#VALUE!" },
  FORMULATEXT: (n, ctx, ev) => {
    const b = refOf(n[0], ctx, ev); if (!b) return "#VALUE!"
    const raw = cellAt(ctx, keyOfBox(b))?.raw
    return isFormula(raw) ? raw : "#N/A"
  },
  CELL: (n, ctx, ev) => {
    const kind = String(ev(n[0])).toLowerCase()
    const b = n[1] ? refOf(n[1], ctx, ev) : selfBox(ctx); if (!b) return "#VALUE!"
    const key = keyOfBox(b)
    switch (kind) {
      case "address": return `$${indexToCol(b.c1)}$${b.r1}`
      case "row": return b.r1
      case "col": return b.c1 + 1
      case "contents": return resolveCell(key, ctx)
      case "filename": return b.sheet ?? ""
      case "type": { const v = resolveCell(key, ctx); return !cellAt(ctx, key) ? "b" : typeof v === "string" ? "l" : "v" }
      default: return "#VALUE!"
    }
  },
}
export type Lambda = { lambda: true; params?: string[]; body?: ASTNode; scope?: Map<string, unknown>; native?: Fn }
export const isLambda = (v: unknown): v is Lambda => typeof v === "object" && v !== null && (v as any).lambda === true
export function callLambda(f: unknown, args: unknown[], ctx: EvalContext): unknown {
  if (!isLambda(f)) return "#VALUE!"
  if (f.native) return normalizeResult(f.native(args, ctx))
  if (args.length !== f.params!.length) return "#VALUE!"
  const scope = new Map(f.scope ?? [])
  f.params!.forEach((p, i) => scope.set(p, args[i]))
  return evaluateAst(f.body!, { ...ctx, scope })
}
const nameOf = (n: ASTNode) => (n.type === "Name" ? n.name.toUpperCase() : undefined)
const grid = (v: unknown): unknown[][] =>  Array.isArray(v) ? (Array.isArray(v[0]) ? (v as unknown[][]) : [v as unknown[]]) : [[v]]
const transposeGrid = (m: unknown[][]) => m[0]?.map((_, i) => m.map(r => r[i])) ?? []
export const lambdaFns: Record<string, Fn> = {
  BYROW: (a, ctx) => grid(a[0]).map(row => [callLambda(a[1], [[row]], ctx)]),
  BYCOL: (a, ctx) => {
    const g = grid(a[0]), w = g[0]?.length ?? 0
    return [Array.from({ length: w }, (_, c) => callLambda(a[1], [g.map(r => [r[c]])], ctx))]
  },
  MAP: (a, ctx) => {
    const f = a[a.length - 1], gs = a.slice(0, -1).map(grid)
    const h = gs[0].length, w = gs[0][0]?.length ?? 0
    if (gs.some(g => g.length !== h || (g[0]?.length ?? 0) !== w)) return "#N/A"
    return Array.from({ length: h }, (_, r) => Array.from({ length: w }, (_, c) => callLambda(f, gs.map(g => g[r][c]), ctx)))
  },
  REDUCE: (a, ctx) => {
    let acc = a[0]
    for (const v of flattenRange(a[1])) { acc = callLambda(a[2], [acc, v], ctx); if (isErr(acc)) return acc }
    return acc
  },
  SCAN: (a, ctx) => { let acc = a[0]; return grid(a[1]).map(row => row.map(v => (acc = callLambda(a[2], [acc, v], ctx)))) },
  MAKEARRAY: (a, ctx) => {
    const rows = toNum(a[0]), cols = toNum(a[1])
    if (rows < 1 || cols < 1) return "#VALUE!"
    return Array.from({ length: rows }, (_, r) => Array.from({ length: cols }, (_, c) => callLambda(a[2], [r + 1, c + 1], ctx)))
  },
  WRAPROWS: (a) => { const n = toNum(a[1]); return n < 1 ? "#NUM!" : wrapVec(a) },
  WRAPCOLS: (a) => { const n = toNum(a[1]); return n < 1 ? "#NUM!" : transposeGrid(wrapVec(a)) },
  SINGLE: (a) => scalar(a[0]),
                      // needs a spill model, see end
  SEQUENCE: (a) => {                                // unchanged, moved here
    const rows = toNum(a[0]), cols = toNum(a[1] ?? 1), start = toNum(a[2] ?? 1), step = toNum(a[3] ?? 1)
    return Array.from({ length: rows }, (_, r) => Array.from({ length: cols }, (_, c) => start + step * (r * cols + c)))
  },
  RANDARRAY: (a) => {
    const rows = toNum(a[0] ?? 1), cols = toNum(a[1] ?? 1), min = toNum(a[2] ?? 0), max = toNum(a[3] ?? 1), whole = toBool(a[4])
    return Array.from({ length: rows }, () => Array.from({ length: cols }, () =>
      whole ? Math.floor(Math.random() * (max - min + 1)) + min : Math.random() * (max - min) + min))
  },
}
const wrapVec = (a: unknown[]) => {
  const v = flattenRange(a[0]), n = toNum(a[1]), pad = a[2] ?? "#N/A"
  const out: unknown[][] = []
  for (let i = 0; i < v.length; i += n) out.push(Array.from({ length: n }, (_, j) => (i + j < v.length ? v[i + j] : pad)))
  return out
}
export function asNumber(value: unknown): number | SpreadsheetError {
  if (isSpreadsheetError(value)) return value;

  if (value === null || value === undefined || value === "") {
    return 0;
  }

  if (typeof value === "number") {
    return Number.isFinite(value)
      ? value
      : "#NUM!";
  }

  if (typeof value === "boolean") {
    return value ? 1 : 0;
  }

  if (typeof value === "string") {
    const n = Number(value);

    return Number.isFinite(n)
      ? n
      : "#VALUE!";
  }

  return "#VALUE!";
}
export function scalar(value: unknown): unknown {
  if (!Array.isArray(value)) return value;

  const flat = flattenRange(value);

  return flat.length > 0
    ? flat[0]
    : 0;
}
export function compareValues(
  left: unknown,
  right: unknown
): number | SpreadsheetError {
  if (isSpreadsheetError(left)) return left;
  if (isSpreadsheetError(right)) return right;

  const leftNumber = asNumber(left);
  const rightNumber = asNumber(right);

  if (
    !isSpreadsheetError(leftNumber) &&
    !isSpreadsheetError(rightNumber)
  ) {
    return leftNumber - rightNumber;
  }

  return String(left).localeCompare(String(right));
}
export function evaluateBinary(
  op: string,
  left: unknown,
  right: unknown
): unknown {
  const a = scalar(left);
  const b = scalar(right);

  if (isSpreadsheetError(a)) return a;
  if (isSpreadsheetError(b)) return b;

  if (op === "&") {
    return `${a ?? ""}${b ?? ""}`;
  }

  if (
    op === "=" ||
    op === "==" ||
    op === "<>" ||
    op === "<" ||
    op === "<=" ||
    op === ">" ||
    op === ">="
  ) {
    const comparison = compareValues(a, b);

    if (isSpreadsheetError(comparison)) return comparison;

    switch (op) {
      case "=":
      case "==":
        return comparison === 0;

      case "<>":
        return comparison !== 0;

      case "<":
        return comparison < 0;

      case "<=":
        return comparison <= 0;

      case ">":
        return comparison > 0;

      case ">=":
        return comparison >= 0;
    }
  }

  const leftNumber = asNumber(a);
  const rightNumber = asNumber(b);

  if (isSpreadsheetError(leftNumber)) return leftNumber;
  if (isSpreadsheetError(rightNumber)) return rightNumber;

  switch (op) {
    case "+":
      return leftNumber + rightNumber;

    case "-":
      return leftNumber - rightNumber;

    case "*":
      return leftNumber * rightNumber;

    case "/":
      return rightNumber === 0
        ? "#DIV/0!"
        : leftNumber / rightNumber;

    case "^": {
      const result = leftNumber ** rightNumber;

      return Number.isFinite(result)
        ? result
        : "#NUM!";
    }

    default:
      return "#ERROR!";
  }
}
export function evaluateUnary(
  op: string,
  value: unknown
): unknown {
  const current = scalar(value);

  if (isSpreadsheetError(current)) return current;

  const number = asNumber(current);

  if (isSpreadsheetError(number)) return number;

  switch (op) {
    case "-":
      return -number;

    case "%":
      return number / 100;

    default:
      return "#ERROR!";
  }
}
export function recalculateCell(
  graph: SpreadsheetGraphState,
  key: string,
  visiting = new Set<string>(),
  completed = new Set<string>()
): unknown {
  if (completed.has(key)) {
    return graph.computed.get(key);
  }

  if (visiting.has(key)) {
    const cycle = "#CYCLE!";

    graph.computed.set(key, cycle);

    const cell = graph.cells.get(key);
    if (cell) cell.computed = cycle;

    return cycle;
  }

  const cell = graph.cells.get(key);

  if (!cell) {
    return 0;
  }

  if (!isFormula(cell.raw)) {
    graph.computed.delete(key);
    completed.add(key);
    return cell.raw ?? 0;
  }

  visiting.add(key);

  const dependencies = graph.deps.get(key) ?? new Set<string>();

  for (const dependency of dependencies) {
    const dependencyCell = graph.cells.get(dependency);

    if (dependencyCell && isFormula(dependencyCell.raw)) {
      recalculateCell(
        graph,
        dependency,
        visiting,
        completed
      );
    }
  }

  let result: unknown;

  try {
    result = evaluateSpreadsheetAst(
      cell.ast ?? parseFormula(cell.raw),
      graph
    );
  } catch {
    result = "#ERROR!";
  }

  cell.ast ??= parseFormula(cell.raw);
  cell.computed = result;
  graph.computed.set(key, result);

  visiting.delete(key);
  completed.add(key);

  return result;
}
export function recalculateAffected(
  graph: SpreadsheetGraphState,
  changed: string
) {
  const affected = collectAffected(graph, changed);
  const completed = new Set<string>();

  for (const key of affected) {
    recalculateCell(graph, key, new Set(), completed);
  }
}
export function createEvalContext(graph: SpreadsheetGraphState): EvalContext {
  return {
    store: graph.cells,
    computed: graph.computed
  };
}
export function isFormula(value: unknown): value is string {
  return typeof value === "string" && value.startsWith("=");
}
export function normalizeCellRef(input: string, defaultSheet?: string): string {
  const match = input.match(
    /^(?:(.+)!)?([A-Za-z]+)(\d+)$/
  );

  if (!match) {
    throw new Error(
      `[useSpreadsheet] Invalid cell reference "${input}". Expected A1 or Sheet!A1.`
    );
  }

  const sheet = match[1] ?? defaultSheet;
  const col = match[2].toUpperCase();
  const row = Number(match[3]);

  return cellKey(col, row, sheet);
}
export function parseCellRef(input: string, defaultSheet?: string): SpreadsheetCellRef {
  const normalized = normalizeCellRef(input, defaultSheet);
  const match = normalized.match(
    /^(?:(.+)!)?([A-Z]+)(\d+)$/
  )!;

  return {
    sheet: match[1],
    col: match[2],
    row: Number(match[3])
  };
}
export function createSpreadsheetGraph(initial?: SpreadsheetGraphState): SpreadsheetGraphState {
  return cloneGraph(initial);
}
const numbersOrError = (v: unknown): number[] | string => {
  const out: number[] = []
  for (const x of flattenRange(v)) {
    const e = errCode(x)
    if (e) return e
    if (typeof x === "number") out.push(x)
  }
  return out
}
export const math: Record<string, Fn> = {
  SUM: (a) => { const n = numbersOrError(a); return typeof n === "string" ? n : n.reduce((s, v) => s + v, 0) },
  SUMIF: (a, ctx) => {
    const range = flattenRange(a[0])
    const crit = a[1]
    const sumRange = a[2] !== undefined ? flattenRange(a[2]) : range
    return range.reduce((s: any, v: any, i: any) => s + (matchCriteria(v, crit) ? toNum(sumRange[i] ?? 0) : 0), 0)
  },
  SUMIFS: (a) => {
    const sumRange = flattenRange(a[0])
    let result = 0
    for (let i = 0; i < sumRange.length; i++) {
      let match = true
      for (let p = 1; p < a.length - 1; p += 2) {
        const cr = flattenRange(a[p])
        if (!matchCriteria(cr[i], a[p + 1])) { match = false; break }
      }
      if (match) result += toNum(sumRange[i])
    }
    return result
  },
  PRODUCT: (a) => { const n = numbersOrError(a); return typeof n === "string" ? n : n.length ? n.reduce((p, v) => p * v, 1) : 0 },
  AVERAGE: (a) => { const n = numbersOrError(a); return typeof n === "string" ? n : n.length ? n.reduce((s, v) => s + v, 0) / n.length : "#DIV/0!" },
  AVERAGEIF: (a) => {
    const range = flattenRange(a[0])
    const crit = a[1]
    const avgRange = a[2] !== undefined ? flattenRange(a[2]) : range
    const nums = range.reduce<number[]>((acc, v, i) => { if (matchCriteria(v, crit)) acc.push(toNum(avgRange[i] ?? 0)); return acc }, [])
    return nums.length ? nums.reduce((s, v) => s + v, 0) / nums.length : { error: '#DIV/0!' }
  },
  COUNT: (a) => flattenRange(a).filter(v => typeof v === "number").length,
  COUNTA: (a) => flattenRange(a).filter(v => v !== null && v !== undefined && v !== '').length,
  COUNTBLANK: (a) => flattenRange(a).filter(v => v === null || v === undefined || v === '').length,
  COUNTIF: (a) => flattenRange(a[0]).filter(v => matchCriteria(v, a[1])).length,
  COUNTIFS: (a) => {
    const ranges = []
    const crits: any = []
    for (let i = 0; i < a.length; i += 2) { ranges.push(flattenRange(a[i])); crits.push(a[i + 1]) }
    const len = ranges[0]?.length ?? 0
    let count = 0
    for (let i = 0; i < len; i++) if (ranges.every((r, ri) => matchCriteria(r[i], crits[ri]))) count++
    return count
  },
  MIN: (a) => { const n = numbersOrError(a); return typeof n === "string" ? n : n.length ? n.reduce((m, v) => (v < m ? v : m)) : 0 },
  MAX: (a) => { const n = numbersOrError(a); return typeof n === "string" ? n : n.length ? n.reduce((m, v) => (v > m ? v : m)) : 0 },
  MINA: (a) => { const n = flattenRange(a).map(toNum); return n.length ? Math.min(...n) : 0 },
  MAXA: (a) => { const n = flattenRange(a).map(toNum); return n.length ? Math.max(...n) : 0 },
  SMALL: (a) => { const n = flattenRange(a[0]).map(toNum).sort((x, y) => x - y); return n[toNum(a[1]) - 1] ?? { error: '#NUM!' } },
  LARGE: (a) => { const n = flattenRange(a[0]).map(toNum).sort((x, y) => y - x); return n[toNum(a[1]) - 1] ?? { error: '#NUM!' } },
  ABS: (a) => Math.abs(toNum(a[0])),
  SIGN: (a) => Math.sign(toNum(a[0])),
  SQRT: (a) => { const v = toNum(a[0]); return v < 0 ? { error: '#NUM!' } : Math.sqrt(v) },
  SQRTPI: (a) => Math.sqrt(Math.PI * toNum(a[0])),
  PI: () => Math.PI,
  EXP: (a) => Math.exp(toNum(a[0])),
  LN: (a) => Math.log(toNum(a[0])),
  LOG: (a) => Math.log(toNum(a[0])) / Math.log(toNum(a[1] ?? 10)),
  LOG10: (a) => Math.log10(toNum(a[0])),
  POWER: (a) => Math.pow(toNum(a[0]), toNum(a[1])),
  MOD: (a) => { const d = toNum(a[1]); return d === 0 ? { error: '#DIV/0!' } : toNum(a[0]) % d },
  QUOTIENT: (a) => { const d = toNum(a[1]); return d === 0 ? { error: '#DIV/0!' } : Math.trunc(toNum(a[0]) / d) },
  INT: (a) => Math.floor(toNum(a[0])),
  TRUNC: (a) => Math.trunc(toNum(a[0])),
  ROUND: (a) => round(toNum(a[0]), toNum(a[1] ?? 0)),
  ROUNDUP: (a) => { const f = Math.pow(10, toNum(a[1] ?? 0)); return Math.ceil(toNum(a[0]) * f) / f },
  ROUNDDOWN: (a) => { const f = Math.pow(10, toNum(a[1] ?? 0)); return Math.floor(toNum(a[0]) * f) / f },
  MROUND: (a) => { const m = toNum(a[1]); return m === 0 ? 0 : Math.round(toNum(a[0]) / m) * m },
  CEILING: (a) => { const m = toNum(a[1] ?? 1); return m === 0 ? 0 : Math.ceil(toNum(a[0]) / m) * m },
  FLOOR: (a) => { const m = toNum(a[1] ?? 1); return m === 0 ? 0 : Math.floor(toNum(a[0]) / m) * m },
  EVEN: (a) => { const v = Math.ceil(Math.abs(toNum(a[0]))); return (v % 2 === 0 ? v : v + 1) * (toNum(a[0]) < 0 ? -1 : 1) },
  ODD: (a) => { const v = Math.ceil(Math.abs(toNum(a[0]))); return (v % 2 !== 0 ? v : v + 1) * (toNum(a[0]) < 0 ? -1 : 1) },
  RAND: () => Math.random(),
  RANDBETWEEN: (a) => Math.floor(Math.random() * (toNum(a[1]) - toNum(a[0]) + 1)) + toNum(a[0]),
  FACT: (a) => { let n = toNum(a[0]); let r = 1; while (n > 1) r *= n--; return r },
  COMBIN: (a) => combin(toNum(a[0]), toNum(a[1])),
  PERMUT: (a) => { let n = toNum(a[0]), k = toNum(a[1]), r = 1; for (let i = 0; i < k; i++) r *= (n - i); return r },
  GCD: (a) => flattenRange(a).map(toNum).reduce((g, v) => gcd(Math.abs(g), Math.abs(v))),
  LCM: (a) => flattenRange(a).map(toNum).reduce((l, v) => Math.abs(l * v) / gcd(Math.abs(l), Math.abs(v))),
  SIN: (a) => Math.sin(toNum(a[0])), COS: (a) => Math.cos(toNum(a[0])), TAN: (a) => Math.tan(toNum(a[0])),
  ASIN: (a) => Math.asin(toNum(a[0])), ACOS: (a) => Math.acos(toNum(a[0])), ATAN: (a) => Math.atan(toNum(a[0])),
  ATAN2: (a) => Math.atan2(toNum(a[0]), toNum(a[1])),
  SINH: (a) => Math.sinh(toNum(a[0])), COSH: (a) => Math.cosh(toNum(a[0])), TANH: (a) => Math.tanh(toNum(a[0])),
  DEGREES: (a) => toNum(a[0]) * 180 / Math.PI,
  RADIANS: (a) => toNum(a[0]) * Math.PI / 180,
  ROMAN: (a) => toRoman(toNum(a[0])),
  SUMPRODUCT: (a) => {
    const ranges = a.map(x => flattenRange(x).map(toNum))
    const len = ranges[0]?.length ?? 0
    let s = 0
    for (let i = 0; i < len; i++) s += ranges.reduce((p, r) => p * r[i], 1)
    return s
  },
  MMULT: (a) => {
    const A = a[0] as number[][], B = a[1] as number[][]
    if (!Array.isArray(A) || !Array.isArray(B)) return { error: '#VALUE!' }
    return A.map(row => B[0].map((_, j) => row.reduce((s, _, k) => s + row[k] * B[k][j], 0)))
  },
  TRANSPOSE: (a) => {
    const m = a[0] as unknown[][]
    if (!Array.isArray(m)) return [[a[0]]]
    return m[0].map((_, i) => m.map(r => r[i]))
  },
}
export const text: Record<string, Fn> = {
  LEN: (a) => toStr(a[0]).length,
  LEFT: (a) => toStr(a[0]).slice(0, toNum(a[1] ?? 1)),
  RIGHT: (a) => { const s = toStr(a[0]); return s.slice(s.length - toNum(a[1] ?? 1)) },
  MID: (a) => toStr(a[0]).slice(toNum(a[1]) - 1, toNum(a[1]) - 1 + toNum(a[2])),
  UPPER: (a) => toStr(a[0]).toUpperCase(),
  LOWER: (a) => toStr(a[0]).toLowerCase(),
  PROPER: (a) => toStr(a[0]).replace(/\b\w/g, c => c.toUpperCase()),
  TRIM: (a) => toStr(a[0]).trim().replace(/\s+/g, ' '),
  CLEAN: (a) => toStr(a[0]).replace(/[\x00-\x1F]/g, ''),
  CONCATENATE: (a) => flattenRange(a).map(toStr).join(''),
  CONCAT: (a) => flattenRange(a).map(toStr).join(''),
  TEXTJOIN: (a) => {
    const delim = toStr(a[0])
    const ignoreEmpty = toBool(a[1])
    const vals = flattenRange(a.slice(2)).map(toStr)
    return (ignoreEmpty ? vals.filter(v => v !== '') : vals).join(delim)
  },
  FIND: (a) => { const i = toStr(a[1]).indexOf(toStr(a[0]), toNum(a[2] ?? 1) - 1); return i === -1 ? { error: '#VALUE!' } : i + 1 },
  SEARCH: (a) => {
    const hay = toStr(a[1]).toLowerCase()
    const needle = toStr(a[0]).toLowerCase().replace(/[*?]/g, m => m === '*' ? '.*' : '.')
    const re = new RegExp(needle)
    const m = hay.slice(toNum(a[2] ?? 1) - 1).search(re)
    return m === -1 ? { error: '#VALUE!' } : m + toNum(a[2] ?? 1)
  },
  SUBSTITUTE: (a) => {
    const src = toStr(a[0]), from = toStr(a[1]), to = toStr(a[2])
    if (a[3] !== undefined) {
      let n = 0, i = -1, result = src
      const nth = toNum(a[3])
      while ((i = result.indexOf(from, i + 1)) !== -1) { n++; if (n === nth) return result.slice(0, i) + to + result.slice(i + from.length) }
      return src
    }
    return src.split(from).join(to)
  },
  REPLACE: (a) => { const s = toStr(a[0]); return s.slice(0, toNum(a[1]) - 1) + toStr(a[3]) + s.slice(toNum(a[1]) - 1 + toNum(a[2])) },
  REPT: (a) => toStr(a[0]).repeat(Math.max(0, toNum(a[1]))),
  CHAR: (a) => String.fromCharCode(toNum(a[0])),
  CODE: (a) => toStr(a[0]).charCodeAt(0),
  EXACT: (a) => toStr(a[0]) === toStr(a[1]),
  TEXT: (a) => formatText(toNum(a[0]), toStr(a[1])),
  VALUE: (a) => { const n = parseFloat(toStr(a[0]).replace(/[,$%]/g, '')); return isNaN(n) ? { error: '#VALUE!' } : n },
  FIXED: (a) => toNum(a[0]).toFixed(toNum(a[1] ?? 2)),
  DOLLAR: (a) => '$' + toNum(a[0]).toFixed(toNum(a[1] ?? 2)),
  NUMBERVALUE: (a) => parseFloat(toStr(a[0]).replace(toStr(a[2] ?? ','), '').replace(toStr(a[1] ?? '.'), '.')),
  UNICHAR: (a) => String.fromCodePoint(toNum(a[0])),
  UNICODE: (a) => toStr(a[0]).codePointAt(0) ?? 0,
  ASC: (a) => toStr(a[0]),
  BAHTTEXT: (a) => String(a[0]),
  PHONETIC: (a) => toStr(a[0]),
  ARRAYTOTEXT: (a) => flattenRange(a[0]).map(toStr).join(toNum(a[1] ?? 0) === 1 ? ', ' : ','),
  VALUETOTEXT: (a) => typeof a[0] === 'string' ? (toNum(a[1] ?? 0) === 1 ? `"${a[0]}"` : toStr(a[0])) : toStr(a[0]),
}
export const logical: Record<string, Fn> = {
  IF: (a) => toBool(a[0]) ? a[1] : (a[2] !== undefined ? a[2] : false),
  IFS: (a) => {
    for (let i = 0; i < a.length - 1; i += 2) if (toBool(a[i])) return a[i + 1]
    return { error: '#N/A' }
  },
  AND: (a) => flattenRange(a).every(toBool),
  OR: (a) => flattenRange(a).some(toBool),
  NOT: (a) => !toBool(a[0]),
  XOR: (a) => flattenRange(a).filter(toBool).length % 2 !== 0,
  TRUE: () => true,
  FALSE: () => false,
  SWITCH: (a) => {
    const expr = a[0]
    for (let i = 1; i < a.length - 1; i += 2) if (looseEqual(expr, a[i])) return a[i + 1]
    return a.length % 2 === 0 ? a[a.length - 1] : { error: '#N/A' }
  },
  IFERROR: (a) => isErr(a[0]) ? a[1] : a[0],
  IFNA: (a) => errCode(a[0]) === "#N/A" ? a[1] : a[0],
  ISBLANK: (a) => a[0] === null || a[0] === undefined || a[0] === '',
  ISERROR: (a) => isErr(a[0]),
  ISERR: (a) => isErr(a[0]) && errCode(a[0]) !== "#N/A",
  ISNA: (a) => errCode(a[0]) === "#N/A",
  "ERROR.TYPE": (a) => {
    const map: Record<string, number> = { "#NULL!": 1, "#DIV/0!": 2, "#VALUE!": 3, "#REF!": 4, "#NAME?": 5, "#NUM!": 6, "#N/A": 7 }
    return map[errCode(a[0]) ?? ""] ?? "#N/A"
  },
  ISLOGICAL: (a) => typeof a[0] === 'boolean',
  ISNUMBER: (a) => typeof a[0] === 'number',
  ISTEXT: (a) => typeof a[0] === 'string',
  ISNONTEXT: (a) => typeof a[0] !== 'string',
  ISEVEN: (a) => toNum(a[0]) % 2 === 0,
  ISODD: (a) => toNum(a[0]) % 2 !== 0,
  N: (a) => typeof a[0] === 'number' ? a[0] : typeof a[0] === 'boolean' ? (a[0] ? 1 : 0) : 0,
  NA: () => ({ error: '#N/A' }),
  TYPE: (a) => { if (typeof a[0] === 'number') return 1; if (typeof a[0] === 'string') return 2; if (typeof a[0] === 'boolean') return 4; if (isErr(a[0])) return 16; if (Array.isArray(a[0])) return 64; return 0 },

}
export const lookup: Record<string, Fn> = {
  VLOOKUP: (a) => {
    const val = a[0], data = a[1] as unknown[][], colIdx = toNum(a[2]) - 1, approx = a[3] !== false && a[3] !== 0
    if (!Array.isArray(data)) return { error: '#VALUE!' }
    const col0 = data.map(r => r[0])
    const i = approx ? approxMatch(val, col0) : col0.findIndex(v => looseEqual(v, val))
    return i === -1 ? { error: '#N/A' } : (data[i]?.[colIdx] ?? { error: '#REF!' })
  },
  HLOOKUP: (a) => {
    const val = a[0], data = a[1] as unknown[][], rowIdx = toNum(a[2]) - 1, approx = a[3] !== false && a[3] !== 0
    if (!Array.isArray(data)) return { error: '#VALUE!' }
    const row0 = data[0] ?? []
    const i = approx ? approxMatch(val, row0) : row0.findIndex(v => looseEqual(v, val))
    return i === -1 ? { error: '#N/A' } : (data[rowIdx]?.[i] ?? { error: '#REF!' })
  },
  XLOOKUP: (a) => {
    const val = a[0], look = flattenRange(a[1]), ret = flattenRange(a[2])
    const notFound = a[3] === undefined ? "#N/A" : a[3]
    const mode = toNum(a[4] ?? 0), search = toNum(a[5] ?? 1)
    const order = [...look.keys()]
    if (search < 0) order.reverse()
    const i = findMatch(val, look, order, mode)
    return i === -1 ? notFound : (ret[i] ?? "#N/A")
  },
  INDEX: (a) => {
    const data = a[0] as unknown[][]
    if (!Array.isArray(data)) return data
    const r = toNum(a[1]), c = toNum(a[2] ?? 1)
    if (r === 0) return data.map(row => row[c - 1])
    if (c === 0) return data[r - 1]
    return data[r - 1]?.[c - 1] ?? { error: '#REF!' }
  },
  MATCH: (a) => {
    const val = a[0], arr = flattenRange(a[1]), mode = toNum(a[2] ?? 1)
    let i = -1
    if (mode === 0) i = findMatch(val, arr, [...arr.keys()], typeof val === "string" ? 2 : 0)
    else if (mode > 0) i = approxMatch(val, arr)
    else { for (let j = 0; j < arr.length; j++) { if (compare(arr[j], val) >= 0) i = j; else break } }
    return i === -1 ? "#N/A" : i + 1
  },
  LOOKUP: (a) => {
    const val = a[0], lookArr = flattenRange(a[1]), retArr = a[2] !== undefined ? flattenRange(a[2]) : lookArr
    const i = approxMatch(val, lookArr)
    return i === -1 ? { error: '#N/A' } : retArr[i]
  },
  CHOOSE: (a) => { const i = toNum(a[0]); return a[i] ?? { error: '#VALUE!' } },


  COLUMNS: (a) => Array.isArray(a[0]) ? ((a[0] as unknown[][])[0]?.length ?? 1) : 1,
  SORT: (a) => {
    const data = a[0] as unknown[][]
    if (!Array.isArray(data)) return [[a[0]]]
    const col = toNum(a[1] ?? 1) - 1
    const dir = toNum(a[2] ?? 1)
    return [...data].sort((x, y) => {
      const vx = Array.isArray(x) ? x[col] : x
      const vy = Array.isArray(y) ? y[col] : y
      return dir * compare(vx, vy)
    })
  },
  SORTBY: (a) => {
    const data = a[0] as unknown[][]
    if (!Array.isArray(data)) return [[a[0]]]
    const keys: [unknown[], number][] = []
    for (let i = 1; i < a.length; i += 2) keys.push([flattenRange(a[i]), toNum(a[i + 1] ?? 1)])
    return [...data].sort((x, y) => {
      const ix = (data as unknown[][]).indexOf(x)
      const iy = (data as unknown[][]).indexOf(y)
      for (const [arr, dir] of keys) { const c = dir * compare(arr[ix], arr[iy]); if (c !== 0) return c }
      return 0
    })
  },
  FILTER: (a) => {
    const data = a[0] as unknown[][]
    const mask = flattenRange(a[1])
    if (!Array.isArray(data)) return a[2] ?? { error: '#CALC!' }
    const out = data.filter((_, i) => toBool(mask[i]))
    return out.length ? out : (a[2] ?? { error: '#CALC!' })
  },
  UNIQUE: (a) => {
    const data = a[0] as unknown[][]
    if (!Array.isArray(data)) return [[a[0]]]
    const seen = new Set<string>()
    return data.filter(r => { const k = JSON.stringify(r); if (seen.has(k)) return false; seen.add(k); return true })
  },
  XMATCH: (a) => {
    const arr = flattenRange(a[1]), mode = toNum(a[2] ?? 0), search = toNum(a[3] ?? 1)
    const order = [...arr.keys()]
    if (search < 0) order.reverse()
    const i = findMatch(a[0], arr, order, mode)
    return i === -1 ? "#N/A" : i + 1
  },
  HYPERLINK: (a) => a[1] ?? a[0],
  GETPIVOTDATA: () => ({ error: '#REF!' }),
}
export const dateTime: Record<string, Fn> = {
TODAY: () => Math.floor(dateSerial(new Date())),
NOW: () => dateSerial(new Date()),
DATE: (a) => Math.floor(dateSerial(new Date(toNum(a[0]), toNum(a[1]) - 1, toNum(a[2])))),
EDATE: (a) => { const d = fromSerial(toNum(a[0])); d.setMonth(d.getMonth() + toNum(a[1])); return Math.floor(dateSerial(d)) },
EOMONTH: (a) => { const d = fromSerial(toNum(a[0])); d.setMonth(d.getMonth() + toNum(a[1]) + 1, 0); return Math.floor(dateSerial(d)) },
DATEVALUE: (a) => { const d = new Date(toStr(a[0])); return isNaN(d.getTime()) ? "#VALUE!" : Math.floor(dateSerial(d)) },
DAYS360: (a) => days360(fromSerial(toNum(a[0])), fromSerial(toNum(a[1])), toBool(a[2])),
YEARFRAC: (a) => {
  let s = toNum(a[0]), e = toNum(a[1]); const basis = toNum(a[2] ?? 0)
  if (s > e) [s, e] = [e, s]
  const sd = fromSerial(s), ed = fromSerial(e)
  switch (basis) {
    case 0: return days360(sd, ed, false) / 360
    case 4: return days360(sd, ed, true) / 360
    case 2: return (e - s) / 360
    case 3: return (e - s) / 365
    case 1: {
      const y1 = sd.getFullYear(), y2 = ed.getFullYear()
      let total = 0; for (let y = y1; y <= y2; y++) total += isLeap(y) ? 366 : 365
      return (e - s) / (total / (y2 - y1 + 1))
    }
    default: return "#NUM!"
  }
},
  TIME: (a) => (toNum(a[0]) * 3600 + toNum(a[1]) * 60 + toNum(a[2])) / 86400,
  YEAR: (a) => fromSerial(toNum(a[0])).getFullYear(),
  MONTH: (a) => fromSerial(toNum(a[0])).getMonth() + 1,
  DAY: (a) => fromSerial(toNum(a[0])).getDate(),
  HOUR: (a) => fromSerial(toNum(a[0])).getHours(),
  MINUTE: (a) => fromSerial(toNum(a[0])).getMinutes(),
  SECOND: (a) => fromSerial(toNum(a[0])).getSeconds(),
  WEEKDAY: (a) => { const d = fromSerial(toNum(a[0])).getDay(); const mode = toNum(a[1] ?? 1); return mode === 2 ? (d === 0 ? 7 : d) : mode === 3 ? (d === 0 ? 6 : d - 1) : d + 1 },
  WEEKNUM: (a) => { const d = fromSerial(toNum(a[0])); const jan1 = new Date(d.getFullYear(), 0, 1); return Math.ceil(((d.getTime() - jan1.getTime()) / 86400000 + jan1.getDay() + 1) / 7) },
  ISOWEEKNUM: (a) => { const d = fromSerial(toNum(a[0])); d.setHours(0, 0, 0, 0); d.setDate(d.getDate() + 4 - (d.getDay() || 7)); const y = new Date(d.getFullYear(), 0, 1); return Math.ceil(((d.getTime() - y.getTime()) / 86400000 + 1) / 7) },
  DAYS: (a) => toNum(a[0]) - toNum(a[1]),
  DAYS360: (a) => Math.round((toNum(a[0]) - toNum(a[1])) * 360 / 365),
  NETWORKDAYS: (a) => networkDays(toNum(a[0]), toNum(a[1]), a[2] ? flattenRange(a[2]).map(toNum) : []),
  WORKDAY: (a) => workday(toNum(a[0]), toNum(a[1]), a[2] ? flattenRange(a[2]).map(toNum) : []),
  DATEVALUE: (a) => { const d = new Date(toStr(a[0])); return isNaN(d.getTime()) ? { error: '#VALUE!' } : dateSerial(d) },
  TIMEVALUE: (a) => { const p = toStr(a[0]).split(':').map(Number); return (p[0] * 3600 + (p[1] ?? 0) * 60 + (p[2] ?? 0)) / 86400 },
  DATEDIF: (a) => { const s = fromSerial(toNum(a[0])), e = fromSerial(toNum(a[1])), u = toStr(a[2]).toUpperCase(); return dateDif(s, e, u) },
}
export const statistical: Record<string, Fn> = {
  
PERCENTILE: (a) => pctInc(sortedNums(a[0]), toNum(a[1])),
QUARTILE: (a) => { const q = Math.trunc(toNum(a[1])); return q < 0 || q > 4 ? "#NUM!" : pctInc(sortedNums(a[0]), q / 4) },
PERCENTRANK: (a) => {
  const arr = sortedNums(a[0]), x = toNum(a[1]), sig = toNum(a[2] ?? 3), n = arr.length
  if (!n || x < arr[0] || x > arr[n - 1]) return "#N/A"
  if (n === 1) return 1
  const i = arr.findIndex(v => v >= x)
  const r = arr[i] === x ? i / (n - 1) : (i - 1 + (x - arr[i - 1]) / (arr[i] - arr[i - 1])) / (n - 1)
  const f = 10 ** sig; return Math.floor(r * f) / f
},
MODE: (a) => {
  const freq = new Map<number, number>()
  for (const v of flattenRange(a)) if (typeof v === "number") freq.set(v, (freq.get(v) ?? 0) + 1)
  let max = 0, mode = 0; for (const [v, f] of freq) if (f > max) { max = f; mode = v }
  return max < 2 ? "#N/A" : mode
},
  STDEV: (a) => stddev(flattenRange(a).map(toNum), false),
  STDEVP: (a) => stddev(flattenRange(a).map(toNum), true),
  STDEVA: (a) => stddev(flattenRange(a).map(toNum), false),
  STDEVPA: (a) => stddev(flattenRange(a).map(toNum), true),
  VAR: (a) => variance(flattenRange(a).map(toNum), false),
  VARP: (a) => variance(flattenRange(a).map(toNum), true),
  VARA: (a) => variance(flattenRange(a).map(toNum), false),
  VARPA: (a) => variance(flattenRange(a).map(toNum), true),
  MEDIAN: (a) => { const n = flattenRange(a).map(toNum).sort((x, y) => x - y); const m = Math.floor(n.length / 2); return n.length % 2 ? n[m] : (n[m - 1] + n[m]) / 2 },
  RANK: (a) => { const v = toNum(a[0]); const arr = flattenRange(a[1]).map(toNum); const desc = a[2] !== 1; return desc ? arr.filter(x => x > v).length + 1 : arr.filter(x => x < v).length + 1 },
  NORMDIST: (a) => { const x = toNum(a[0]), m = toNum(a[1]), s = toNum(a[2]); return toBool(a[3]) ? normalCDF((x - m) / s) : normalPDF((x - m) / s) / s },
  NORMSDIST: (a) => normalCDF(toNum(a[0])),
  NORMINV: (a) => normalInv(toNum(a[0]), toNum(a[1] ?? 0), toNum(a[2] ?? 1)),
  NORMSINV: (a) => normalInv(toNum(a[0]), 0, 1),
  CORREL: (a) => { const x = flattenRange(a[0]).map(toNum), y = flattenRange(a[1]).map(toNum); return correl(x, y) },
  COVAR: (a) => { const x = flattenRange(a[0]).map(toNum), y = flattenRange(a[1]).map(toNum); return covariance(x, y, true) },
  FORECAST: (a) => { const x = toNum(a[0]), y = flattenRange(a[1]).map(toNum), xs = flattenRange(a[2]).map(toNum); const [b, m] = linreg(xs, y); return m * x + b },
  INTERCEPT: (a) => linreg(flattenRange(a[1]).map(toNum), flattenRange(a[0]).map(toNum))[0],
  SLOPE: (a) => linreg(flattenRange(a[1]).map(toNum), flattenRange(a[0]).map(toNum))[1],
  RSQ: (a) => Math.pow(correl(flattenRange(a[0]).map(toNum), flattenRange(a[1]).map(toNum)), 2),
  SKEW: (a) => { const n = flattenRange(a).map(toNum); const m = n.reduce((s, v) => s + v, 0) / n.length; const s = stddev(n, false); return n.length / (n.length - 1) / (n.length - 2) * n.reduce((acc, v) => acc + Math.pow((v - m) / s, 3), 0) },
  KURT: (a) => { const n = flattenRange(a).map(toNum), len = n.length; const m = n.reduce((s, v) => s + v, 0) / len; const s = stddev(n, false); return len * (len + 1) / (len - 1) / (len - 2) / (len - 3) * n.reduce((acc, v) => acc + Math.pow((v - m) / s, 4), 0) - 3 * (len - 1) * (len - 1) / (len - 2) / (len - 3) },
  GEOMEAN: (a) => { const n = flattenRange(a).map(toNum); return Math.pow(n.reduce((p, v) => p * v, 1), 1 / n.length) },
  HARMEAN: (a) => { const n = flattenRange(a).map(toNum); return n.length / n.reduce((s, v) => s + 1 / v, 0) },
  TRIMMEAN: (a) => { const n = flattenRange(a[0]).map(toNum).sort((x, y) => x - y); const cut = Math.floor(n.length * toNum(a[1]) / 2); const trimmed = n.slice(cut, n.length - cut); return trimmed.reduce((s, v) => s + v, 0) / trimmed.length },
  FREQUENCY: (a) => {
    const data = flattenRange(a[0]).map(toNum), bins = flattenRange(a[1]).map(toNum).sort((x, y) => x - y)
    const result = Array(bins.length + 1).fill(0)
    for (const v of data) { let placed = false; for (let i = 0; i < bins.length; i++) if (v <= bins[i]) { result[i]++; placed = true; break }; if (!placed) result[bins.length]++ }
    return result.map(v => [v])
  },
}
export const financial: Record<string, Fn> = {
  
RATE: (a) => {
  const n = toNum(a[0]), pmt = toNum(a[1]), pv = toNum(a[2]), fv = toNum(a[3] ?? 0), type = toNum(a[4] ?? 0)
  const f = (r: number) => r === 0 ? pv + pmt * n + fv : pv * (1 + r) ** n + pmt * (1 + r * type) * ((1 + r) ** n - 1) / r + fv
  return newton(f, toNum(a[5] ?? 0.1)) ?? "#NUM!"
},
IRR: (a) => {
  const vals = flattenRange(a[0]).filter(v => typeof v === "number") as number[]
  if (!vals.some(v => v > 0) || !vals.some(v => v < 0)) return "#NUM!"
  return newton(r => vals.reduce((s, v, i) => s + v / (1 + r) ** i, 0), toNum(a[1] ?? 0.1)) ?? "#NUM!"
},
DDB: (a) => {
  const cost = toNum(a[0]), salv = toNum(a[1]), life = toNum(a[2]), per = toNum(a[3]), factor = toNum(a[4] ?? 2)
  if (cost < 0 || salv < 0 || life <= 0 || per < 1 || per > life) return "#NUM!"
  let book = cost, dep = 0
  for (let p = 1; p <= per; p++) { dep = Math.max(0, Math.min(book * factor / life, book - salv)); book -= dep }
  return dep
},
DB: (a) => {
  const cost = toNum(a[0]), salv = toNum(a[1]), life = toNum(a[2]), per = toNum(a[3]), month = toNum(a[4] ?? 12)
  if (cost <= 0 || salv < 0 || life <= 0 || per < 1 || per > life + 1 || month < 1 || month > 12) return "#NUM!"
  const rate = Math.round((1 - (salv / cost) ** (1 / life)) * 1000) / 1000        // Excel rounds to 3 places
  let total = 0, dep = 0
  for (let p = 1; p <= per; p++) {
    dep = p === 1 ? cost * rate * month / 12 : p === life + 1 ? (cost - total) * rate * (12 - month) / 12 : (cost - total) * rate
    total += dep
  }
  return dep
},
  PV: (a) => { const r = toNum(a[0]), n = toNum(a[1]), pmt = toNum(a[2]), fv = toNum(a[3] ?? 0), type = toNum(a[4] ?? 0); return r === 0 ? -pmt * n - fv : -(pmt * (1 + r * type) * (1 - Math.pow(1 + r, -n)) / r + fv * Math.pow(1 + r, -n)) },
  FV: (a) => { const r = toNum(a[0]), n = toNum(a[1]), pmt = toNum(a[2]), pv = toNum(a[3] ?? 0), type = toNum(a[4] ?? 0); return r === 0 ? -pmt * n - pv : -(pmt * (1 + r * type) * (Math.pow(1 + r, n) - 1) / r + pv * Math.pow(1 + r, n)) },
  PMT: (a) => { const r = toNum(a[0]), n = toNum(a[1]), pv = toNum(a[2]), fv = toNum(a[3] ?? 0), type = toNum(a[4] ?? 0); return r === 0 ? -(pv + fv) / n : r * (fv + pv * Math.pow(1 + r, n)) / ((1 + r * type) * (1 - Math.pow(1 + r, n))) },
  NPER: (a) => { const r = toNum(a[0]), pmt = toNum(a[1]), pv = toNum(a[2]), fv = toNum(a[3] ?? 0), type = toNum(a[4] ?? 0); return r === 0 ? -(pv + fv) / pmt : Math.log((pmt * (1 + r * type) - fv * r) / (pmt * (1 + r * type) + pv * r)) / Math.log(1 + r) },
  NPV: (a) => { const r = toNum(a[0]); return flattenRange(a.slice(1)).map(toNum).reduce((s, v, i) => s + v / Math.pow(1 + r, i + 1), 0) },
  SLN: (a) => (toNum(a[0]) - toNum(a[1])) / toNum(a[2]),
  SYD: (a) => { const cost = toNum(a[0]), salv = toNum(a[1]), life = toNum(a[2]), per = toNum(a[3]); return (cost - salv) * (life - per + 1) * 2 / (life * (life + 1)) },
  EFFECT: (a) => Math.pow(1 + toNum(a[0]) / toNum(a[1]), toNum(a[1])) - 1,
  NOMINAL: (a) => (Math.pow(1 + toNum(a[0]), 1 / toNum(a[1])) - 1) * toNum(a[1]),
}
export const info: Record<string, Fn> = {
  ERROR: { TYPE: (a: any) => isErr(a[0]) ? 1 : { error: '#N/A' } } as unknown as Fn,
  INFO: (a) => ({ recalc: "Automatic", release: "1", system: "sheet" } as Record<string, string>)[toStr(a[0]).toLowerCase()] ?? "#VALUE!",
  ADDRESS: (a) => {
    const r = toNum(a[0]), c = toNum(a[1]), abs = toNum(a[2] ?? 1)
    if (r < 1 || c < 1 || abs < 1 || abs > 4) return "#VALUE!"
    let s: string
    if (a[3] === false || a[3] === 0) {
      s = `R${abs === 1 || abs === 3 ? r : `[${r}]`}C${abs === 1 || abs === 2 ? c : `[${c}]`}`
    } else {
      const col = indexToCol(c - 1)
      s = abs === 1 ? `$${col}$${r}` : abs === 2 ? `${col}$${r}` : abs === 3 ? `$${col}${r}` : `${col}${r}`
    }
    return a[4] ? `${toStr(a[4])}!${s}` : s
  },
}
export const testFn: Record<string, Fn> = {
  TEST: (a) => {
    const label = toStr(a[0])
    const result = toBool(a[1])
    return { test: true, label, pass: result }
  },
}
export const SPREADSHEET_FUNCTIONS: Record<string, Fn> = {
  ...math, ...text, ...logical, ...lookup, ...dateTime, ...statistical, ...financial, ...info, ...testFn,...lambdaFns,...SPECIAL_FORMS,
  LAMBDA: (a) => a[a.length - 1],
  BYROW: (a) => {
    const data = a[0] as unknown[][]
    if (!Array.isArray(data)) return [[a[0]]]
    return data.map(row => [a[1] instanceof Function ? (a[1] as Function)(row) : a[1]])
  },
  BYCOL: (a) => {
    const data = a[0] as unknown[][]
    if (!Array.isArray(data)) return [[a[0]]]
    const cols = (data[0]?.length ?? 1)
    return [Array.from({ length: cols }, (_, i) => data.map(r => r[i]))]
  },
  MAP: (a) => {
    const data = flattenRange(a[0])
    const fn = a[1]
    return data.map(v => fn instanceof Function ? (fn as Function)(v) : v)
  },
  REDUCE: (a) => {
    const init = a[0], data = flattenRange(a[1])
    return data.reduce((acc, v) => v, init)
  },
  MAKEARRAY: (a) => Array.from({ length: toNum(a[0]) }, (_, r) => Array.from({ length: toNum(a[1]) }, (_, c) => [r, c])),
  SEQUENCE: (a) => {
    const rows = toNum(a[0]), cols = toNum(a[1] ?? 1), start = toNum(a[2] ?? 1), step = toNum(a[3] ?? 1)
    return Array.from({ length: rows }, (_, r) => Array.from({ length: cols }, (_, c) => start + step * (r * cols + c)))
  },
  RANDARRAY: (a) => Array.from({ length: toNum(a[0] ?? 1) }, () => Array.from({ length: toNum(a[1] ?? 1) }, () => Math.random())),
  WRAP: (a) => a[0],
  ANCHORARRAY: (a) => a[0],
  SINGLE: (a) => Array.isArray(a[0]) ? (Array.isArray((a[0] as unknown[][])[0]) ? (a[0] as unknown[][])[0][0] : (a[0] as unknown[])[0]) : a[0],
}
export function round(v: number, dp: number): number {
  const f = Math.pow(10, dp)
  return Math.round(v * f) / f
}
export function gcd(a: number, b: number): number { return b === 0 ? a : gcd(b, a % b) }
export function combin(n: number, k: number): number {
  if (k > n) return 0
  let r = 1
  for (let i = 0; i < k; i++) r = r * (n - i) / (i + 1)
  return Math.round(r)
}
export function looseEqual(a: unknown, b: unknown): boolean {
  if (typeof a === 'string' && typeof b === 'string') return a.toUpperCase() === b.toUpperCase()
  return a === b
}
export function compare(a: unknown, b: unknown): number {
  if (typeof a === 'string' && typeof b === 'string') return a.localeCompare(b)
  return toNum(a) - toNum(b)
}
export function approxMatch(val: unknown, arr: unknown[]): number {
  let result = -1
  for (let i = 0; i < arr.length; i++) {
    if (compare(arr[i], val) <= 0) result = i
    else break
  }
  return result
}
// mode 0 exact, 2 wildcard, -1 exact-or-next-smaller, 1 exact-or-next-larger. `order` is the scan order.
export function findMatch(val: unknown, arr: unknown[], order: number[], mode: number): number {
  if (mode === 0) { for (const i of order) if (looseEqual(arr[i], val)) return i; return -1 }
  if (mode === 2) { for (const i of order) if (wildcardMatch(toStr(val), toStr(arr[i]))) return i; return -1 }
  let best = -1
  for (const i of order) {
    if (looseEqual(arr[i], val)) return i
    const c = compare(arr[i], val)
    if (mode === -1 && c < 0 && (best === -1 || compare(arr[i], arr[best]) > 0)) best = i
    if (mode === 1 && c > 0 && (best === -1 || compare(arr[i], arr[best]) < 0)) best = i
  }
  return best
}
export function wildcardMatch(pattern: string, value: string): boolean {
  const re = new RegExp('^' + pattern.replace(/\*/g, '.*').replace(/\?/g, '.') + '$', 'i')
  return re.test(value)
}
export function matchCriteria(v: unknown, crit: unknown): boolean {
  if (typeof crit === 'string') {
    const m = crit.match(/^([<>]=?|<>)(.*)$/)
    if (m) {
      const op = m[1], rhs = m[2]
      const n = parseFloat(rhs)
      if (!isNaN(n)) {
        switch (op) { case '>': return toNum(v) > n; case '>=': return toNum(v) >= n; case '<': return toNum(v) < n; case '<=': return toNum(v) <= n; case '<>': return !looseEqual(v, n) }
      }
      switch (op) { case '<>': return !looseEqual(v, rhs); default: return looseEqual(v, rhs) }
    }
    return wildcardMatch(crit, toStr(v))
  }
  return looseEqual(v, crit)
}
export function stddev(vals: number[], population: boolean): number {
  const v = variance(vals, population)
  return Math.sqrt(v)
}
export function variance(vals: number[], population: boolean): number {
  const n = vals.length
  if (n < 2) return 0
  const m = vals.reduce((s, v) => s + v, 0) / n
  const ss = vals.reduce((s, v) => s + Math.pow(v - m, 2), 0)
  return ss / (population ? n : n - 1)
}
export function correl(x: number[], y: number[]): number {
  const n = x.length
  const mx = x.reduce((s, v) => s + v, 0) / n
  const my = y.reduce((s, v) => s + v, 0) / n
  const num = x.reduce((s, v, i) => s + (v - mx) * (y[i] - my), 0)
  const den = Math.sqrt(x.reduce((s, v) => s + Math.pow(v - mx, 2), 0) * y.reduce((s, v) => s + Math.pow(v - my, 2), 0))
  return den === 0 ? 0 : num / den
}
export function covariance(x: number[], y: number[], population: boolean): number {
  const n = x.length
  const mx = x.reduce((s, v) => s + v, 0) / n
  const my = y.reduce((s, v) => s + v, 0) / n
  return x.reduce((s, v, i) => s + (v - mx) * (y[i] - my), 0) / (population ? n : n - 1)
}
export function linreg(x: number[], y: number[]): [number, number] {
  const n = x.length
  const sx = x.reduce((s, v) => s + v, 0)
  const sy = y.reduce((s, v) => s + v, 0)
  const sxx = x.reduce((s, v) => s + v * v, 0)
  const sxy = x.reduce((s, v, i) => s + v * y[i], 0)
  const m = (n * sxy - sx * sy) / (n * sxx - sx * sx)
  const b = (sy - m * sx) / n
  return [b, m]
}
export function normalPDF(z: number): number { return Math.exp(-0.5 * z * z) / Math.sqrt(2 * Math.PI) }
export function normalCDF(z: number): number {
  const t = 1 / (1 + 0.2316419 * Math.abs(z))
  const poly = t * (0.319381530 + t * (-0.356563782 + t * (1.781477937 + t * (-1.821255978 + t * 1.330274429))))
  const p = 1 - normalPDF(z) * poly
  return z >= 0 ? p : 1 - p
}
export function normalInv(p: number, mu: number, sigma: number): number {
  // Beasley-Springer-Moro approximation
  if (p <= 0) return -Infinity
  if (p >= 1) return Infinity
  const a = [2.50662823884, -18.61500062529, 41.39119773534, -25.44106049637]
  const b = [-8.47351093090, 23.08336743743, -21.06224101826, 3.13082909833]
  const c = [0.3374754822726147, 0.9761690190917186, 0.1607979714918209, 0.0276438810333863, 0.0038405729373609, 0.0003951896511349, 0.0000321767881768, 0.0000002888167364, 0.0000003960315187]
  const r = p < 0.5 ? p : 1 - p
  let x: number
  if (r > 0.02425) {
    const u = r - 0.5
    const v = u * u
    x = u * (((a[3] * v + a[2]) * v + a[1]) * v + a[0]) / ((((b[3] * v + b[2]) * v + b[1]) * v + b[0]) * v + 1)
  } else {
    const u = Math.sqrt(-2 * Math.log(r))
    x = ((((((((c[8] * u + c[7]) * u + c[6]) * u + c[5]) * u + c[4]) * u + c[3]) * u + c[2]) * u + c[1]) * u + c[0])
  }
  return mu + sigma * (p < 0.5 ? -x : x)
} 
export function networkDays(start: number, end: number, holidays: number[]): number {
  let count = 0
  const holidaySet = new Set(holidays)
  for (let d = Math.min(start, end); d <= Math.max(start, end); d++) {
    const day = fromSerial(d).getDay()
    if (day !== 0 && day !== 6 && !holidaySet.has(d)) count++
  }
  return count * (start <= end ? 1 : -1)
}
export function workday(start: number, days: number, holidays: number[]): number {
  const holidaySet = new Set(holidays)
  let d = start, rem = Math.abs(days), dir = days >= 0 ? 1 : -1
  while (rem > 0) {
    d += dir
    const day = fromSerial(d).getDay()
    if (day !== 0 && day !== 6 && !holidaySet.has(d)) rem--
  }
  return d
}
export function dateDif(s: Date, e: Date, unit: string): number | string {
  if (s > e) return "#NUM!"
  const utc = (d: Date) => Date.UTC(d.getFullYear(), d.getMonth(), d.getDate())
  const months = (e.getFullYear() - s.getFullYear()) * 12 + e.getMonth() - s.getMonth() - (e.getDate() < s.getDate() ? 1 : 0)
  switch (unit) {
    case "Y": return Math.floor(months / 12)
    case "M": return months
    case "D": return Math.round((utc(e) - utc(s)) / 86400000)
    case "YM": return months % 12
    case "MD": {
      let d = e.getDate() - s.getDate()
      if (d < 0) d += new Date(e.getFullYear(), e.getMonth(), 0).getDate()
      return d
    }
    case "YD": {
      let adj = new Date(e.getFullYear(), s.getMonth(), s.getDate())
      if (adj > e) adj = new Date(e.getFullYear() - 1, s.getMonth(), s.getDate())
      return Math.round((utc(e) - utc(adj)) / 86400000)
    }
    default: return "#NUM!"
  }
}
export function toRoman(n: number): string {
  const vals = [1000, 900, 500, 400, 100, 90, 50, 40, 10, 9, 5, 4, 1]
  const syms = ['M', 'CM', 'D', 'CD', 'C', 'XC', 'L', 'XL', 'X', 'IX', 'V', 'IV', 'I']
  let result = ''
  for (let i = 0; i < vals.length; i++) while (n >= vals[i]) { result += syms[i]; n -= vals[i] }
  return result
}
export function evaluate(node: ASTNode, ctx: EvalContext): unknown {
  switch (node.type) {
    case 'Number': return node.value
    case 'String': return node.value
    case 'Boolean': return node.value
    case 'ErrorLiteral': return { error: node.value }
    case 'ArrayLiteral': return node.rows.map((row: any) => row.map((cell: any) => evaluate(cell, ctx)))

    case 'UnaryOp': {
      const v = evaluate(node.operand, ctx)
      if (node.op === '-') return -(toNum(v))
      if (node.op === '%') return toNum(v) / 100
      return v
    }

    case 'BinOp': {
      const l = evaluate(node.left, ctx)
      const r = evaluate(node.right, ctx)
      if (isErr(l)) return l
      if (isErr(r)) return r
      switch (node.op) {
        case '+': return toNum(l) + toNum(r)
        case '-': return toNum(l) - toNum(r)
        case '*': return toNum(l) * toNum(r)
        case '/': { const d = toNum(r); return d === 0 ? { error: '#DIV/0!' } : toNum(l) / d }
        case '^': return Math.pow(toNum(l), toNum(r))
        case '&': return toStr(l) + toStr(r)
        case '=': case '==': return looseEq(l, r)
        case '<>': return !looseEq(l, r)
        case '<': return toNum(l) < toNum(r)
        case '<=': return toNum(l) <= toNum(r)
        case '>': return toNum(l) > toNum(r)
        case '>=': return toNum(l) >= toNum(r)
      }
      return { error: '#VALUE!' }
    }

    case 'CellRef': {
      const key = cellKey(node.col, node.row, node.sheet)
      return resolveCell(key, ctx)
    }

    case 'Range': {
      return expandRange(node, ctx)
    }

    case 'ColRange': {
      return { type: 'ColRange', start: node.start, end: node.end }
    }

    case 'Name': {
      const named = ctx.namedRanges.get(node.name)
      if (named) {
        const parsed = parseRangeStr(named)
        if (parsed) return expandRange(parsed, ctx)
      }
      return { error: '#NAME?' }
    }

    case 'StructuredRef': return { error: '#REF!' }

    case 'FunctionCall': {
      const fn = SPREADSHEET_FUNCTIONS[node.name]
      if (!fn) return { error: `#NAME? (${node.name})` }
      const args = node.args.map((a: any) => evaluate(a, ctx))
      try { return fn(args, ctx) }
      catch (e) { return { error: e instanceof Error ? e.message : '#VALUE!' } }
    }

    default: return { error: '#VALUE!' }
  }
}
export function toNum(v: unknown): number {
  if (typeof v === 'number') return v
  if (typeof v === 'boolean') return v ? 1 : 0
  if (typeof v === 'string') {
    const n = parseFloat(v)
    if (!isNaN(n)) return n
  }
  return 0
}
export function toStr(v: unknown): string {
  if (v === null || v === undefined) return ''
  if (typeof v === 'boolean') return v ? 'TRUE' : 'FALSE'
  return String(v)
}
export function toBool(v: unknown): boolean {
  if (typeof v === 'boolean') return v
  if (typeof v === 'number') return v !== 0
  if (typeof v === 'string') return v.toUpperCase() === 'TRUE'
  return false
}
export function looseEq(a: unknown, b: unknown): boolean {
  if (typeof a === 'string' && typeof b === 'string') return a.toUpperCase() === b.toUpperCase()
  return a === b
}
export function cellKey(col: string, row: number, sheet?: string): string {
  return sheet ? `${sheet}!${col}${row}` : `${col}${row}`
}
export function cellAt(ctx: EvalContext, key: string): SpreadsheetCell | undefined {
  if (!ctx.store.has(key)) ctx.fetch?.(key)
  return ctx.store.get(key)
}
export function resolveCell(key: string, ctx: EvalContext): unknown {
  if (!ctx.store.has(key)) ctx.fetch?.(key)
  const computed = ctx.computed.get(key)
  if (computed !== undefined) return computed
  const cell = ctx.store.get(key)
  if (!cell) return 0
  if (isFormula(cell.raw)) return cell.computed ?? 0
  return cell.raw ?? 0
}
const KEY_RE = /^(?:(.+)!)?([A-Z]+)(\d+)$/
const maxRowIn = (g: SpreadsheetGraphState, sheet?: string) => {
  let r = 0
  for (const k of g.cells.keys()) { const m = KEY_RE.exec(k); if (m && (m[1] ?? undefined) === sheet) r = Math.max(r, +m[3]) }
  return r
}
export function evaluateSpreadsheetAst(ast: ASTNode, graph: SpreadsheetGraphState, self?: string): unknown {
  return evaluateAst(ast, {
    store: graph.cells, computed: graph.computed, fetch: graph.fetch,
    self, sheet: self ? parseCellRef(self).sheet : undefined,
    extent: (sh) => Math.max(graph.extent?.(sh) ?? 0, maxRowIn(graph, sh)),
  })
}
export function expandRange(node: { start: { col: string; row: number }; end: { col: string; row: number }; sheet?: string }, ctx: EvalContext): unknown[][] {
  const sc = colToIndex(node.start.col), ec = colToIndex(node.end.col)
  const sr = node.start.row, er = node.end.row
  const rows: unknown[][] = []
  for (let r = Math.min(sr, er); r <= Math.max(sr, er); r++) {
    const row: unknown[] = []
    for (let c = Math.min(sc, ec); c <= Math.max(sc, ec); c++) {
      row.push(resolveCell(cellKey(indexToCol(c), r, node.sheet), ctx))
    }
    rows.push(row)
  }
  return rows
}
export function flattenRange(v: unknown): unknown[] {
  if (!Array.isArray(v)) return [v]
  return v.flatMap(flattenRange)
}
export function colToIndex(col: string): number {
  let n = 0
  for (let i = 0; i < col.length; i++) n = n * 26 + col.charCodeAt(i) - 64
  return n - 1
}
export function indexToCol(n: number): string {
  let s = ''; let m = n + 1
  while (m > 0) { s = String.fromCharCode(((m - 1) % 26) + 65) + s; m = Math.floor((m - 1) / 26) }
  return s
}
export function parseRangeStr(ref: string): { start: { col: string; row: number }; end: { col: string; row: number }; sheet?: string } | null {
  const m = ref.match(/^(?:(.+)!)?([A-Z]+)(\d+):([A-Z]+)(\d+)$/)
  if (!m) return null
  return { sheet: m[1], start: { col: m[2], row: parseInt(m[3]) }, end: { col: m[4], row: parseInt(m[5]) } }
}
export function tokenize(src: string): Tok[] {
  const toks: Tok[] = []
  let i = 0
  const len = src.length
  while (i < len) {
    const pos = i
    const ch = src[i]
    if (ch === ' ' || ch === '\t' || ch === '\r' || ch === '\n') { i++; continue }
    if (ch === '"') {
      let s = ''; i++
      while (i < len) {
        if (src[i] === '"') { if (src[i + 1] === '"') { s += '"'; i += 2 } else { i++; break } }
        else { s += src[i++] }
      }
      toks.push({ type: 'STR', value: s, pos }); continue
    }
    if (/\d/.test(ch) || (ch === '.' && /\d/.test(src[i + 1] ?? ''))) {
      let s = ''
      while (i < len && /[\d.eE+\-]/.test(src[i])) {
        if ((src[i] === '+' || src[i] === '-') && !/[eE]/.test(src[i - 1] ?? '')) break
        s += src[i++]
      }
      toks.push({ type: 'NUM', value: s, pos }); continue
    }
    if (/[A-Za-z_]/.test(ch) || ch === "'") {
      // sheet name with spaces: 'Sheet Name'!
      if (ch === "'") {
        let s = ''; i++
        while (i < len && src[i] !== "'") s += src[i++]
        i++ // closing '
        if (src[i] === '!') { toks.push({ type: 'SHEET_PREFIX', value: s, pos }); i++; continue }
        toks.push({ type: 'IDENT', value: s, pos }); continue
      }
      let s = ''
      while (i < len && /[\w$]/.test(src[i])) s += src[i++]
      if (src[i] === '!') { toks.push({ type: 'SHEET_PREFIX', value: s, pos }); i++; continue }
      const up = s.toUpperCase()
      if (up === 'TRUE' || up === 'FALSE') { toks.push({ type: 'BOOL', value: up, pos }); continue }
      toks.push({ type: 'IDENT', value: s, pos }); continue
    }
    switch (ch) {
      case '+': toks.push({ type: 'PLUS', value: '+', pos }); i++; break
      case '-': toks.push({ type: 'MINUS', value: '-', pos }); i++; break
      case '*': toks.push({ type: 'STAR', value: '*', pos }); i++; break
      case '/': toks.push({ type: 'SLASH', value: '/', pos }); i++; break
      case '^': toks.push({ type: 'CARET', value: '^', pos }); i++; break
      case '%': toks.push({ type: 'PERCENT', value: '%', pos }); i++; break
      case '&': toks.push({ type: 'AMP', value: '&', pos }); i++; break
      case '(': toks.push({ type: 'LPAREN', value: '(', pos }); i++; break
      case ')': toks.push({ type: 'RPAREN', value: ')', pos }); i++; break
      case '{': toks.push({ type: 'LBRACE', value: '{', pos }); i++; break
      case '}': toks.push({ type: 'RBRACE', value: '}', pos }); i++; break
      case ':': toks.push({ type: 'COLON', value: ':', pos }); i++; break
      case ';': toks.push({ type: 'SEMI', value: ';', pos }); i++; break
      case ',': toks.push({ type: 'COMMA', value: ',', pos }); i++; break
      case '!': toks.push({ type: 'BANG', value: '!', pos }); i++; break
      case '#': {
        let s = '#'; i++
        while (i < len && /[A-Z\/!?]/.test(src[i])) s += src[i++]
        toks.push({ type: 'ERROR', value: s, pos }); break
      }
      case '=': if (src[i + 1] === '=') { toks.push({ type: 'EQ', value: '==', pos }); i += 2 } else { toks.push({ type: 'EQ', value: '=', pos }); i++ }; break
      case '<': if (src[i + 1] === '=') { toks.push({ type: 'LTE', value: '<=', pos }); i += 2 } else if (src[i + 1] === '>') { toks.push({ type: 'NEQ', value: '<>', pos }); i += 2 } else { toks.push({ type: 'LT', value: '<', pos }); i++ }; break
      case '>': if (src[i + 1] === '=') { toks.push({ type: 'GTE', value: '>=', pos }); i += 2 } else { toks.push({ type: 'GT', value: '>', pos }); i++ }; break
      default: i++
    }
  }
  toks.push({ type: 'EOF', value: '', pos: i })
  return toks
}
export function parseFormula(raw: string): ASTNode {
  const src = raw[0] === '=' ? raw.slice(1) : raw
  return new Parser(src).parse()
}
export class Parser {
  private toks: Tok[]
  private pos = 0

  constructor(src: string) {
    this.toks = tokenize(src)
  }
  private peek(): Tok { return this.toks[this.pos] }
  private eat(): Tok { return this.toks[this.pos++] }
  private expect(type: TokType): Tok {
    const t = this.eat()
    if (t.type !== type) throw new Error(`Expected ${type} at pos ${t.pos}, got ${t.type}`)
    return t
  }
  private match(...types: TokType[]): boolean { return types.includes(this.peek().type) }
  parse(): ASTNode {
    const node = this.parseExpr()
    if (this.peek().type !== 'EOF') throw new Error(`Unexpected token at pos ${this.peek().pos}`)
    return node
  }
  // Expr → Comparison
  private parseExpr(): ASTNode { return this.parseComparison() }
  private parseComparison(): ASTNode {
    let left = this.parseConcat()
    while (this.match('EQ', 'NEQ', 'LT', 'LTE', 'GT', 'GTE')) {
      const op = this.eat().value
      left = { type: 'BinOp', op, left, right: this.parseConcat() }
    }
    return left
  }
  private parseConcat(): ASTNode {
    let left = this.parseAddSub()
    while (this.match('AMP')) { this.eat(); left = { type: 'BinOp', op: '&', left, right: this.parseAddSub() } }
    return left
  }
  private parseAddSub(): ASTNode {
    let left = this.parseMulDiv()
    while (this.match('PLUS', 'MINUS')) {
      const op = this.eat().value
      left = { type: 'BinOp', op, left, right: this.parseMulDiv() }
    }
    return left
  }
  private parseMulDiv(): ASTNode {
    let left = this.parseExponent()
    while (this.match('STAR', 'SLASH')) {
      const op = this.eat().value
      left = { type: 'BinOp', op, left, right: this.parseExponent() }
    }
    return left
  }
  private parseExponent(): ASTNode {
    let left = this.parseUnary()
    if (this.match('CARET')) { this.eat(); return { type: 'BinOp', op: '^', left, right: this.parseExponent() } }
    return left
  }
  private parseUnary(): ASTNode {
    if (this.match('MINUS')) { this.eat(); return { type: 'UnaryOp', op: '-', operand: this.parsePostfix() } }
    if (this.match('PLUS')) { this.eat(); return this.parsePostfix() }
    return this.parsePostfix()
  }
  private parsePostfix(): ASTNode {
    let node = this.parsePrimary()
    if (this.match('PERCENT')) { this.eat(); node = { type: 'UnaryOp', op: '%', operand: node } }
    return node
  }
  private parsePrimary(): ASTNode {
    const t = this.peek()

    // Array literal {1,2;3,4}
    if (t.type === 'LBRACE') {
      this.eat()
      const rows: ASTNode[][] = [[]]
      while (!this.match('RBRACE') && !this.match('EOF')) {
        rows[rows.length - 1].push(this.parseExpr())
        if (this.match('SEMI')) { this.eat(); rows.push([]) }
        else if (this.match('COMMA')) this.eat()
      }
      this.expect('RBRACE')
      return { type: 'ArrayLiteral', rows }
    }

    // Parenthesised
    if (t.type === 'LPAREN') {
      this.eat()
      const inner = this.parseExpr()
      this.expect('RPAREN')
      return inner
    }

    // Number
    if (t.type === 'NUM') { this.eat(); return { type: 'Number', value: parseFloat(t.value) } }

    // String
    if (t.type === 'STR') { this.eat(); return { type: 'String', value: t.value } }

    // Bool
    if (t.type === 'BOOL') { this.eat(); return { type: 'Boolean', value: t.value === 'TRUE' } }

    // Error literal
    if (t.type === 'ERROR') { this.eat(); return { type: 'ErrorLiteral', value: t.value } }

    // Sheet prefix followed by cell/range
    if (t.type === 'SHEET_PREFIX') {
      this.eat()
      const sheet = t.value
      const ref = this.parseCellOrRange(sheet)
      return ref
    }

    // IDENT — could be: function call, named range, cell ref, range
    if (t.type === 'IDENT') {
      this.eat()
      // Function call
      if (this.peek().type === 'LPAREN') {
        this.eat()
        const args: ASTNode[] = []
        if (!this.match('RPAREN')) {
          args.push(this.parseExpr())
          while (this.match('COMMA', 'SEMI')) { this.eat(); args.push(this.parseExpr()) }
        }
        this.expect('RPAREN')
        return { type: 'FunctionCall', name: t.value.toUpperCase(), args }
      }
      // Cell ref or range
      const cellMatch = t.value.match(/^([A-Za-z]+)(\d+)$/)
      if (cellMatch) {
        const col = cellMatch[1].toUpperCase()
        const row = parseInt(cellMatch[2])
        if (this.peek().type === 'COLON') {
          this.eat()
          const t2 = this.peek()
          // end of range could be IDENT
          if (t2.type === 'IDENT') {
            this.eat()
            const m2 = t2.value.match(/^([A-Za-z]+)(\d+)?$/)
            if (m2) {
              const col2 = m2[1].toUpperCase()
              const row2 = m2[2] ? parseInt(m2[2]) : row
              return { type: 'Range', start: { col, row }, end: { col: col2, row: row2 } }
            }
          }
          return { type: 'CellRef', col, row }
        }
        return { type: 'CellRef', col, row }
      }
      // Col-only range e.g. A:A
      const colOnly = t.value.match(/^([A-Za-z]+)$/)
      if (colOnly && this.peek().type === 'COLON') {
        this.eat()
        const t2 = this.peek()
        if (t2.type === 'IDENT' && t2.value.match(/^[A-Za-z]+$/)) {
          this.eat()
          return { type: 'ColRange', start: t.value.toUpperCase(), end: t2.value.toUpperCase() }
        }
      }
      // Named range / structured ref
      if (t.value.includes('[')) {
        return { type: 'StructuredRef', raw: t.value }
      }
      return { type: 'Name', name: t.value }
    }

    throw new Error(`Unexpected token "${t.value}" (${t.type}) at pos ${t.pos}`)
  }
  private parseCellOrRange(sheet: string): ASTNode {
    const t = this.peek()
    if (t.type !== 'IDENT') throw new Error(`Expected cell ref after sheet prefix at pos ${t.pos}`)
    this.eat()
    const m = t.value.match(/^([A-Za-z]+)(\d+)$/)
    if (!m) {
      if (/^[A-Za-z]+$/.test(t.value) && this.peek().type === 'COLON') {   // Sheet!A:A
        this.eat()
        const t2 = this.peek()
        if (t2.type === 'IDENT' && /^[A-Za-z]+$/.test(t2.value)) {
          this.eat()
          return { type: 'ColRange', sheet, start: t.value.toUpperCase(), end: t2.value.toUpperCase() }
        }
      }
      return { type: 'Name', name: t.value, sheet }
    }
    const col = m[1].toUpperCase()
    const row = parseInt(m[2])
    if (this.peek().type === 'COLON') {
      this.eat()
      const t2 = this.peek()
      if (t2.type === 'IDENT') {
        this.eat()
        const m2 = t2.value.match(/^([A-Za-z]+)(\d+)?$/)
        if (m2) return { type: 'Range', sheet, start: { col, row }, end: { col: m2[1].toUpperCase(), row: m2[2] ? parseInt(m2[2]) : row } }
      }
    }
    return { type: 'CellRef', sheet, col, row }
  }
}
function isComplex(node: any): boolean {
  const children = node.properties || node.elements || [];
  return children.some((c: any) => {
    const val = c.type === 'Property' ? c.value : c;
    return val?.type === 'ObjectExpression' || val?.type === 'ArrayExpression';
  });
}
export function formatBifrost(filePath: string): string {
  const ast = parseAST(filePath);
  const generator = Registry.maelstrom.rift.use('generator', 0)

  walkTSNodes(ast.raw, (node) => {
    if (['ObjectExpression', 'ArrayExpression', 'CallExpression'].includes(node.type)) {

      const dryRun = (generator as any).default(node, { compact: true, minified: true }).code;

      const fits = dryRun.length <= 800;
      const flat = !isComplex(node);

      if (fits && flat) {
        node.compact = true;
        node.retainLines = true;
      } else {
        node.compact = false;
        node.retainLines = false;
      }
    }
  });

  // 2. Hard-coded branding based on your reference
  return (generator as any).default(ast.raw, {
    indent: "\t",              // Always Tabs
    quotes: "double",          // Always Double Quotes
    semi: true,                // Always Semicolons
    compact: false,            // Controlled by node-level hints above
    retainLines: false,
    bracketSpacing: true,      // { label: "..." } instead of {label:"..."}
    jsescOption: { minimal: true }
  }).code;
}
export function parseASTFromContent(content: string, filePath: string): ASTResult {
  const lang = detectLang(filePath);

  switch (lang) {
    case 'ts':
    case 'tsx':
    case 'js':
    case 'jsx': {
      const raw = parseTS(content, {
        jsx: lang === 'tsx' || lang === 'jsx',
        loc: true,
        range: true,
        tokens: false,
        comment: false,
      });
      return { lang, filePath, raw };
    }
    case 'css':
    case 'md':
    case 'json':
    case 'jsonc':
      // For non-JS languages, you'd need similar handling
      return parseAST(filePath); // Fallback to file-based parsing
  }
}
export function regenerateCode(ast: ASTResult): string {
  const generator = Registry.maelstrom.rift.use('generator', 0)
  return (generator as any).default(ast.raw, { retainLines: true }).code;
}
function detectLang(filePath: string): SupportedLang {
  const ext = path.extname(filePath).replace('.', '').toLowerCase();
  // Handle .tsx and .jsx correctly
  if (ext === 'tsx') return 'tsx';
  if (ext === 'jsx') return 'jsx';
  if (ext === 'ts') return 'ts';
  if (ext === 'js') return 'js';
  if (ext === 'css') return 'css';
  if (ext === 'md') return 'md';
  if (ext === 'json') return 'json';
  if (ext === 'jsonc') return 'jsonc';
  throw new Error(`Unsupported file extension: .${ext}`);
}
export function evaluateFormula(
  raw: string,
  ctx: EvalContext,
  functions: Record<string, FormulaFunction> = SPREADSHEET_FUNCTIONS
): unknown {
  try {
    return evaluateAst(
      parseFormula(raw),
      ctx,
      functions
    );
  } catch {
    return "#ERROR!";
  }
}
export function isGraph(value: unknown): value is SpreadsheetGraphState {
  return !!value &&
    typeof value === "object" &&
    (value as SpreadsheetGraphState).cells instanceof Map &&
    (value as SpreadsheetGraphState).computed instanceof Map &&
    (value as SpreadsheetGraphState).deps instanceof Map &&
    (value as SpreadsheetGraphState).dependents instanceof Map;
}
export function readGraphCell(graph: SpreadsheetGraphState, ref: string): unknown {
  const key = normalizeCellRef(ref);
  return graph.computed.has(key)
    ? graph.computed.get(key)
    : graph.cells.get(key)?.raw ?? 0;
}
export function readGraphRange(
  graph: SpreadsheetGraphState,
  ref: string
): unknown[][] {
  const parsed = parseRangeStr(ref);
  if (!parsed) {
    return [[readGraphCell(graph, ref)]];
  }

  const startCol = colToIndex(parsed.start.col);
  const endCol = colToIndex(parsed.end.col);
  const minCol = Math.min(startCol, endCol);
  const maxCol = Math.max(startCol, endCol);
  const minRow = Math.min(parsed.start.row, parsed.end.row);
  const maxRow = Math.max(parsed.start.row, parsed.end.row);

  const rows: unknown[][] = [];

  for (let row = minRow; row <= maxRow; row++) {
    const values: unknown[] = [];

    for (let col = minCol; col <= maxCol; col++) {
      const address = `${indexToCol(col)}${row}`;
      values.push(readGraphCell(graph, parsed.sheet
        ? `${parsed.sheet}!${address}`
        : address));
    }

    rows.push(values);
  }

  return rows;
}
export function qualifyAst(node: ASTNode, sheet?: string): ASTNode {
  if (!sheet) return node;
  switch (node.type) {
    case "CellRef":
    case "Range":
    case "ColRange":
      return node.sheet ? node : { ...node, sheet };
    case "BinOp":
      return { ...node, left: qualifyAst(node.left, sheet), right: qualifyAst(node.right, sheet) };
    case "UnaryOp":
      return { ...node, operand: qualifyAst(node.operand, sheet) };
    case "FunctionCall":
      return { ...node, args: node.args.map(a => qualifyAst(a, sheet)) };
    case "ArrayLiteral":
      return { ...node, rows: node.rows.map(r => r.map(i => qualifyAst(i, sheet))) };
    default:
      return node;
  }
}
const visible = (g: SpreadsheetGraphState, key: string) => g.computed.get(key) ?? g.cells.get(key)?.raw;

export function evaluateCell(graph: SpreadsheetGraphState, key: string) {
  const cell = graph.cells.get(key);

  if (!cell || !isFormula(cell.raw)) {
    graph.computed.delete(key);
    if (cell) delete cell.computed;
    return;
  }

  let result: any;
  try {
    result = cell.ast ? evaluateSpreadsheetAst(cell.ast, graph, key) : "#ERROR!";

  } catch {
    result = "#ERROR!";
  }

  // normalize function-returned { error } objects to the string convention
  if (isErr(result)) result = result.error;
if (isLambda(result)) result = "#CALC!"
  cell.computed = result;
  graph.computed.set(key, result);
}
export function writeGraphCellInPlace(graph: any, address: any, raw: any, defaultSheet?: any, volatileKeys: Iterable<string> = []) {
  const key = normalizeCellRef(address, defaultSheet)
  stageCell(graph, key, raw, defaultSheet)
  return recalculateDirty(graph, [key, ...volatileKeys], [key])
}
export function clearGraphCellInPlace(graph: any, address: any, defaultSheet?: any, volatileKeys: Iterable<string> = []) {
  const key = normalizeCellRef(address, defaultSheet)
  removeDependencies(graph, key); graph.cells.delete(key); graph.computed.delete(key)
  return recalculateDirty(graph, [key, ...volatileKeys], [key])
}
export function writeGraphCellsInPlace(
  graph: SpreadsheetGraphState,
  edits: Iterable<readonly [string, unknown]>,
  defaultSheet?: string
): Set<string> {
  // normalize everything first so a bad address can't leave a half-applied batch
  const staged = [...edits].map(([addr, raw]) => [normalizeCellRef(addr, defaultSheet), raw] as const);

  for (const [key, raw] of staged) stageCell(graph, key, raw, defaultSheet);

  return recalculateDirty(graph, staged.map(([key]) => key));   // ONE recalc for the batch
}
export function writeGraphCell(
  graph: SpreadsheetGraphState,
  address: string,
  raw: unknown,
  defaultSheet?: string
): SpreadsheetGraphState {
  const next = cloneGraph(graph);
  writeGraphCellInPlace(next, address, raw, defaultSheet);
  return next;
}
export function writeGraphCells(
  graph: SpreadsheetGraphState,
  edits: Iterable<readonly [string, unknown]>,
  defaultSheet?: string
): SpreadsheetGraphState {
  const next = cloneGraph(graph);
  writeGraphCellsInPlace(next, edits, defaultSheet);
  return next;
}
export function clearGraphCell(
  graph: SpreadsheetGraphState,
  address: string,
  defaultSheet?: string
): SpreadsheetGraphState {
  const next = cloneGraph(graph);
  clearGraphCellInPlace(next, address, defaultSheet);
  return next;
}
const ERR_RE = /^#(NULL!|DIV\/0!|VALUE!|REF!|NAME\?|NUM!|N\/A|ERROR!|CYCLE!|CALC!|SPILL!)/
export function errCode(v: unknown): string | undefined {
  if (typeof v === "string") return ERR_RE.test(v) ? v : undefined
  if (typeof v === "object" && v !== null && "error" in v) return String((v as any).error)
  return undefined
}
export function isErr(v: unknown): v is string | { error: string } { return errCode(v) !== undefined }
export function isSpreadsheetError(v: unknown): v is SpreadsheetError { return typeof v === "string" && ERR_RE.test(v) }
export type SpecialForm = (nodes: ASTNode[], ctx: EvalContext, ev: (n: ASTNode, c?: EvalContext) => unknown) => unknown
const normalizeResult = (v: unknown) => {
  if (typeof v === "number" && !Number.isFinite(v)) return "#NUM!"
  if (typeof v === "object" && v !== null && "error" in v) return String((v as any).error)
  return v
}
export function evaluateAst(node: ASTNode, ctx: EvalContext, functions: Record<string, FormulaFunction> = SPREADSHEET_FUNCTIONS): unknown {
  const ev = (n: ASTNode, c: EvalContext = ctx) => evaluateAst(n, c, functions)
  switch (node.type) {
    case "Number": case "String": case "Boolean": return node.value
    case "ErrorLiteral": return node.value
    case "CellRef": return resolveCell(cellKey(node.col, node.row, node.sheet), ctx)
    case "Range": return expandRange(node, ctx)
    case "ColRange": {
      const sheet = node.sheet ?? ctx.sheet
      const last = ctx.extent?.(sheet) ?? 0
      if (last < 1) return [[0]]
      return expandRange({ sheet, start: { col: node.start, row: 1 }, end: { col: node.end, row: last } }, ctx)
    }
    case "StructuredRef": return "#REF!"
    case "Name": {
      const key = node.name.toUpperCase()
      if (ctx.scope?.has(key)) return ctx.scope.get(key)
      if (functions[key]) return { lambda: true, native: functions[key] } as Lambda   // BYROW(A1:B3, SUM)
      return "#NAME?"
    }
    case "ArrayLiteral": return node.rows.map(row => row.map(item => ev(item)))
    case "UnaryOp": return evaluateUnary(node.op, ev(node.operand))
    case "BinOp": return evaluateBinary(node.op, ev(node.left), ev(node.right))
    case "FunctionCall": {
      try {
        const special = SPECIAL_FORMS[node.name]
        if (special) return normalizeResult(special(node.args, ctx, ev))
        const fn = functions[node.name]
        if (!fn) return "#NAME?"
        return normalizeResult(fn(node.args.map(a => ev(a)), ctx))
      } catch { return "#ERROR!" }
    }
    default: return "#ERROR!"
  }
}

export function collectDependencies(node: ASTNode, defaultSheet?: string, output = new Set<string>()): Set<string> {
  switch (node.type) {
    case "CellRef": {
      output.add(
        cellKey(
          node.col.toUpperCase(),
          node.row,
          node.sheet ?? defaultSheet
        )
      );
      return output;
    }

    case "Range": {
      const start = colToIndex(node.start.col);
      const end = colToIndex(node.end.col);

      const minCol = Math.min(start, end);
      const maxCol = Math.max(start, end);
      const minRow = Math.min(node.start.row, node.end.row);
      const maxRow = Math.max(node.start.row, node.end.row);

      for (let row = minRow; row <= maxRow; row++) {
        for (let col = minCol; col <= maxCol; col++) {
          output.add(
            cellKey(
              indexToCol(col),
              row,
              node.sheet ?? defaultSheet
            )
          );
        }
      }

      return output;
    }

    case "BinOp":
      collectDependencies(node.left, defaultSheet, output);
      collectDependencies(node.right, defaultSheet, output);
      return output;

    case "UnaryOp":
      collectDependencies(node.operand, defaultSheet, output);
      return output;

    case "FunctionCall":
      for (const arg of node.args) {
        collectDependencies(arg, defaultSheet, output);
      }
      return output;

    case "ArrayLiteral":
      for (const row of node.rows) {
        for (const item of row) {
          collectDependencies(item, defaultSheet, output);
        }
      }
      return output;

    default:
      return output;
  }
}






export type RangeEntry = { t: string; box: RangeBox }
// type changes
export type SpreadsheetError = "#ERROR!" | "#REF!" | "#NAME?" | "#VALUE!" | "#DIV/0!" | "#NUM!" | "#CYCLE!" | "#N/A" | "#CALC!" | "#NULL!" | "#SPILL!"
export type SpreadsheetCell = { raw: SpreadsheetRaw; ast?: ASTNode; computed?: unknown; volatile?: boolean }
export type RangeBox = { sheet?: string; c1: number; r1: number; c2: number; r2: number };
export const MAX_ROW = 1048576;
export type SpreadsheetGraphState = {
  cells: Map<string, SpreadsheetCell>;
  computed: Map<string, unknown>;
  deps: Map<string, Set<string>>;
  dependents: Map<string, Set<string>>;
  ranges: Map<string, RangeBox[]>;                         // target -> range boxes it reads
  fetch?: (key: string) => SpreadsheetCell | undefined;
  extent?: (sheet?: string) => number;                     // used rows, for A:A
};
interface EvalContext {
  namedRanges?: any
  computed?: any
  store?: any
  scope?: Map<string, unknown>
  self?: string
  sheet?: string
  fetch?: (key: string) => SpreadsheetCell | undefined
  extent?: (sheet?: string) => number
}
export type SpreadsheetValue = unknown;
export type SpreadsheetActions = {
  set(value: SpreadsheetValue): void;
  formula(expression: string): void;
  clear(): void;
};
export type GridActions = {
  get(ref: string): SpreadsheetValue;
  raw(ref: string): SpreadsheetValue | undefined;
  set(ref: string, value: SpreadsheetValue): void;
  formula(ref: string, expression: string): void;
  clear(ref: string): void;
  setMany(edits: Iterable<readonly [string, SpreadsheetValue]>): void;
  range(ref: string): SpreadsheetValue[][];
};
export type CellRefNode = {
  type: "CellRef";
  col: string;
  row: number;
  sheet?: string;
};
export type RangeNode = {
  type: "Range";
  start: {
    col: string;
    row: number;
  };
  end: {
    col: string;
    row: number;
  };
  sheet?: string;
};
export type ColRangeNode = {
  type: "ColRange";
  start: string;
  end: string;
  sheet?: string;
};
export type NumberNode = {
  type: "Number";
  value: number;
};
export type StringNode = {
  type: "String";
  value: string;
};
export type BooleanNode = {
  type: "Boolean";
  value: boolean;
};
export type ErrorLiteralNode = {
  type: "ErrorLiteral";
  value: string;
};
export type NameNode = {
  type: "Name";
  name: string;
  sheet?: string;
};
export type StructuredRefNode = {
  type: "StructuredRef";
  raw: string;
};
export type ArrayLiteralNode = {
  type: "ArrayLiteral";
  rows: ASTNode[][];
};
export type UnaryOpNode = {
  type: "UnaryOp";
  op: "-" | "%";
  operand: ASTNode;
};
export type BinOpNode = {
  type: "BinOp";
  op:
  | "+"
  | "-"
  | "*"
  | "/"
  | "^"
  | "&"
  | "="
  | "=="
  | "<>"
  | "<"
  | "<="
  | ">"
  | ">=";
  left: ASTNode;
  right: ASTNode;
};
export type FunctionCallNode = {
  type: "FunctionCall";
  name: string;
  args: ASTNode[];
};
export type ASTNode = | NumberNode | StringNode | BooleanNode | ErrorLiteralNode | CellRefNode | RangeNode | ColRangeNode | NameNode | StructuredRefNode | ArrayLiteralNode | UnaryOpNode | BinOpNode | FunctionCallNode;
interface Tok { type: TokType; value: string; pos: number }
interface EvalContext {
  namedRanges?: any
  computed?: any
  store?: any
}
type Fn = (args: unknown[], ctx: EvalContext) => unknown
type TokType = | 'NUM' | 'STR' | 'BOOL' | 'ERROR' | 'IDENT' | 'SHEET_PREFIX' | 'COLON' | 'SEMI' | 'COMMA' | 'BANG' | 'LPAREN' | 'RPAREN' | 'LBRACE' | 'RBRACE' | 'PLUS' | 'MINUS' | 'STAR' | 'SLASH' | 'CARET' | 'PERCENT' | 'AMP' | 'EQ' | 'NEQ' | 'LT' | 'LTE' | 'GT' | 'GTE' | 'HASH' | 'EOF'
export type SpreadsheetMode = "waitFor" | "sub" | "cell" | "none";
export type SpreadsheetChain = "chain" | "rechain";
export type SpreadsheetRaw = unknown;

export type SpreadsheetCellRef = {
  sheet?: string;
  col: string;
  row: number;
};
type FormulaFunction = (args: unknown[], ctx: EvalContext) => unknown;
export type SupportedLang = 'ts' | 'tsx' | 'js' | 'jsx' | 'css' | 'md' | 'json' | 'jsonc'
export interface ASTResult {
  lang: SupportedLang
  filePath: string
  raw: any
}
export interface ShapedImport {
  source: string
  specifiers: string[]
  line: number
}
export interface ShapedExport {
  name: string
  type: 'function' | 'class' | 'variable' | 'default' | 'unknown'
  line: number
}
export interface ShapedFunction {
  name: string
  params: string[]
  line: number
  async: boolean
}
export interface ShapedComponent {
  name: string
  props: string[]
  line: number
}
export interface ShapedCSSRule {
  selector: string
  declarations: { property: string; value: string }[]
  line: number
}
export interface ShapedMarkdownHeading {
  depth: number
  text: string
  line: number
}
export interface ShapedMarkdownLink {
  text: string
  url: string
  line: number
}
export interface ShapedJSONKey {
  keyPath: string
  value: any
}











// #endregion
