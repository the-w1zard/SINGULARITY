export declare function walkTSNodes(node: any, visit: (n: any) => void): void;
export declare function parseAST(filePath: string): ASTResult;
export declare function boxContains(b: RangeBox, key: string): boolean;
export declare function analyzeDependencies(node: ASTNode, cells?: Set<string>, ranges?: RangeBox[]): {
    cells: Set<string>;
    ranges: RangeBox[];
};
export declare function directDependents(graph: SpreadsheetGraphState, key: string): Set<string>;
export declare function removeDependencies(graph: SpreadsheetGraphState, target: string): void;
export declare function registerDependencies(graph: SpreadsheetGraphState, target: string, dependencies: Set<string>, ranges?: RangeBox[]): void;
export declare function collectAffected(graph: SpreadsheetGraphState, changed: string): Set<string>;
export declare function cloneGraph(graph: SpreadsheetGraphState | undefined): SpreadsheetGraphState;
export declare function recalculateDirty(graph: SpreadsheetGraphState, seeds: Iterable<string>, forced?: Iterable<string>): Set<string>;
export declare function formatText(value: unknown, fmt: string): string;
export declare function dateSerial(d: Date): number;
export declare function fromSerial(n: number): Date;
export declare function isVolatileAst(n: ASTNode): boolean;
export declare const SPECIAL_FORMS: Record<string, SpecialForm>;
export type Lambda = {
    lambda: true;
    params?: string[];
    body?: ASTNode;
    scope?: Map<string, unknown>;
    native?: Fn;
};
export declare const isLambda: (v: unknown) => v is Lambda;
export declare function callLambda(f: unknown, args: unknown[], ctx: EvalContext): unknown;
export declare const lambdaFns: Record<string, Fn>;
export declare function asNumber(value: unknown): number | SpreadsheetError;
export declare function scalar(value: unknown): unknown;
export declare function compareValues(left: unknown, right: unknown): number | SpreadsheetError;
export declare function evaluateBinary(op: string, left: unknown, right: unknown): unknown;
export declare function evaluateUnary(op: string, value: unknown): unknown;
export declare function recalculateCell(graph: SpreadsheetGraphState, key: string, visiting?: Set<string>, completed?: Set<string>): unknown;
export declare function recalculateAffected(graph: SpreadsheetGraphState, changed: string): void;
export declare function createEvalContext(graph: SpreadsheetGraphState): EvalContext;
export declare function isFormula(value: unknown): value is string;
export declare function normalizeCellRef(input: string, defaultSheet?: string): string;
export declare function parseCellRef(input: string, defaultSheet?: string): SpreadsheetCellRef;
export declare function createSpreadsheetGraph(initial?: SpreadsheetGraphState): SpreadsheetGraphState;
export declare const math: Record<string, Fn>;
export declare const text: Record<string, Fn>;
export declare const logical: Record<string, Fn>;
export declare const lookup: Record<string, Fn>;
export declare const dateTime: Record<string, Fn>;
export declare const statistical: Record<string, Fn>;
export declare const financial: Record<string, Fn>;
export declare const info: Record<string, Fn>;
export declare const testFn: Record<string, Fn>;
export declare const SPREADSHEET_FUNCTIONS: Record<string, Fn>;
export declare function round(v: number, dp: number): number;
export declare function gcd(a: number, b: number): number;
export declare function combin(n: number, k: number): number;
export declare function looseEqual(a: unknown, b: unknown): boolean;
export declare function compare(a: unknown, b: unknown): number;
export declare function approxMatch(val: unknown, arr: unknown[]): number;
export declare function findMatch(val: unknown, arr: unknown[], order: number[], mode: number): number;
export declare function wildcardMatch(pattern: string, value: string): boolean;
export declare function matchCriteria(v: unknown, crit: unknown): boolean;
export declare function stddev(vals: number[], population: boolean): number;
export declare function variance(vals: number[], population: boolean): number;
export declare function correl(x: number[], y: number[]): number;
export declare function covariance(x: number[], y: number[], population: boolean): number;
export declare function linreg(x: number[], y: number[]): [number, number];
export declare function normalPDF(z: number): number;
export declare function normalCDF(z: number): number;
export declare function normalInv(p: number, mu: number, sigma: number): number;
export declare function networkDays(start: number, end: number, holidays: number[]): number;
export declare function workday(start: number, days: number, holidays: number[]): number;
export declare function dateDif(s: Date, e: Date, unit: string): number | string;
export declare function toRoman(n: number): string;
export declare function evaluate(node: ASTNode, ctx: EvalContext): unknown;
export declare function toNum(v: unknown): number;
export declare function toStr(v: unknown): string;
export declare function toBool(v: unknown): boolean;
export declare function looseEq(a: unknown, b: unknown): boolean;
export declare function cellKey(col: string, row: number, sheet?: string): string;
export declare function cellAt(ctx: EvalContext, key: string): SpreadsheetCell | undefined;
export declare function resolveCell(key: string, ctx: EvalContext): unknown;
export declare function evaluateSpreadsheetAst(ast: ASTNode, graph: SpreadsheetGraphState, self?: string): unknown;
export declare function expandRange(node: {
    start: {
        col: string;
        row: number;
    };
    end: {
        col: string;
        row: number;
    };
    sheet?: string;
}, ctx: EvalContext): unknown[][];
export declare function flattenRange(v: unknown): unknown[];
export declare function colToIndex(col: string): number;
export declare function indexToCol(n: number): string;
export declare function parseRangeStr(ref: string): {
    start: {
        col: string;
        row: number;
    };
    end: {
        col: string;
        row: number;
    };
    sheet?: string;
} | null;
export declare function tokenize(src: string): Tok[];
export declare function parseFormula(raw: string): ASTNode;
export declare class Parser {
    private toks;
    private pos;
    constructor(src: string);
    private peek;
    private eat;
    private expect;
    private match;
    parse(): ASTNode;
    private parseExpr;
    private parseComparison;
    private parseConcat;
    private parseAddSub1;
    private parseAddSub;
    private parseMulDiv;
    private parseExponent;
    private parseUnary;
    private parsePostfix;
    private parsePrimary;
    private parseCellOrRange;
}
export declare function formatBifrost(filePath: string): string;
export declare function parseASTFromContent(content: string, filePath: string): ASTResult;
export declare function regenerateCode(ast: ASTResult): string;
export declare function evaluateFormula(raw: string, ctx: EvalContext, functions?: Record<string, FormulaFunction>): unknown;
export declare function isGraph(value: unknown): value is SpreadsheetGraphState;
export declare function readGraphCell(graph: SpreadsheetGraphState, ref: string): unknown;
export declare function readGraphRange(graph: SpreadsheetGraphState, ref: string): unknown[][];
export declare function qualifyAst(node: ASTNode, sheet?: string): ASTNode;
export declare function evaluateCell(graph: SpreadsheetGraphState, key: string): void;
export declare function writeGraphCellInPlace(graph: any, address: any, raw: any, defaultSheet?: any, volatileKeys?: Iterable<string>): Set<string>;
export declare function clearGraphCellInPlace(graph: any, address: any, defaultSheet?: any, volatileKeys?: Iterable<string>): Set<string>;
export declare function writeGraphCellsInPlace(graph: SpreadsheetGraphState, edits: Iterable<readonly [string, unknown]>, defaultSheet?: string): Set<string>;
export declare function writeGraphCell(graph: SpreadsheetGraphState, address: string, raw: unknown, defaultSheet?: string): SpreadsheetGraphState;
export declare function writeGraphCells(graph: SpreadsheetGraphState, edits: Iterable<readonly [string, unknown]>, defaultSheet?: string): SpreadsheetGraphState;
export declare function clearGraphCell(graph: SpreadsheetGraphState, address: string, defaultSheet?: string): SpreadsheetGraphState;
export declare function errCode(v: unknown): string | undefined;
export declare function isEr1(v: unknown): v is string | {
    error: string;
};
export declare function isErr(value: unknown): value is {
    error: string;
};
export declare function isSpreadsheetError(v: unknown): v is SpreadsheetError;
export type SpecialForm = (nodes: ASTNode[], ctx: EvalContext, ev: (n: ASTNode, c?: EvalContext) => unknown) => unknown;
export declare function evaluateAst(node: ASTNode, ctx: EvalContext, functions?: Record<string, FormulaFunction>): unknown;
export declare function collectDependencies(node: ASTNode, defaultSheet?: string, output?: Set<string>): Set<string>;
export type RangeEntry = {
    t: string;
    box: RangeBox;
};
export type SpreadsheetError = "#ERROR!" | "#REF!" | "#NAME?" | "#VALUE!" | "#DIV/0!" | "#NUM!" | "#CYCLE!" | "#N/A" | "#CALC!" | "#NULL!" | "#SPILL!";
export type SpreadsheetCell = {
    raw: SpreadsheetRaw;
    ast?: ASTNode;
    computed?: unknown;
    volatile?: boolean;
};
export type RangeBox = {
    sheet?: string;
    c1: number;
    r1: number;
    c2: number;
    r2: number;
};
export declare const MAX_ROW = 1048576;
export type SpreadsheetGraphState = {
    cells: Map<string, SpreadsheetCell>;
    computed: Map<string, unknown>;
    deps: Map<string, Set<string>>;
    dependents: Map<string, Set<string>>;
    ranges: Map<string, RangeBox[]>;
    fetch?: (key: string) => SpreadsheetCell | undefined;
    extent?: (sheet?: string) => number;
};
interface EvalContext {
    namedRanges?: any;
    computed?: any;
    store?: any;
    scope?: Map<string, unknown>;
    self?: string;
    sheet?: string;
    fetch?: (key: string) => SpreadsheetCell | undefined;
    extent?: (sheet?: string) => number;
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
    op: "+" | "-" | "*" | "/" | "^" | "&" | "=" | "==" | "<>" | "<" | "<=" | ">" | ">=";
    left: ASTNode;
    right: ASTNode;
};
export type FunctionCallNode = {
    type: "FunctionCall";
    name: string;
    args: ASTNode[];
};
export type ASTNode = NumberNode | StringNode | BooleanNode | ErrorLiteralNode | CellRefNode | RangeNode | ColRangeNode | NameNode | StructuredRefNode | ArrayLiteralNode | UnaryOpNode | BinOpNode | FunctionCallNode;
interface Tok {
    type: TokType;
    value: string;
    pos: number;
}
interface EvalContext {
    namedRanges?: any;
    computed?: any;
    store?: any;
}
type Fn = (args: unknown[], ctx: EvalContext) => unknown;
type TokType = 'NUM' | 'STR' | 'BOOL' | 'ERROR' | 'IDENT' | 'SHEET_PREFIX' | 'COLON' | 'SEMI' | 'COMMA' | 'BANG' | 'LPAREN' | 'RPAREN' | 'LBRACE' | 'RBRACE' | 'PLUS' | 'MINUS' | 'STAR' | 'SLASH' | 'CARET' | 'PERCENT' | 'AMP' | 'EQ' | 'NEQ' | 'LT' | 'LTE' | 'GT' | 'GTE' | 'HASH' | 'EOF';
export type SpreadsheetMode = "waitFor" | "sub" | "cell" | "none";
export type SpreadsheetChain = "chain" | "rechain";
export type SpreadsheetRaw = unknown;
export type SpreadsheetCellRef = {
    sheet?: string;
    col: string;
    row: number;
};
type FormulaFunction = (args: unknown[], ctx: EvalContext) => unknown;
export type SupportedLang = 'ts' | 'tsx' | 'js' | 'jsx' | 'css' | 'md' | 'json' | 'jsonc';
export interface ASTResult {
    lang: SupportedLang;
    filePath: string;
    raw: any;
}
export interface ShapedImport {
    source: string;
    specifiers: string[];
    line: number;
}
export interface ShapedExport {
    name: string;
    type: 'function' | 'class' | 'variable' | 'default' | 'unknown';
    line: number;
}
export interface ShapedFunction {
    name: string;
    params: string[];
    line: number;
    async: boolean;
}
export interface ShapedComponent {
    name: string;
    props: string[];
    line: number;
}
export interface ShapedCSSRule {
    selector: string;
    declarations: {
        property: string;
        value: string;
    }[];
    line: number;
}
export interface ShapedMarkdownHeading {
    depth: number;
    text: string;
    line: number;
}
export interface ShapedMarkdownLink {
    text: string;
    url: string;
    line: number;
}
export interface ShapedJSONKey {
    keyPath: string;
    value: any;
}
export {};
//# sourceMappingURL=cell.d.ts.map