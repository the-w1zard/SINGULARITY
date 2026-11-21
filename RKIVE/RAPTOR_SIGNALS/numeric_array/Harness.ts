// Minimal, dependency-free test harness.
// Node's built-in assert does the actual comparison work.

import { strict as assert } from "node:assert";

export { assert };

export type TestBody = () => void | Promise<void>;

interface TestCase {
    group: string;
    name: string;
    fn: TestBody;
    advisory: boolean;
}

const cases: TestCase[] = [];

let currentGroup = "general";

export function describe(name: string, body: () => void): void {
    const previous = currentGroup;

    currentGroup = name;

    try {
        body();
    } finally {
        currentGroup = previous;
    }
}

export function it(name: string, fn: TestBody): void {
    cases.push({
        group: currentGroup,
        name,
        fn,
        advisory: false
    });
}

// Advisory checks describe behaviour that is worth knowing about but is not
// part of the core contract. They are reported, and never fail the run.
export function advisory(name: string, fn: TestBody): void {
    cases.push({
        group: currentGroup,
        name,
        fn,
        advisory: true
    });
}

const tty = process.stdout.isTTY === true && !process.env.NO_COLOR;

const paint = (code: number) => (text: string) =>
    tty ? `\u001b[${code}m${text}\u001b[0m` : text;

const green = paint(32);
const red = paint(31);
const yellow = paint(33);
const dim = paint(90);
const bold = paint(1);

function ms(value: number): string {
    return value < 1 ? `${value.toFixed(3)}ms` : `${value.toFixed(1)}ms`;
}

export async function run(filter?: string): Promise<number> {
    let passed = 0;
    let failed = 0;
    let advisoryPassed = 0;
    let advisoryFailed = 0;
    let lastGroup = "";
    const failures: string[] = [];

    const matches = (testCase: TestCase): boolean => {
        if (!filter) return true;

        return `${testCase.group} ${testCase.name}`
            .toLowerCase()
            .includes(filter.toLowerCase());
    };

    const started = performance.now();

    for (const testCase of cases) {
        if (!matches(testCase)) continue;

        if (testCase.group !== lastGroup) {
            lastGroup = testCase.group;
            console.log(`\n${bold(lastGroup)}`);
        }

        const at = performance.now();

        try {
            await testCase.fn();

            const took = performance.now() - at;

            if (testCase.advisory) {
                advisoryPassed++;
                console.log(
                    `  ${yellow("~")} ${testCase.name} ${dim(ms(took))}`
                );
            } else {
                passed++;
                console.log(
                    `  ${green("✓")} ${testCase.name} ${dim(ms(took))}`
                );
            }
        } catch (error) {
            const took = performance.now() - at;
            const message =
                error instanceof Error
                    ? `${error.name}: ${error.message}`
                    : String(error);

            if (testCase.advisory) {
                advisoryFailed++;
                console.log(
                    `  ${yellow("!")} ${testCase.name} ${dim(ms(took))}`
                );
                console.log(`    ${yellow(message)}`);

                const stack = error instanceof Error ? error.stack : "";

                if (stack) {
                    console.log(
                        dim(
                            stack
                                .split("\n")
                                .slice(1, 4)
                                .map((line) => `    ${line.trim()}`)
                                .join("\n")
                        )
                    );
                }
            } else {
                failed++;
                console.log(
                    `  ${red("✗")} ${testCase.name} ${dim(ms(took))}`
                );
                console.log(`    ${red(message)}`);

                const stack = error instanceof Error ? error.stack : "";

                if (stack) {
                    console.log(
                        dim(
                            stack
                                .split("\n")
                                .slice(1, 5)
                                .map((line) => `    ${line.trim()}`)
                                .join("\n")
                        )
                    );
                }

                failures.push(`${testCase.group} › ${testCase.name}`);
            }
        }
    }

    const total = performance.now() - started;

    console.log("\n" + "─".repeat(64));
    console.log(
        `${bold("Correctness")}  ${green(`${passed} passed`)}` +
            (failed ? `  ${red(`${failed} failed`)}` : "") +
            `  ${dim(ms(total))}`
    );

    if (advisoryPassed || advisoryFailed) {
        console.log(
            `${bold("Advisory")}     ${advisoryPassed} held` +
                (advisoryFailed
                    ? `  ${yellow(`${advisoryFailed} flagged`)}`
                    : "")
        );
    }

    if (failures.length) {
        console.log(`\n${red("Failed:")}`);

        for (const failure of failures) {
            console.log(`  • ${failure}`);
        }
    }

    console.log("─".repeat(64));

    return failed === 0 ? 0 : 1;
}

export function filterFromArgv(argv: string[]): string | undefined {
    const flag = argv.find((argument) => argument.startsWith("--only="));

    if (flag) return flag.slice("--only=".length);

    return undefined;
}

export async function main(runTests: () => void): Promise<void> {
    runTests();

    const code = await run(filterFromArgv(process.argv.slice(2)));

    process.exitCode = code;
}
