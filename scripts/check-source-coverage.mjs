import fs from "node:fs";
import path from "node:path";

const [reportPath] = process.argv.slice(2);
if (reportPath === undefined) {
  console.error("usage: node scripts/check-source-coverage.mjs <llvm-cov.json>");
  process.exit(2);
}

const report = JSON.parse(fs.readFileSync(reportPath, "utf8"));
const sourceRoot = `${path.resolve("src")}${path.sep}`;
const sourceFiles = new Map();

for (const datum of report.data ?? []) {
  for (const file of datum.files ?? []) {
    const filename = path.resolve(file.filename);
    if (filename.startsWith(sourceRoot)) {
      sourceFiles.set(filename, file);
    }
  }
}

if (sourceFiles.size === 0) {
  console.error("coverage report contains no crate source files");
  process.exit(1);
}

const regions = new Map();
for (const datum of report.data ?? []) {
  for (const fn of datum.functions ?? []) {
    for (const region of fn.regions ?? []) {
      const filename = path.resolve(fn.filenames[region[5]]);
      const isCodeRegion = region[7] === 0;
      if (!isCodeRegion || !sourceFiles.has(filename)) {
        continue;
      }

      const coordinates = region.slice(0, 4).join(":");
      const key = `${filename}:${coordinates}`;
      regions.set(key, Math.max(regions.get(key) ?? 0, region[4]));
    }
  }
}

if (regions.size === 0) {
  console.error("coverage report contains no crate source regions");
  process.exit(1);
}

const uncoveredRegions = [...regions]
  .filter(([, count]) => count === 0)
  .map(([region]) => region);
const functions = [...sourceFiles.values()].reduce(
  (totals, file) => ({
    count: totals.count + file.summary.functions.count,
    covered: totals.covered + file.summary.functions.covered,
  }),
  { count: 0, covered: 0 },
);
const lines = [...sourceFiles.values()].reduce(
  (count, file) => count + file.summary.lines.count,
  0,
);

if (functions.covered !== functions.count || uncoveredRegions.length > 0) {
  console.error(
    `source coverage failed: functions ${functions.covered}/${functions.count}, ` +
      `regions ${regions.size - uncoveredRegions.length}/${regions.size}`,
  );
  for (const region of uncoveredRegions.slice(0, 20)) {
    console.error(`uncovered: ${region}`);
  }
  process.exit(1);
}

console.log(`source functions: ${functions.count}/${functions.count} (100.00%)`);
console.log(`source lines: ${lines}/${lines} (100.00%)`);
console.log(`source regions: ${regions.size}/${regions.size} (100.00%)`);
