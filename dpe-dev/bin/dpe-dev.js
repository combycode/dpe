#!/usr/bin/env node
// dispatch shim for the `dpe-dev` npm wrapper. Resolves the matching
// @combycode/dpe-dev-<os>-<cpu> package and execs its binary. Frameworks
// are embedded into the dpe-dev binary itself (include_dir!), so no
// auxiliary config is needed — just exec.

"use strict";
const { spawnSync } = require("node:child_process");
const path = require("node:path");
const fs = require("node:fs");

const platform =
    process.platform === "win32"  ? "win32"  :
    process.platform === "darwin" ? "darwin" :
                                    "linux";
const arch  = process.arch === "arm64" ? "arm64" : "x64";
const pkg   = `@combycode/dpe-dev-${platform}-${arch}`;
const exe   = process.platform === "win32" ? ".exe" : "";

let pkgDir;
try {
    pkgDir = path.dirname(require.resolve(`${pkg}/package.json`));
} catch {
    console.error(
        `dpe-dev: native package "${pkg}" not installed for ${platform}-${arch}.\n` +
        `Run: npm install -g ${pkg}\n` +
        `or:  npm install -g dpe-dev`
    );
    process.exit(1);
}

const binPath = path.join(pkgDir, "bin", `dpe-dev${exe}`);
if (!fs.existsSync(binPath)) {
    console.error(`dpe-dev: binary missing at ${binPath} — package corruption; reinstall dpe-dev.`);
    process.exit(1);
}

const child = spawnSync(binPath, process.argv.slice(2), { stdio: "inherit" });
process.exit(child.status ?? 1);
