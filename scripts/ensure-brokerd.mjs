import { spawnSync } from "node:child_process";
import { existsSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const repoRoot = path.resolve(
  path.dirname(fileURLToPath(import.meta.url)),
  "..",
);

export function brokerBinaryPath(platform = process.platform) {
  const binary = platform === "win32" ? "brokerd.exe" : "brokerd";
  return path.join(repoRoot, "target", "debug", binary);
}

export function ensureBrokerd() {
  const binary = brokerBinaryPath();
  if (existsSync(binary)) {
    return binary;
  }

  const result = spawnSync(
    "cargo",
    [
      "build",
      "--manifest-path",
      path.join(repoRoot, "Cargo.toml"),
      "-p",
      "msg-runtime",
      "--bin",
      "brokerd",
    ],
    {
      cwd: repoRoot,
      stdio: "inherit",
    },
  );

  if (result.error !== undefined) {
    throw result.error;
  }
  if (result.status !== 0) {
    process.exitCode = result.status ?? 1;
    return null;
  }
  if (!existsSync(binary)) {
    throw new Error(`cargo build succeeded but ${binary} was not created`);
  }
  return binary;
}

if (
  process.argv[1] !== undefined &&
  path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)
) {
  try {
    ensureBrokerd();
  } catch (error) {
    process.stderr.write(
      `Failed to prepare brokerd: ${
        error instanceof Error ? error.message : String(error)
      }\n`,
    );
    process.exitCode = 1;
  }
}
