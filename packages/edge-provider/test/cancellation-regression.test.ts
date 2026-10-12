import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";

// The synthesis WebSocket now runs in the Python bridge process (the
// replacement host fingerprints TLS and rejects Node/Bun handshakes), so the
// wire-level lifecycle scenarios are driven against a fake bridge instead of
// a fake Microsoft server. What must still hold:
//   - a completed synthesis hands the audio through,
//   - a bridge crash surfaces as a stream error,
//   - aborting the caller cancels the stream,
//   - a bridge that never answers fails the request (audio idle timeout).
// Child scripts signal via events (exit, stdout data), not fixed sleeps.
describe("python bridge synthesis lifecycle", () => {
  it.each([
    "complete",
    "bridge-crash",
    "abort-consumer",
    "bridge-timeout",
  ])("keeps process and stream semantics intact: %s", (scenario) => {
    const child = spawnSync(
      process.execPath,
      [
        "--import",
        "tsx",
        "--input-type=module",
        "--eval",
        `
import assert from "node:assert/strict";
import { EdgeTtsProvider } from "../src/index.ts";

const scenario = ${JSON.stringify(scenario)};

// Fake bridge: speaks the same stdio JSON protocol as py/bridge.py.
const script = {
  complete: \`
const line = await new Promise(r => process.stdin.once("data", r));
const cmd = JSON.parse(line.toString());
process.stdout.write(JSON.stringify({ id: cmd.id, ok: true, audioB64: Buffer.from("audio bytes").toString("base64") }) + String.fromCharCode(10));
process.stdin.resume();
\`,
  "bridge-crash": \`process.stdin.once("data", () => process.exit(9));\`,
  "abort-consumer": \`
// Never answer: the caller aborts on its own signal, not on a timer here.
process.stdin.resume();
\`,
  "bridge-timeout": \`process.stdin.resume();\`,
}[scenario];

process.env.EDGETTS_BRIDGE_PYTHON = process.execPath;
process.env.EDGETTS_BRIDGE_ARGS = JSON.stringify([
  "--input-type=module", "--eval", script,
]);

const provider = new EdgeTtsProvider({ setupTimeoutMs: 2000, audioIdleTimeoutMs: 500 });
const controller = new AbortController();
try {
  const result = await provider.synthesize(
    { text: "synthetic regression sample", voice: "en-US-JennyNeural" },
    controller.signal,
  );
  const iterator = result.audio[Symbol.asyncIterator]();
  if (scenario === "complete") {
    const item = await iterator.next();
    assert.equal(Buffer.from(item.value).toString(), "audio bytes");
    assert.equal((await iterator.next()).done, true);
  } else if (scenario === "bridge-crash") {
    await assert.rejects(iterator.next(), /bridge process exited|audio/);
  } else if (scenario === "abort-consumer") {
    const pending = iterator.next();
    controller.abort();
    await assert.rejects(pending, { name: "AbortError" });
  } else {
    await assert.rejects(iterator.next(), /timed out/);
  }
} finally {
  controller.abort();
}
console.log("PASS");
`,
        ],
      {
        cwd: fileURLToPath(new URL(".", import.meta.url)),
        encoding: "utf8",
        timeout: 20_000,
      },
    );
    expect(child.stdout.trim().endsWith("PASS"), `${child.stdout}\n${child.stderr}`).toBe(true);
  });
});
