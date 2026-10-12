import type { Readable } from "node:stream";
import { Readable as ReadableStream } from "node:stream";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import { join } from "node:path";
import { OUTPUT_FORMAT } from "msedge-tts";
import type { MetadataOptions, ProsodyOptions, Voice } from "msedge-tts";

export interface EdgeClient {
  getVoices(signal: AbortSignal): Promise<readonly Voice[]>;
  /**
   * Connects, or keeps the open connection when voice, format and options are unchanged.
   * Pass metadataOptions on every call: msedge-tts throws on repeat calls without it.
   */
  setMetadata(
    voiceName: string,
    outputFormat: OUTPUT_FORMAT,
    metadataOptions?: MetadataOptions,
  ): Promise<void>;
  toStream(
    input: string,
    options?: ProsodyOptions,
  ): {
    audioStream: Readable;
  };
  close(): void;
}

export type EdgeClientFactory = () => EdgeClient;

/**
 * Microsoft retired the v1 host on 2026-10-10. The replacement host
 * (api.msedgeservices.com) accepts the legacy token as an
 * Ocp-Apim-Subscription-Key header but its WAF fingerprints the TLS
 * handshake and rejects Node's OpenSSL stack with 403 while letting
 * Python aiohttp through from the same IP. So the WebSocket turn runs in
 * a resident Python child process (py/bridge.py) over stdio JSON lines;
 * voice listing still works over plain HTTPS from Node.
 */
class PythonBridgeClient implements EdgeClient {
  private voice = "";
  private outputFormat = OUTPUT_FORMAT.AUDIO_24KHZ_48KBITRATE_MONO_MP3;
  private proc: BridgeProcess | undefined;

  async getVoices(signal: AbortSignal): Promise<readonly Voice[]> {
    const controller = new AbortController();
    const onAbort = () => controller.abort(signal.reason);
    signal.addEventListener("abort", onAbort, { once: true });
    try {
      const res = await fetch(VOICES_URL, { signal: controller.signal });
      if (!res.ok) throw new Error(`voice list ${res.status}`);
      return (await res.json()) as readonly Voice[];
    } finally {
      signal.removeEventListener("abort", onAbort);
    }
  }

  async setMetadata(
    voiceName: string,
    outputFormat: OUTPUT_FORMAT,
    metadataOptions?: MetadataOptions,
  ): Promise<void> {
    this.voice = voiceName;
    this.outputFormat = outputFormat;
    void metadataOptions;
  }

  toStream(input: string, options?: ProsodyOptions): { audioStream: Readable } {
    const rate = options?.rate ?? "+0%";
    const request = {
      op: "synth",
      text: input,
      voice: this.voice,
      outputFormat: this.outputFormat,
      rate,
    } as const;
    const audioStream = new ReadableStream({ read() {} });
    void (async () => {
      try {
        this.proc ??= spawnBridge();
        const reply = await this.proc.request(request);
        if (!reply.ok) throw new Error(`bridge: ${reply.error}`);
        const audio = Buffer.from(reply.audioB64 ?? "", "base64");
        if (audio.length === 0) throw new Error("bridge returned empty audio");
        audioStream.push(audio);
        audioStream.push(null);
      } catch (error) {
        audioStream.destroy(error instanceof Error ? error : new Error(String(error)));
      }
    })();
    return { audioStream };
  }

  close(): void {
    this.proc?.dispose();
    this.proc = undefined;
  }
}

interface BridgeReply {
  id: number;
  ok: boolean;
  audioB64?: string;
  error?: string;
}

class BridgeProcess {
  private static nextId = 1;

  // EDGETTS_BRIDGE_PYTHON/ARGS let tests inject a fake bridge speaking the
  // same stdio protocol instead of spawning real Python.
  private readonly child = spawn(
    process.env.EDGETTS_BRIDGE_PYTHON ?? "python3",
    process.env.EDGETTS_BRIDGE_ARGS
      ? (JSON.parse(process.env.EDGETTS_BRIDGE_ARGS) as string[])
      : [BRIDGE_PATH],
    { stdio: ["pipe", "pipe", "inherit"] },
  );

  private readonly pending = new Map<number, (reply: BridgeReply) => void>();
  private buffer = "";
  private disposed = false;

  constructor() {
    this.child.stdout.setEncoding("utf8");
    this.child.stdout.on("data", (chunk: string) => {
      this.buffer += chunk;
      let newline: number;
      while ((newline = this.buffer.indexOf("\n")) >= 0) {
        const line = this.buffer.slice(0, newline);
        this.buffer = this.buffer.slice(newline + 1);
        if (!line) continue;
        try {
          const reply = JSON.parse(line) as BridgeReply;
          this.pending.get(reply.id)?.(reply);
          this.pending.delete(reply.id);
        } catch {
          // ignore malformed lines
        }
      }
    });
  }

  request(payload: Record<string, unknown>): Promise<BridgeReply> {
    // Executor form (not Promise.withResolvers): package targets ES2022 lib.
    return new Promise<BridgeReply>((resolve, reject) => {
      if (this.disposed || this.child.stdin.destroyed) {
        reject(new Error("bridge process not running"));
        return;
      }
      this.#send(payload, resolve, reject);
    });
  }

  #send(
    payload: Record<string, unknown>,
    resolve: (reply: BridgeReply) => void,
    reject: (reason: unknown) => void,
  ): void {
    const id = BridgeProcess.nextId++;
    this.pending.set(id, resolve);
    this.child.stdin.write(JSON.stringify({ ...payload, id }) + "\n", (error) => {
      if (error) {
        this.pending.delete(id);
        reject(error);
      }
    });
    this.child.once("exit", () => {
      this.pending.delete(id);
      reject(new Error("bridge process exited"));
    });
  }

  dispose(): void {
    if (this.disposed) return;
    this.disposed = true;
    this.child.stdin.end();
    this.child.kill();
  }
}

let bridgeSingleton: BridgeProcess | undefined;

function spawnBridge(): BridgeProcess {
  bridgeSingleton ??= new BridgeProcess();
  return bridgeSingleton;
}

// Source layout: dist/client.js -> ../py/bridge.py. In the deployed image the
// file is copied to a flat path and EDGETTS_BRIDGE_PATH points at it.
const BRIDGE_PATH =
  process.env.EDGETTS_BRIDGE_PATH ??
  join(fileURLToPath(new URL(".", import.meta.url)), "..", "py", "bridge.py");

const VOICES_URL =
  "https://speech.platform.bing.com/consumer/speech/synthesize/readaloud/voices/list?trustedclienttoken=6A5AA1D4EAFF4E9FB37E23D68491D6F4";

export const defaultEdgeClientFactory: EdgeClientFactory = () => new PythonBridgeClient();
