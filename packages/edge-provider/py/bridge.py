"""edgeTTS synthesis bridge.

Microsoft retired the speech.platform.bing.com v1 host on 2026-10-10. The
replacement host api.msedgeservices.com accepts the legacy token (as an
Ocp-Apim-Subscription-Key header, never as a query parameter) and the legacy
Sec-MS-GEC algorithm, but its WAF fingerprints the TLS handshake and rejects
Node's OpenSSL stack (and Bun's BoringSSL) with 403 while letting Python's
aiohttp through from the same IP. Verified 2026-10-12.

So the Node server keeps everything except the WebSocket: this process owns
the connection. Protocol on stdio (one JSON object per line, both ways):

  request:  {"id": number, "op": "synth", "text": str, "voice": str,
             "outputFormat": str, "rate": str}
  response: {"id": number, "ok": true, "audioB64": str} or
            {"id": number, "ok": false, "error": str}
"""

import asyncio
import base64
import hashlib
import json
import sys
import time
import uuid

import aiohttp

TOKEN = "6A5AA1D4EAFF4E9FB37E23D68491D6F4"
WIN_EPOCH = 11644473600
UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36 Edg/143.0.0.0"
)
WSS_URL = "wss://api.msedgeservices.com/tts/cognitiveservices/websocket/v1"


def sec_ms_gec() -> str:
    ticks = int((time.time() + WIN_EPOCH) * 10_000_000)
    ticks -= ticks % (300 * 10_000_000)
    return hashlib.sha256((str(ticks) + TOKEN).encode()).hexdigest().upper()


def connect_params() -> tuple[str, dict]:
    """One GEC value shared by the query string and the headers: the host
    rejects a handshake when Sec-MS-GEC is only in the query."""
    gec = sec_ms_gec()
    url = (
        f"{WSS_URL}?Sec-MS-GEC={gec}"
        "&Sec-MS-GEC-Version=1-143.0.3650.96"
        f"&ConnectionId={uuid.uuid4()}"
    )
    headers = {
        "User-Agent": UA,
        "Origin": "chrome-extension://jdiccldimpdaibmpdkjnbmckianbfold",
        "Ocp-Apim-Subscription-Key": TOKEN,
        "Sec-MS-GEC": gec,
        "Sec-MS-GEC-Version": "1-143.0.3650.96",
    }
    return url, headers


async def synth(text: str, voice: str, output_format: str, rate: str) -> bytes:
    """One WebSocket turn: connect, configure, synthesize, collect audio."""
    url, headers = connect_params()
    async with aiohttp.ClientSession() as session:
        timeout = aiohttp.ClientWSTimeout(ws_close=10, ws_receive=60)
        async with session.ws_connect(url, headers=headers, timeout=timeout) as ws:
            config_payload = json.dumps(
                {
                    "context": {
                        "synthesis": {
                            "audio": {
                                "metadataoptions": {
                                    "sentenceBoundaryEnabled": "false",
                                    "wordBoundaryEnabled": "false",
                                },
                                "outputFormat": output_format,
                            }
                        }
                    }
                }
            )
            await ws.send_str(
                "Content-Type:application/json; charset=utf-8\r\n"
                f"Path:speech.config\r\n\r\n{config_payload}"
            )
            request_id = uuid.uuid4().hex.upper()
            ssml = (
                '<speak version="1.0" xmlns="http://www.w3.org/2001/10/synthesis"'
                ' xmlns:mstts="https://www.w3.org/2001/mstts" xml:lang="zh-CN">'
                f'<voice name="{voice}"><prosody pitch="+0Hz" rate="{rate}"'
                f' volume="+0%">{text}</prosody></voice></speak>'
            )
            await ws.send_str(
                f"X-RequestId:{request_id}\r\n"
                "Content-Type:application/ssml+xml\r\n"
                f"Path:ssml\r\n\r\n{ssml}"
            )
            audio = b""
            while True:
                msg = await ws.receive(timeout=60)
                if msg.type == aiohttp.WSMsgType.TEXT:
                    if "Path:turn.end" in msg.data:
                        break
                elif msg.type == aiohttp.WSMsgType.BINARY:
                    if len(msg.data) < 2:
                        continue
                    header_len = int.from_bytes(msg.data[:2], "big")
                    header = msg.data[2 : 2 + header_len].decode("latin1")
                    if "Content-Type:audio" in header or "Path:audio" in header:
                        audio += msg.data[2 + header_len :]
                elif msg.type in (
                    aiohttp.WSMsgType.CLOSED,
                    aiohttp.WSMsgType.CLOSE,
                    aiohttp.WSMsgType.ERROR,
                ):
                    raise RuntimeError("websocket closed before turn.end")
            return audio


async def handle(cmd: dict) -> dict:
    if cmd.get("op") != "synth":
        return {"id": cmd.get("id"), "ok": False, "error": f"unknown op {cmd.get('op')}"}
    try:
        audio = await synth(
            cmd["text"], cmd["voice"], cmd.get("outputFormat", "audio-24khz-48kbitrate-mono-mp3"),
            cmd.get("rate", "+0%"),
        )
        return {
            "id": cmd["id"],
            "ok": True,
            "audioB64": base64.b64encode(audio).decode(),
        }
    except Exception as exc:  # noqa: BLE001 - bridge must report, not die
        return {"id": cmd.get("id"), "ok": False, "error": repr(exc)[:500]}


async def main() -> None:
    loop = asyncio.get_event_loop()
    pending: set[asyncio.Task[None]] = set()
    while True:
        line = await loop.run_in_executor(None, sys.stdin.readline)
        if not line:
            break
        line = line.strip()
        if not line:
            continue
        try:
            cmd = json.loads(line)
        except json.JSONDecodeError:
            continue
        # Concurrent synthesis requests must not queue behind each other:
        # the role-cast adapter fires one request per text segment.
        task = asyncio.create_task(respond(cmd))
        pending.add(task)
        task.add_done_callback(pending.discard)


async def respond(cmd: dict) -> None:
    result = await handle(cmd)
    sys.stdout.write(json.dumps(result) + "\n")
    sys.stdout.flush()


if __name__ == "__main__":
    asyncio.run(main())
