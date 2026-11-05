#!/usr/bin/env node
// Headless-Chrome regression check for the operator UI transcript
// (crates/wrkspc-ui/src/assets/index.html). Not part of `just quality`:
// it needs a real browser. Run it after changing the page's transcript,
// polling, or boot-gate code.
//
// Against mock_api.py, for an agent turn streamed as 5, 500 and 1200 chunks
// (below, at, and above the 500-event page), it asserts:
//   1. once the boot gate drops, the transcript never changes, and the
//      turn's chunks render as one continuous block labelled with its agent;
//   2. the backlog loads once: polls don't start a second load while one
//      is in flight;
//   3. switching A -> B while an A poll is in flight never shows A's
//      content in B's view;
//   4. message markdown renders as elements (bold, code, fences, web
//      links), while a javascript: link, a local path and a raw tag stay
//      inert text;
//   5. every transcript entry shows its own real time, on the viewer's
//      local clock, in the left gutter;
//   6. a newly streamed chunk extends its turn's block in place, and a
//      new agent message (message_id change) starts a new paragraph;
//   7. the page throws no exceptions;
//   8. the cohort rename editor survives several 1 s polls whose peers
//      payload churns (live `updated_at`), and the committed PATCH carries
//      a JSON content type and a JSON body;
//   9. boot/ban/restore/forget carry the right PATCH/DELETE wire shapes,
//      render their badges, stamp rows from the moderation feed, and the
//      hide-ejected toggle filters the cohort.
//
// Prerequisites: Node >= 22 (global WebSocket and fetch), python3, and
// Google Chrome. Set CHROME to the browser binary when it isn't at the
// macOS default path.
//
// Usage: node scripts/ui-transcript-check/check.mjs [path/to/index.html]
// Exits 0 when every scenario passes, 1 otherwise.

import { spawn } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { createInterface } from "node:readline";
import { setTimeout as sleep } from "node:timers/promises";
import { fileURLToPath } from "node:url";

const HERE = dirname(fileURLToPath(import.meta.url));
const PAGE = resolve(process.argv[2] ?? join(HERE, "../../crates/wrkspc-ui/src/assets/index.html"));
const CHROME = process.env.CHROME ?? "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome";
const SCENARIOS = [5, 500, 1200];
// How long each phase watches the page; several 1 s polls must land.
const WATCH_MS = 3500;
const STARTUP_TIMEOUT_MS = 15000;
// B's entries' created_at in mock_api.py, in transcript order: the message,
// dispatch.started, and the turn (its first chunk).
const B_TIMES = ["2026-11-02T09:00:00Z", "2026-11-02T09:01:00Z", "2026-11-02T09:01:05.123456+00:00"];
// The local wall-clock time a gutter must show for an instant.
const LOCAL_CLOCK = new Intl.DateTimeFormat("en-GB", {
  hour: "2-digit", minute: "2-digit", second: "2-digit", hourCycle: "h23",
});

/**
 * Polls `probe` until it returns a truthy value.
 *
 * @param {() => Promise<*>} probe - Returns the awaited value, or falsy.
 * @param {number} timeoutMs - How long to keep polling.
 * @param {string} what - Names the condition in the timeout error.
 * @returns {Promise<*>} The first truthy value.
 * @throws {Error} When `timeoutMs` passes first.
 */
async function waitFor(probe, timeoutMs, what) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      const value = await probe();
      if (value) return value;
    } catch {
      /* not ready yet */
    }
    await sleep(50);
  }
  throw new Error("timed out waiting for " + what);
}

/**
 * Starts mock_api.py serving `PAGE` on an ephemeral port.
 *
 * @param {number} aCount - Number of events in conversation A.
 * @returns {Promise<{proc: import("node:child_process").ChildProcess, base: string}>}
 *   The mock process and its base URL.
 * @throws {Error} When the mock exits or never reports its port.
 */
function startMock(aCount) {
  const proc = spawn("python3", ["-I", join(HERE, "mock_api.py"), PAGE, String(aCount)], {
    stdio: ["ignore", "pipe", "inherit"],
  });
  return new Promise((resolvePort, reject) => {
    const timer = setTimeout(() => reject(new Error("mock did not report its port")), STARTUP_TIMEOUT_MS);
    proc.once("error", (e) => reject(new Error("could not start python3: " + e.message)));
    proc.once("exit", (code) => reject(new Error("mock exited with " + code)));
    createInterface({ input: proc.stdout }).once("line", (line) => {
      clearTimeout(timer);
      const port = /^PORT=(\d+)$/.exec(line)?.[1];
      if (port) resolvePort({ proc, base: "http://127.0.0.1:" + port });
      else reject(new Error("unexpected mock output: " + line));
    });
  });
}

/**
 * Starts headless Chrome with a throwaway profile and DevTools on an
 * ephemeral port.
 *
 * @returns {Promise<{proc: import("node:child_process").ChildProcess, profile: string, port: number}>}
 *   The browser process, its profile directory, and its DevTools port.
 * @throws {Error} When DevTools never comes up.
 */
async function startChrome() {
  const profile = mkdtempSync(join(tmpdir(), "wrkspc-ui-check-"));
  const proc = spawn(CHROME, [
    "--headless=new", "--no-first-run", "--no-default-browser-check",
    "--remote-debugging-port=0", "--user-data-dir=" + profile, "about:blank",
  ], { stdio: "ignore" });
  proc.once("error", () => { /* surfaced by the timeout below */ });
  try {
    const port = await waitFor(
      async () => Number(readFileSync(join(profile, "DevToolsActivePort"), "utf8").split("\n")[0]),
      STARTUP_TIMEOUT_MS,
      "Chrome DevTools (is CHROME set to the browser binary?)",
    );
    return { proc, profile, port };
  } catch (e) {
    await stop(proc);
    rmSync(profile, { recursive: true, force: true });
    throw e;
  }
}

/**
 * Opens a DevTools session on Chrome's first page target.
 *
 * @param {number} port - Chrome's DevTools port.
 * @returns {Promise<{evaluate: (expr: string) => Promise<*>, navigate: (url: string) => Promise<void>,
 *   errors: string[], close: () => void}>} The session.
 * @throws {Error} When no page target is found or a command times out.
 */
async function connect(port) {
  const target = await waitFor(
    async () => (await (await fetch(`http://127.0.0.1:${port}/json/list`)).json()).find((t) => t.type === "page"),
    STARTUP_TIMEOUT_MS,
    "a Chrome page target",
  );
  const ws = new WebSocket(target.webSocketDebuggerUrl);
  await new Promise((open, fail) => {
    ws.addEventListener("open", open, { once: true });
    ws.addEventListener("error", () => fail(new Error("DevTools socket failed")), { once: true });
  });
  let nextId = 0;
  const pending = new Map();
  const errors = [];
  ws.addEventListener("message", (message) => {
    const msg = JSON.parse(message.data);
    if (msg.id !== undefined && pending.has(msg.id)) {
      pending.get(msg.id)(msg);
      pending.delete(msg.id);
    } else if (msg.method === "Runtime.exceptionThrown") {
      const details = msg.params.exceptionDetails;
      errors.push(details.exception?.description ?? details.text);
    }
  });
  const send = (method, params = {}) => new Promise((done, fail) => {
    const id = ++nextId;
    const timer = setTimeout(() => { pending.delete(id); fail(new Error(method + " timed out")); }, STARTUP_TIMEOUT_MS);
    pending.set(id, (msg) => { clearTimeout(timer); done(msg); });
    ws.send(JSON.stringify({ id, method, params }));
  });
  await send("Runtime.enable");
  return {
    evaluate: async (expression) => {
      const msg = await send("Runtime.evaluate", { expression, returnByValue: true, awaitPromise: true });
      if (msg.result?.exceptionDetails) throw new Error("evaluate failed: " + expression);
      return msg.result?.result?.value;
    },
    navigate: async (url) => { await send("Page.navigate", { url }); },
    errors,
    close: () => ws.close(),
  };
}

/**
 * Terminates a child process and waits (bounded) for it to exit.
 *
 * @param {import("node:child_process").ChildProcess} proc - The process.
 * @returns {Promise<void>} Resolves once it exited or 5 s passed.
 */
async function stop(proc) {
  if (proc.exitCode !== null || proc.signalCode !== null) return;
  const exited = new Promise((done) => proc.once("exit", done));
  proc.kill();
  await Promise.race([exited, sleep(5000)]);
}

/**
 * Reduces probe samples to their distinct transcript states.
 *
 * @param {Array<{msgs: number, events: number, text: string}>} samples - Probe samples.
 * @returns {string[]} One `msgs=… events=…` key per distinct transcript text.
 */
function distinctStates(samples) {
  const states = new Map();
  for (const s of samples) states.set(s.text, `msgs=${s.msgs} events=${s.events}`);
  return [...states.values()];
}

/**
 * The paragraph text of fragments `from`..`to` of a conversation's turn:
 * mock_api.py `_chunk_text` fragments joined, trailing space dropped (as
 * the rendered paragraph drops it).
 *
 * @param {string} cid - The conversation id.
 * @param {number} from - First fragment number (1-based).
 * @param {number} to - Last fragment number, inclusive.
 * @returns {string} The joined fragments.
 */
function chunkRun(cid, from, to) {
  let text = "";
  for (let n = from; n <= to; n++) text += `${cid}-chunk-${n} `;
  return text.trimEnd();
}

/**
 * The text a single-message turn block shows: its label, then the joined
 * fragments `from`..`to`.
 *
 * @param {string} cid - The conversation id.
 * @param {string} agent - The agent the turn's dispatch.started names.
 * @param {number} from - First fragment number (1-based).
 * @param {number} to - Last fragment number, inclusive.
 * @returns {string} The block's text content.
 */
function turnText(cid, agent, from, to) {
  return `turn output · ${agent}` + chunkRun(cid, from, to);
}

/**
 * Reads how B's message rendered. Runs in the page (serialized), so it may
 * only touch the DOM.
 *
 * @returns {{strong: ?string, code: ?string, fenced: ?string, hrefs: string[],
 *   inert: string[], imgs: number, rawMarkdown: boolean, tagAsText: boolean}}
 *   What the message body contains.
 */
function inspectMarkdown() {
  const body = document.querySelector("#transcript .msg .content");
  return {
    strong: body.querySelector("strong")?.textContent ?? null,
    code: body.querySelector("p code")?.textContent ?? null,
    fenced: body.querySelector("pre code")?.textContent ?? null,
    hrefs: [...body.querySelectorAll("a")].map((a) => a.getAttribute("href")),
    inert: [...body.querySelectorAll("span[title]")].map((s) => s.textContent + "=" + s.title),
    imgs: document.querySelectorAll("#transcript img").length,
    rawMarkdown: /\*\*|```/.test(body.textContent),
    tagAsText: body.textContent.includes("<img src=x onerror=alert(1)>"),
  };
}

/**
 * Runs every assertion against one turn size.
 *
 * @param {number} aCount - Number of chunks in conversation A's turn.
 * @returns {Promise<string[]>} Failure descriptions; empty on success.
 */
async function runScenario(aCount) {
  const failures = [];
  const mock = await startMock(aCount);
  let chrome;
  let page;
  try {
    chrome = await startChrome();
    page = await connect(chrome.port);
    const stats = async () => (await fetch(mock.base + "/__stats")).json();
    const samples = () => page.evaluate("window.__probe.samples");

    // 1 + 2: boot onto A, then watch several polls.
    await page.navigate(mock.base + "/");
    await waitFor(
      () => page.evaluate("window.__probe?.samples.some((s) => !s.gate && s.bootHidden)"),
      STARTUP_TIMEOUT_MS + Math.ceil(aCount / 500 + 1) * 2000,
      "the boot gate to drop",
    );
    await sleep(WATCH_MS);
    // A's events render as three lines: started, the turn block, stopped.
    const bootSamples = (await samples()).filter((s) => !s.gate && s.bootHidden);
    const boot = distinctStates(bootSamples);
    if (boot.length !== 1 || boot[0] !== "msgs=1 events=3") {
      failures.push(`transcript changed after the gate dropped, or chunks not grouped: ${boot.join(" -> ")}`);
    }
    if (!bootSamples.at(-1)?.text.includes(turnText("A", "codex", 1, aCount))) {
      failures.push(`A's ${aCount} chunks are not one continuous codex turn block`);
    }
    const { a_from_zero: loads } = await stats();
    if (loads !== 1) failures.push(`backlog loaded ${loads} times, expected once`);

    // 3: switch to B while an A poll is in flight.
    await waitFor(async () => (await stats()).a_inflight > 0, STARTUP_TIMEOUT_MS, "an in-flight A poll");
    const switchedAt = await page.evaluate(
      "(async () => { const t = performance.now() - window.__probe.t0; await select('B'); return t; })()",
    );
    await sleep(WATCH_MS);
    const afterSwitch = (await samples()).filter((s) => s.t > switchedAt && !s.loading);
    if (afterSwitch.some((s) => s.text.includes("A-"))) failures.push("A's content appeared in B's view");
    // B's events render as two lines: started and the open turn block.
    const switched = distinctStates(afterSwitch);
    if (switched.length !== 1 || switched[0] !== "msgs=1 events=2") {
      failures.push(`B's view changed after the switch: ${switched.join(" -> ")}`);
    }
    if (!afterSwitch.at(-1)?.text.includes(turnText("B", "claude", 1, 2))) {
      failures.push("B's chunks are not one continuous claude turn block");
    }

    // 4: B's message markdown rendered, and nothing in it became markup.
    const md = await page.evaluate(`(${inspectMarkdown})()`);
    const expectMd = {
      strong: "bold", code: "code", fenced: "fenced", hrefs: ["https://example.com/"],
      inert: ["a file=/Users/someone/SKILL.md", "bad=javascript:alert(1)"],
      imgs: 0, rawMarkdown: false, tagAsText: true,
    };
    for (const [key, want] of Object.entries(expectMd)) {
      if (JSON.stringify(md[key]) !== JSON.stringify(want)) {
        failures.push(`markdown ${key}: got ${JSON.stringify(md[key])}, want ${JSON.stringify(want)}`);
      }
    }

    // 5: each row's gutter shows that entry's real time, locally.
    const times = await page.evaluate(
      "[...document.querySelectorAll('#transcript .tx-row > time.ts')].map((t) => [t.dateTime, t.textContent])",
    );
    const wantTimes = B_TIMES.map((iso) => {
      const at = new Date(iso.replace(/(\.\d{3})\d+/, "$1"));
      return [at.toISOString(), LOCAL_CLOCK.format(at)];
    });
    const timesOk = times.length === wantTimes.length
      && times.every(([dateTime, text], i) => dateTime === wantTimes[i][0] && text.includes(wantTimes[i][1]));
    if (!timesOk) failures.push(`gutter times: got ${JSON.stringify(times)}, want ${JSON.stringify(wantTimes)}`);

    // 6: a chunk of a new agent message streams into B's open turn.
    const { chunks } = await (await fetch(mock.base + "/__add_b_chunk", { method: "POST" })).json();
    const wantParagraphs = [chunkRun("B", 1, chunks - 1), chunkRun("B", chunks, chunks)];
    try {
      await waitFor(
        async () => {
          const last = (await samples()).at(-1);
          const paragraphs = await page.evaluate(
            "[...document.querySelectorAll('#transcript .turn-body p')].map((p) => p.textContent)",
          );
          return last.events === 2 && JSON.stringify(paragraphs) === JSON.stringify(wantParagraphs);
        },
        WATCH_MS,
        `B's turn block to grow in place into paragraphs ${JSON.stringify(wantParagraphs)}`,
      );
    } catch (e) {
      failures.push(e.message);
    }

    // 7: no page exceptions throughout.
    if (page.errors.length) failures.push("page exceptions: " + page.errors.join(" | "));
  } finally {
    page?.close();
    if (chrome) {
      await stop(chrome.proc);
      rmSync(chrome.profile, { recursive: true, force: true, maxRetries: 3, retryDelay: 200 });
    }
    await stop(mock.proc);
  }
  return failures;
}

/**
 * Runs the cohort-rename regression scenario once against a fresh mock:
 * the rename editor must survive several 1 s polls whose peers payload
 * churns (a live `updated_at`, as real presence does), and the committed
 * PATCH must carry a JSON content type and a JSON body.
 *
 * @returns {Promise<string[]>} Failure descriptions; empty on success.
 */
async function runPeerRenameScenario() {
  const failures = [];
  const mock = await startMock(5);
  let chrome;
  let page;
  try {
    chrome = await startChrome();
    page = await connect(chrome.port);
    await page.navigate(mock.base + "/");
    await waitFor(
      () => page.evaluate("document.querySelectorAll('#peer-list .peer-row').length === 2"),
      STARTUP_TIMEOUT_MS,
      "the cohort rows to render",
    );
    // Open codex's rename editor (the row's first peer button).
    const opened = await page.evaluate(
      "(() => { const row = [...document.querySelectorAll('#peer-list .peer-row')].find((r) => r.querySelector('.peer-name').textContent === 'codex'); if (!row) return false; row.querySelector('.peer-btn').click(); return true; })()",
    );
    if (!opened) failures.push("codex row or its rename button not found");
    const focused = await page.evaluate(
      "document.activeElement !== null && document.activeElement.classList.contains('peer-rename-input')",
    );
    if (!focused) failures.push("rename input not focused after opening");
    // Several 1 s polls land while the peers payload churns every tick.
    await sleep(WATCH_MS);
    const survived = await page.evaluate(
      "document.querySelectorAll('#peer-list input.peer-rename-input').length === 1 && document.activeElement.classList.contains('peer-rename-input') && document.querySelectorAll('#peer-list .peer-row').length === 2",
    );
    if (!survived) failures.push("rename editor destroyed by a poll re-render");
    // Commit with Enter; the PATCH must be a JSON body with a JSON type.
    let commitOk = true;
    try {
      await page.evaluate(
        "(() => { const i = document.querySelector('#peer-list input.peer-rename-input'); i.value = 'renamed-codex'; i.dispatchEvent(new KeyboardEvent('keydown', { key: 'Enter', bubbles: true })); return true; })()",
      );
    } catch {
      commitOk = false;
      failures.push("rename editor gone before commit (a poll re-render destroyed it)");
    }
    if (commitOk) {
      const patch = await waitFor(
        async () => (await (await fetch(mock.base + "/__stats")).json()).last_patch?.path
          ? (await (await fetch(mock.base + "/__stats")).json()).last_patch
          : null,
        WATCH_MS,
        "the rename PATCH to reach the mock",
      );
      let parsed = null;
      try { parsed = JSON.parse(patch.body); } catch { /* stays null */ }
      if (!patch.content_type.includes("application/json")) {
        failures.push(`PATCH content type ${JSON.stringify(patch.content_type)}, want application/json`);
      }
      if (JSON.stringify(parsed) !== JSON.stringify({ alias: "renamed-codex" })) {
        failures.push(`PATCH body ${JSON.stringify(patch.body)}, want {"alias":"renamed-codex"}`);
      }
      if (patch.path !== "/api/peers/codex") failures.push(`PATCH path ${patch.path}, want /api/peers/codex`);
    }
    if (page.errors.length) failures.push("page exceptions: " + page.errors.join(" | "));
  } finally {
    page?.close();
    if (chrome) {
      await stop(chrome.proc);
      rmSync(chrome.profile, { recursive: true, force: true, maxRetries: 3, retryDelay: 200 });
    }
    await stop(mock.proc);
  }
  return failures;
}

/**
 * Runs the cohort-moderation regression scenario once against a fresh
 * mock: boot, restore, ban, hide-ejected, and forget must all carry the
 * right wire shapes, update the rendered roster, and stamp the row from
 * the moderation feed.
 *
 * @returns {Promise<string[]>} Failure descriptions; empty on success.
 */
async function runPeerModerationScenario() {
  const failures = [];
  const mock = await startMock(5);
  let chrome;
  let page;
  const stats = async () => (await fetch(mock.base + "/__stats")).json();
  try {
    chrome = await startChrome();
    page = await connect(chrome.port);
    await page.navigate(mock.base + "/");
    await waitFor(
      () => page.evaluate("document.querySelectorAll('#peer-list .peer-row').length === 2"),
      STARTUP_TIMEOUT_MS,
      "the cohort rows to render",
    );
    // Destructive actions confirm via window.confirm; accept them all.
    await page.evaluate("window.confirm = () => true;");
    const codexBtn = (i) => page.evaluate(
      `(() => { const row = [...document.querySelectorAll('#peer-list .peer-row')].find((r) => r.querySelector('.peer-name').textContent === 'codex'); const b = row?.querySelectorAll('.peer-btn')[${i}]; if (!b) return false; b.click(); return true; })()`,
    );

    // Boot (enrolled row: rename, boot, ban).
    if (!(await codexBtn(1))) failures.push("boot button not found");
    let patch = await waitFor(
      async () => (await stats()).last_patch?.body ? (await stats()).last_patch : null,
      WATCH_MS,
      "the boot PATCH to reach the mock",
    );
    let parsed = null;
    try { parsed = JSON.parse(patch.body); } catch { /* stays null */ }
    if (!patch.content_type.includes("application/json")) failures.push(`boot PATCH content type ${JSON.stringify(patch.content_type)}`);
    if (JSON.stringify(parsed) !== JSON.stringify({ status: "booted" })) failures.push(`boot PATCH body ${JSON.stringify(patch.body)}`);
    await waitFor(
      () => page.evaluate("document.querySelector('#peer-list .peer-status.booted') !== null"),
      WATCH_MS,
      "the booted badge to render",
    );
    // The audit feed stamps the row.
    const stamp = await page.evaluate(
      "(() => { const row = [...document.querySelectorAll('#peer-list .peer-row')].find((r) => r.querySelector('.peer-name').textContent === 'codex'); return row?.querySelector('.mod-stamp')?.textContent ?? ''; })()",
    );
    if (!stamp.startsWith("boot ")) failures.push(`moderation stamp after boot: got ${JSON.stringify(stamp)}`);

    // Restore (ejected row: rename, restore, forget).
    if (!(await codexBtn(1))) failures.push("restore button not found");
    await waitFor(
      async () => { const p = (await stats()).last_patch; return p?.body?.includes("enrolled"); },
      WATCH_MS,
      "the restore PATCH to reach the mock",
    );
    // The re-render must swap the row back to enrolled buttons before
    // the ban click (button indices differ between the two layouts).
    await waitFor(
      () => page.evaluate("document.querySelector('#peer-list .peer-status') === null"),
      WATCH_MS,
      "the enrolled row to re-render",
    );

    // Ban (enrolled row again: rename, boot, ban).
    if (!(await codexBtn(2))) failures.push("ban button not found");
    await waitFor(
      async () => { const p = (await stats()).last_patch; return p?.body?.includes("banned"); },
      WATCH_MS,
      "the ban PATCH to reach the mock",
    );
    await waitFor(
      () => page.evaluate("document.querySelector('#peer-list .peer-status.banned') !== null"),
      WATCH_MS,
      "the banned badge to render",
    );

    // Hide ejected: the banned row disappears from the cohort, and
    // comes back when the toggle clears.
    await page.evaluate("document.getElementById('hide-ejected').click()");
    await waitFor(
      () => page.evaluate("document.querySelectorAll('#peer-list .peer-row').length === 1"),
      WATCH_MS,
      "the ejected row to hide",
    );
    await page.evaluate("document.getElementById('hide-ejected').click()");
    await waitFor(
      () => page.evaluate("document.querySelectorAll('#peer-list .peer-row').length === 2"),
      WATCH_MS,
      "the ejected row to return",
    );

    // Forget (ejected row: rename, restore, forget).
    if (!(await codexBtn(2))) failures.push("forget button not found");
    const del = await waitFor(
      async () => (await stats()).last_delete?.path ? (await stats()).last_delete : null,
      WATCH_MS,
      "the forget DELETE to reach the mock",
    );
    if (del.path !== "/api/peers/codex") failures.push(`forget DELETE path ${del.path}`);
    await waitFor(
      () => page.evaluate("[...document.querySelectorAll('#peer-list .peer-name')].every((n) => n.textContent !== 'codex')"),
      WATCH_MS,
      "the forgotten peer to leave the cohort",
    );

    if (page.errors.length) failures.push("page exceptions: " + page.errors.join(" | "));
  } finally {
    page?.close();
    if (chrome) {
      await stop(chrome.proc);
      rmSync(chrome.profile, { recursive: true, force: true, maxRetries: 3, retryDelay: 200 });
    }
    await stop(mock.proc);
  }
  return failures;
}

let failed = false;
for (const aCount of SCENARIOS) {
  let failures;
  try {
    failures = await runScenario(aCount);
  } catch (e) {
    failures = [e.message];
  }
  failed ||= failures.length > 0;
  console.log(`${failures.length ? "FAIL" : "ok  "} ${aCount} chunks`);
  for (const f of failures) console.log("     " + f);
}

{
  let failures;
  try {
    failures = await runPeerRenameScenario();
  } catch (e) {
    failures = [e.message];
  }
  failed ||= failures.length > 0;
  console.log(`${failures.length ? "FAIL" : "ok  "} peer rename`);
  for (const f of failures) console.log("     " + f);
}

{
  let failures;
  try {
    failures = await runPeerModerationScenario();
  } catch (e) {
    failures = [e.message];
  }
  failed ||= failures.length > 0;
  console.log(`${failures.length ? "FAIL" : "ok  "} peer moderation`);
  for (const f of failures) console.log("     " + f);
}
process.exit(failed ? 1 : 0);
