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
//      hide-ejected toggle filters the cohort;
//  10. the conversation strip carries the room's name (or `unnamed room`), its
//      short id and its members in one block, the edit control renames it over
//      PATCH, and the gates menu floats over the transcript, keeps its header
//      fixed while its rows scroll, and gates and kicks from each row;
//  11. each of the four side panes (cohort, conversations, unknown attempts,
//      system events) is its own scroll container, and a hovered element's
//      tooltip arrives in under 400 ms with the native title held off. The
//      operator is in every room by construction and has no deliveries to gate,
//      so it is neither listed nor given pause/block.
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
const B_TIMES = ["2026-11-02T09:00:00Z", "2026-11-02T09:01:05.123456+00:00"];
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
function startMock(aCount, peerCount = 2, unknownCount = 0, eventCount = 0) {
  const proc = spawn(
    "python3",
    ["-I", join(HERE, "mock_api.py"), PAGE, String(aCount), String(peerCount), String(unknownCount),
      String(eventCount)],
    { stdio: ["ignore", "pipe", "inherit"] },
  );
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
    send,
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
    // A's transcript holds authored content only: its message, the work report,
    // and the turn block. Dispatch narration lives in the system feed now.
    const bootSamples = (await samples()).filter((s) => !s.gate && s.bootHidden);
    const boot = distinctStates(bootSamples);
    if (boot.length !== 1 || boot[0] !== "msgs=1 events=2") {
      failures.push(`transcript changed after the gate dropped, or chunks not grouped: ${boot.join(" -> ")}`);
    }
    if (!bootSamples.at(-1)?.text.includes(turnText("A", "codex", 1, aCount))) {
      failures.push(`A's ${aCount} chunks are not one continuous codex turn block`);
    }
    const { a_from_zero: loads } = await stats();
    if (loads !== 1) failures.push(`backlog loaded ${loads} times, expected once`);

    // The transcript is one chronological timeline of authored content; the
    // hub narrating itself (delivery, dispatch) belongs in the system feed beside
    // the unknown attempts. A claim and its ack describe one delivery.
    const feedText = await page.evaluate("window.__feedText || document.getElementById('events-feed').textContent");
    const split = await page.evaluate(`(() => {
      const centre = document.getElementById("transcript").textContent;
      const feed = document.getElementById("events-feed").textContent;
      window.__feedText = feed;
      const count = (hay, needle) => hay.split(needle).length - 1;
      return {
        reportAt: centre.indexOf("A-report-1"),
        messageAt: centre.indexOf("A-msg-1"),
        centreHasNarration: /delivery\\.(claimed|acked)|dispatch\\.(started|stopped)/.test(centre),
        collapsedDeliveryLines: count(feed, "delivery · codex · acked"),
        feedHasDispatch: feed.includes("dispatch.started"),
        feedHasReport: feed.includes("A-report-1"),
        unstampedLines: [...document.querySelectorAll("#events-feed .feed-line")]
          .filter((l) => !l.querySelector("time.feed-ts")?.dateTime).length,
        deliveryStamp: (() => {
          const line = [...document.querySelectorAll("#events-feed .feed-line")]
            .find((l) => l.textContent.includes("delivery · codex · acked"));
          const ts = line?.querySelector("time.feed-ts");
          return ts ? { dateTime: ts.dateTime, text: ts.textContent } : null;
        })(),
      };
    })()`);
    if (split.reportAt < 0) failures.push("the work report is missing from the transcript");
    if (!(split.reportAt < split.messageAt)) {
      failures.push("the work report must sort before the message it precedes, not after it");
    }
    if (split.centreHasNarration) failures.push("hub narration is still rendered in the transcript");
    if (split.collapsedDeliveryLines !== 1) {
      failures.push(`a delivery's claim and ack must collapse to one feed line, got ${split.collapsedDeliveryLines}`);
    }
    // A claim recorded before the recipient field existed must still show the
    // peer its ack names, rather than latching the unknown.
    if (feedText.includes("delivery · ?")) {
      failures.push("a delivery line shows an unresolved peer even though its ack names one");
    }
    if (!split.feedHasDispatch) failures.push("the system feed is missing dispatch events");
    if (split.feedHasReport) failures.push("the work report leaked into the system feed");

    // The delivery row is a name, a state dot, and three icon controls — no
    // badge wall, no words on the buttons.
    const delivery = await page.evaluate(`(() => {
      const chips = [...document.querySelectorAll("#transcript .deliveries .delivery")];
      return {
        chips: chips.length,
        names: chips.map((c) => c.querySelector(".dname")?.textContent ?? null),
        states: chips.map((c) => (c.querySelector(".dstate")?.className ?? "").replace("dstate ", "")),
        stateTitles: chips.map((c) => c.querySelector(".dstate")?.title ?? null),
        buttons: chips.map((c) => c.querySelectorAll("button.delivery-btn").length),
        menuIcons: chips.map((c) => c.querySelectorAll(".delivery-menu button.peer-btn").length),
        menuHidden: chips.map((c) => c.querySelector(".delivery-menu")?.hidden ?? null),
        wordButtons: [...document.querySelectorAll("#transcript .deliveries button")]
          .map((b) => b.textContent.trim())
          .filter((t) => ["hold", "release", "cancel"].includes(t)).length,
      };
    })()`);
    if (delivery.chips !== 2) failures.push(`delivery chips: got ${delivery.chips}, want 2`);
    if (JSON.stringify(delivery.names) !== JSON.stringify(["codex", "kestrel"])) {
      failures.push(`delivery names: got ${JSON.stringify(delivery.names)}`);
    }
    if (JSON.stringify(delivery.states) !== JSON.stringify(["delivered", "stored"])) {
      failures.push(`delivery state dots: got ${JSON.stringify(delivery.states)}`);
    }
    if (!delivery.stateTitles.every((t) => typeof t === "string" && t.includes("—"))) {
      failures.push(`delivery state titles: got ${JSON.stringify(delivery.stateTitles)}`);
    }
    if (!delivery.buttons.every((n) => n === 1)) {
      failures.push(`every delivery must be one button: got ${JSON.stringify(delivery.buttons)}`);
    }
    if (!delivery.menuIcons.every((n) => n === 3)) {
      failures.push(`delivery action menu: got ${JSON.stringify(delivery.menuIcons)} icons, want 3`);
    }
    if (!delivery.menuHidden.every((h) => h === true)) {
      failures.push(`delivery menus must start closed: got ${JSON.stringify(delivery.menuHidden)}`);
    }
    if (delivery.wordButtons !== 0) {
      failures.push(`delivery row still renders word buttons: ${delivery.wordButtons}`);
    }
    if (split.unstampedLines !== 0) {
      failures.push(`${split.unstampedLines} feed line(s) render without a timestamp`);
    }
    // The collapsed line carries the time of the hop that produced its state:
    // the ack at 10:00:06, not the claim at 10:00:05.
    const wantDeliveryStamp = new Date("2026-11-02T10:00:06Z").toISOString();
    if (split.deliveryStamp?.dateTime !== wantDeliveryStamp) {
      failures.push(`collapsed delivery stamp: got ${JSON.stringify(split.deliveryStamp)}, want ${wantDeliveryStamp}`);
    }

    // 3: switch to B while an A poll is in flight.
    await waitFor(async () => (await stats()).a_inflight > 0, STARTUP_TIMEOUT_MS, "an in-flight A poll");
    const switchedAt = await page.evaluate(
      "(async () => { const t = performance.now() - window.__probe.t0; await select('B'); return t; })()",
    );
    await sleep(WATCH_MS);
    const afterSwitch = (await samples()).filter((s) => s.t > switchedAt && !s.loading);
    if (afterSwitch.some((s) => s.text.includes("A-"))) failures.push("A's content appeared in B's view");
    // B's transcript holds its message and the open turn block; its dispatch
    // narration is in the feed.
    const switched = distinctStates(afterSwitch);
    if (switched.length !== 1 || switched[0] !== "msgs=1 events=1") {
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
          return last.events === 1 && JSON.stringify(paragraphs) === JSON.stringify(wantParagraphs);
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
 * Runs the pane-scrolling and tooltip scenario once against a fresh mock with a
 * wide roster and seeded unknown attempts: all four panes (cohort and
 * conversations on the left, unknown attempts and system events on the right)
 * must each be their own scroll container, and a hovered element's tooltip must
 * arrive far faster than the browser's own.
 *
 * @returns {Promise<string[]>} Failure descriptions; empty on success.
 */
async function runScrollAndTooltipScenario() {
  const failures = [];
  const mock = await startMock(5, 14, 6, 30);
  let chrome;
  let page;
  try {
    chrome = await startChrome();
    page = await connect(chrome.port);
    // A short window is what makes every pane overflow at once; the assertions
    // are about each pane owning its own scroll, not about a particular height.
    await page.send("Emulation.setDeviceMetricsOverride", {
      width: 1100,
      height: 420,
      deviceScaleFactor: 1,
      mobile: false,
    });
    await page.navigate(mock.base + "/");
    await waitFor(
      () => page.evaluate("document.querySelectorAll('#peer-list .peer-row').length >= 12"),
      STARTUP_TIMEOUT_MS,
      "the wide cohort to render",
    );

    // Every pane has to have finished loading before it can be measured: the
    // feed renders after its events fetch lands.
    await waitFor(
      () => page.evaluate("document.querySelectorAll('#events-feed .feed-line').length >= 10 && document.querySelectorAll('#unknown-list .attempt').length >= 6"),
      STARTUP_TIMEOUT_MS,
      "the right column to load",
    );

    // The operator holds the top of the cohort, however the roster is ordered.
    const firstPeer = await page.evaluate(
      "document.querySelector('#peer-list .peer-row .peer-name')?.textContent ?? null",
    );
    if (firstPeer !== "operator") {
      failures.push(`the cohort starts with ${JSON.stringify(firstPeer)}, want "operator"`);
    }

    // Each rail heading draws a hairline, so its list is bounded at the top the
    // same way the rows are separated below.
    const headings = JSON.parse(
      await page.evaluate(
        "JSON.stringify([...document.querySelectorAll('.split-slot > .rail-row')].map((r) => ({ label: r.querySelector('h2').textContent, border: getComputedStyle(r).borderBottomWidth })))",
      ),
    );
    if (headings.length !== 2) failures.push(`${headings.length} rail headings, want 2 (cohort and conversations)`);
    for (const heading of headings) {
      if (heading.border !== "1px") failures.push(`the ${heading.label} heading draws no rule (border-bottom: ${heading.border})`);
    }

    const panes = ["peer-list", "convo-list", "unknown-list", "events-feed"];
    const measured = JSON.parse(
      await page.evaluate(
        `JSON.stringify(${JSON.stringify(panes)}.map((id) => {
          const el = document.getElementById(id);
          const style = getComputedStyle(el);
          return { id, overflow: style.overflowY, client: el.clientHeight, scroll: el.scrollHeight };
        }))`,
      ),
    );
    for (const pane of measured) {
      if (pane.overflow !== "auto" && pane.overflow !== "scroll") {
        failures.push(`${pane.id} does not scroll on its own (overflow-y: ${pane.overflow})`);
      }
      if (!(pane.scroll > pane.client)) {
        failures.push(`${pane.id} has nothing to scroll: ${pane.scroll} <= ${pane.client}`);
      }
    }

    // Each pane scrolls alone: moving one must leave the other three at the top.
    for (const id of panes) {
      await page.evaluate(`document.getElementById(${JSON.stringify(id)}).scrollTop = 10000`);
      const state = JSON.parse(
        await page.evaluate(
          `JSON.stringify(${JSON.stringify(panes)}.map((p) => ({ id: p, top: document.getElementById(p).scrollTop })))`,
        ),
      );
      const moved = state.find((p) => p.id === id).top;
      if (moved <= 0) failures.push(`${id} did not scroll when asked`);
      for (const other of state) {
        // The feed follows new traffic by design, so its scrollTop moves on its
        // own; the independence check only covers panes that never self-scroll.
        if (other.id !== id && other.id !== "events-feed" && other.top !== 0) {
          failures.push(`scrolling ${id} also moved ${other.id} to ${other.top}`);
        }
      }
      await page.evaluate(`document.getElementById(${JSON.stringify(id)}).scrollTop = 0`);
    }

    // Let the page settle: the transcript follows new traffic, which used to be
    // enough to cancel a pending tooltip.
    await sleep(1200);

    // The tooltip: fast, and the native one never gets a chance to appear.
    const timing = await page.evaluate(`(async () => {
      const toggle = document.getElementById('gates-toggle');
      const box = toggle.getBoundingClientRect();
      const text = toggle.getAttribute('title');
      if (!text) return JSON.stringify({ error: 'the trigger carries no title' });
      const started = performance.now();
      toggle.dispatchEvent(new MouseEvent('mouseover', {
        bubbles: true, clientX: box.left + 4, clientY: box.top + 4,
      }));
      let shown = null;
      while (performance.now() - started < 1500) {
        const tip = document.getElementById('tooltip');
        if (tip && !tip.hidden && tip.textContent) {
          shown = { ms: performance.now() - started, text: tip.textContent, title: toggle.getAttribute('title') };
          break;
        }
        await new Promise((r) => setTimeout(r, 10));
      }
      toggle.dispatchEvent(new MouseEvent('mouseout', {
        bubbles: true, relatedTarget: document.body, clientX: box.left + 4, clientY: box.top + 4,
      }));
      await new Promise((r) => setTimeout(r, 60));
      const tip = document.getElementById('tooltip');
      return JSON.stringify({
        text, shown,
        afterHide: { hidden: tip.hidden, title: toggle.getAttribute('title') },
      });
    })()`);
    const tip = JSON.parse(timing);
    if (tip.error) failures.push(tip.error);
    else {
      if (!tip.shown) failures.push("the tooltip never appeared: " + JSON.stringify(tip));
      else {
        if (tip.shown.ms > 400) failures.push(`the tooltip took ${Math.round(tip.shown.ms)}ms, want under 400ms`);
        if (tip.shown.text !== tip.text) {
          failures.push(`the tooltip reads ${JSON.stringify(tip.shown.text)}, want ${JSON.stringify(tip.text)}`);
        }
        if (tip.shown.title !== null) {
          failures.push("the native title stayed on the element while the tooltip showed");
        }
      }
      if (!tip.afterHide?.hidden) failures.push("the tooltip stayed up after the pointer left");
      if (tip.afterHide?.title !== tip.text) failures.push("the title was not restored after the tooltip hid");
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
 * Runs the conversation-strip regression scenario once against a fresh mock:
 * the strip carries the room's name (or `unnamed room`), its id, and its
 * members in one block; the gates menu floats over the transcript instead of
 * pushing it down, keeps its header put while its rows scroll, and each member
 * row carries pause, block, and (except for the operator) kick.
 *
 * @returns {Promise<string[]>} Failure descriptions; empty on success.
 */
async function runRoomStripScenario() {
  const failures = [];
  const mock = await startMock(5);
  const stats = async () => (await fetch(mock.base + "/__stats")).json();
  let chrome;
  let page;
  try {
    chrome = await startChrome();
    page = await connect(chrome.port);
    // A desktop-sized column, so the composer's single-row footer is the shape
    // being asserted rather than a narrow-window wrap.
    await page.send("Emulation.setDeviceMetricsOverride", {
      width: 1280,
      height: 860,
      deviceScaleFactor: 1,
      mobile: false,
    });
    await page.navigate(mock.base + "/");
    await waitFor(
      () => page.evaluate("document.querySelectorAll('.convo').length === 3"),
      STARTUP_TIMEOUT_MS,
      "the conversation blocks to render",
    );

    // The composer is one box: tabs inside its top edge, send inside its bottom,
    // and the writing surface taking the room the outside controls used to hold.
    const composer = JSON.parse(
      await page.evaluate(
        "(() => { const box = document.querySelector('.composer'); if (!box) return JSON.stringify({missing: true}); const foot = box.querySelector('.composer-foot'); const t = document.getElementById('compose-text').getBoundingClientRect(); const seg = foot.querySelector('.seg').getBoundingClientRect(); const send = document.getElementById('compose-send').getBoundingClientRect(); const fb = foot.getBoundingClientRect(); return JSON.stringify({border: getComputedStyle(box).borderTopWidth, sendInside: box.contains(document.getElementById('compose-send')), tabsInside: box.contains(document.getElementById('compose-mode-msg')), tabsInFoot: foot.contains(document.getElementById('compose-mode-msg')), hintText: document.getElementById('compose-hint').textContent, sameRow: Math.abs(seg.top - send.top) < 6 && Math.abs(seg.bottom - send.bottom) < 8, padL: parseFloat(getComputedStyle(foot).paddingLeft), padR: parseFloat(getComputedStyle(foot).paddingRight), segLeft: Math.round(seg.left - fb.left), sendRight: Math.round(fb.right - send.right), textHeight: Math.round(t.height)}); })()",
      ),
    );
    if (composer.missing) failures.push("the composer box is gone");
    else {
      if (composer.border !== "1px") failures.push(`the composer draws no border (${composer.border})`);
      if (!composer.sendInside) failures.push("the send control is outside the composer box");
      if (!composer.tabsInside) failures.push("the mode tabs are outside the composer box");
      // The standing "as operator · to …" line is gone; the hint is only for
      // something the operator must act on.
      if (composer.hintText !== "") failures.push(`the compose hint reads ${JSON.stringify(composer.hintText)}, want it empty`);
      if (!composer.tabsInFoot) failures.push("the mode tabs are not in the composer footer");
      if (!composer.sameRow) failures.push("the mode tabs and send do not share a row");
      // Measured from the footer's content box: its padding is not a gap.
      if (Math.abs(composer.segLeft - composer.padL) > 1) {
        failures.push(`the mode tabs sit ${Math.round(composer.segLeft - composer.padL)}px off the footer's left edge`);
      }
      if (Math.abs(composer.sendRight - composer.padR) > 1) {
        failures.push(`send sits ${Math.round(composer.sendRight - composer.padR)}px off the footer's right edge`);
      }
      if (composer.textHeight < 110) failures.push(`the writing surface is ${composer.textHeight}px tall, want at least 110px`);
    }

    // The left column: a named room, and an unnamed one that says so.
    const blocks = JSON.parse(
      await page.evaluate(
        "JSON.stringify([...document.querySelectorAll('.convo')].map((b) => ({ name: b.querySelector('.cname').textContent, id: b.querySelector('.cid').textContent })))",
      ),
    );
    const named = blocks.find((b) => b.id === "A");
    const unnamed = blocks.find((b) => b.id === "B");
    if (named?.name !== "Veritas design") {
      failures.push(`room A reads ${JSON.stringify(named?.name)}, want "Veritas design"`);
    }
    if (unnamed?.name !== "unnamed room") {
      failures.push(`room B reads ${JSON.stringify(unnamed?.name)}, want "unnamed room"`);
    }

    // The inline rename: an edit control, a focused field, and a PATCH on Enter.
    const glyph = await page.evaluate(
      "[...document.querySelectorAll('.convo')].find((b) => b.querySelector('.cid').textContent === 'A').querySelector('.convo-edit').textContent",
    );
    if (glyph !== "\u270e") failures.push(`the edit control reads ${JSON.stringify(glyph)}, want the low pencil U+270E`);
    const opened = await page.evaluate(
      "(() => { const block = [...document.querySelectorAll('.convo')].find((b) => b.querySelector('.cid').textContent === 'A'); if (!block) return false; block.querySelector('.convo-edit').click(); return true; })()",
    );
    if (!opened) failures.push("room A block or its edit control not found");
    const editing = await page.evaluate(
      "document.querySelectorAll('input.convo-rename-input').length === 1 && document.activeElement.classList.contains('convo-rename-input')",
    );
    if (!editing) failures.push("the rename field did not open focused");

    // The field takes the block, with the id and both controls on the row below.
    const layout = JSON.parse(
      await page.evaluate(
        "(() => { const b = [...document.querySelectorAll('.convo')].find((x) => x.querySelector('input.convo-rename-input')); const i = b.querySelector('input.convo-rename-input'); const shell = b.querySelector('.convo-rename').getBoundingClientRect(); const id = b.querySelector('.convo-rename-foot .cid').getBoundingClientRect(); const ctl = b.querySelector('.convo-rename-foot .convo-actions').getBoundingClientRect(); const ib = i.getBoundingClientRect(); const ed = b.querySelector('.convo-edit').getBoundingClientRect(); const dl = b.querySelector('.convo-del').getBoundingClientRect(); return JSON.stringify({ field: ib.width, block: shell.width, idBelow: id.top >= ib.bottom - 1, idLeft: id.left - shell.left, ctlBelow: ctl.top >= ib.bottom - 1, ctlRight: shell.right - ctl.right, sameRow: Math.abs(ed.top - dl.top) < 4 && dl.left >= ed.left, delOffRight: Math.abs(shell.right - dl.right) }); })()",
      ),
    );
    if (layout.field < layout.block * 0.75) {
      failures.push(`rename field is ${Math.round(layout.field)}px of a ${Math.round(layout.block)}px block, want it full width`);
    }
    if (!layout.idBelow) failures.push("the room id is not beneath the field");
    if (!layout.ctlBelow) failures.push("the controls are not beneath the field");
    if (layout.idLeft > 4) failures.push(`the room id starts ${Math.round(layout.idLeft)}px into the block, want it left-aligned`);
    if (layout.ctlRight > 4) failures.push(`the controls end ${Math.round(layout.ctlRight)}px short of the block edge, want them pinned right`);
    if (!layout.sameRow) failures.push("the edit and delete controls do not share a row");
    if (layout.delOffRight > 4) failures.push(`the delete control ends ${Math.round(layout.delOffRight)}px off the block edge`);

    // The resting block keeps both controls in one right-pinned cluster.
    const resting = JSON.parse(
      await page.evaluate(
        "(() => { const b = [...document.querySelectorAll('.convo')].find((x) => x.querySelector('.cid')?.textContent === 'A'); const a = b.querySelector('.convo-actions').getBoundingClientRect(); const ed = b.querySelector('.convo-edit').getBoundingClientRect(); const dl = b.querySelector('.convo-del').getBoundingClientRect(); const bb = b.getBoundingClientRect(); return JSON.stringify({ delInside: dl.left >= a.left - 1 && dl.right <= a.right + 1, sameRow: Math.abs(ed.top - dl.top) < 4, delOffRight: bb.right - dl.right }); })()",
      ),
    );
    if (!resting.delInside) failures.push("the resting delete control is outside the action cluster");
    if (!resting.sameRow) failures.push("the resting edit and delete controls do not share a row");

    // Escape must always get out, and must leave the block rebuilt.
    await page.evaluate(
      "(() => { const i = document.querySelector('input.convo-rename-input'); i.dispatchEvent(new KeyboardEvent('keydown', { key: 'Escape', bubbles: true })); return true; })()",
    );
    const escaped = await waitFor(
      async () => {
        const state = JSON.parse(
          await page.evaluate(
            "(() => { const b = [...document.querySelectorAll('.convo')].find((x) => x.querySelector('.cid')?.textContent === 'A'); return JSON.stringify({ inputs: document.querySelectorAll('input.convo-rename-input').length, name: b?.querySelector('.cname')?.textContent ?? null }); })()",
          ),
        );
        return state.inputs === 0 && state.name === "Veritas design" ? state : null;
      },
      WATCH_MS,
      "Escape to leave the rename and restore the block",
    );
    if (escaped === null) failures.push("Escape did not leave the rename");

    // Clicking the edit control again cancels without saving.
    await page.evaluate(
      "[...document.querySelectorAll('.convo')].find((b) => b.querySelector('.cid')?.textContent === 'A').querySelector('.convo-edit').click()",
    );
    await waitFor(
      () => page.evaluate("document.querySelectorAll('input.convo-rename-input').length === 1"),
      WATCH_MS,
      "the rename field to reopen for the cancel path",
    );
    const beforeCancel = (await stats()).last_patch?.path ?? null;
    await page.evaluate(
      "(() => { const i = document.querySelector('input.convo-rename-input'); i.value = 'never saved'; const b = [...document.querySelectorAll('.convo')].find((x) => x.querySelector('input.convo-rename-input')); b.querySelector('.convo-edit').click(); return true; })()",
    );
    const cancelled = await waitFor(
      async () => {
        const state = JSON.parse(
          await page.evaluate(
            "(() => { const b = [...document.querySelectorAll('.convo')].find((x) => x.querySelector('.cid')?.textContent === 'A'); return JSON.stringify({ inputs: document.querySelectorAll('input.convo-rename-input').length, name: b?.querySelector('.cname')?.textContent ?? null }); })()",
          ),
        );
        return state.inputs === 0 ? state : null;
      },
      WATCH_MS,
      "the cancel click to close the field",
    );
    if (cancelled === null) failures.push("clicking the edit control again did not close the field");
    if (cancelled?.name !== "Veritas design") failures.push(`cancel saved a name: ${JSON.stringify(cancelled?.name)}`);
    if (((await stats()).last_patch?.path ?? null) !== beforeCancel) {
      failures.push("cancel sent a PATCH");
    }

    // Re-open for the commit path.
    await page.evaluate(
      "[...document.querySelectorAll('.convo')].find((b) => b.querySelector('.cid').textContent === 'A').querySelector('.convo-edit').click()",
    );
    await waitFor(
      () => page.evaluate("document.querySelectorAll('input.convo-rename-input').length === 1"),
      WATCH_MS,
      "the rename field to open again",
    );
    // A poll must not eat the field while it is open.
    await sleep(WATCH_MS);
    const survived = await page.evaluate("document.querySelectorAll('input.convo-rename-input').length === 1");
    if (!survived) failures.push("rename field destroyed by a poll re-render");
    await page.evaluate(
      "(() => { const i = document.querySelector('input.convo-rename-input'); i.value = 'Veritas design 2'; i.dispatchEvent(new KeyboardEvent('keydown', { key: 'Enter', bubbles: true })); return true; })()",
    );
    const patch = await waitFor(
      async () => {
        const seen = (await stats()).last_patch;
        return seen?.path === "/api/conversations/A" ? seen : null;
      },
      WATCH_MS,
      "the rename PATCH to reach the mock",
    );
    let patchBody = null;
    try {
      patchBody = JSON.parse(patch.body);
    } catch {
      /* stays null */
    }
    if (!patch.content_type.includes("application/json")) {
      failures.push(`rename content type ${JSON.stringify(patch.content_type)}, want application/json`);
    }
    if (patchBody?.name !== "Veritas design 2") {
      failures.push(`rename body ${JSON.stringify(patch.body)}, want {"name":"Veritas design 2"}`);
    }
    const renamed = await waitFor(
      async () => {
        const current = await page.evaluate(
          "[...document.querySelectorAll('.convo')].find((b) => b.querySelector('.cid')?.textContent === 'A')?.querySelector('.cname')?.textContent ?? null",
        );
        return current === "Veritas design 2" ? current : null;
      },
      WATCH_MS,
      "the renamed room to re-render",
    );
    if (renamed !== "Veritas design 2") failures.push("the renamed room did not re-render");

    // Select the wide room and read the strip.
    await page.evaluate(
      "[...document.querySelectorAll('.convo')].find((b) => b.querySelector('.cid')?.textContent === 'C').click()",
    );
    await waitFor(
      () => page.evaluate("document.getElementById('room-id').textContent === 'C'"),
      STARTUP_TIMEOUT_MS,
      "the wide room to open",
    );
    const strip = JSON.parse(
      await page.evaluate(
        "JSON.stringify({ name: document.getElementById('room-name').textContent, id: document.getElementById('room-id').textContent, members: document.getElementById('room-members').textContent })",
      ),
    );
    if (strip.name !== "unnamed room") {
      failures.push(`strip name ${JSON.stringify(strip.name)}, want "unnamed room"`);
    }
    if (!strip.members.startsWith("operator, m01")) {
      failures.push(`members line ${JSON.stringify(strip.members)}, want it to start "operator, m01"`);
    }

    // The trigger is pinned to the strip's right edge, and the menu opens
    // beneath it rather than under the room name.
    const pinned = JSON.parse(
      await page.evaluate(
        "(() => { const t = document.getElementById('gates-toggle').getBoundingClientRect(); const h = document.getElementById('convo-head').getBoundingClientRect(); return JSON.stringify({ gap: h.right - t.right, toggleRight: t.right }); })()",
      ),
    );
    if (pinned.gap > 24) failures.push(`the gates trigger sits ${Math.round(pinned.gap)}px from the strip's right edge`);

    // The menu floats: opening it must not move the transcript.
    const closedTop = await page.evaluate("document.getElementById('transcript').getBoundingClientRect().top");
    const hiddenAtStart = await page.evaluate("document.getElementById('gates-menu').hidden");
    if (!hiddenAtStart) failures.push("the gates menu starts open");
    await page.evaluate("document.getElementById('gates-toggle').click()");
    await sleep(150);
    const openTop = await page.evaluate("document.getElementById('transcript').getBoundingClientRect().top");
    if (Math.abs(openTop - closedTop) > 0.5) {
      failures.push(`opening the menu moved the transcript by ${openTop - closedTop}px`);
    }
    const menuAligned = JSON.parse(
      await page.evaluate(
        "(() => { const m = document.getElementById('gates-menu').getBoundingClientRect(); const t = document.getElementById('gates-toggle').getBoundingClientRect(); return JSON.stringify({ delta: Math.abs(m.right - t.right) }); })()",
      ),
    );
    if (menuAligned.delta > 2) failures.push(`the menu's right edge is ${Math.round(menuAligned.delta)}px off the trigger's`);
    const rows = await page.evaluate("document.querySelectorAll('#gates-list .gate-row').length");
    if (rows !== 19) failures.push(`${rows} gate rows, want 19 (20 members less the operator)`);
    const listed = JSON.parse(
      await page.evaluate(
        "JSON.stringify([...document.querySelectorAll('#gates-list .gate-row .gate-name')].map((n) => n.textContent))",
      ),
    );
    if (listed.includes("operator")) failures.push("the operator is listed in the gates menu");

    // A fixed header over a scrolling list.
    const scroll = JSON.parse(
      await page.evaluate(
        "(() => { const l = document.getElementById('gates-list'); return JSON.stringify({ client: l.clientHeight, scroll: l.scrollHeight, headTop: document.querySelector('.gates-head').getBoundingClientRect().top }); })()",
      ),
    );
    if (!(scroll.scroll > scroll.client)) {
      failures.push(`the gates list does not scroll: ${scroll.scroll} <= ${scroll.client}`);
    }
    await page.evaluate("document.getElementById('gates-list').scrollTop = 10000");
    await sleep(100);
    const headAfter = await page.evaluate("document.querySelector('.gates-head').getBoundingClientRect().top");
    if (Math.abs(headAfter - scroll.headTop) > 0.5) failures.push("the menu header scrolled with the list");

    // One member's gate: the POST shape, then the icon lit from the re-read.
    await page.evaluate(
      "(() => { const row = [...document.querySelectorAll('#gates-list .gate-row')].find((r) => r.querySelector('.gate-name').textContent === 'm01'); row.querySelector('.peer-btn').click(); return true; })()",
    );
    const gate = await waitFor(
      async () => {
        const seen = (await stats()).last_fence;
        return seen?.path === "/api/conversations/C/fences" ? seen : null;
      },
      WATCH_MS,
      "the pause gate to reach the mock",
    );
    let gateBody = null;
    try {
      gateBody = JSON.parse(gate.body);
    } catch {
      /* stays null */
    }
    if (gateBody?.peer_id !== "m01" || gateBody?.kind !== "pause") {
      failures.push(`pause body ${JSON.stringify(gate.body)}, want {"peer_id":"m01","kind":"pause"}`);
    }
    const lit = await waitFor(
      async () => {
        const count = await page.evaluate("document.querySelectorAll('#gates-list .peer-btn.on').length");
        return count === 1 ? count : null;
      },
      WATCH_MS,
      "the pause gate to light up",
    );
    if (lit !== 1) failures.push("the pause gate did not render as set");

    // The bulk header: pause everyone, then clear every gate in the room.
    await page.evaluate(`document.querySelector('#gates-menu [data-bulk="pause"]').click()`);
    const allPaused = await waitFor(
      async () => {
        const count = await page.evaluate("document.querySelectorAll('#gates-list .peer-btn.on:not(.block)').length");
        return count === 19 ? count : null;
      },
      2 * WATCH_MS,
      "every gateable member to be paused",
    );
    if (allPaused !== 19) failures.push("the bulk pause did not apply to every gateable member");
    await page.evaluate(`document.querySelector('#gates-menu [data-bulk="clear"]').click()`);
    let cleared = null;
    try {
      cleared = await waitFor(
        async () => {
          // `0` is falsy, so the probe returns a word: waitFor reads a falsy
          // result as "not yet".
          const count = await page.evaluate("document.querySelectorAll('#gates-list .gate-row .peer-btn.on').length");
          return count === 0 ? "cleared" : null;
        },
        2 * WATCH_MS,
        "every gate to clear",
      );
    } catch (error) {
      const left = await page.evaluate("document.querySelectorAll('#gates-list .gate-row .peer-btn.on').length");
      failures.push(
        `bulk clear left ${left} gates set (errors: ${page.errors.join(" | ") || "none"}, last delete ${JSON.stringify((await stats()).last_fence_delete)})`,
      );
    }
    if (cleared !== null && cleared !== "cleared") failures.push("the bulk clear left gates set");
    const clearedPath = (await stats()).last_fence_delete?.path ?? "";
    if (!clearedPath.startsWith("/api/fences/")) {
      failures.push(`the bulk clear deleted ${JSON.stringify(clearedPath)}, want /api/fences/{id}`);
    }

    // Kick arms on the first click and fires on the second.
    const beforeKick = (await stats()).last_delete?.path ?? null;
    const armed = await page.evaluate(
      "(() => { const row = [...document.querySelectorAll('#gates-list .gate-row')].find((r) => r.querySelector('.gate-name').textContent === 'm02'); const k = row.querySelector('.peer-btn.danger'); k.click(); return k.textContent; })()",
    );
    if (armed !== "✓") failures.push(`the kick did not arm: ${JSON.stringify(armed)}`);
    await sleep(200);
    if (((await stats()).last_delete?.path ?? null) !== beforeKick) {
      failures.push("the first click already kicked");
    }
    await page.evaluate(
      "(() => { const row = [...document.querySelectorAll('#gates-list .gate-row')].find((r) => r.querySelector('.gate-name').textContent === 'm02'); row.querySelector('.peer-btn.danger').click(); return true; })()",
    );
    const kickedDelete = await waitFor(
      async () => {
        const seen = (await stats()).last_delete;
        return seen?.path?.includes("/members/m02") ? seen : null;
      },
      WATCH_MS,
      "the kick to reach the mock",
    );
    if (kickedDelete.method !== "DELETE") failures.push("the kick did not use DELETE");

    // Deleting a room arms first: one stray click must not lose a transcript.
    const beforeRoomDelete = (await stats()).last_delete?.path ?? null;
    const armedRoom = await page.evaluate(
      "(() => { const b = [...document.querySelectorAll('.convo')].find((x) => x.querySelector('.cid').textContent === 'B'); const d = b.querySelector('.convo-del'); d.click(); return d.textContent; })()",
    );
    if (armedRoom !== "✓") failures.push(`the room delete did not arm: ${JSON.stringify(armedRoom)}`);
    await sleep(200);
    if (((await stats()).last_delete?.path ?? null) !== beforeRoomDelete) {
      failures.push("the first click already deleted the room");
    }
    await page.evaluate(
      "(() => { const b = [...document.querySelectorAll('.convo')].find((x) => x.querySelector('.cid').textContent === 'B'); b.querySelector('.convo-del').click(); return true; })()",
    );
    const roomDeleted = await waitFor(
      async () => {
        const seen = (await stats()).last_delete;
        return seen?.path === "/api/conversations/B" ? seen : null;
      },
      WATCH_MS,
      "the confirmed delete to reach the mock",
    );
    if (roomDeleted.method !== "DELETE") failures.push("the room delete did not use DELETE");

    // Escape closes the menu.
    await page.evaluate(`document.dispatchEvent(new KeyboardEvent('keydown', { key: 'Escape' }))`);
    await sleep(100);
    if (!(await page.evaluate("document.getElementById('gates-menu').hidden"))) {
      failures.push("Escape did not close the gates menu");
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
    // The audit feed shows as a bare duration in the row counter, with the
    // action itself kept in the tooltip rather than printed on the row.
    const stamp = await page.evaluate(
      "(() => { const row = [...document.querySelectorAll('#peer-list .peer-row')].find((r) => r.querySelector('.peer-name').textContent === 'codex'); const c = row?.querySelector('.peer-counter'); return JSON.stringify({ text: c?.textContent ?? '', title: c?.title ?? '' }); })()",
    );
    const counter = JSON.parse(stamp);
    if (!/^(\d+d \d+h \d{2}m \d{2}s|\d+h \d{2}m \d{2}s|\d+m \d{2}s|—)$/.test(counter.text)) {
      failures.push(`row counter after boot: got ${JSON.stringify(counter.text)}`);
    }
    if (!counter.title.includes("boot")) {
      failures.push(`row counter tooltip must name the action: got ${JSON.stringify(counter.title)}`);
    }
    if (/\bago\b/.test(counter.text) || /enrol\b/.test(counter.text)) {
      failures.push(`row counter must carry no words: got ${JSON.stringify(counter.text)}`);
    }

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

    // Forget (ejected row: rename, restore, forget). Deleting is a two-click
    // confirm: the first click arms the button, the second commits the DELETE,
    // because a suppressed window.confirm used to make one click look dead.
    if (!(await codexBtn(2))) failures.push("forget button not found");
    const armed = await page.evaluate(
      `(() => { const row = [...document.querySelectorAll('#peer-list .peer-row')].find((r) => r.querySelector('.peer-name').textContent === 'codex'); return row?.querySelectorAll('.peer-btn')[2]?.textContent === '✓'; })()`,
    );
    if (!armed) failures.push("the delete button did not arm on the first click");
    await codexBtn(2);
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

/**
 * Verifies sticky-bottom scrolling in both reader panes: a pane sitting at its
 * bottom follows new content, a pane scrolled up keeps its view, and scrolling
 * back to the bottom resumes the follow.
 *
 * @returns {Promise<string[]>} Failure messages, empty when the behaviour holds.
 */
async function runFollowScenario() {
  const failures = [];
  const mock = await startMock(5);
  let chrome;
  let page;
  try {
    chrome = await startChrome();
    page = await connect(chrome.port);
    const post = (path) => fetch(mock.base + path, { method: "POST" }).then((r) => r.json());
    const panes = () => page.evaluate(`(() => {
      const read = (id) => {
        const e = document.getElementById(id);
        return {
          top: Math.round(e.scrollTop),
          gap: Math.round(e.scrollHeight - e.scrollTop - e.clientHeight),
          height: e.scrollHeight,
          client: e.clientHeight,
        };
      };
      return { transcript: read("transcript-scroll"), feed: read("events-feed") };
    })()`);
    const scrollTo = (value) => page.evaluate(
      `(() => { for (const id of ["transcript-scroll", "events-feed"]) document.getElementById(id).scrollTop = ${value}; return true; })()`,
    );

    await page.navigate(mock.base + "/");
    await waitFor(
      () => page.evaluate("window.__probe?.samples.some((s) => !s.gate && s.bootHidden)"),
      STARTUP_TIMEOUT_MS,
      "the boot gate to drop",
    );
    await page.evaluate("select('B')");
    // Shrink both panes so ordinary traffic overflows them. They are flex
    // children, so the basis has to be pinned too or flex grows them back.
    await page.evaluate(`(() => {
      for (const id of ["transcript-scroll", "events-feed"]) {
        const e = document.getElementById(id);
        e.style.flex = "none";
        e.style.height = "120px";
        e.style.minHeight = "120px";
      }
      return true;
    })()`);
    for (let i = 0; i < 60; i += 1) await post("/__add_b_chunk");
    for (let i = 0; i < 40; i += 1) await post("/__add_event?conversation=B");
    let overflow = null;
    try {
      await waitFor(async () => {
        const p = await panes();
        return p.transcript.height > p.transcript.client + 1
          && p.feed.height > p.feed.client + 1;
      }, WATCH_MS, "both panes to overflow");
    } catch (e) {
      overflow = await panes();
      failures.push(`${e.message} (transcript ${JSON.stringify(overflow.transcript)}, feed ${JSON.stringify(overflow.feed)})`);
    }

    // 1: at the bottom, new content keeps both panes pinned.
    await scrollTo(1e6);
    await sleep(250);
    const pinnedBefore = await panes();
    await post("/__add_b_chunk");
    await post("/__add_event?conversation=B");
    await waitFor(async () => {
      const p = await panes();
      return p.transcript.height > pinnedBefore.transcript.height
        && p.feed.height > pinnedBefore.feed.height;
    }, WATCH_MS, "the new content to render in both panes");
    const pinnedAfter = await panes();
    if (pinnedAfter.transcript.gap >= 60) {
      failures.push(`transcript stopped following at the bottom: gap ${pinnedAfter.transcript.gap}px`);
    }
    if (pinnedAfter.feed.gap >= 60) {
      failures.push(`feed stopped following at the bottom: gap ${pinnedAfter.feed.gap}px`);
    }

    // 2: scrolled up, new content must not move the reader.
    await scrollTo(30);
    await sleep(250);
    const upBefore = await panes();
    await post("/__add_b_chunk");
    await post("/__add_event?conversation=B");
    await waitFor(async () => {
      const p = await panes();
      return p.transcript.height > upBefore.transcript.height;
    }, WATCH_MS, "the new content to render while scrolled up");
    const upAfter = await panes();
    if (Math.abs(upAfter.transcript.top - upBefore.transcript.top) > 2) {
      failures.push(`a scrolled-up transcript moved: ${upBefore.transcript.top} -> ${upAfter.transcript.top}`);
    }
    if (Math.abs(upAfter.feed.top - upBefore.feed.top) > 2) {
      failures.push(`a scrolled-up feed moved: ${upBefore.feed.top} -> ${upAfter.feed.top}`);
    }

    // 3: back at the bottom, the follow resumes.
    await scrollTo(1e6);
    await sleep(250);
    const resumeBefore = await panes();
    await post("/__add_b_chunk");
    await post("/__add_event?conversation=B");
    await waitFor(async () => {
      const p = await panes();
      return p.transcript.height > resumeBefore.transcript.height
        && p.feed.height > resumeBefore.feed.height;
    }, WATCH_MS, "the new content to render after resuming");
    const resumeAfter = await panes();
    if (resumeAfter.transcript.gap >= 60) {
      failures.push(`transcript did not resume following: gap ${resumeAfter.transcript.gap}px`);
    }
    if (resumeAfter.feed.gap >= 60) {
      failures.push(`feed did not resume following: gap ${resumeAfter.feed.gap}px`);
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
{
  let failures;
  try {
    failures = await runScrollAndTooltipScenario();
  } catch (e) {
    failures = [e.message];
  }
  failed ||= failures.length > 0;
  console.log(`${failures.length ? "FAIL" : "ok  "} panes + tooltip`);
  for (const f of failures) console.log("     " + f);
}
{
  let failures;
  try {
    failures = await runRoomStripScenario();
  } catch (e) {
    failures = [e.message];
  }
  failed ||= failures.length > 0;
  console.log(`${failures.length ? "FAIL" : "ok  "} room strip`);
  for (const f of failures) console.log("     " + f);
}
{
  let failures;
  try {
    failures = await runFollowScenario();
  } catch (e) {
    failures = [e.message];
  }
  failed ||= failures.length > 0;
  console.log(`${failures.length ? "FAIL" : "ok  "} sticky scroll`);
  for (const f of failures) console.log("     " + f);
}

process.exit(failed ? 1 : 0);
