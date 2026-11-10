#!/usr/bin/env node
// Headless-Chrome auth regressions for the operator UI login flow
// (crates/wrkspc-ui/src/assets/index.html). Not part of `just quality`:
// it needs a real browser. Run it after changing the page's login,
// boot, or api() code.
//
// Correctness regressions (not defect pins): the accepted login must
// survive older 401s, remembered-login boot must authenticate without
// manual interaction, and non-auth startup failures must never blame or
// discard an accepted credential.
//   S0 fresh browser, no stored credential: the login card shows, the
//      boot gate is gone, the page throws nothing, and no API request
//      fires behind the login card (the poll stays silent when logged
//      out).
//   S1 valid manual login (control): verification 200 -> authenticated
//      view, transcript renders, the credential is remembered, no
//      rejection text, no page exceptions.
//   S2 stale 401 vs fresh login: a boot request carrying an old stored
//      credential is held, the operator logs in with the valid one, and
//      the held 401 then arrives — the accepted login must survive.
//   S2b same race, opposite order: the old 401 lands while the login
//      verification is still in flight — the login must still complete.
//   S2c same-token re-login: a poll request carrying the CURRENT token
//      is held, connectWith re-runs with the same value, and the held
//      401 must not clear the newer generation.
//   S3 remembered-login boot: a fresh document with a valid stored
//      credential must authenticate, select the room and render history
//      with no manual interaction (and no swallowed ReferenceError).
//   S3b remembered-login boot with an empty room list: authenticated
//      empty state, login card stays hidden.
//   S3c stale stored credential: honest rejection, the stored
//      credential is cleared, the operator is told to log in again.
//   S4 startup failure after acceptance: verification succeeds, a later
//      startup call fails with 500 — the credential survives, the error
//      says what failed (never "rejected"), and a retry succeeds.
//   S4b invalid credential (control): a real 401 rejects and clears.
//
// Prerequisites: Node >= 22 (global WebSocket and fetch), python3, and
// Google Chrome. Set CHROME to the browser binary when it isn't at the
// macOS default path.
//
// Usage: node scripts/ui-auth-check/check.mjs [path/to/index.html]
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
const VALID = "wrkspc-auth-valid-token";
const INVALID = "wrkspc-auth-invalid-token";
const ROTATED = "wrkspc-auth-rotated";
const STARTUP_TIMEOUT_MS = 15000;
// How long the delayed-401 scenarios watch for a false rejection.
const SETTLE_MS = 800;

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

function startMock(empty = false) {
  const args = ["-I", join(HERE, "mock_auth.py"), PAGE];
  if (empty) args.push("--empty");
  const proc = spawn("python3", args, { stdio: ["ignore", "pipe", "inherit"] });
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

async function stop(proc) {
  if (!proc || (proc.exitCode !== null && proc.exitCode !== undefined) || proc.signalCode !== null) return;
  const exited = new Promise((done) => proc.once("exit", done));
  proc.kill();
  await Promise.race([exited, sleep(5000)]);
}

async function startChrome() {
  const profile = mkdtempSync(join(tmpdir(), "wrkspc-auth-check-"));
  const proc = spawn(CHROME, [
    "--headless=new", "--no-first-run", "--no-default-browser-check",
    "--remote-debugging-port=0", "--user-data-dir=" + profile, "about:blank",
  ], { stdio: "ignore" });
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
  await send("Page.enable");
  return {
    evaluate: async (expression) => {
      const msg = await send("Runtime.evaluate", { expression, returnByValue: true, awaitPromise: true });
      if (msg.result?.exceptionDetails) throw new Error("evaluate failed: " + expression + " -> " +
        (msg.result.exceptionDetails.exception?.description ?? msg.result.exceptionDetails.text));
      return msg.result?.result?.value;
    },
    navigate: async (url) => { await send("Page.navigate", { url }); },
    reload: async () => { await send("Page.reload", { ignoreCache: true }); },
    errors,
    close: () => ws.close(),
  };
}

/** The page's auth-facing state, read from the real DOM and storage. */
const readState = (page) => page.evaluate(`(() => {
  const boot = document.getElementById("boot");
  const error = document.getElementById("login-error");
  return {
    bootHidden: boot.hidden,
    connecting: boot.classList.contains("connecting"),
    gate: !!document.getElementById("boot-gate"),
    error: error.textContent,
    stored: localStorage.getItem("wrkspc_token"),
    input: document.getElementById("login-token").value,
    msgs: document.querySelectorAll("#transcript .msg").length,
    roomTitle: (document.querySelector("#convo-head .room-line") || {}).textContent || null,
  };
})()`);

const control = (base, path, body) => fetch(base + path, {
  method: "POST",
  headers: { "Content-Type": "application/json" },
  body: JSON.stringify(body ?? {}),
});
const stats = async (base) => (await fetch(base + "/__state")).json();

/** True once the mock has actually SERVED a delayed response matching. */
const served = async (base, predicate) =>
  (await stats(base)).requests.some((r) => r.served && predicate(r));

/** Types a credential into the real login card and clicks connect. */
async function submitLogin(page, value) {
  await page.evaluate(`(async () => {
    document.getElementById("login-token").value = ${JSON.stringify(value)};
    document.getElementById("login-submit").click();
    return true;
  })()`);
}

/** Rotates the server's accepted credential (server-side rotation). */
async function rotateToken(base, value) {
  await control(base, "/__token", { token: value });
}

async function runScenario(name, fn) {
  const mock = await startMock(name === "S3b");
  let chrome;
  let page;
  const failures = [];
  try {
    chrome = await startChrome();
    page = await connect(chrome.port);
    await fn({ page, base: mock.base, failures });
    if (page.errors.length) {
      failures.push(`page threw ${page.errors.length} exception(s): ${page.errors.join(" | ")}`);
    }
  } catch (e) {
    failures.push(String(e.message ?? e));
  } finally {
    if (page) page.close();
    await stop(chrome?.proc);
    if (chrome?.profile) rmSync(chrome.profile, { recursive: true, force: true });
    await stop(mock.proc);
  }
  const ok = failures.length === 0;
  console.log(`${ok ? "ok " : "FAIL"} ${name}`);
  for (const failure of failures) console.log("     " + failure);
  return ok;
}

const assert = (failures, condition, message) => {
  if (!condition) failures.push(message);
};

const scenarios = [
  ["S0 fresh browser, login card, silent poll", async ({ page, base, failures }) => {
    await page.navigate(base + "/");
    await waitFor(
      () => page.evaluate(`!document.getElementById("boot").hidden && !document.getElementById("boot-gate")`),
      STARTUP_TIMEOUT_MS,
      "the login card with the gate dropped",
    );
    // The fresh boot fires ONE cookie probe burst (each view 401s and
    // the login card appears). After that settles, two poll ticks must
    // fire NOTHING behind the login card.
    const settle = async () => (await stats(base)).requests.filter((r) => r.path.startsWith("/api/")).length;
    await waitFor(async () => (await settle()) >= 5, STARTUP_TIMEOUT_MS, "the boot probe to fire");
    await sleep(2200);
    const { requests } = await stats(base);
    const apiCalls = requests.filter((r) => r.path.startsWith("/api/"));
    assert(failures, apiCalls.length <= 5,
      `unauthenticated poll fired ${apiCalls.length} API request(s) behind the login card (boot probe + poll)`);
    const state = await readState(page);
    assert(failures, state.stored === null, "no credential may be stored");
    assert(failures, state.error === "log in to continue",
      `unexpected login-card message: ${JSON.stringify(state.error)}`);
  }],

  ["S1 valid manual login (control)", async ({ page, base, failures }) => {
    await page.navigate(base + "/");
    await waitFor(
      () => page.evaluate(`!document.getElementById("boot").hidden && !document.getElementById("boot").classList.contains("connecting")`),
      STARTUP_TIMEOUT_MS,
      "the login card",
    );
    await submitLogin(page, VALID);
    await waitFor(
      () => page.evaluate("document.getElementById('boot').hidden"),
      STARTUP_TIMEOUT_MS,
      "the authenticated view",
    );
    const state = await readState(page);
    assert(failures, state.stored === null,
      `the credential must never be persisted client-side, got ${JSON.stringify(state.stored)}`);
    assert(failures, state.error === "", `unexpected rejection text: ${JSON.stringify(state.error)}`);
    assert(failures, state.msgs === 1, `transcript rendered ${state.msgs} message(s), want 1`);
    assert(failures, state.roomTitle?.includes("Auth room"), "the room with history was not selected");
  }],

  ["S2 stale 401 cannot clear a newer valid login", async ({ page, base, failures }) => {
    // Establish a remembered session, then rotate the credential on
    // the server: the next boot fires with the STALE cookie.
    await page.navigate(base + "/");
    await waitFor(
      () => page.evaluate(`!document.getElementById("boot").hidden && !document.getElementById("boot").classList.contains("connecting")`),
      STARTUP_TIMEOUT_MS,
      "the login card",
    );
    await submitLogin(page, VALID);
    await waitFor(
      () => page.evaluate("document.getElementById('boot').hidden"),
      STARTUP_TIMEOUT_MS,
      "the initial login",
    );
    await rotateToken(base, ROTATED);
    // The next boot's conversations request (carrying the stale cookie)
    // is answered with a DELAYED 401 so it lands after the fresh login.
    await control(base, "/__delay", { path_prefix: "/api/conversations", auth: false, status: 401, delay_ms: 1500 });
    await page.reload();
    await waitFor(async () => (await stats(base)).requests.some((r) => r.delayed), STARTUP_TIMEOUT_MS,
      "the stale boot request to be delayed");
    await submitLogin(page, ROTATED);
    try {
      await waitFor(
        () => page.evaluate("document.getElementById('boot').hidden"),
        STARTUP_TIMEOUT_MS,
        "the login to complete before the stale 401 lands",
      );
    } catch (e) {
      const { requests } = await stats(base);
      throw new Error(e.message + " :: requests=" + JSON.stringify(requests.map((r) => [r.seq, r.path, r.cookie, r.status, r.delayed, r.served])));
    }
    // The stale 401 arrives now. It must not touch the newer login.
    await waitFor(
      async () => served(base, (r) => r.status === 401 && r.cookie === VALID),
      STARTUP_TIMEOUT_MS,
      "the stale 401 to be served",
    );
    await sleep(SETTLE_MS);
    const state = await readState(page);
    assert(failures, state.bootHidden, "the accepted login was cleared back to the login card");
    assert(failures, !state.error.includes("rejected"), `false rejection shown: ${JSON.stringify(state.error)}`);
    assert(failures, state.msgs === 1, "the transcript disappeared after the stale 401");
  }],

  ["S2b stale 401 arriving mid-login", async ({ page, base, failures }) => {
    // Establish a remembered session, rotate the credential, then
    // reload: rule 1 delays the boot's stale request (401 at ~1.2 s);
    // rule 2 delays the login's own POST (204 at ~2.4 s), so the old
    // 401 lands WHILE the login is still in flight.
    await page.navigate(base + "/");
    await waitFor(
      () => page.evaluate(`!document.getElementById("boot").hidden && !document.getElementById("boot").classList.contains("connecting")`),
      STARTUP_TIMEOUT_MS,
      "the login card",
    );
    await submitLogin(page, VALID);
    await waitFor(
      () => page.evaluate("document.getElementById('boot').hidden"),
      STARTUP_TIMEOUT_MS,
      "the initial login",
    );
    await rotateToken(base, ROTATED);
    await control(base, "/__delay", { path_prefix: "/api/conversations", auth: false, status: 401, delay_ms: 1200 });
    await control(base, "/__delay", { path_prefix: "/api/login", auth: false, status: 204, delay_ms: 2400 });
    await page.reload();
    await waitFor(async () => (await stats(base)).requests.filter((r) => r.delayed).length === 1,
      STARTUP_TIMEOUT_MS, "the stale boot request to be delayed");
    await submitLogin(page, ROTATED);
    // The stale 401 lands while the verification is still pending.
    await waitFor(
      async () => served(base, (r) => r.status === 401 && r.cookie === VALID),
      STARTUP_TIMEOUT_MS,
      "the stale 401 to land mid-login",
    );
    try {
      await waitFor(
        () => page.evaluate("document.getElementById('boot').hidden"),
        STARTUP_TIMEOUT_MS,
        "the login to complete after the stale 401",
      );
    } catch (e) {
      const { requests } = await stats(base);
      throw new Error(e.message + " :: state=" + JSON.stringify(await readState(page)) +
        " :: requests=" + JSON.stringify(requests.map((r) => [r.seq, r.path, r.cookie, r.status, r.delayed, r.served]).slice(-14)));
    }
    await sleep(SETTLE_MS);
    const state = await readState(page);
    assert(failures, state.bootHidden, "the login did not reach the authenticated view");
    assert(failures, state.msgs === 1, "the authenticated transcript did not render");
  }],

  ["S2c same-token re-login survives an older 401", async ({ page, base, failures }) => {
    await page.navigate(base + "/");
    await waitFor(
      () => page.evaluate(`!document.getElementById("boot").hidden && !document.getElementById("boot").classList.contains("connecting")`),
      STARTUP_TIMEOUT_MS,
      "the login card",
    );
    await submitLogin(page, VALID);
    await waitFor(
      () => page.evaluate("document.getElementById('boot').hidden"),
      STARTUP_TIMEOUT_MS,
      "the first login",
    );
    // Delay the poll's authenticated peers request (sent under the
    // first generation), then re-login with the SAME value: the old
    // 401 must not clear the newer generation.
    await control(base, "/__delay", { path_prefix: "/api/peers", auth: false, status: 401, delay_ms: 1500 });
    await waitFor(async () => (await stats(base)).requests.some((r) => r.delayed), STARTUP_TIMEOUT_MS,
      "the poll's peers request to be delayed");
    await page.evaluate(`(async () => { await connectWith(${JSON.stringify(VALID)}); return true; })()`);
    await waitFor(
      () => page.evaluate("document.getElementById('boot').hidden"),
      STARTUP_TIMEOUT_MS,
      "the re-login to complete",
    );
    await waitFor(
      async () => served(base, (r) => r.status === 401),
      STARTUP_TIMEOUT_MS,
      "the older 401 to be served",
    );
    await sleep(SETTLE_MS);
    const state = await readState(page);
    if (!state.bootHidden) {
      const { requests } = await stats(base);
      failures.push("S2c state=" + JSON.stringify(state) + " requests=" +
        JSON.stringify(requests.map((r) => [r.seq, r.path, r.cookie, r.status, r.delayed, r.served]).slice(-12)));
    }
    assert(failures, state.bootHidden, "the older 401 cleared the same-value re-login");
  }],

  ["S3 remembered-login boot authenticates without interaction", async ({ page, base, failures }) => {
    // Log in once (the cookie is set), then reload: the remembered
    // session must authenticate with no manual interaction.
    await page.navigate(base + "/");
    await waitFor(
      () => page.evaluate(`!document.getElementById("boot").hidden && !document.getElementById("boot").classList.contains("connecting")`),
      STARTUP_TIMEOUT_MS,
      "the login card",
    );
    await submitLogin(page, VALID);
    await waitFor(
      () => page.evaluate("document.getElementById('boot').hidden"),
      STARTUP_TIMEOUT_MS,
      "the initial login",
    );
    await page.reload();
    await waitFor(
      () => page.evaluate("document.getElementById('boot').hidden"),
      STARTUP_TIMEOUT_MS,
      "remembered-login boot to authenticate (no manual login)",
    );
    const state = await readState(page);
    assert(failures, state.error === "", `boot showed an error: ${JSON.stringify(state.error)}`);
    assert(failures, state.msgs === 1, `history did not render: ${state.msgs} message(s)`);
    assert(failures, state.roomTitle?.includes("Auth room"), "the room with history was not auto-selected");
  }],

  ["S3b remembered-login boot with an empty room list", async ({ page, base, failures }) => {
    await page.navigate(base + "/");
    await waitFor(
      () => page.evaluate(`!document.getElementById("boot").hidden && !document.getElementById("boot").classList.contains("connecting")`),
      STARTUP_TIMEOUT_MS,
      "the login card",
    );
    await submitLogin(page, VALID);
    await waitFor(
      () => page.evaluate("document.getElementById('boot').hidden"),
      STARTUP_TIMEOUT_MS,
      "the initial login",
    );
    await page.reload();
    await waitFor(
      () => page.evaluate("document.getElementById('boot').hidden"),
      STARTUP_TIMEOUT_MS,
      "authenticated empty-state boot",
    );
    const state = await readState(page);
    assert(failures, state.error === "", `empty boot showed an error: ${JSON.stringify(state.error)}`);
  }],

  ["S3c a rotated credential lands the next boot on the login card", async ({ page, base, failures }) => {
    // Remembered session, then server-side rotation: the next boot's
    // stale cookie is rejected and the login card appears with the
    // honest message.
    await page.navigate(base + "/");
    await waitFor(
      () => page.evaluate(`!document.getElementById("boot").hidden && !document.getElementById("boot").classList.contains("connecting")`),
      STARTUP_TIMEOUT_MS,
      "the login card",
    );
    await submitLogin(page, VALID);
    await waitFor(
      () => page.evaluate("document.getElementById('boot').hidden"),
      STARTUP_TIMEOUT_MS,
      "the initial login",
    );
    await rotateToken(base, ROTATED);
    await page.reload();
    await waitFor(
      () => page.evaluate(`!document.getElementById("boot").hidden &&
        document.getElementById("login-error").textContent.includes("log in")`),
      STARTUP_TIMEOUT_MS,
      "the rotated credential to land on the login card",
    );
    const state = await readState(page);
    assert(failures, state.bootHidden === false, "the login card must be visible");
  }],

  ["S4 startup failure after acceptance keeps the credential", async ({ page, base, failures }) => {
    await page.navigate(base + "/");
    await waitFor(
      () => page.evaluate(`!document.getElementById("boot").hidden && !document.getElementById("boot").classList.contains("connecting")`),
      STARTUP_TIMEOUT_MS,
      "the login card",
    );
    // Fail startApp's final conversations read (the first one is
    // loadConversations; the verification no longer touches it).
    await control(base, "/__fail", { path_prefix: "/api/conversations", auth: false, status: 500, skip: 1, times: 1 });
    await submitLogin(page, VALID);
    try {
      await waitFor(
        () => page.evaluate(`!document.getElementById('boot').hidden &&
          document.getElementById('login-error').textContent.includes("could not start")`),
        STARTUP_TIMEOUT_MS,
        "the honest startup-failure message",
      );
    } catch (e) {
      const { requests } = await stats(base);
      throw new Error(e.message + " :: requests=" + JSON.stringify(requests.map((r) => [r.seq, r.path, r.auth, r.status, r.delayed, r.served]))
        + " :: state=" + JSON.stringify(await readState(page)));
    }
    const state = await readState(page);
    assert(failures, state.stored === null,
      `the credential must never be persisted client-side, got ${JSON.stringify(state.stored)}`);
    assert(failures, !state.error.includes("rejected"),
      `the startup failure blamed the credential: ${JSON.stringify(state.error)}`);
    // A retry with the same accepted credential must succeed once the
    // failure is gone.
    await control(base, "/__reset", {});
    await submitLogin(page, VALID);
    await waitFor(
      () => page.evaluate("document.getElementById('boot').hidden"),
      STARTUP_TIMEOUT_MS,
      "the retry to reach the authenticated view",
    );
    const after = await readState(page);
    assert(failures, after.msgs === 1, "the retry did not render the transcript");
  }],

  ["S4b invalid credential is genuinely rejected (control)", async ({ page, base, failures }) => {
    await page.navigate(base + "/");
    await waitFor(
      () => page.evaluate(`!document.getElementById("boot").hidden && !document.getElementById("boot").classList.contains("connecting")`),
      STARTUP_TIMEOUT_MS,
      "the login card",
    );
    await submitLogin(page, INVALID);
    await waitFor(
      () => page.evaluate(`document.getElementById("login-error").textContent === "rejected — check the token"`),
      STARTUP_TIMEOUT_MS,
      "the rejection message",
    );
    const state = await readState(page);
    assert(failures, state.stored === null, "a rejected credential must never be remembered");
    assert(failures, state.bootHidden === false, "the login card must stay visible after a rejection");
    assert(failures, state.msgs === 0, "no transcript may render after a rejection");
  }],
];

let failed = 0;
for (const [name, fn] of scenarios) {
  if (!(await runScenario(name, fn))) failed += 1;
}
console.log(failed === 0 ? "all auth scenarios passed" : `${failed} auth scenario(s) failed`);
process.exit(failed === 0 ? 0 : 1);
