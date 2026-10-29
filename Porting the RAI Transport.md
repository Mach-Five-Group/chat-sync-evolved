# Porting the RAI Transport

Oct 29, 2026 · @Scott

## Brief

**Audience:** a coding agent working in a Lit codebase whose chat components were converted from `@machfivetechchicago/machvive-chat-syncopation-ai`. Everything you need is in this document. The source project's playground is not in git, so do not look for it.

**Task:** connect the Lit chat surface to the StreamSyncEngage `rai` LLM endpoint by adding one transport module, `rai.js` (full source below), and registering it with the chat engine.

**Done when:**

- A prompt typed in the Lit surface streams a real model reply into one assistant bubble.
- The second request of a conversation carries the earlier turns.
- Stop leaves the partial reply as a normal finished turn and cancels the network request.
- A failed request shows as an error turn with the reason, never a hang.
- The acceptance test at the end passes.

**Do not:**

- Add `rai.js`, the endpoint URL or the key to the chat component library. It belongs to the app, so the library stays usable without this server.
- Render reply text with `innerHTML` or Lit's `unsafeHTML`. Model output is untrusted; use normal Lit text bindings, which escape it.
- Drive the conversation by hand around the engine. You would lose the busy guard, Stop, the error turn and the idle event that turns Stop back into Send.

## First: which engine does your port use?

The original library splits into visible components (container, canvas, prompt and so on) and a headless engine: the `<machvive-chat-syncopation-services>` element plus its `Daemon`, `Conversation` and `PubSub` classes. Search your codebase to find out which engine you have, then follow that case through the rest of this doc.

| Case | How to recognise it | What you do |
| --- | --- | --- |
| **A: original engine** | Your Lit components import from `@machfivetechchicago/machvive-chat-syncopation-ai` (or `/services`) and a `machvive-chat-syncopation-services` element exists at runtime | Register `rai.js` on that element. The package does not pass `history` or `signal`, so use the `getHistory` option. Stop will show an error turn until the package bug is fixed |
| **B: ported engine** | You have your own daemon or controller class that calls transport functions, with no import of the package's services | Register `rai.js` on your engine, and make your engine pass `{ config, history, signal }` and handle Stop as described under *Engine behaviour to match* |

If both exist, Case B wins: the engine that calls the transport is the one that matters.

## The transport contract

A transport is an async generator function. The engine calls it once per user turn with the prompt and a context object, and appends every string it yields to the pending assistant reply.

```ts
// The contract this doc targets. The 1.0.0 package passes only { config }.
type Transport = (
  prompt: string,
  context: {
    config: SyncopationConfig;   // engine settings; read config.model if you need it
    history?: ChatRecord[];      // the conversation so far (see rules below)
    signal?: AbortSignal;        // aborted when the user presses Stop
  }
) => AsyncGenerator<string, void, unknown>;

interface ChatRecord {
  id: string;
  role: 'user' | 'assistant' | 'system' | 'tool';
  text: string;
  at: string;                          // ISO 8601
  status: 'complete' | 'pending' | 'error';
  meta: Record<string, unknown>;       // reserved keys are prefixed 'mcs:'
}
```

One turn, end to end:

&#91;embedded content: one turn, from Send to the finished reply\]

Solid arrows are requests, dashed arrows are the reply streaming back. Everything outside the highlighted box comes from the package.

Rules the transport and the engine both rely on:

- **Yield deltas, never the running total.** The engine appends each chunk. The `rai` endpoint sends the whole message so far in every event, so `rai.js` keeps the last length and yields only the new tail.
- **Throw to fail the turn.** The engine turns a thrown error into an `error` record whose `meta['mcs:error']` holds the message. Don't catch and swallow inside the transport.
- **Return to finish.** When the generator ends, the engine marks the reply `complete` and records `meta['mcs:latencyMs']`.
- **`history` contains the current turn.** The engine adds the user's record and an empty `pending` assistant record *before* calling the transport. So the transport must keep only `complete` user and assistant records, then drop the last one, which is the prompt it was handed. `rai.js` does exactly this.
- **Pass `signal` to every `fetch`.** Aborting a `fetch` ends the network request; without it, Stop only hides text while the server keeps generating.
- **Each call creates a new `rai` thread.** The endpoint is not relied on to remember anything. Context comes only from the `messages` array the transport sends.

## Engine behaviour to match (Case B)

If your Lit port has its own engine, its `send` must do what the original `Daemon#send` does, with one bug left out. Your composer and canvas almost certainly depend on these events, so keep the names and payloads.

| Moment | Engine does | Event on the bus |
| --- | --- | --- |
| `send` while a turn is running | Refuses and returns `null`; nothing is added | none |
| Turn starts | Adds the user record, then an empty `pending` assistant record with `meta['mcs:source']` and `meta['mcs:model']` | `record:added` twice, then `daemon:busy` `{ id }` |
| Each chunk | Appends it to the pending record | `record:appended` `{ id, chunk, record }` (from the conversation) |
| Generator returns | Status `complete`, sets `meta['mcs:latencyMs']` | none |
| Transport throws | Status `error`, `meta['mcs:error']` = the message | `daemon:error` `{ id, error, name }`, `error` as a plain string |
| User presses Stop | Aborts the signal; keeps the partial text as a `complete` turn | none |
| Always, last | Clears the busy state | `daemon:idle` `{ id }`, which turns Stop back into Send |
| Unknown transport name | Falls back to `echo` instead of failing | none |

**Do not copy the 1.0.0 Stop bug.** The package's `stop()` sets its abort controller to `null`, and the streaming loop then reads `this.#abort.signal` on the next chunk. The turn ends as an `error` with "Cannot read properties of null (reading 'signal')". This reproduces in 1.0.0, and the package's own Stop test misses it because it checks only the text. The fix is to hold the controller in a local variable and treat an abort as Stop rather than failure.

Reference implementation, tested with the adapter below:

```js
export async function send(engine, text) {
  if (engine.abort) return null;                          // busy guard
  engine.conversation.add({ role: 'user', text });
  const reply = engine.conversation.add({ role: 'assistant', text: '', status: 'pending',
    meta: { 'mcs:source': engine.config.transport, 'mcs:model': engine.config.model } });
  const transport = engine.transports[engine.config.transport] ?? engine.transports.echo;
  const controller = new AbortController();               // local: stop() can't null it mid-loop
  engine.abort = controller;
  const startedAt = Date.now();
  engine.bus.emit('daemon:busy', { id: reply.id });
  try {
    const history = engine.conversation.records;          // a copy, current turn included
    for await (const chunk of transport(text, { config: engine.config, history, signal: controller.signal })) {
      if (controller.signal.aborted) break;
      engine.conversation.append(reply.id, chunk);
    }
    engine.conversation.update(reply.id, { status: 'complete',
      meta: { ...reply.meta, 'mcs:latencyMs': Date.now() - startedAt } });
  } catch (err) {
    if (controller.signal.aborted) {                      // Stop, not a failure
      engine.conversation.update(reply.id, { status: 'complete' });
    } else {
      engine.conversation.update(reply.id, { status: 'error',
        meta: { ...reply.meta, 'mcs:error': String(err?.message ?? err) } });
      engine.bus.emit('daemon:error', { id: reply.id, error: String(err?.message ?? err), name: err?.name ?? 'Error' });
    }
  } finally {
    if (engine.abort === controller) engine.abort = null;
    engine.bus.emit('daemon:idle', { id: reply.id });
  }
  return reply;
}

export function stop(engine) { engine.abort?.abort(); engine.abort = null; }
```

Adapt the shape to your class (private fields, a Lit reactive controller); keep the order of operations. `conversation.records` must return a copy, so a transport cannot change the transcript.

## Endpoint wire format

The `rai` endpoint follows the LangGraph Platform HTTP shape: create a thread, then stream a run over server-sent events (SSE). Everything below was recorded from the live endpoint on 2026-10-29.

**Base URL:** `https://streamsyncengage-saas.onrender.com/m5t/v5/rai`\
**Headers on both requests:** `content-type: application/json` and `x-api-key: 0197` (a test key)

**1. Create a thread:** `POST {base}/threads` with body `{}`.

```json
{"thread_id":"c46c9df0-a642-4540-93ad-6a1e46d96631","created_at":"2026-10-29T18:41:43.618Z","status":"idle","metadata":{},"values":null,"interrupts":{}}
```

**2. Stream a run:** `POST {base}/threads/{thread_id}/runs/stream`. The endpoint treats the last message as the prompt and the earlier ones as context.

```json
{
  "assistant_id": "agent",
  "input": { "messages": [
    { "role": "system", "content": "optional system prompt" },
    { "role": "user", "content": "earlier question" },
    { "role": "assistant", "content": "earlier answer" },
    { "role": "user", "content": "the new prompt" }
  ]},
  "stream_mode": "messages"
}
```

The response is `text/event-stream`. This is the complete stream for the prompt "Reply with exactly: two words", with IDs shortened:

```text
event: metadata
data: {"run_id":"d09ab11d-…","thread_id":"c46c9df0-…","attempt":1}

event: messages/metadata
data: {"5333e1b7-…":{"metadata":{"thread_id":"c46c9df0-…","run_id":"d09ab11d-…"}}}

event: messages/partial
data: [{"type":"ai","id":"5333e1b7-…","content":"two","additional_kwargs":{},"response_metadata":{},"tool_calls":[],"invalid_tool_calls":[]}]

event: messages/partial
data: [{"type":"ai","id":"5333e1b7-…","content":"two words", …}]

event: messages/complete
data: [{"type":"ai","id":"5333e1b7-…","content":"two words", …}]
```

| Event | Payload | What the adapter does |
| --- | --- | --- |
| `metadata` | Run and thread IDs | Ignores it |
| `messages/metadata` | Per-message metadata | Ignores it |
| `messages/partial` | One-element array; `content` is the **whole text so far** | Yields the part after the previous length |
| `messages/complete` | Same shape, final text | Ignores it; the stream ending finishes the turn |
| `error` | `{ message }` | Throws, which makes an error turn |

Frames are separated by a blank line (`\n\n`), and a frame can arrive split across network reads, so buffer until the separator.

**Auth and access, as measured:**

- A wrong `x-api-key` returns HTTP **401**. The adapter throws `rai threads: 401`.
- CORS preflight answers `access-control-allow-origin: *` and allows `content-type, x-api-key`, so a browser can call it from **any** origin. Your app's domain will work, and so will anyone's who reads the key.

## rai.js: the full adapter

Create this file as-is in the app, for example `src/chat/rai.js`. It has no imports, uses only `fetch`, `TextDecoder` and `ReadableStream`, and works unchanged in both cases. Compared with the playground original, history is read correctly and `getHistory` is added for Case A. This exact text passed the acceptance tests below.

```js
/**
 * rai transport — binds the chat surface to the StreamSyncEngage `rai` endpoint.
 *
 * This is the app's adapter, not part of the chat component library: the library
 * owns everything up to the transport seam, and this file is the binding.
 *
 * The endpoint mimics the LangGraph Platform HTTP surface: create a thread, then
 * stream a run as SSE. `messages/partial` frames carry the *accumulated* message,
 * so this adapter yields the delta since the last frame (the daemon appends).
 *
 * Auth is a static test gate key sent as `x-api-key`. It is a mock harness — a
 * real binding proxies through your own server and never puts a key in page JS.
 */

const RAI_BASE = 'https://streamsyncengage-saas.onrender.com/m5t/v5/rai';
const RAI_KEY = '0197';

/** Reads an SSE body, yielding { event, data } per frame. */
async function* sseFrames(body, signal) {
  const reader = body.getReader();
  const decoder = new TextDecoder();
  let buf = '';
  for (;;) {
    if (signal?.aborted) return;
    const { value, done } = await reader.read();
    if (done) return;
    buf += decoder.decode(value, { stream: true });
    let idx;
    while ((idx = buf.indexOf('\n\n')) !== -1) {
      const frame = buf.slice(0, idx);
      buf = buf.slice(idx + 2);
      const event = frame.match(/^event: (.*)$/m)?.[1];
      const dataLine = frame.match(/^data: (.*)$/m)?.[1];
      if (!dataLine) continue;
      let data;
      try { data = JSON.parse(dataLine); } catch { continue; }
      yield { event, data };
    }
  }
}

/**
 * Builds a transport for the rai endpoint. The returned function is what the
 * daemon calls at the seam: `(prompt, { config, history, signal })`.
 *
 * The adapter shapes history into LangChain messages (system + prior turns +
 * the new prompt); the endpoint uses the last message as the prompt, so the
 * history is what gives the model context.
 *
 * history comes from the engine's call when it passes one (a ported engine
 * should). Otherwise pass getHistory, e.g. () => services.conversation.records,
 * for an engine that calls with { config } only (the 1.0.0 package).
 */
export function makeRaiTransport({ base = RAI_BASE, key = RAI_KEY, systemPrompt, getHistory } = {}) {
  return async function* rai(prompt, { config, history = getHistory?.(), signal } = {}) {
    // 1. Create (or reuse) a thread.
    const threadRes = await fetch(`${base}/threads`, {
      method: 'POST', signal,
      headers: { 'content-type': 'application/json', 'x-api-key': key },
      body: JSON.stringify({})
    });
    if (!threadRes.ok) throw new Error(`rai threads: ${threadRes.status}`);
    const thread = await threadRes.json();

    // 2. Shape history into messages. The endpoint uses the LAST message as the
    //    prompt; prior turns are context.
    const messages = [];
    if (systemPrompt) messages.push({ role: 'system', content: systemPrompt });
    // The records end with this turn: the prompt, then the empty pending reply.
    // Keep finished turns only, and drop the prompt itself; it is added below.
    const past = (history ?? [])
      .filter((r) => r.status === 'complete' && (r.role === 'user' || r.role === 'assistant'))
      .slice(0, -1);
    for (const r of past) messages.push({ role: r.role, content: r.text });
    messages.push({ role: 'user', content: prompt });

    // 3. Stream the run as SSE.
    const res = await fetch(`${base}/threads/${thread.thread_id}/runs/stream`, {
      method: 'POST', signal,
      headers: { 'content-type': 'application/json', 'x-api-key': key },
      body: JSON.stringify({
        assistant_id: 'agent',
        input: { messages },
        stream_mode: 'messages'
      })
    });
    if (!res.ok) {
      const body = await res.text().catch(() => '');
      throw new Error(`rai stream: ${res.status} ${body}`.trim());
    }

    // partial frames carry accumulated text — yield only the delta.
    let accumulated = '';
    for await (const { event, data } of sseFrames(res.body, signal)) {
      if (event === 'messages/partial') {
        const msg = Array.isArray(data) ? data[0] : data;
        const full = msg?.content ?? '';
        if (full.length > accumulated.length) {
          yield full.slice(accumulated.length);
          accumulated = full;
        }
      } else if (event === 'error') {
        throw new Error(data?.message ?? 'stream error');
      }
      // metadata / messages/metadata / messages/complete / values need no yield —
      // the daemon marks the turn complete when the generator returns.
    }
  };
}
```

Options for `makeRaiTransport`: `base` (endpoint URL; point it at your proxy in production), `key` (sent as `x-api-key`; omit once a proxy adds it), `systemPrompt` (prepended as a `system` message), `getHistory` (Case A only).

## Wiring it into Lit

**Case A: register on the original services element.** The element creates its engine in `connectedCallback`. Until then `services.daemon` is `null`, and `registerTransport` drops the transport *without an error*, leaving the default `echo` selected. In Lit, `firstUpdated()` runs after the first render has attached children, so register there. `registerTransport` also selects the transport.

```js
import { LitElement, html } from 'lit';
import '@machfivetechchicago/machvive-chat-syncopation-ai/services'; // defines the element
import { makeRaiTransport } from './chat/rai.js';

export class AppChat extends LitElement {
  render() {
    return html`
      <machvive-chat-syncopation-services>
        <!-- your Lit canvas and prompt components -->
      </machvive-chat-syncopation-services>`;
  }

  firstUpdated() {
    const services = this.renderRoot.querySelector('machvive-chat-syncopation-services');
    if (!services?.daemon) throw new Error('services element not connected; transport would be dropped');
    services.registerTransport('rai', makeRaiTransport({
      systemPrompt: 'You are a helpful support assistant.',
      getHistory: () => services.conversation.records
    }));
  }
}
customElements.define('app-chat', AppChat);
```

If the services element is created elsewhere (by a container, or higher in the page), find it with `whenServices(node)` from the `/services` subpath. It searches the node's ancestors and their children, then resolves to the element or `null` after 2 seconds. Check `daemon` before registering, as above.

**Case B: register on your own engine.** Add the transport wherever your engine is constructed, select it, and make `send` follow the reference implementation so the adapter receives `history` and `signal`.

```js
import { makeRaiTransport } from './chat/rai.js';

engine.transports.rai = makeRaiTransport({ systemPrompt: 'You are a helpful support assistant.' });
engine.config.transport = 'rai';
// no getHistory: your send() passes history and signal itself
```

**Rendering the stream in Lit** (both cases):

- On `record:appended`, update only the bubble whose `id` matches, for example a `repeat()` keyed on `record.id`. Re-rendering the whole list steals focus and makes screen readers re-announce old turns.
- Bind reply text as a normal Lit expression, `${record.text}`. Never `unsafeHTML`.
- Show Stop instead of Send between `daemon:busy` and `daemon:idle`, and keep it clickable. Call the engine's `stop()`.
- Show `status === 'error'` records visibly, using `meta['mcs:error']` as the reason.
- Unsubscribe every bus listener in `disconnectedCallback`. `bus.on()` returns the unsubscribe function.

## Before it goes public

The copied file is a demo binding. Settle these four things before real users reach it.

| Issue | Why it matters | What to do |
| --- | --- | --- |
| Key `0197` is in page JavaScript | Anyone can read it in DevTools and call the endpoint as you | Proxy through your own server, which adds the key; pass that server's URL as `base` and drop `key` |
| Endpoint URL is hardcoded | `RAI_BASE` points at the shared `onrender.com` test instance | Pass `base` from your app's config so each environment chooses its own |
| Browser access rules (CORS) | The endpoint allows every origin (\*), so your domain works, and the key works from any site | Nothing to unblock; the proxy is what limits who can spend the key |
| Cold starts | Hosted on Render; an idle instance can take up to a minute to wake | Expect a slow first reply; the pending caret covers it, but don't set a short timeout |

The proxy is the main one. Once it exists, the key and CORS rows are both settled.

## Acceptance

**Automated, with a fake endpoint.** This test drives `rai.js` through an engine using a fake `fetch` that speaks the recorded wire format. It passes against the reference `send`/`stop` above with the original `Conversation` and `PubSub`. It was mutation-checked: removing history or signal, keeping the prompt twice, yielding running totals, treating Stop as an error, dropping the busy guard, or dropping `signal` from `fetch` each makes a test fail.

Adapt only the `engine()` factory and the `send`/`stop` import to your engine. In Case A, use the services element and its `send()` and `daemon.stop()` instead; until the package Stop bug is fixed, the two Stop tests are expected to fail there. The `/services` import defines a custom element, so the test runner needs a DOM (jsdom, happy-dom or a browser runner).

```js
import nodeTest from 'node:test';
const test = (name, fn) => nodeTest(name, { timeout: 2000 }, fn);
import assert from 'node:assert/strict';
import { Conversation, PubSub } from '@machfivetechchicago/machvive-chat-syncopation-ai/services';
import { makeRaiTransport } from '../src/chat/rai.js';
import { send, stop } from '../src/chat/engine.js'; // your engine's send/stop

const enc = new TextEncoder();
const frame = (t) => enc.encode('event: messages/partial\ndata: ' + JSON.stringify([{ type: 'ai', content: t }]) + '\n\n');
function fakeFetch({ slow = false } = {}) {
  const calls = [];
  globalThis.fetch = async (url, init) => {
    calls.push({ url, body: JSON.parse(init.body), signal: init.signal });
    if (url.endsWith('/threads')) return { ok: true, json: async () => ({ thread_id: 't' }) };
    const n = calls.length;
    return { ok: true, body: new ReadableStream({ async start(c) {
      c.enqueue(frame('r' + n));
      if (slow) { await new Promise((res) => init.signal.addEventListener('abort', res)); c.error(new DOMException('aborted', 'AbortError')); return; }
      c.enqueue(frame('r' + n + ' done')); c.close(); } }) };
  };
  return calls;
}
const engine = () => {                                   // adapt: build your engine here
  const bus = new PubSub();
  const events = []; bus.on('*', ({ topic }) => events.push(topic));
  return { bus, events, abort: null, conversation: new Conversation({ bus }), config: { transport: 'rai' },
    transports: { rai: makeRaiTransport() } };
};

test('history reaches the endpoint, deltas join correctly', async () => {
  const calls = fakeFetch(); const e = engine();
  await send(e, 'first'); await send(e, 'second');
  const streams = calls.filter((c) => c.url.endsWith('/runs/stream'));
  assert.deepEqual(streams[1].body.input.messages, [
    { role: 'user', content: 'first' }, { role: 'assistant', content: 'r2 done' }, { role: 'user', content: 'second' }]);
  assert.equal(e.conversation.records.at(-1).text, 'r4 done');
  assert.ok(e.events.includes('daemon:busy') && e.events.includes('daemon:idle'));
});

test('stop keeps the partial reply as complete and aborts the fetch', async () => {
  const calls = fakeFetch({ slow: true }); const e = engine();
  const p = send(e, 'hi');
  await new Promise((r) => setTimeout(r, 20));
  stop(e);
  await p;
  const last = e.conversation.records.at(-1);
  assert.equal(last.status, 'complete');
  assert.equal(last.text, 'r2');
  assert.ok(calls.at(-1).signal.aborted, 'fetch signal aborted');
  assert.equal(e.abort, null);
});

test('a 401 becomes an error turn with the reason', async () => {
  globalThis.fetch = async () => ({ ok: false, status: 401, text: async () => '' });
  const e = engine();
  await send(e, 'hi');
  const last = e.conversation.records.at(-1);
  assert.equal(last.status, 'error');
  assert.match(last.meta['mcs:error'], /rai threads: 401/);
});

test('concurrent send is refused', async () => {
  fakeFetch({ slow: true }); const e = engine();
  const p = send(e, 'a');
  assert.equal(await send(e, 'b'), null);
  stop(e); await p;
});

test('stop is a clean finish even if the transport ignores the signal', async () => {
  const e = engine();
  e.transports.stubborn = async function* () { for (const w of ['a ', 'b ', 'c ']) { await new Promise((r) => setTimeout(r, 15)); yield w; } };
  e.config.transport = 'stubborn';
  const p = send(e, 'hi');
  await new Promise((r) => setTimeout(r, 25));
  stop(e);
  await p;
  const last = e.conversation.records.at(-1);
  assert.equal(last.status, 'complete');
  assert.equal(last.text, 'a ');
});
```

**Manual, against the live endpoint.** Run the app with DevTools open on the Network tab, filtered to `rai`.

- [ ] Send a message: `POST /threads`, then `POST /threads/<id>/runs/stream`, whose EventStream tab shows `messages/partial` frames. One assistant bubble grows.
- [ ] Ask a question, then "what did I just ask?". The second `runs/stream` request body lists the first exchange, and the model answers correctly.
- [ ] Press Stop mid-reply: the text stays as a normal turn, Send returns, and the `runs/stream` request shows as cancelled.
- [ ] Pass `key: 'wrong'`: the reply becomes an error turn reading `rai threads: 401`.
- [ ] Expect the first reply after idle time to take up to a minute (Render cold start); later replies are fast.

| Symptom | Cause |
| --- | --- |
| Replies echo the prompt ("You said: …") | The transport was registered before the engine existed, or under a name `config.transport` doesn't select; the engine fell back to `echo` |
| The model forgets the previous question | `history` is not reaching the adapter: Case A without `getHistory`, or a Case B `send` that doesn't pass it |
| The prompt appears twice in the request body | History was passed without dropping the current turn; use the adapter's filter unchanged |
| The reply text repeats itself ("twotwo words") | The adapter is yielding the running total instead of the delta |
| Stop turns the reply red with a `null` error | Case A on the 1.0.0 package, or a Case B port of its `stop()` bug |
