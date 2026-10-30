# Restoring the Conversation API in Lit

Oct 30, 2026 · @Scott

## Brief

**Audience:** the coding agent that converted the Chat Syncopation components (`@machfivetechchicago/machvive-chat-syncopation-ai`) to Lit. Everything needed is in this document; the original's source is quoted where it matters.

**The gap:** the conversion dropped the conversation layer. In the original, every turn passes through one `Conversation` per chat surface, and every change to it is announced on a per-surface bus. That layer is what sends a turn to the model API, to the canvas, or to both. Without it, a Lit prompt has to call the model and the canvas separately, and nothing keeps them in agreement.

**Task:** rebuild the three pieces as plain TypeScript (`PubSub`, `Conversation`, and the turn runner `Daemon`), then connect your Lit components to them through one services host and a reactive controller.

**Done when:**

- Pressing Send shows the user turn, calls the model, and streams one reply bubble, all through the conversation.
- Code can add a turn to the canvas without calling the model, and clear the conversation.
- Two chat surfaces on one page keep separate conversations.
- The canvas updates one bubble per change, never re-rendering the whole list.
- The acceptance tests at the end pass against your port.

**Do not:**

- Let components call the model or edit the transcript directly. All writes go through `Conversation`; all model calls go through the `Daemon`.
- Make the bus a `LitElement` or an `EventTarget`, or tie events to Lit render cycles. The bus is synchronous, plain JavaScript.
- Use a module-level singleton for the conversation. Two surfaces on one page must not share one.
- Render record text with `unsafeHTML`. Model output is untrusted.

## Routing: API, canvas or both

The original has no routing flag. Where a turn goes depends on which method is called, and every route ends in the same `Conversation`, so the canvas only ever shows what the conversation holds.

| Route | Call | Model API called? | On the canvas? | Used for |
| --- | --- | --- | --- | --- |
| **Both** | `services.send(text)` | Yes, through the `Daemon` and its transport | Yes: the user turn, then the reply streaming in | The composer's Send button; CLI lines that don't start with `/` |
| **Canvas only** | `services.conversation.add({ role, text })` | No | Yes, immediately | A greeting, a notice, a handoff message, restoring a saved transcript |
| **API only** | Call a transport function directly, outside the `Daemon` | Yes | No; nothing enters the conversation | Background work the user should not see as a turn. The original never does this |
| **Neither yet** | `bus.emit('prompt:fill', { text, send })` | Only if `send: true` | Only if `send: true` | Nudges and voice dictation put text in the composer; the user stays the author |
| **Remove** | `services.conversation.clear()` | No | Empties it | The CLI's `/clear` |

CLI slash commands never reach the model; their output goes to the CLI's own output line, not the canvas.

If your Lit code needs an explicit switch, build it as a thin function over these calls, for example `route(text, 'api' | 'canvas' | 'both')`. Do not add a second path to the model or a second copy of the transcript.

## How it fits together

&#91;embedded content: the service layer for one chat surface\]

Components never talk to each other or to the model directly. They write through the host and listen on the bus, so the prompt also learns about `daemon:busy` and `daemon:idle` from the bus, not from the canvas. This is the layer the Lit conversion is missing; the highlighted box is its centre.

## The contract

Port these three modules as they are. They are plain JavaScript with no DOM or Lit imports, so they convert to TypeScript by adding types. The code below is the original 1.0.0 source with comments trimmed; the logic is unchanged.

**Record:** one turn, plain data, never a DOM node.

```js
let seq = 0;
const nextId = () => `m-${Date.now().toString(36)}-${(seq++).toString(36)}`;

export const ROLES = Object.freeze(['user', 'assistant', 'system', 'tool']);

export function createRecord({ role = 'user', text = '', ...rest } = {}) {
  if (!ROLES.includes(role)) throw new TypeError(`record.role must be one of ${ROLES.join(', ')}`);
  return {
    id: nextId(),
    role,
    text,
    at: new Date().toISOString(),
    status: 'complete',          // 'pending' while streaming, 'error' on failure
    meta: {},                    // open map; reserved keys are prefixed 'mcs:'
    ...rest
  };
}
```

Reserved `meta` keys: `mcs:tokens`, `mcs:latencyMs`, `mcs:model`, `mcs:source`, `mcs:error`, `mcs:citations`, `mcs:toolCalls`. Any other key belongs to the integrator and must be carried through unchanged; they hold CRM and ticket identifiers.

**PubSub:** the per-surface bus.

```js
export class PubSub {
  #topics = new Map();

  /** @returns {() => void} unsubscribe */
  on(topic, handler) {
    if (typeof handler !== 'function') return () => {};
    const set = this.#topics.get(topic) ?? new Set();
    set.add(handler);
    this.#topics.set(topic, set);
    return () => set.delete(handler);
  }

  once(topic, handler) {
    const off = this.on(topic, (payload) => { off(); handler(payload); });
    return off;
  }

  emit(topic, payload) {
    for (const handler of this.#topics.get(topic) ?? []) {
      try { handler(payload); } catch (err) { console.warn(`[syncopation] "${topic}" subscriber failed:`, err); }
    }
    for (const handler of this.#topics.get('*') ?? []) {
      try { handler({ topic, payload }); } catch { /* as above */ }
    }
  }

  get topics() { return [...this.#topics.keys()]; }
}
```

**Conversation:** the only thing allowed to change records, and it announces every change.

```js
import { createRecord } from './record.js';

export class Conversation {
  #records = [];
  #bus;
  #max;

  constructor({ bus, maxTurns = 200, id } = {}) {
    this.#bus = bus;
    this.#max = maxTurns;
    this.id = id ?? `c-${Date.now().toString(36)}`;
  }

  get records() { return [...this.#records]; }
  get length() { return this.#records.length; }
  get last() { return this.#records.at(-1) ?? null; }

  add(input) {
    const record = input?.id ? input : createRecord(input);
    this.#records.push(record);
    if (this.#records.length > this.#max) this.#records.splice(0, this.#records.length - this.#max);
    this.#bus?.emit('record:added', record);
    return record;
  }

  append(id, chunk) {
    const record = this.#records.find((r) => r.id === id);
    if (!record) return null;
    record.text += chunk;
    this.#bus?.emit('record:appended', { id, chunk, record });
    return record;
  }

  update(id, patch) {
    const record = this.#records.find((r) => r.id === id);
    if (!record) return null;
    Object.assign(record, patch);
    this.#bus?.emit('record:updated', record);
    return record;
  }

  clear() {
    this.#records = [];
    this.#bus?.emit('conversation:cleared', { id: this.id });
  }

  toJSON() { return { id: this.id, records: this.#records }; }
}
```

**Bus topics** your Lit components will listen to:

| Topic | Payload | Emitted by | Typical listener |
| --- | --- | --- | --- |
| `record:added` | the record | `Conversation.add` | Canvas adds a bubble; history refreshes; nudge watches for the first turn |
| `record:appended` | `{ id, chunk, record }` | `Conversation.append` | Canvas updates that one bubble's text |
| `record:updated` | the record | `Conversation.update` | Canvas updates status; read-aloud speaks completed turns |
| `conversation:cleared` | `{ id }` | `Conversation.clear` | Canvas empties |
| `daemon:busy` | `{ id }` of the reply | `Daemon.send` | Composer shows Stop; container shows waiting |
| `daemon:idle` | `{ id }` | `Daemon.send`, always last | Composer shows Send again |
| `daemon:error` | `{ id, error, name }`, `error` a string | `Daemon.send` | Composer shows the reason |
| `prompt:fill` | `{ text, send }` | Nudge, voice, your code | Composer fills its field and sends only if `send` |
| `*` | `{ topic, payload }` | every `emit` | Inspector, recorder, analytics |

Two known weaknesses to fix or keep deliberately: `records` copies the array but not the record objects, so a caller can still mutate a record; and `toJSON()` returns the live array.

## Behaviour to match

The `Daemon` runs one model turn at a time and is the only caller of a transport. Port this version: it is the original with the Stop bug fixed and `history` and `signal` passed to transports. Run against the original's own suite, it passes all 125 tests, plus a Stop test that the published code fails.

```js
import { META } from './metadata.js'; // the reserved 'mcs:' key names listed above

const TRANSPORTS = {
  /** No backend. Useful for building UI before a model exists. */
  async *echo(prompt) {
    for (const word of `You said: ${prompt}`.split(' ')) {
      await new Promise((r) => setTimeout(r, 40));
      yield word + ' ';
    }
  }
};

export class Daemon {
  #bus; #conversation; #config; #abort = null;
  #transports;

  constructor({ bus, conversation, config, transports } = {}) {
    this.#bus = bus; this.#conversation = conversation; this.#config = config;
    this.#transports = { ...TRANSPORTS, ...transports };
  }

  register(name, transport) {
    if (typeof transport !== 'function') {
      throw new TypeError(`transport "${name}" must be an async generator function`);
    }
    this.#transports[name] = transport;
    this.#bus?.emit('daemon:transport-registered', { name });
    return this;
  }

  get transports() { return Object.keys(this.#transports); }
  get busy() { return this.#abort !== null; }

  /** Stop generating. The partial turn is kept: the user saw it. */
  stop() { this.#abort?.abort(); this.#abort = null; }

  async send(text) {
    if (this.busy) return null;
    this.#conversation.add({ role: 'user', text });

    const reply = this.#conversation.add({
      role: 'assistant', text: '', status: 'pending',
      meta: { [META.SOURCE]: this.#config.transport, [META.MODEL]: this.#config.model }
    });

    const transport = this.#transports[this.#config.transport] ?? this.#transports.echo;
    // Turn-local: stop() clears the shared field, so the loop must not read it.
    const controller = new AbortController();
    this.#abort = controller;
    const startedAt = Date.now();
    this.#bus?.emit('daemon:busy', { id: reply.id });

    try {
      const history = this.#conversation.records;
      for await (const chunk of transport(text, { config: this.#config, history, signal: controller.signal })) {
        if (controller.signal.aborted) break;
        this.#conversation.append(reply.id, chunk);
      }
      this.#conversation.update(reply.id, {
        status: 'complete',
        meta: { ...reply.meta, [META.LATENCY]: Date.now() - startedAt }
      });
    } catch (err) {
      if (controller.signal.aborted) {
        // Stop, not a failure: the partial text stays as a finished turn.
        this.#conversation.update(reply.id, { status: 'complete' });
      } else {
        this.#conversation.update(reply.id, {
          status: 'error',
          meta: { ...reply.meta, [META.ERROR]: String(err?.message ?? err) }
        });
        this.#bus?.emit('daemon:error', { id: reply.id, error: String(err?.message ?? err), name: err?.name ?? 'Error' });
      }
    } finally {
      if (this.#abort === controller) this.#abort = null;
      this.#bus?.emit('daemon:idle', { id: reply.id });
    }
    return reply;
  }
}
```

The original also ships `local` and `remote` stub transports that only echo a placeholder; add them only if your UI lists them.

Invariants to keep. The original tests all of them except `history`, which it does not pass:

- **One turn at a time.** `send` during a running turn returns `null` and adds nothing. Interleaving two streams into one transcript produces output nobody can attribute.
- **A failed turn is visible.** It becomes an `error` record carrying the reason, never a silent drop.
- **Stop keeps the partial text** as a `complete` turn, and `daemon:idle` always fires once per turn, which turns Stop back into Send.
- **Unknown transport names fall back to `echo`.** A typo should not make the surface look broken.
- **Registering cannot remove `echo`.** Registered transports are merged over the built-ins.
- **Transports receive `history` including the current turn:** the user record and the empty pending reply. An adapter keeps `complete` user and assistant records and drops the last, which is the prompt it was handed.
- **A throwing bus subscriber cannot silence the others,** and the `*` topic sees every topic, including ones your app invents.
- **Persistence is opt-in.** The original writes `conversation.toJSON()` to IndexedDB on `record:added` and `record:updated` only when `persist` is set. Nothing is stored by default.

## Rebuilding it in Lit

The four files below were run in jsdom with Lit 3 against the core modules above, and pass the acceptance tests in the next section. They are plain JavaScript with static `properties`, so they run without a build step. Add types and decorators as your codebase prefers, and rename tags to match your components.

1. **Put the core in its own folder** with no Lit or DOM imports: `record.js`, `metadata.js`, `pubsub.js`, `conversation.js`, `daemon.js`. `metadata.js` only needs the `META` constant listed in the contract.
2. **Add one services host per chat surface.** It builds the runtime in its constructor, so the runtime exists before any child connects. This also removes the original's trap, where a transport registered too early was dropped without an error.

   ```js
   import { LitElement, html, css } from 'lit';
   import { PubSub } from './core/pubsub.js';
   import { Conversation } from './core/conversation.js';
   import { Daemon } from './core/daemon.js';
   
   export class ChatServices extends LitElement {
     static styles = css`:host { display: contents; }`;   // headless, but its children stay visible
   
     constructor() {
       super();
       this.config = { transport: 'echo', model: '' };
       this.bus = new PubSub();
       this.conversation = new Conversation({ bus: this.bus });
       this.daemon = new Daemon({ bus: this.bus, conversation: this.conversation, config: this.config });
     }
   
     render() { return html`<slot></slot>`; }
   
     /** Route: both. The user turn goes on the canvas and the prompt to the model. */
     send(text) { return this.daemon.send(text); }
   
     /** Route: canvas only. The record is shown; the model is not called. */
     add(record) { return this.conversation.add(record); }
   
     stop() { this.daemon.stop(); }
     clear() { this.conversation.clear(); }
   
     registerTransport(name, transport, { select = true } = {}) {
       this.daemon.register(name, transport);
       if (select) this.config.transport = name;
       return this;
     }
   }
   customElements.define('chat-services', ChatServices);
   
   /** Nearest <chat-services> above a node, crossing shadow roots. */
   export function findServices(node) {
     for (let n = node; n; n = n.parentNode ?? n.host) {
       if (n.localName === 'chat-services') return n;
     }
     return null;
   }
   ```
3. **Connect components with a reactive controller.** It finds the surface's host, subscribes to the topics a component names, and releases every subscription on disconnect. The generation counter discards a lookup that finishes after the component was removed or moved.

   ```js
   import { findServices } from './chat-services.js';
   
   export class ServicesController {
     services = null;
     #off = [];
     #generation = 0;
   
     constructor(host, topics = {}) {
       this.host = host;
       this.topics = topics;
       host.addController(this);
     }
   
     async hostConnected() {
       const generation = ++this.#generation;
       await customElements.whenDefined('chat-services');
       // Disconnected or reconnected while waiting: this lookup is stale.
       if (generation !== this.#generation || !this.host.isConnected) return;
       this.services = findServices(this.host);
       if (!this.services) return;
       this.#off = Object.entries(this.topics).map(([topic, handler]) => this.services.bus.on(topic, handler));
       this.host.servicesConnected?.(this.services);
       this.host.requestUpdate();
     }
   
     hostDisconnected() {
       this.#generation++;
       for (const off of this.#off) off();
       this.#off = [];
       this.services = null;
     }
   }
   ```
4. **Rebuild the canvas on conversation events.** Lit does not see changes inside the records, so the canvas replaces the one changed record in a new array. `repeat()` keyed on `id` keeps every other bubble's DOM node untouched, which preserves focus, text selection and what a screen reader has already announced.

   ```js
   import { LitElement, html, css } from 'lit';
   import { repeat } from 'lit/directives/repeat.js';
   import { ServicesController } from './services-controller.js';
   
   export class ChatCanvas extends LitElement {
     static properties = { records: { state: true } };
     static styles = css`
       :host { display: block; overflow-y: auto; }
       ol { list-style: none; margin: 0; padding: 0; }
     `;
   
     constructor() {
       super();
       this.records = [];
       // Replace only the changed record; repeat() keeps every other <li> as-is.
       const upsert = (record) => {
         const i = this.records.findIndex((r) => r.id === record.id);
         this.records = i === -1
           ? [...this.records, { ...record }]
           : this.records.map((r, j) => (j === i ? { ...record } : r));
       };
       this.svc = new ServicesController(this, {
         'record:added': upsert,
         'record:appended': ({ record }) => upsert(record),
         'record:updated': upsert,
         'conversation:cleared': () => { this.records = []; }
       });
     }
   
     /** Called once connected: show what the conversation already holds. */
     servicesConnected(services) { this.records = services.conversation.records.map((r) => ({ ...r })); }
   
     render() {
       return html`<ol>${repeat(this.records, (r) => r.id, (r) => html`
         <li data-id=${r.id} data-role=${r.role} data-status=${r.status}>${r.text}</li>`)}</ol>`;
     }
   }
   customElements.define('chat-canvas', ChatCanvas);
   ```

   Add the original's autoscroll rule to the class. Follow new text only when the reader is within 48px of the bottom, so someone reading earlier turns is never pulled down. Keep `scroll-behavior` unset: smooth scrolling lags a stream and makes the position reading unreliable.

   ```js
     // Autoscroll only when the reader is already at the bottom (original: 48px).
     #pinned = true;
     #trackScroll = () => {
       this.#pinned = this.scrollHeight - this.scrollTop - this.clientHeight <= 48;
     };
     connectedCallback() { super.connectedCallback(); this.addEventListener('scroll', this.#trackScroll); }
     disconnectedCallback() { super.disconnectedCallback(); this.removeEventListener('scroll', this.#trackScroll); }
     updated() { if (this.#pinned) this.scrollTop = this.scrollHeight; }
   ```
5. **Send through the host from the prompt.** The prompt never calls the model itself. It listens for `daemon:busy` and `daemon:idle` to swap Send and Stop, and for `prompt:fill` so nudges and dictation can fill it.

   ```js
   import { LitElement, html } from 'lit';
   import { ServicesController } from './services-controller.js';
   
   export class ChatPrompt extends LitElement {
     static properties = { busy: { state: true }, status: { state: true } };
   
     constructor() {
       super();
       this.busy = false;
       this.status = '';
       this.svc = new ServicesController(this, {
         'daemon:busy': () => { this.busy = true; this.status = ''; },
         'daemon:idle': () => { this.busy = false; },
         'daemon:error': ({ error }) => { this.status = `Something went wrong: ${error}`; },
         'prompt:fill': ({ text, send }) => this.fill(text, { send })
       });
     }
   
     get field() { return this.renderRoot.querySelector('textarea'); }
   
     fill(text, { send = false } = {}) {
       this.field.value = text;
       if (send) this.#submit();
     }
   
     #submit() {
       const text = this.field.value.trim();
       if (!text || !this.svc.services) return;
       this.field.value = '';
       this.svc.services.send(text);
     }
   
     render() {
       // While generating, the button becomes Stop. It never disables.
       return html`
         <textarea aria-label="Message"></textarea>
         ${this.busy
           ? html`<button type="button" @click=${() => this.svc.services?.stop()}>Stop</button>`
           : html`<button type="button" @click=${() => this.#submit()}>Send</button>`}
         <p role="status">${this.status}</p>`;
     }
   }
   customElements.define('chat-prompt', ChatPrompt);
   ```
6. **Compose a surface and pick a model.** Each `<chat-services>` is an independent conversation.

   ```html
   <chat-services id="support">
     <chat-canvas></chat-canvas>
     <chat-prompt></chat-prompt>
   </chat-services>
   <script type="module">
     import './chat-services.js';
     import './chat-canvas.js';
     import './chat-prompt.js';
     import { makeRaiTransport } from './rai.js';
   
     const services = document.getElementById('support');
     services.registerTransport('rai', makeRaiTransport({ systemPrompt: 'You are a support assistant.' }));
     services.add({ role: 'assistant', text: 'Hi, how can I help?' }); // canvas only
   </script>
   ```

   `makeRaiTransport` is in the companion doc, *Porting the RAI Transport*. With this `Daemon`, it receives `history` and `signal` directly, so it needs no `getHistory` option.

Your existing Lit components keep their markup and styles. What changes is where their data comes from: the canvas reads records from these bus events, and the prompt calls `send()` instead of the model.

## Acceptance

These seven tests passed against the files above with Lit 3 and jsdom 29 under the Node test runner. Each was checked by breaking the code on purpose. Leaving subscriptions attached, sharing one runtime across surfaces, rendering with `unsafeHTML`, and a canvas that ignores updates each made tests fail.

Point the imports and tag names at your components, keep the assertions. `setup.js` gives Node a DOM before any component loads:

```js
// setup.js: run with  node --test --import ./setup.js lit.test.js
import { JSDOM } from 'jsdom';
const dom = new JSDOM('<!doctype html><html><body></body></html>', { pretendToBeVisual: true });
const g = dom.window;
for (const k of ['window', 'document', 'customElements', 'HTMLElement', 'Node', 'Element', 'ShadowRoot', 'Document', 'DocumentFragment', 'CSSStyleSheet', 'MutationObserver', 'Event', 'CustomEvent', 'requestAnimationFrame']) {
  Object.defineProperty(globalThis, k, { value: g[k], configurable: true, writable: true });
}
```

```js
// lit.test.js
import test from 'node:test';
import assert from 'node:assert/strict';
import './src/chat-services.js';
import './src/chat-canvas.js';
import './src/chat-prompt.js';

const tick = (ms = 0) => new Promise((r) => setTimeout(r, ms));
async function surface() {
  const host = document.createElement('chat-services');
  host.innerHTML = '<chat-canvas></chat-canvas><chat-prompt></chat-prompt>';
  document.body.append(host);
  const canvas = host.querySelector('chat-canvas'), prompt = host.querySelector('chat-prompt');
  await tick(); await canvas.updateComplete; await prompt.updateComplete;
  const items = () => [...canvas.renderRoot.querySelectorAll('li')];
  return { host, canvas, prompt, items };
}
async function* slow(prompt, { signal }) {
  for (const w of ['one ', 'two ', 'three ']) { await tick(15); if (signal?.aborted) return; yield w; }
}

test('route both: Send shows the user turn and streams one reply bubble', async () => {
  const s = await surface();
  s.host.registerTransport('slow', slow);
  s.prompt.fill('hello', { send: true });
  await tick(5); await s.canvas.updateComplete;
  const reply = s.items()[1];
  assert.equal(s.items().length, 2);
  assert.equal(s.items()[0].textContent, 'hello');
  assert.equal(s.prompt.renderRoot.querySelector('button').textContent, 'Stop');
  await tick(80); await s.canvas.updateComplete; await s.prompt.updateComplete;
  assert.equal(s.items()[1], reply, 'same <li> node across streamed chunks');
  assert.equal(reply.textContent, 'one two three ');
  assert.equal(reply.dataset.status, 'complete');
  assert.equal(s.prompt.renderRoot.querySelector('button').textContent, 'Send');
});

test('route canvas only: add() shows a turn without calling the model', async () => {
  const s = await surface();
  let called = 0;
  s.host.registerTransport('spy', async function* () { called++; yield 'x'; });
  s.host.add({ role: 'assistant', text: 'Welcome back.' });
  await s.canvas.updateComplete;
  assert.deepEqual(s.items().map((li) => li.textContent), ['Welcome back.']);
  assert.equal(called, 0);
  s.host.clear(); await s.canvas.updateComplete;
  assert.equal(s.items().length, 0);
});

test('two surfaces keep separate conversations', async () => {
  const a = await surface(), b = await surface();
  a.host.add({ role: 'user', text: 'only in a' });
  await a.canvas.updateComplete; await b.canvas.updateComplete;
  assert.equal(a.items().length, 1);
  assert.equal(b.items().length, 0);
});

test('stop keeps the partial reply and turns Stop back into Send', async () => {
  const s = await surface();
  s.host.registerTransport('slow', slow);
  s.host.send('hi');
  await tick(20);
  s.prompt.renderRoot.querySelector('button').click();
  await tick(40); await s.canvas.updateComplete; await s.prompt.updateComplete;
  assert.equal(s.items()[1].dataset.status, 'complete');
  assert.equal(s.items()[1].textContent, 'one ');
  assert.equal(s.prompt.renderRoot.querySelector('button').textContent, 'Send');
});

test('a failing transport shows an error turn and the reason', async () => {
  const s = await surface();
  s.host.registerTransport('bad', async function* () { throw new Error('rai threads: 401'); });
  await s.host.send('hi');
  await s.canvas.updateComplete; await s.prompt.updateComplete;
  assert.equal(s.items()[1].dataset.status, 'error');
  assert.match(s.prompt.renderRoot.querySelector('[role=status]').textContent, /401/);
});

test('model text renders inert', async () => {
  const s = await surface();
  s.host.add({ role: 'assistant', text: '<img src=x onerror=alert(1)>' });
  await s.canvas.updateComplete;
  assert.equal(s.items()[0].querySelector('img'), null);
  assert.equal(s.items()[0].textContent, '<img src=x onerror=alert(1)>');
});

test('a detached canvas stops listening', async () => {
  const s = await surface();
  s.canvas.remove();
  assert.doesNotThrow(() => s.host.add({ role: 'user', text: 'after detach' }));
  assert.equal(s.canvas.records.length, 0);
});
```

Three checks jsdom cannot make, to do in a real browser before calling it done:

- [ ] Scroll up mid-reply: the canvas must not pull you back to the bottom. It follows new text only when you are already at the bottom.
- [ ] Select text in an earlier bubble while a reply streams: the selection survives.
- [ ] With a screen reader on, earlier turns are not re-announced as each chunk arrives.
