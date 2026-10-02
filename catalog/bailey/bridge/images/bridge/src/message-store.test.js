// The store's two jobs the trigger depends on: keeping the copy of a
// message that has its content, and saying when a message should fire
// the trigger (once, when its content is first known).
// Run with `node --test` in the bridge's folder.

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { existsSync, mkdtempSync, readdirSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { MessageStore, contentDisposition, extractTextContent, mediaFacts, ownsFile } from './message-store.js';

const CHAT = '33600000000@s.whatsapp.net';

function message(id, content) {
  return { key: { remoteJid: CHAT, id, fromMe: false }, messageTimestamp: 1700000000, message: content };
}

const voiceNote = { audioMessage: { mimetype: 'audio/ogg', fileLength: 48213, seconds: 12 } };

test('the real copy replaces a placeholder Baileys could not decrypt yet', () => {
  const store = new MessageStore();
  store.add(message('A', null));
  store.add(message('A', voiceNote));
  assert.deepEqual(store.findByMessageId('A').message, voiceNote);
  assert.equal(store.count(CHAT), 1, 'still one message');
});

test('a later empty copy never hides the one with content', () => {
  const store = new MessageStore();
  store.add(message('A', voiceNote));
  store.add(message('A', null));
  assert.deepEqual(store.findByMessageId('A').message, voiceNote);
});

test('a message fires once, when its content is first known', () => {
  const store = new MessageStore();
  assert.equal(store.add(message('A', null)), false, 'a placeholder has nothing to fire yet');
  assert.equal(store.add(message('A', voiceNote)), true, 'its real copy fires');
  assert.equal(store.add(message('A', voiceNote)), false, 'delivered twice, fired once');
});

test('what history sync brought in never fires later', () => {
  const store = new MessageStore();
  store.addBatch([message('A', voiceNote)]);
  assert.equal(store.add(message('A', voiceNote)), false);
});

test('what was stored before a restart never fires again', () => {
  const dir = mkdtempSync(join(tmpdir(), 'bridge-store-'));
  const path = join(dir, 'messages.json');
  const before = new MessageStore(500, dir);
  assert.equal(before.add(message('A', voiceNote)), true);
  before.flushSync();

  const after = new MessageStore(500, dir);
  assert.equal(after.add(message('A', voiceNote)), false, 'Baileys redelivers it after the restart');
  rmSync(dir, { recursive: true, force: true });
});

test('a seen-ids file that does not read stops the store, naming the file and the way out', () => {
  const dir = mkdtempSync(join(tmpdir(), 'bridge-store-'));
  const path = join(dir, 'messages.json');
  writeFileSync(`${path}.seen`, '["A", "B"');
  assert.throws(
    () => new MessageStore(500, dir),
    (err) => err.message.includes(`${path}.seen`) && err.message.includes('weft infra node-terminate'),
  );
  rmSync(dir, { recursive: true, force: true });
});

test('a message history that does not read stops the store instead of starting empty and overwriting it', () => {
  const dir = mkdtempSync(join(tmpdir(), 'bridge-store-'));
  const path = join(dir, 'messages.json');
  writeFileSync(path, '{"chat": [');
  assert.throws(
    () => new MessageStore(500, dir),
    (err) => err.message.includes(path) && err.message.includes('weft infra node-terminate'),
  );
  rmSync(dir, { recursive: true, force: true });
});

test('a flush replaces both files whole, leaving no temporary behind', () => {
  const dir = mkdtempSync(join(tmpdir(), 'bridge-store-'));
  const path = join(dir, 'messages.json');
  const store = new MessageStore(500, dir);
  store.add(message('A', voiceNote));
  store.flushSync();
  assert.equal(existsSync(`${path}.tmp`), false);
  assert.equal(existsSync(`${path}.seen.tmp`), false);
  assert.equal(new MessageStore(500, dir).add(message('A', voiceNote)), false);
  rmSync(dir, { recursive: true, force: true });
});

test('a media message says its size and length; a text one says neither', () => {
  assert.deepEqual(mediaFacts(message('A', voiceNote)), { fileSize: 48213, seconds: 12 });
  // A protobuf Long, as Baileys hands it over on the live path.
  const long = { documentMessage: { fileLength: { low: 5, high: 1, unsigned: true } } };
  assert.deepEqual(mediaFacts(message('B', long)), { fileSize: 4294967301 });
  assert.deepEqual(mediaFacts(message('C', { conversation: 'hi' })), {});
});

test('a sender-named file becomes a header that can always be set', () => {
  assert.equal(contentDisposition('voice.ogg'), "inline; filename=\"voice.ogg\"; filename*=UTF-8''voice.ogg");
  const odd = contentDisposition('ça "va"\r\n🎉.pdf');
  assert.match(odd, /^inline; filename="[\x20-\x7e]*"; filename\*=UTF-8''[\x21-\x7e]*$/);
  // `'` ends the charset part of `filename*`, so one in the name is escaped.
  assert.ok(!contentDisposition("l'été.pdf").split("UTF-8''")[1].includes("'"));
  assert.equal(decodeURIComponent(odd.split("UTF-8''")[1]), 'ça "va"\r\n🎉.pdf');
});

test('a late message older than a full chat is kept, and fires', () => {
  const store = new MessageStore(2);
  const at = (id, ts) => ({ ...message(id, { conversation: id }), messageTimestamp: ts });
  store.add(at('B', 200));
  store.add(at('C', 300));
  assert.equal(store.add(at('A', 100)), true);
  assert.ok(store.findByMessageId('A'), 'the late one is kept');
  assert.equal(store.count(CHAT), 2);
  store.add(at('D', 400));
  assert.equal(store.findByMessageId('A'), null, 'the next message evicts it, the oldest');
});

test('an evicted message redelivered never fires again, nor after a restart', () => {
  const dir = mkdtempSync(join(tmpdir(), 'bridge-store-'));
  const path = join(dir, 'messages.json');
  const at = (id, ts) => ({ ...message(id, { conversation: id }), messageTimestamp: ts });
  const store = new MessageStore(1, dir);
  assert.equal(store.add(at('A', 100)), true);
  assert.equal(store.add(at('B', 200)), true);
  assert.equal(store.findByMessageId('A'), null, 'A was evicted');
  assert.equal(store.add(at('A', 100)), false, 'its redelivery does not fire');
  store.flushSync();

  const after = new MessageStore(1, dir);
  assert.equal(after.add(at('A', 100)), false, 'nor after a restart');
  rmSync(dir, { recursive: true, force: true });
});

test('the seen ids are bounded, oldest forgotten first', () => {
  const store = new MessageStore(1, null, 2);
  for (const id of ['A', 'B', 'C']) store.add(message(id, { conversation: id }));
  assert.deepEqual([...store.seen], ['B', 'C']);
});

test('a kind the store does not know is unknown with no content, never an empty text', () => {
  for (const content of [null, { reactionMessage: { text: 'x' } }, { pollCreationMessageV3: { name: 'Lunch?' } }]) {
    assert.deepEqual(extractTextContent(message('U', content)), { content: null, messageType: 'unknown' });
  }
});

test('a known kind keeps its type and its text or caption', () => {
  assert.deepEqual(extractTextContent(message('T', { conversation: 'hi' })), { content: 'hi', messageType: 'text' });
  assert.deepEqual(extractTextContent(message('V', voiceNote)), { content: null, messageType: 'audio' });
  assert.deepEqual(
    extractTextContent(message('I', { imageMessage: { caption: 'look' } })),
    { content: 'look', messageType: 'image' },
  );
});

test('the store names every file it writes, so a pairing reset keeps them', () => {
  const dir = mkdtempSync(join(tmpdir(), 'bridge-store-'));
  const store = new MessageStore(500, dir);
  store.add(message('A', voiceNote));
  store.flushSync();
  for (const name of readdirSync(dir)) assert.ok(ownsFile(name), name);
  assert.ok(ownsFile('messages.json.tmp') && ownsFile('messages.json.seen.tmp'));
  assert.equal(ownsFile('creds.json'), false);
  rmSync(dir, { recursive: true, force: true });
});
