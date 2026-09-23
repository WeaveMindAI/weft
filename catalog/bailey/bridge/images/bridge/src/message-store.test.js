// The store's two jobs the trigger depends on: keeping the copy of a
// message that has its content, and saying when a message should fire
// the trigger (once, when its content is first known).
// Run with `node --test` in the bridge's folder.

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { MessageStore, contentDisposition, mediaFacts } from './message-store.js';

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
  const before = new MessageStore(500, path);
  assert.equal(before.add(message('A', voiceNote)), true);
  before.flushSync();

  const after = new MessageStore(500, path);
  assert.equal(after.add(message('A', voiceNote)), false, 'Baileys redelivers it after the restart');
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
});
