// What each piece of weft source turns into on the page. The Rust side
// (`crates/weft-compiler/tests/highlighting_vocabulary.rs`) checks that the
// grammars know every reserved word; this checks that they paint it.
import { describe, expect, it } from 'vitest';
import hljs from 'highlight.js/lib/core';
import weftHighlight from './highlight-weft.js';

hljs.registerLanguage('weft', weftHighlight.language);

/// The classes highlight.js puts on `source`, innermost first.
function paint(source) {
  return hljs.highlight(source, { language: 'weft' }).value;
}

describe('a weft block', () => {
  it('paints a comment, and only to the end of its line', () => {
    expect(paint('# hi\nx = Debug')).toBe(
      '<span class="hljs-comment"># hi</span>\nx = <span class="hljs-title">Debug</span>',
    );
  });

  it('leaves node ids plain and paints their type', () => {
    expect(paint('mailbox = EmailAccess')).toBe(
      'mailbox = <span class="hljs-title">EmailAccess</span>',
    );
  });

  it('paints the port half of an endpoint, not the node half', () => {
    expect(paint('a.port = b.other')).toBe(
      'a.<span class="hljs-symbol">port</span> = b.<span class="hljs-symbol">other</span>',
    );
  });

  it('paints a group boundary as a keyword on both sides', () => {
    expect(paint('g = Group(x: String) -> (y: String) {}')).toContain(
      '<span class="hljs-keyword">Group</span>',
    );
    expect(paint('self.y = c.out')).toContain('<span class="hljs-keyword">self</span>');
  });

  it('paints a config key, its string, and its number', () => {
    expect(paint('n = T { k: "v", size: 12 }')).toContain('<span class="hljs-attr">k</span>');
    expect(paint('n = T { k: "v", size: 12 }')).toContain(
      '<span class="hljs-string">&quot;v&quot;</span>',
    );
    expect(paint('n = T { k: "v", size: 12 }')).toContain('<span class="hljs-number">12</span>');
  });

  it('paints a reserved key apart from an ordinary one', () => {
    for (const key of ['_label', '_tags', '_should_flow']) {
      expect(paint(`n = T { ${key}: "x" }`)).toContain(
        `<span class="hljs-keyword">${key}</span>`,
      );
    }
  });

  it('paints a directive as a marker', () => {
    expect(paint('@require_one_of(a, b)')).toContain(
      '<span class="hljs-meta">@require_one_of</span>',
    );
  });

  it('treats a ``` block as one opaque string, whatever is inside it', () => {
    const src = 'n = T {\n  code: ```\n  x = NotANode\n  ```\n}';
    expect(paint(src)).toContain('<span class="hljs-string">```\n  x = NotANode\n  ```</span>');
  });

  it('paints a declared port type', () => {
    expect(paint('n = Exec(a: Number) -> (b: String?) {}')).toContain(
      '<span class="hljs-type">Number</span>',
    );
  });

  // The four rules where this grammar and the tmLanguage most easily
  // drift from the lexer. Each case pins the LEXER's answer.

  it('stops a marker at a hyphen, like the lexer', () => {
    // marker_len's charset is [A-Za-z0-9_]: `@my-file` is the marker
    // `@my` and then ordinary tokens.
    expect(paint('x: @my-file(a)')).toContain('<span class="hljs-meta">@my</span>');
    expect(paint('x: @my-file(a)')).not.toContain('<span class="hljs-meta">@my-file</span>');
  });

  it('does not end a ``` block on an escaped fence', () => {
    const src = 'code: ```a \\``` b```\nafter = Debug';
    // One string to the real closing fence; `after` is code again.
    expect(paint(src)).toContain('<span class="hljs-string">```a \\``` b```</span>');
    expect(paint(src)).toContain('<span class="hljs-title">Debug</span>');
  });

  it('stops an unterminated string at the end of its line', () => {
    // The lexer's string never crosses a newline; the next line has to
    // paint as code, not as the string’s tail.
    expect(paint('k: "unterminated\nnext = Debug')).toContain(
      '<span class="hljs-title">Debug</span>',
    );
  });

  it('does not paint hex as a number', () => {
    // The lexer's numbers are decimal digits with an optional fraction.
    expect(paint('k: 0x10')).not.toContain('hljs-number');
    expect(paint('k: -3.5')).toContain('<span class="hljs-number">-3.5</span>');
  });
});
