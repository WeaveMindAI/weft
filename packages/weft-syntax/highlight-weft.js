/*
 * Weft for highlight.js.
 *
 * The same token rules as `weft.tmLanguage.json`, restated because the two
 * engines share no grammar format. Both describe one lexer,
 * `crates/weft-compiler/src/cst/lexer.rs`: when a token rule changes there,
 * change it in BOTH files here.
 *
 * SYNC: weft highlighting <-> packages/weft-syntax/weft.tmLanguage.json,
 *       crates/weft-compiler/src/cst/lexer.rs (the token rules),
 *       crates/weft-compiler/src/weft_compiler.rs RESERVED_CONFIG_KEYS +
 *       RESERVED_WORDS + RESERVED_TYPE_KEYWORDS,
 *       crates/weft-core/src/weft_type.rs (the type vocabulary),
 *       crates/weft-compiler/tests/highlighting_vocabulary.rs (the word-list alarm)
 *
 * Drop it in a page that already has highlight.js and it installs itself. Or
 * import it and call `install(hljs)` yourself, which is what a page needs when
 * it brings its own copy of highlight.js.
 */
(function (root, factory) {
  var api = factory();

  if (typeof module === 'object' && module.exports) {
    module.exports = api;
  }
  if (!root) {
    return;
  }
  root.weftHighlight = api;
  if (root.hljs) {
    api.install(root.hljs);
  }
})(typeof window !== 'undefined' ? window : null, function () {
  var IDENT = '[A-Za-z_][A-Za-z0-9_-]*';

  function language(hljs) {
    return {
      name: 'Weft',
      aliases: ['weft'],
      case_insensitive: false,
      contains: [
        hljs.HASH_COMMENT_MODE,

        // A ```...``` block carries code or a template. Nothing inside it is
        // weft, so nothing inside it gets coloured as weft. An escaped
        // backtick fence inside does not end it, matching the lexer.
        { className: 'string', begin: '```', end: '(?<!\\\\)```', relevance: 10 },

        // Ends at the closing quote OR the end of the line: the lexer's
        // strings never cross a newline, so an unterminated one must not
        // swallow the rest of the file.
        {
          className: 'string',
          begin: '"',
          end: '"|(?=\\n)',
          contains: [{ begin: '\\\\.' }],
        },

        // @file(...), @include(...), @require_one_of(...). The lexer's
        // marker charset has no `-`, unlike IDENT.
        { className: 'meta', begin: '@[A-Za-z_][A-Za-z0-9_]*' },

        // The four keys the language reserves.
        { className: 'keyword', begin: '\\b(_label|_tags|_should_flow)\\b(?=\\s*:)' },

        // A config field key, left of its colon.
        { className: 'attr', begin: '\\b' + IDENT + '\\b(?=\\s*:)' },

        // The port half of `node.port`.
        { className: 'symbol', begin: '(?<=\\.)' + IDENT },

        { className: 'keyword', begin: '\\b(Group|Loop|LoopIn|LoopOut|Passthrough|self|type)\\b' },

        {
          className: 'type',
          begin:
            '\\b(String|Number|Boolean|Null|Image|Video|Audio|Blob|Empty|Media|File' +
            '|List|Dict|JsonDict|Bus|Access|Generator|MustOverride)\\b',
        },

        { className: 'literal', begin: '\\b(true|false)\\b' },

        // A node type. Types are capitalised, node ids are not.
        { className: 'title', begin: '\\b[A-Z][A-Za-z0-9_]*\\b' },

        // The lexer's numbers: decimal digits with an optional fraction.
        // Not C_NUMBER_MODE, which would also paint hex and exponents the
        // language does not have.
        { className: 'number', begin: '-?\\b[0-9]+(\\.[0-9]+)?\\b' },
      ],
    };
  }

  /// Teach `hljs` weft, then colour every weft block on the page.
  function install(hljs) {
    hljs.registerLanguage('weft', language);

    if (typeof document === 'undefined') {
      return;
    }

    // highlight.js has usually walked the page before this runs, and it left
    // every weft block plain because the language did not exist yet, so paint
    // them again. `data-highlighted` is highlight.js's own "already done" flag
    // from version 11 on, and it refuses to touch an element that carries it.
    var highlight = hljs.highlightElement || hljs.highlightBlock;
    var paint = function () {
      var blocks = document.querySelectorAll('code.language-weft');
      for (var i = 0; i < blocks.length; i++) {
        blocks[i].removeAttribute('data-highlighted');
        highlight.call(hljs, blocks[i]);
      }
    };

    if (document.readyState === 'loading') {
      document.addEventListener('DOMContentLoaded', paint);
    } else {
      paint();
    }

    // VS Code's markdown preview rewrites its whole body on every keystroke,
    // and announces it with this event. Without the repaint, a weft block goes
    // plain again the moment you type.
    if (typeof window !== 'undefined') {
      window.addEventListener('vscode.markdown.updateContent', paint);
    }
  }

  return { language: language, install: install };
});
