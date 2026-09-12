/* Inline examples use the same Weft grammar as fenced examples. */
(function () {
  function highlightInlineCode() {
    var highlight = window.hljs.highlightElement || window.hljs.highlightBlock;
    document.querySelectorAll('.content :not(pre) > code').forEach(function (code) {
      code.classList.add('language-weft');
      code.removeAttribute('data-highlighted');
      highlight.call(window.hljs, code);
    });
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', highlightInlineCode);
  } else {
    highlightInlineCode();
  }
})();
