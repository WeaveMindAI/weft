"""Attach each chapter's Markdown before HTML and diagram rendering."""

import html
import json
import sys


def add_copy_buttons(sections):
    for section in sections:
        if "Chapter" not in section:
            continue
        chapter = section["Chapter"]
        markdown = html.escape(chapter["content"], quote=True).replace(
            "\n", "&#10;"
        ).replace("\r", "&#13;")
        chapter["content"] = (
            '<div class="page-actions">'
            '<button type="button" class="copy-markdown" '
            f'data-markdown="{markdown}">Copy Markdown</button>'
            '<span class="copy-markdown-status" role="status"></span>'
            '</div>\n\n' + chapter["content"]
        )
        add_copy_buttons(chapter["sub_items"])


def main():
    if len(sys.argv) > 1 and sys.argv[1] == "supports":
        sys.exit(0 if sys.argv[2] == "html" else 1)
    _, book = json.load(sys.stdin)
    add_copy_buttons(book["items"])
    json.dump(book, sys.stdout)


if __name__ == "__main__":
    main()
