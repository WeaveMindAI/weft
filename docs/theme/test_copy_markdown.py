"""The copy payload must preserve Markdown, including nested chapters."""

from html.parser import HTMLParser
import unittest

from copy_markdown import add_copy_buttons


class ButtonParser(HTMLParser):
    def handle_starttag(self, tag, attrs):
        if tag == "button":
            self.markdown = dict(attrs)["data-markdown"]


class CopyMarkdownTests(unittest.TestCase):
    def test_exact_source_in_nested_chapters(self):
        source = '# Title\n\n```weft\nx = Text { value: "<&>" }\n```\n\nIt\'s café.\n'
        child = {"content": source, "sub_items": []}
        parent = {"content": source, "sub_items": [{"Chapter": child}]}
        add_copy_buttons(["Separator", {"Chapter": parent}])
        for chapter in [parent, child]:
            button_html, remaining = chapter["content"].split("\n\n", 1)
            parser = ButtonParser()
            parser.feed(button_html)
            self.assertEqual(parser.markdown, source)
            self.assertEqual(remaining, source)


if __name__ == "__main__":
    unittest.main()
