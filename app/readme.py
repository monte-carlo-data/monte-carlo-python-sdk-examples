from __future__ import annotations

from app.categories import CategoriesScreen
from app.header import MCHeader
from textual.app import ComposeResult
from textual.containers import VerticalScroll
from textual.screen import Screen
from textual.widgets import Collapsible, Footer, Markdown
from pathlib import Path

import re
import os

WHAT_IS_TEXTUAL_MD = """\
# What is MC SDK SAMPLES?

Set of utilities around MC operations that run in the terminal.

🐍 All you need is Python!

"""


def read_md_by_sections(file_path):
    """
    Reads a markdown file and splits it into sections based on headers.

    Args:
        file_path (str): The path to the markdown file.

    Returns:
        dict: A dictionary where keys are section headers and values are the
              corresponding section content.
    """
    sections = {}
    stack = []  # Stack to maintain header hierarchy

    with open(file_path, 'r', encoding='utf-8') as file:
        for line in file:
            header_match = re.match(r'^(#+)\s+(.*)$', line)
            if header_match:
                level = len(header_match.group(1))
                header_text = header_match.group(2).strip()

                # Create a new section for the header
                new_section = {"content": "", "subsections": {}}

                # Adjust stack based on header level
                while stack and stack[-1][0] >= level:
                    stack.pop()

                if stack:
                    parent_section = stack[-1][1]["subsections"]
                    parent_section[header_text] = new_section
                else:
                    sections[header_text] = new_section

                stack.append((level, new_section))
            elif stack:
                stack[-1][1]["content"] += line

    return sections


MD_SECTIONS = read_md_by_sections(os.path.join(str(Path(os.path.abspath(__file__)).parent.parent), 'README.md'))


class Content(VerticalScroll, can_focus=False):
    """Non focusable vertical scroll."""


class ReadmeScreen(Screen):
    DEFAULT_CSS = """
    ReadmeScreen {

        Content {
            align-horizontal: center;
            & > * {
                max-width: 100;
            }      
            margin: 0 1;          
            overflow-y: auto;
            height: 1fr;
            scrollbar-gutter: stable;
            MarkdownFence {
                height: auto;
                max-height: initial;
            }
            Collapsible {
                padding-right: 0;               
                &.-collapsed { padding-bottom: 1; }
            }
            Markdown {
                margin-right: 1;
                padding-right: 1;
                background: transparent;
            }
        }
    }
    """

    def render_markdown_sections(self, sections, collapsed_level=1):
        for i, (title, content) in enumerate(sections.items()):
            with Collapsible(title=title, collapsed=False if i < collapsed_level else True):
                yield Markdown(content['content'])
                if 'subsections' in content:
                    yield from self.render_markdown_sections(content['subsections'], collapsed_level)

    def compose(self) -> ComposeResult:
        yield MCHeader()
        with Content():
            yield Markdown(WHAT_IS_TEXTUAL_MD)
            yield from self.render_markdown_sections(MD_SECTIONS)
        yield Footer()

    def action_go_back(self) -> None:
        """Return to CategoriesScreen when Escape is pressed."""
        if len(self.app.screen_stack) > 1:
            self.app.pop_screen()
        else:
            self.app.push_screen(CategoriesScreen())

    def action_go_categories(self) -> None:
        """Return to CategoriesScreen when Escape is pressed."""
        from app.categories import CategoriesScreen
        self.app.push_screen(CategoriesScreen())
