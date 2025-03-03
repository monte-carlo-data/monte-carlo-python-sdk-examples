from __future__ import annotations

from app.categories import CATEGORIES
from dataclasses import dataclass
from textual.app import ComposeResult
from textual.binding import Binding
from textual import events, on
from textual.containers import Center, Horizontal, ItemGrid, Vertical, VerticalScroll
from textual.widgets import Footer, Label, Markdown, Static
from app.controls import Controls
from pathlib import Path
from app.header import MCHeader
import os
import json

PARSER_CONFIG = f'{os.path.dirname(os.path.dirname(os.path.abspath(__file__)))}/lib/helpers/parser_config.json'


def parse_utility_help():
    with open(PARSER_CONFIG, 'r') as file:
        config = json.load(file)

    utils = {}
    for category in CATEGORIES:
        category.title = category.title.lower()
        path = os.path.join(os.getcwd(), category.title)
        subpaths = sorted(Path(f'{path}').glob('[!__]*.py'))
        for path in subpaths:
            utility_exec = str(path).split('/')[-1]
            utility_title = utility_exec.replace('.py', '').replace('_', ' ').title()
            if config.get(category.title):
                if config.get(category.title).get(utility_exec):
                    if config.get(category.title).get(utility_exec).get('description'):
                        utility_desc = config[category.title][utility_exec]['description']
                        utility_args = config[category.title][utility_exec].get('arguments', {})
                        utility_subparsers = config[category.title][utility_exec].get('subparsers', {})
                        utils[utility_title] = UtilityInfo(utility_title, path, truncate_string(utility_desc), utility_desc,
                                                           category.title, utility_args, utility_subparsers)

    return utils


def truncate_string(text, max_length=60):
    if len(text) <= max_length:
        return text
    else:
        truncated_text = text[:max_length]
        last_space_index = truncated_text.rfind(' ')
        if last_space_index == -1:
            return ""
        else:
            return truncated_text[:last_space_index] + "... \[more]"


@dataclass
class UtilityInfo:
    """Dataclass for storing utility information."""

    title: str
    executable: str
    short_description: str
    description: str
    parent: str
    arguments: dict
    subparsers: dict


UTILITIES = parse_utility_help()


class Utility(Vertical, can_focus=True, can_focus_children=False):
    """Display all utilities from a category"""

    ALLOW_MAXIMIZE = True
    DEFAULT_CSS = """
        Utility {
            width: 1fr;
            height: auto;      
            padding: 0 1;
            border: tall transparent;
            box-sizing: border-box;
            &:focus { 
                border: tall $text-primary;
                background: $primary 20%;
                &.link {
                    color: red !important;
                }        
            }
            #title { text-style: bold italic; width: 1fr; color: #ffffff;}
            .header { height: 1; }
            .link {
                color: #0c5395;
                text-style: underline;
            }
            .description { color: $text-muted; }
            &.-hover { opacity: 1; }
        }
        """

    def __init__(self, utility_info: UtilityInfo) -> None:
        self.utility_info = utility_info
        super().__init__()

    def compose(self) -> ComposeResult:
        info = self.utility_info
        with Horizontal(classes="header"):
            yield Label(info.title, id="title")
        yield Static(info.short_description, classes="description")


class UtilitiesScreen(Controls):
    AUTO_FOCUS = None
    CSS = """
        UtilitiesScreen {        
            align-horizontal: center;                      
            ItemGrid {
                margin: 2 4;
                padding: 1 2;
                background: $boost;
                width: 1fr;
                height: auto;            
                grid-gutter: 1 1;
                grid-rows: auto;           
                keyline:thin $foreground 30%;        
            }              
            Markdown { margin: 0; padding: 0 2; max-width: 100; background: transparent; }
        }
        """

    BINDINGS = [
        Binding("escape", "go_back", "Categories", tooltip="Go to previous screen"),
        Binding("enter", "open_utility", "Open Utility", tooltip="Open the utility"),
    ]

    def __init__(
        self,
        category: str,
        name: str | None = None,
        id: str | None = None,
        classes: str | None = None,
    ):
        super().__init__(name, id, classes)
        self.category = category
        self.utility_info = None
        self.utilities = None
        self.grid = None

    def compose(self) -> ComposeResult:
        self.app.bind("r", "void")
        self.grid = ItemGrid(min_column_width=40)
        yield MCHeader()
        with VerticalScroll():
            with Center():
                utilities_md = f"# {self.category.title()}"
                yield Markdown(utilities_md)
            yield self.grid  # Ensure ItemGrid is mounted first
        yield Footer()

    def on_mount(self) -> None:
        """Mount utilities after the grid is ready and focus the first item."""
        self.utilities = [Utility(utility) for title, utility in UTILITIES.items() if utility.parent.lower() == self.category.lower()]
        self.grid.mount(*self.utilities)
        if self.utilities:
            self.utilities[0].focus()

    @on(events.Enter)
    @on(events.Leave)
    def on_enter(self, event: events.Enter):
        event.stop()
        self.set_class(self.is_mouse_over, "-hover")

    def action_open_utility(self) -> None:
        current_focus = self.app.focused
        if isinstance(current_focus, Utility):
            # self.notify(f"Opening utility: {current_focus.utility_info.title}", severity="info")
            from app.executor import ExecutorScreen
            self.app.push_screen(ExecutorScreen(current_focus.utility_info))

    def action_move(self, direction: str) -> None:
        """Move focus within the utility grid."""
        self.action_move_grid(direction, Utility)

    def action_go_back(self) -> None:
        """Return to CategoriesScreen when Escape is pressed."""
        self.app.pop_screen()
