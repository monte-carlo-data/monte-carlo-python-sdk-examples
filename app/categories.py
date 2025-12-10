from __future__ import annotations

from dataclasses import dataclass
from textual import events, on
from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Center, Horizontal, ItemGrid, Vertical, VerticalScroll
from textual.widgets import Footer, Label, Markdown, Static
from app.controls import Controls
from app.header import MCHeader


@dataclass
class CategoryInfo:
    """Dataclass for storing category information."""

    title: str
    description: str


CATEGORIES_MD = """\
# Categories
"""

CATEGORIES = [CategoryInfo('Admin', '\nAdmin related operations and utilities.'),
              CategoryInfo('Tables', '\nCollection of actions and utilities around tables/views.'),
              CategoryInfo('Monitors', '\nCollection of actions and utilities for MC monitors.'),
              CategoryInfo('Lineage', '\nCollection of actions and utilities around lineage.'),
              CategoryInfo('Migration', '\nCollection of actions and utilities for workspace migration.')]


class Category(Vertical, can_focus=True, can_focus_children=False):
    """Display category information and show utilities within"""

    ALLOW_MAXIMIZE = True
    DEFAULT_CSS = """
    Category {
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
        #title { text-style: bold italic; width: 1fr; color: #33acff; }
        .header { height: 1; }
        .link {
            color: #0c5395;
            text-style: underline;
        }
        .description { color: $text-muted; }
        &.-hover { opacity: 1; }
    }
    """

    def __init__(self, category_info: CategoryInfo) -> None:
        self.category_info = category_info
        super().__init__()

    def compose(self) -> ComposeResult:
        info = self.category_info
        with Horizontal(classes="header"):
            yield Label(info.title, id="title")
        yield Static(info.description, classes="description")


class CategoriesScreen(Controls):
    AUTO_FOCUS = None
    CSS = """
    CategoriesScreen {        
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
        Binding("enter", "open_category", "Open Category", tooltip="Open the category"),
    ]

    def __init__(
            self,
            name: str | None = None,
            id: str | None = None,
            classes: str | None = None,
    ):
        super().__init__(name, id, classes)
        self.category_info = None
        self.categories = None
        self.grid = None

    def compose(self) -> ComposeResult:
        self.grid = ItemGrid(min_column_width=40)
        yield MCHeader()
        with VerticalScroll():
            with Center():
                yield Markdown(CATEGORIES_MD)
            yield self.grid  # Ensure ItemGrid is mounted first
        yield Footer()

    def on_mount(self) -> None:
        """Mount categories after the grid is ready and focus the first item."""
        self.categories = [Category(category) for category in CATEGORIES]
        self.grid.mount(*self.categories)
        if self.categories:
            self.categories[0].focus()

    @on(events.Enter)
    @on(events.Leave)
    def on_enter(self, event: events.Enter):
        event.stop()
        self.set_class(self.is_mouse_over, "-hover")

    def action_open_category(self) -> None:
        current_focus = self.app.focused
        if isinstance(current_focus, Category):  # Ensure it's a Category instance
            # self.notify(f"Opening category: {current_focus.category_info.title}", severity="info")
            from app.utilities import UtilitiesScreen
            self.app.push_screen(UtilitiesScreen(current_focus.category_info.title))

    def action_move(self, direction: str) -> None:
        """Move focus within the utility grid."""
        self.action_move_grid(direction, Category)

    def action_go_readme(self) -> None:
        """Return to CategoriesScreen when Escape is pressed."""
        from app.readme import ReadmeScreen
        self.app.push_screen(ReadmeScreen())
