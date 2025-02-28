from textual.screen import Screen
from textual import events, on
from textual.containers import Vertical, VerticalScroll, ItemGrid, HorizontalGroup, Center
from textual.widgets import Switch, Label, Footer
from textual.app import ComposeResult
from textual.theme import BUILTIN_THEMES
from app.header import MCHeader

import json
from pathlib import Path

# Define the path for the theme configuration file
CONFIG_PATH = Path.home() / ".mc_sdk_app" / "config.json"


class Themes(Vertical):
    """Switch themes."""

    ALLOW_MAXIMIZE = True
    DEFAULT_CLASSES = "column"

    DEFAULT_CSS = """\
Switches {    
    Label {
        padding: 1;
        &:hover {text-style:underline; }
    }
}
"""

    def compose(self) -> ComposeResult:
        with ItemGrid(min_column_width=32):
            for theme in BUILTIN_THEMES:
                if theme.endswith("-ansi"):
                    continue
                with HorizontalGroup():
                    yield Switch(id=theme)
                    yield Label(theme, name=theme)

    @on(events.Click, "Label")
    def on_click(self, event: events.Click) -> None:
        """Make the label toggle the switch."""
        event.stop()
        if event.widget is not None:
            self.query_one(f"#{event.widget.name}", Switch).toggle()

    def on_mount(self):
        self.query_one(f"#{self.app.theme}", Switch).value = True

    def on_switch_changed(self, event: Switch.Changed) -> None:
        # Don't issue more Changed events
        if not event.value:
            self.query_one("#textual-dark", Switch).value = True
            return

        with self.prevent(Switch.Changed):
            # Reset all other switches
            for switch in self.query("Switch").results(Switch):
                if switch.id != event.switch.id:
                    switch.value = False
        assert event.switch.id is not None
        theme_id = event.switch.id

        def switch_theme() -> None:
            """Callback to switch the theme."""
            self.app.theme = theme_id
            self.save_theme()

        # Call after a short delay, so we see the Switch animation
        self.set_timer(0.3, switch_theme)

    def save_theme(self) -> None:
        """Save theme to local file."""
        with open(CONFIG_PATH, "w") as file:
            json.dump({"theme": self.app.theme}, file, indent=4)


class ThemesScreen(Screen):
    AUTO_FOCUS = None
    CSS = """
    ThemesScreen {        
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

    def __init__(self):
        super().__init__()

    def compose(self) -> ComposeResult:
        yield MCHeader()
        with VerticalScroll():
            with Center():
                yield Themes()
        yield Footer()
