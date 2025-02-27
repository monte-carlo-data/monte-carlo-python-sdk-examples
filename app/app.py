from __future__ import annotations

from textual.app import App
from textual.binding import Binding
from app.readme import ReadmeScreen
from app.categories import CategoriesScreen
from app.themes import ThemesScreen
from pathlib import Path
from textual.theme import BUILTIN_THEMES
import json

# Define the path for the theme configuration file
CONFIG_PATH = Path.home() / ".mc_sdk_app" / "config.json"

# Ensure directory exists
CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)


class MCSDKApp(App):
    """The demo app defines the modes and sets a few bindings."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    CSS = """
    .column {          
        align: center top;
        &>*{ max-width: 100; }        
    }
    Screen .-maximized {
        margin: 1 2;        
        max-width: 100%;
        &.column { margin: 1 2; padding: 1 2; }
        &.column > * {        
            max-width: 100%;           
        }        
    }
    """

    ENABLE_COMMAND_PALETTE = False

    MODES = {
        "readme": ReadmeScreen,
        "categories": CategoriesScreen,
        "themes": ThemesScreen
    }
    DEFAULT_MODE = "categories"
    BINDINGS = [
        Binding(
            "r",
            "app.switch_mode('readme')",
            "ReadMe",
            tooltip="Show the readme screen",
        ),
        Binding(
            "c",
            "app.switch_mode('categories')",
            "Categories",
            tooltip="Show utilities categories",
        ),
        Binding(
            "t",
            "app.switch_mode('themes')",
            "Themes",
            tooltip="Change app theme",
        ),
        Binding(
            "ctrl+s",
            "app.screenshot",
            "Screenshot",
            tooltip="Save an SVG 'screenshot' of the current screen",
        )
    ]

    def check_action(self, action: str, parameters: tuple[object, ...]) -> bool | None:
        """Disable switching to a mode we are already on."""
        if (
            action == "switch_mode"
            and parameters
            and self.current_mode == parameters[0]
        ):
            return None
        return True

    def on_mount(self) -> None:
        """Set initial theme when the app is mounted."""

        for theme in BUILTIN_THEMES:
            if theme == self.load_theme():
                self.app.theme = theme

    def load_theme(self) -> str:
        """Load theme from local file."""
        if CONFIG_PATH.exists():
            try:
                with open(CONFIG_PATH, "r") as file:
                    config = json.load(file)
                    return config.get("theme", "dark")
            except json.JSONDecodeError:
                pass
        return "dark"


