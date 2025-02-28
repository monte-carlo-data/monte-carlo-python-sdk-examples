from textual.reactive import reactive
from textual.containers import Horizontal, Vertical
from textual.app import ComposeResult
from textual.widgets import Static


class MCHeader(Vertical):
    """Widget to get and display GitHub star count."""

    DEFAULT_CSS = """
    MCHeader {
        dock: top;
        height: 6;
        border-bottom: hkey $background;
        border-top: hkey $background;
        layout: horizontal;
        background: #0c5395;
        padding: 0 0;
        color: $text-warning;
        #logo { align: center top; text-style: bold; color: $foreground; padding: 1 0 0 35;}
        Label { text-style: bold; color: $foreground; }
        LoadingIndicator { background: transparent !important; }
        Digits { width: auto; margin-right: 1; }
        Label { margin-right: 1; }
        align: center top;
        &>Horizontal { max-width: 100;} 
    }
    """
    stars = reactive(25251, recompose=True)
    forks = reactive(776, recompose=True)

    def compose(self) -> ComposeResult:
        with Horizontal():
            with Vertical(id="logo"):
                yield Static("┳┳┓┏┓  ┏┓┳┓┓┏┓  ┏┓┏┓┳┳┓┏┓┓ ┏┓┏┓\n"
                             "┃┃┃┃   ┗┓┃┃┃┫   ┗┓┣┫┃┃┃┃┃┃ ┣ ┗┓\n"
                             "┛ ┗┗┛  ┗┛┻┛┛┗┛  ┗┛┛┗┛ ┗┣┛┗┛┗┛┗┛")

    def on_mount(self) -> None:
        print("")