from __future__ import annotations

from rich.syntax import Syntax
from textual.binding import Binding
from textual.app import ComposeResult
from textual.containers import Grid, Center, Vertical, VerticalScroll, ScrollableContainer
from textual.widgets import Footer, Label, Markdown, Static, Input, Select, RichLog, TabbedContent
from textual.screen import Screen, ModalScreen
from app.header import MCHeader
from textual import events, on, work
from app.utilities import UtilityInfo, UTILITIES
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from pathlib import Path
from datetime import datetime, timedelta
import subprocess
import os
import configparser
import threading
import time


class ExecutorArgs(Vertical, can_focus=False, can_focus_children=True):
	"""Demonstrates Inputs."""

	AUTO_FOCUS = None
	DEFAULT_CSS = """
	ExecutorArgs {
		Grid {
			background: $boost;
			margin: 2 4;
			padding: 0 2;
			height: auto;
			grid-size: 4;
			grid-gutter: 1;
			grid-columns: auto 1fr;
			border: tall blank;
			&:focus-within {
				border: tall white;
			}
			Label {
				width: 100%;
				padding: 1;
				text-align: right;
			}
		}
	}
	"""

	PROFILES = os.path.expanduser("~/.mcd/profiles.ini")

	from app.utilities import UtilityInfo

	def __init__(self, utility_info: UtilityInfo) -> None:
		self.utility_info = utility_info
		self.selected_subparser = None
		self.focusable_widgets = []  # List to track focusable widgets
		self.current_focus_index = 0
		super().__init__()

	def compose(self) -> ComposeResult:
		utilities_md = (f"# {self.utility_info.title}\n"
						f"{self.utility_info.description}")
		arguments = UTILITIES[self.utility_info.title].arguments
		subparsers = UTILITIES[self.utility_info.title].subparsers
		with VerticalScroll():
			with Center():
				yield Markdown(utilities_md)
				if subparsers:
					# Set the first subparser as the default selected value
					default_value = list(subparsers.keys())[0]
					yield Select.from_values(subparsers, value=default_value, id='subparser')

			with Grid():
				if arguments:
					for arg in arguments:
						mandatory = "★ " if arguments[arg]['required'] else ""
						yield Label(f"{mandatory}{arg}")
						if arg == 'profile':
							yield Select((profile, profile) for profile in self.get_profiles())
						else:
							if arguments[arg].get('choices'):
								choices = arguments[arg]['choices']
								yield Select((choice, choice) for choice in choices)
							else:
								yield Input(placeholder=arguments[arg]['help'])

	def get_profiles(self):
		"""Read headers from mcd profiles.ini"""
		try:
			config = configparser.ConfigParser()
			config.read(self.PROFILES)
			return [str(section) for section in config.sections()]
		except Exception:
			return []

	@on(Select.Changed, selector='#subparser')
	def select_changed(self, event: Select.Changed) -> None:
		self.selected_subparser = event.value  # Track the selected subparser
		self.update_grid_with_selected_subparser()

	def update_grid_with_selected_subparser(self) -> None:
		"""Update the grid based on the selected subparser."""

		grid = self.query_one(Grid)
		grid.remove_children()
		subparsers = UTILITIES[self.utility_info.title].subparsers

		if self.selected_subparser:
			# Update the arguments based on the selected subparser
			selected_subparser_arguments = subparsers.get(self.selected_subparser, {}).get('arguments', {})

			# If there are arguments for the selected subparser, display them
			for arg in selected_subparser_arguments:
				mandatory = "★ " if selected_subparser_arguments[arg]['required'] else ""
				grid._add_child(Label(f"{mandatory}{arg}"))
				if arg == 'profile':
					grid._add_child(Select((profile, profile) for profile in self.get_profiles()))
				else:
					if selected_subparser_arguments[arg].get('choices'):
						choices = selected_subparser_arguments[arg]['choices']
						grid._add_child(Select.from_values(choices))
					else:
						grid._add_child(Input(placeholder=selected_subparser_arguments[arg]['help'],
											  tooltip=selected_subparser_arguments[arg]['help']))

		nodes = grid.query_children().nodes
		grid.mount(*nodes)

	def update_focusable_widgets(self, grid: Grid) -> None:
		"""Update the list of focusable widgets in the grid."""
		self.focusable_widgets = grid.query(Label)
		self.current_focus_index = 0  # Reset the focus index to 0 after update
		if self.focusable_widgets:
			self.focusable_widgets[self.current_focus_index].focus()

	def on_mount(self) -> None:
		"""Ensure focus on the first element after the component is mounted."""
		# Focus the first input field or select widget after everything is rendered.
		grid = self.query_one(Grid)
		self.update_focusable_widgets(grid)

	@on(events.Key)  # Bind to key events
	def on_key(self, event: events.Key) -> None:
		"""Handle Tab and Ctrl+Tab for navigation."""

		if event.key == "tab":  # Handle Tab (next)
			self.move_focus(1)
		elif event.key == "shift+tab":  # Handle shift+Tab (previous)
			self.move_focus(-1)
		elif event.key == "escape":
			screen = self.query_one(VerticalScroll)
			self.app.set_focus(screen)
		elif event.key == "ctrl+a":
			self.notify("Triggering utility...", severity="information")
			self.action_run_utility()

	@work(thread=True)
	async def action_run_utility(self):
		grid = self.query_one(Grid)
		elements = grid.children
		args = ['python', str(self.utility_info.executable)]
		if self.selected_subparser:
			args.append(self.selected_subparser)

		mandatory = False
		flags = []
		for i, element in enumerate(elements):
			if isinstance(element, Label):
				flag = element._content.split()[-1]
				flags.append(flag)
				args.append(f"-{flag[0]}")
				if '★' in element._content:
					mandatory = True
			if isinstance(element, Input) or isinstance(element, Select):
				value = element.value
				if mandatory and value == '':
					self.notify(f"'{flags[-1]}' is required", severity="error")
					return
				args.append(value)
				mandatory = False

		args = self.clean_arguments(args)
		result = subprocess.run(
			args,  # Example command that continuously outputs
			capture_output=True,
			text=True,  # Ensure output is decoded as text
			input="y"   # If token needs to get updated
		)

		if result.returncode == 0:
			self.notify(f"Execution completed successfully. Check the logs for more information", severity="information")
		if result.returncode == 1:
			self.notify(f"An error occurred:\n\n{result.stderr}", severity="error")
		elif result.returncode == 2:
			self.notify(f"Unable to run utility:\n\n{result.stderr}", severity="error")

	@staticmethod
	def clean_arguments(input_list):
		# Initialize an empty list to store the cleaned items
		cleaned_list = []

		# Iterate through the list, skipping the first element
		i = 0
		while i < len(input_list):
			# Check if the current item is either empty or Select.BLANK
			if input_list[i] == '' or input_list[i] == Select.BLANK:
				# If so, skip the current element and the previous one
				if i > 0:  # Ensure there is a previous element to remove
					cleaned_list.pop()  # Remove the last element added (previous element)
				# Skip this element
				i += 1
			else:
				# Add valid elements to the cleaned list
				cleaned_list.append(input_list[i])
				i += 1

		return cleaned_list

	def move_focus(self, direction: int):
		"""Move focus to the next or previous widget in the list."""
		if not self.focusable_widgets:
			return

		# Calculate the new focus index
		self.current_focus_index = (self.current_focus_index + direction) % len(self.focusable_widgets)
		# Set focus to the new widget
		self.focusable_widgets[self.current_focus_index].focus()


class CodeScreen(ModalScreen):
	DEFAULT_CSS = """
	CodeScreen {
		#code {
			border: heavy $accent;
			margin: 2 4;
			scrollbar-gutter: stable;
			Static {
				width: auto;
			}
		}
	}
	"""
	BINDINGS = [("escape", "dismiss", "Dismiss code")]

	def __init__(self, title: str, code: str) -> None:
		super().__init__()
		self.code = code
		self.title = title

	def compose(self) -> ComposeResult:
		with ScrollableContainer(id="code"):
			yield Static(
				Syntax(
					self.code, lexer="python", indent_guides=True, line_numbers=True
				),
				expand=True,
			)

	def on_mount(self):
		code_widget = self.query_one("#code")
		code_widget.border_title = self.title
		code_widget.border_subtitle = "Escape to close"


class LogHandler(FileSystemEventHandler):
	def __init__(self, screen, file_path):
		self.screen = screen
		self.file_path = file_path
		self.file = open(file_path, 'r')
		self.file.seek(0, 2)  # Move to the end of the file

	def on_modified(self, event):
		if event.src_path == self.file_path:
			lines = self.file.readlines()
			for line in lines:
				self.screen.add_log_line(line)


class LogScreen(ModalScreen):
	"""Demonstrates Logs."""

	DEFAULT_CSS = """
	LogScreen {
		#log {
			width: 1fr;
			overflow-x: auto;
			border: heavy $border;
			margin: 2 4;
			padding: 1 2;
			scrollbar-gutter: stable;
			Static {
				width: auto;
			}
		}
	}
	"""

	BINDINGS = [("escape", "dismiss", "Dismiss code")]

	def __init__(self, title, log_file: str) -> None:
		super().__init__()
		self.title = title
		self.log_file_path = log_file
		self.observer = None
		self._is_running = True

	def compose(self) -> ComposeResult:
		yield RichLog(max_lines=10_000, id="log", highlight=True, wrap=False, markup=True)

	def on_mount(self) -> None:
		log_widget = self.query_one("#log")
		log_widget.border_title = self.title
		log_widget.border_subtitle = "Escape to close"

		self.display_initial_lines()
		self.start_watchdog()

	def _on_unmount(self) -> None:
		"""Start the watchdog observer when the screen is mounted."""
		self._is_running = False
		if self.observer:
			self.observer.stop()
			self.observer.join()

	def display_initial_lines(self):
		"""Display the last 100 lines of the log file."""
		with open(self.log_file_path, "r") as file:
			lines = file.readlines()[-250:]  # Read the last 250 lines
			rich_log = self.query_one(RichLog)
			for line in lines:
				rich_log.write(line.strip(), animate=True)  # No animation for initial lines

	def add_log_line(self, line: str):
		"""Add a new log line to the RichLog widget."""
		self.app.call_from_thread(self._add_log_line_async, line)

	@work
	async def _add_log_line_async(self, line: str):
		rich_log = self.query_one(RichLog)
		rich_log.write(line.strip(), animate=True)
		# Automatically scroll to the bottom if the user is not scrolling
		if rich_log.auto_scroll and not self.is_scrolling:
			rich_log.scroll_end(animate=True)

	def start_watchdog(self):
		"""Start the watchdog observer in a separate thread."""

		def run_watchdog():
			event_handler = LogHandler(self, self.log_file_path)
			self.observer = Observer()
			self.observer.schedule(event_handler, path=str(Path(self.log_file_path).parent), recursive=False)
			self.observer.start()

			try:
				while self._is_running:
					time.sleep(1)
			except KeyboardInterrupt:
				self.observer.stop()
			self.observer.join()

		threading.Thread(target=run_watchdog, daemon=True).start()


class ExecutorScreen(Screen):
	AUTO_FOCUS = None
	CSS = """
		ExecutorScreen {        
			align-horizontal: center;                                  
			Markdown { margin: 0; padding: 0 0; max-width: 100; background: transparent;}
		}
		Select {
			max-width: 100;
			margin: 0 0;
			align: center middle;
		}
		"""

	LOGS_DIR = Path(str(Path(os.path.abspath(__file__)).parent.parent) + "/logs")

	BINDINGS = [
		Binding("escape", "go_back", "Utilities", tooltip="Go to previous screen"),
		Binding("c", "show_code", "Code", tooltip="Show the code used to generate this screen"),
		Binding("ctrl+a", "void", "Run Utility", tooltip="Run the utility"),
		Binding("l", "show_log", "Show log", tooltip="Show log"),
	]

	def __init__(
		self,
		utility_info: UtilityInfo,
		name: str | None = None,
		id: str | None = None,
		classes: str | None = None,
	):
		super().__init__(name, id, classes)
		self.utility_info = utility_info

	def compose(self) -> ComposeResult:
		yield MCHeader()
		yield ExecutorArgs(self.utility_info)
		yield Footer()

	@on(events.Enter)
	@on(events.Leave)
	def on_enter(self, event: events.Enter):
		event.stop()
		self.set_class(self.is_mouse_over, "-hover")

	def action_go_back(self) -> None:
		"""Return to CategoriesScreen when Escape is pressed."""
		self.app.pop_screen()

	def action_show_log(self):
		date = datetime.today()
		count = 0
		while count < 7:
			log_file = (f"{self.LOGS_DIR}/{self.utility_info.title.lower().replace(' ', '_')}"
						f"-{date.strftime('%Y-%m-%d')}.log")

			if os.path.exists(log_file):
				self.app.push_screen(LogScreen("Utility Logs", log_file))
				break
			elif count == 6:
				self.notify(
					f"Could not get the log for utility {log_file}",
					title="Show log",
					severity="error",
				)
			date = date - timedelta(days=1)

	async def action_show_code(self):
		code = await self.get_file(self.utility_info.executable).wait()
		if code is None:
			self.notify(
				"Could not get the code for utility",
				title="Show code",
				severity="error",
			)
		else:
			await self.app.push_screen(CodeScreen("Utility Code", code))

	@work(thread=True)
	def get_file(self, file: str) -> str | None:
		"""Read file from disk, or return `None` on error."""
		try:
			with open(file, "rt", encoding="utf-8") as file_:
				return file_.read()
		except Exception:
			return None
