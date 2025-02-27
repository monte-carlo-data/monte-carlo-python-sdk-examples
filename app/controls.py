from textual.screen import Screen
from textual.binding import Binding


class Controls(Screen):

	def __init__(self, name: str | None = None, id: str | None = None, classes: str | None = None,):
		super().__init__(name, id, classes)
		self.grid = None

	BINDINGS = [
		Binding("up", "move('up')", "Move Up", priority=True),
		Binding("down", "move('down')", "Move Down", priority=True),
		Binding("left", "move('left')", "Move Left", priority=True),
		Binding("right", "move('right')", "Move Right", priority=True),
	]

	def action_move_grid(self, direction: str, cls) -> None:
		"""Move focus between utilities in the grid based on arrow keys."""
		if not self.grid:
			return

		focusables = list(self.grid.query(cls))  # Convert generator to list
		if not focusables:
			return

		current_focus = self.app.focused
		if current_focus not in focusables:
			focusables[0].focus()
			return

		current_index = focusables.index(current_focus)
		# Dynamically determine row length based on terminal width
		terminal_width = self.app.size.width  # Get terminal width
		min_column_width = 40  # Must match ItemGrid's min_column_width
		row_length = max(1, terminal_width // min_column_width)  # Ensure at least 1

		# Calculate new focus index
		if direction == "up":
			new_index = max(0, current_index - row_length)
		elif direction == "down":
			new_index = min(len(focusables) - 1, current_index + row_length)
		elif direction == "left":
			new_index = max(0, current_index - 1)
		elif direction == "right":
			new_index = min(len(focusables) - 1, current_index + 1)
		else:
			return

		focusables[new_index].focus()
