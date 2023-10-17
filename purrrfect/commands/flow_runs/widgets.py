from rich.text import Text

from textual.app import App, ComposeResult
from textual.widgets import DataTable
from textual import log
from textual.logging import TextualHandler
import logging

logging.basicConfig(
    level="NOTSET",
    handlers=[TextualHandler()],
)

ROWS = [
    ("lane", "swimmer", "country", "time"),
    (4, "Joseph Schooling", "Singapore", 50.39),
    (2, "Michael Phelps", "United States", 51.14),
    (5, "Chad le Clos", "South Africa", 51.14),
    (6, "László Cseh", "Hungary", 51.14),
    (3, "Li Zhuhao", "China", 51.26),
    (8, "Mehdy Metella", "France", 51.58),
    (7, "Tom Shields", "United States", 51.73),
    (1, "Aleksandr Sadovnikov", "Russia", 51.84),
    (10, "Darren Burns", "Scotland", 51.84),
]


class TableApp(App):
    data_ = None
    bindings = {"q": "quit", "enter": "select_row"}

    def __init__(self, table: list[dict]) -> None:
        self.table = table
        self.selected_rows: list[int] = []
        super().__init__()

    def compose(self) -> ComposeResult:
        yield DataTable()

    def on_mount(self) -> None:
        self.data_ = None
        table = self.query_one(DataTable)
        keys = table.add_columns(*self.table[0])
        log(f"Keys: {keys}")
        table.cursor_type = "row"
        for row in self.table[1:]:
            # Adding styled and justified `Text` objects instead of plain strings.
            styled_row = [
                Text(str(cell), style="italic #03AC13", justify="right") for cell in row
            ]
            table.add_row(*styled_row)

    def on_data_table_row_highlighted(self, message: DataTable.RowHighlighted) -> None:
        self.current_row = message.cursor_row

    def select_row(self) -> None:
        self.selected_rows.append(self.current_row)

    def on_data_table_row_selected(self, message: DataTable.RowSelected):
        self.selected_rows.append(message.cursor_row)

    def return_value(self):
        return self.selected_rows
