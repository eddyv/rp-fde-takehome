"""Terminal client for paging through GET /edits without hand-copying cursors.

Run with the API up (see README):

    uv run --directory service python -m app.tui
    EDITS_API_URL=http://localhost:8000 uv run --directory service python -m app.tui

Keys: n next page, p previous page, r reset to first page, f refresh current
page in place, q quit.
Tab into the label/status selects to filter; changing either resets to page 1.
The app polls GET /stats every EDITS_TUI_POLL_SECONDS (default 5, <=0 turns it
off): on the first page new edits appear automatically; while paging deeper a
"new edits" note shows in the status line instead, so the view never jumps.
The data table keeps focus by default — use arrow keys to scroll to columns
that don't fit on screen (the free-text columns are last). Press enter on a
row for the full record on its own screen.
"""

import os

import httpx
from textual import work
from textual.app import App, ComposeResult
from textual.containers import Horizontal, VerticalScroll
from textual.screen import ModalScreen
from textual.widgets import DataTable, Footer, Header, Select, Static

from app.models import VALID_LABELS, VALID_STATUSES

API_URL = os.environ.get("EDITS_API_URL", "http://localhost:8000")
POLL_SECONDS = float(os.environ.get("EDITS_TUI_POLL_SECONDS", "5"))

TIMESTAMP_COLUMNS = ("event_time", "processed_at")

# Every EditOut field (api.py). Short, fixed-vocabulary columns first so
# they're visible without scrolling; free-text columns (title/editor/comment/
# reasoning) last, since they're the ones worth scrolling to or opening via
# the row-detail screen instead.
COLUMNS = (
    "id",
    "label",
    "status",
    "confidence",
    "byte_delta",
    "model",
    "event_time",
    "processed_at",
    "title",
    "editor",
    "comment",
    "reasoning",
)


def _format_cell(column: str, value: object) -> str:
    if value is None:
        return ""
    if column == "confidence":
        return f"{value:.2f}"
    if column in TIMESTAMP_COLUMNS:
        # "2026-07-14T19:08:12.643205+00:00" -> "07-14 19:08:12": drops
        # sub-second precision and the timezone; still full enough to sort by.
        return str(value)[5:19].replace("T", " ")
    return str(value)


class DetailScreen(ModalScreen[None]):
    """Full, untruncated view of one edit's EditOut fields."""

    CSS = """
    DetailScreen { align: center middle; }
    #detail_panel {
        width: 90%; max-width: 100; height: auto; max-height: 90%;
        border: round $accent; padding: 1 2; background: $surface;
    }
    """
    BINDINGS = [("escape,enter,q", "dismiss_detail", "Close")]

    def __init__(self, item: dict) -> None:
        super().__init__()
        self.item = item

    def compose(self) -> ComposeResult:
        lines = []
        for field in COLUMNS:
            value = self.item.get(field)
            lines.append(f"[bold]{field}[/bold]: {value if value is not None else '—'}")
        with VerticalScroll(id="detail_panel"):
            yield Static("\n\n".join(lines))

    def action_dismiss_detail(self) -> None:
        self.dismiss()


class EditsBrowser(App):
    CSS = """
    #filters { height: 3; }
    #filters Select { width: 24; margin-right: 1; }
    #status_line { height: 1; padding: 0 1; color: $text-muted; }
    """
    BINDINGS = [
        ("n", "next_page", "Next page"),
        ("p", "prev_page", "Previous page"),
        ("r", "reset", "First page"),
        ("f", "refresh", "Refresh"),
        ("q", "quit", "Quit"),
    ]

    def __init__(self, base_url: str = API_URL) -> None:
        super().__init__()
        self.base_url = base_url
        self.client = httpx.AsyncClient(base_url=base_url, timeout=10.0)
        self.cursor: str | None = None
        self.next_page: str | None = None
        self.previous_page: str | None = None
        self.label: str | None = None
        self.status: str | None = None
        self.items_by_id: dict[str, dict] = {}
        self.row_count: int = 0
        self.new_edits_available: bool = False
        self._last_stats: dict | None = None

    def compose(self) -> ComposeResult:
        yield Header()
        with Horizontal(id="filters"):
            yield Select(
                [(v, v) for v in sorted(VALID_LABELS)],
                prompt="label: any",
                id="label_select",
            )
            yield Select(
                [(v, v) for v in sorted(VALID_STATUSES)],
                prompt="status: any",
                id="status_select",
            )
        yield Static(id="status_line")
        yield DataTable(id="table")
        yield Footer()

    def on_mount(self) -> None:
        table = self.query_one(DataTable)
        table.add_columns(*COLUMNS)
        table.cursor_type = "row"
        table.focus()
        self.load_page()
        if POLL_SECONDS > 0:
            self.set_interval(POLL_SECONDS, self.poll_stats)

    def on_select_changed(self, event: Select.Changed) -> None:
        value = None if event.select.is_blank() else str(event.value)
        if event.select.id == "label_select":
            self.label = value
        elif event.select.id == "status_select":
            self.status = value
        self.cursor = None
        self.load_page()

    @work(exclusive=True, group="page")
    async def load_page(self) -> None:
        params = {"size": 50}
        if self.cursor:
            params["cursor"] = self.cursor
        if self.label:
            params["label"] = self.label
        if self.status:
            params["status"] = self.status

        status_line = self.query_one("#status_line", Static)
        try:
            # The single await before any mutation: a cancelled (superseded)
            # worker leaves all state untouched.
            response = await self.client.get("/edits", params=params)
            response.raise_for_status()
        except httpx.HTTPError as exc:
            status_line.update(f"[red]request failed: {exc}[/red]")
            return

        page = response.json()
        self.next_page = page.get("next_page")
        self.previous_page = page.get("previous_page")

        table = self.query_one(DataTable)
        selected_id = None
        if table.row_count > 0:
            try:
                cell_key = table.coordinate_to_cell_key(table.cursor_coordinate)
                selected_id = cell_key.row_key.value
            except Exception:
                selected_id = None
        table.clear()
        self.items_by_id.clear()
        for item in page["items"]:
            self.items_by_id[item["id"]] = item
            table.add_row(
                *(_format_cell(col, item.get(col)) for col in COLUMNS),
                key=item["id"],
            )
        # Keep the highlight on the same edit if it's still on the page; new
        # rows sorting in above it must not silently move the selection.
        if selected_id is not None and selected_id in self.items_by_id:
            row = list(self.items_by_id).index(selected_id)
            table.move_cursor(row=row)

        self.row_count = len(page["items"])
        self.new_edits_available = False
        self._update_status_line()

    def _update_status_line(self) -> None:
        filters = f"label={self.label or 'any'} status={self.status or 'any'}"
        prev_flag = "yes" if self.previous_page else "no"
        next_flag = "yes" if self.next_page else "no"
        nav = f"prev={prev_flag} next={next_flag}"
        text = f"{filters}  |  {self.row_count} rows  |  {nav}"
        if self.new_edits_available:
            text += "  |  [yellow]new edits — f to refresh[/yellow]"
        self.query_one("#status_line", Static).update(text)

    @work(exclusive=True, group="stats")
    async def poll_stats(self) -> None:
        # /stats is unfiltered, so with a label/status filter active this can
        # fire for edits outside the filtered view; a page-1 reload then just
        # repaints the same rows, which is harmless. Upserts that change
        # neither the total nor the label/status counts stay undetected.
        try:
            response = await self.client.get("/stats")
            response.raise_for_status()
            stats = response.json()
        except httpx.HTTPError:
            return  # background poll: fail silently, never touch the UI
        if self._last_stats is None:
            self._last_stats = stats  # first poll is just the baseline
            return
        if stats == self._last_stats:
            return
        self._last_stats = stats
        modal_open = len(self.screen_stack) > 1
        if self.cursor is None and not modal_open:
            # First page is where new edits sort to; reload it in place.
            self.load_page()
        else:
            self.new_edits_available = True
            self._update_status_line()

    def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        item = self.items_by_id.get(str(event.row_key.value))
        if item is not None:
            self.push_screen(DetailScreen(item))

    def action_next_page(self) -> None:
        if self.next_page:
            self.cursor = self.next_page
            self.load_page()

    def action_prev_page(self) -> None:
        if self.previous_page:
            self.cursor = self.previous_page
            self.load_page()

    def action_reset(self) -> None:
        self.cursor = None
        self.load_page()

    def action_refresh(self) -> None:
        # Unlike action_reset this keeps the cursor: same page, fresh data.
        self.load_page()

    async def on_unmount(self) -> None:
        await self.client.aclose()


def main() -> None:
    EditsBrowser().run()


if __name__ == "__main__":
    main()
