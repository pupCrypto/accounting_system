from abc import ABC
from .Dataclasses import Cell
from typing import Union, Iterable, Optional


class GoogleSheetsInterface(ABC):
    def update_cells(self, sheet_id: int, cells: Union[Cell, Iterable[Cell]]) -> dict:
        """Replace cells to given cells in spreadsheet
        Each given cell must have 'name' value"""
        raise NotImplementedError

    def append(self, cells: Union[Cell, Iterable[Cell]]) -> dict:
        """Append cell or cells to the end of the spreadsheet"""
        raise NotImplementedError

    def get_values(self, sheet_name: str = str(), from_: Optional[str] = None, to: Optional[str] = None)\
            -> list[tuple[Cell]]:
        """Return values from range(from_, to)
        If from_ is None, return values from range(A, to)
        If to is None, return values from range(from_, ZZZ)
        If the both, return values from range(A, ZZZ)"""
        raise NotImplementedError

    def add_sheet(self, title: str) -> dict:
        """Create table in the spreadsheet"""
        raise NotImplementedError

    def create_spreadsheet(self, title: str) -> dict:
        """Create new spreadsheet"""
        raise NotImplementedError

    def _provide_access(self):
        """Provide access to google account"""
        raise NotImplementedError

    def copy_to_spreadsheet(self, another_spreadsheet_id: str, sheet_name: Optional[str] = None,
                            sheet_id: int = int()) -> dict:
        """Copy spreadsheet by sheet_id and move it to another spreadsheet

        :param another_spreadsheet_id: Id where needed to copy to
        :param sheet_name: where needed to copy from
        :param sheet_id: where needed to copy from

        If copy_all is True sheet_name and sheet_id do not matter"""
        raise NotImplementedError

    def copy_all_to_spreadsheet(self, another_spreadsheet_id: str) -> list[dict]:
        """Copy all sheets to given spreadsheet"""
        raise NotImplementedError

    def paste_data(self, cell: Cell) -> dict:
        """Inserts data into the spreadsheet starting at the specified coordinate"""
        raise NotImplementedError

    def _get_all_sheets(self) -> dict:
        """Get list of all sheets of spreadsheet"""
        raise NotImplementedError
