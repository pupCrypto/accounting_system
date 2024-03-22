from pprint import pprint
from typing import Iterator, Literal, TypeVar

import httplib2
import apiclient.discovery
from oauth2client.service_account import ServiceAccountCredentials
from .interface import *
from .Dataclasses import Cell
from .utils import from_cells_to_google_format, from_google_format_to_cell, sort_cells, \
    to_rows_format, parse_sheets, find_sheet


ColumnCount = TypeVar('CoulmnCount', bound=int)
RowCount = TypeVar('RowCount', bound=int)

class GoogleSheets(GoogleSheetsInterface):
    def __init__(self, creds: dict, spreadsheetId: str):
        credentials = ServiceAccountCredentials._from_parsed_json_keyfile(
            creds,
            [
                'https://www.googleapis.com/auth/spreadsheets',
                'https://www.googleapis.com/auth/drive',
                'https://www.googleapis.com/auth/drive.file',
            ]
        )
        self.httpAuth = credentials.authorize(httplib2.Http())
        self.sheets_v4 = apiclient.discovery.build('sheets', 'v4', http=self.httpAuth)
        self.spreadsheetId = spreadsheetId
        self._sheets = None

    @property
    def sheets(self):
        if self._sheets is None:
            self._sheets = self.get_all_sheets()
        return self._sheets

    def add_sheet(self, title: str) -> dict:
        sheet_body = {
            'addSheet': {
                'properties': {
                    'title': title
                }
            }
        }
        response = self.sheets_v4.spreadsheets().batchUpdate(
            spreadsheetId=self.spreadsheetId,
            body={'requests': [sheet_body]}
        ).execute()
        return response

    def unmerge_cells(self, from_cell: Cell, to_cell: Cell, sheet_id: int):
        sri = from_cell.row_idx
        eri = to_cell.row_idx + 1
        sci = from_cell.col_idx
        eci = to_cell.col_idx + 1
        body = {
            'unmergeCells': {
                'range': {
                    'sheetId': sheet_id,
                    'startRowIndex': sri,
                    'endRowIndex': eri,
                    'startColumnIndex': sci,
                    'endColumnIndex': eci
                }
            }
        }
        response = self.sheets_v4.spreadsheets().batchUpdate(
            spreadsheetId=self.spreadsheetId,
            body={'requests': [body]}
        ).execute()
        return response

    def append_dimension(self, dimension: Literal['ROWS', 'COLUMNS', 'DIMENSION_UNSPECIFIED'], sheet_id: int,
                         length: int):
        body = {
            'appendDimension': {
                'sheetId': sheet_id,
                'dimension': dimension,
                'length': length,
            }
        }
        response = self.sheets_v4.spreadsheets().batchUpdate(
            spreadsheetId=self.spreadsheetId,
            body={'requests': [body]}
        ).execute()
        return response

    def get_size_of_sheet(self, sheet_id: int = 0) -> tuple[ColumnCount, RowCount]:
        """
        Return column and row count of specified sheet
        """
        response = self.sheets_v4.spreadsheets().get(spreadsheetId=self.spreadsheetId).execute()
        for sheet in response['sheets']:
            if sheet['properties']['sheetId'] == sheet_id:
                grid_props = sheet['properties']['gridProperties']
                return grid_props['columnCount'], grid_props['rowCount']
        raise ValueError('Specified sheet_id is not existing in current spreadsheet')

    def insert_range(self, from_cell: Cell, to_cell: Cell, shift_dimension: Literal['ROWS', 'COLUMNS'], sheet_id: int):
        sri = from_cell.row_idx
        eri = to_cell.row_idx + 1
        sci = from_cell.col_idx
        eci = to_cell.col_idx + 1
        body = {
            'insertRange': {
                'range': {
                    'sheetId': sheet_id,
                    'startRowIndex': sri,
                    'endRowIndex': eri,
                    'startColumnIndex': sci,
                    'endColumnIndex': eci
                },
                'shiftDimension': shift_dimension
            }
        }
        response = self.sheets_v4.spreadsheets().batchUpdate(
            spreadsheetId=self.spreadsheetId,
            body={'requests': [body]}
        ).execute()
        return response

    def merge_cells(self, from_cell: Cell, to_cell: Cell, sheet_id: int) -> dict:

        sri = from_cell.row_idx
        eri = to_cell.row_idx + 1
        sci = from_cell.col_idx
        eci = to_cell.col_idx + 1

        body = {
            'mergeCells': {
                'range': {
                    'sheetId': sheet_id,
                    'startRowIndex': sri,
                    'endRowIndex': eri,
                    'startColumnIndex': sci,
                    'endColumnIndex': eci
                },
                'mergeType': 'MERGE_ALL'
            }
        }
        response = self.sheets_v4.spreadsheets().batchUpdate(
            spreadsheetId=self.spreadsheetId,
            body={'requests': [body]}
        ).execute()
        return response

    def append(self, cells: Iterable[Iterable[Cell]], sheet_id: int = 0) -> dict:
        """REDO must have view [[], []]"""
        rows = []
        for row in cells:
            rows.append({'values': from_cells_to_google_format(row)})
        body = {
            'appendCells': {
                'sheetId': sheet_id,
                'rows': rows,
                'fields': '*'
            }
        }

        response = self.sheets_v4.spreadsheets().batchUpdate(
            spreadsheetId=self.spreadsheetId,
            body={'requests': [body]}
        ).execute()
        return response

    def get_values(self, sheet_name: str = None, sheet_id: int = None, from_: Optional[str | Cell] = None,
                   to: Optional[str | Cell] = None) -> Iterator[Cell]:

        if sheet_name is None:
            sheet_name = find_sheet(self.sheets, id=sheet_id).title

        if not from_:
            from_ = 'A1'

        if not to:
            to = 'ZZZ'
        if isinstance(from_, Cell):
            from_ = from_.name
        if isinstance(to, Cell):
            to = to.name

        ranges = ['{0}!{1}:{2}'.format(sheet_name, from_, to)]
        response = self.sheets_v4.spreadsheets().get(
            spreadsheetId=self.spreadsheetId,
            ranges=ranges,
            includeGridData=True
        ).execute()
        return from_google_format_to_cell(response, from_)

    def create_spreadsheet(self, title: str) -> dict:
        spreadsheet = {
            'properties': {
                'title': title,
            }
        }
        response = self.sheets_v4.spreadsheets().create(body=spreadsheet,
                                                        fields='fileId').execute()
        return response

    def _provide_access(self):
        drive_v2 = apiclient.discovery.build('drive', 'v2', http=self.httpAuth)
        response = drive_v2.files().list().execute()
        return response

    def update_cells(self, cells: Iterable[Cell], sheet_id: int = 0) -> list:
        if not cells:
            raise Exception('"cells" must not be empty')
        responses = []
        cells = sort_cells(cells)
        rows = to_rows_format(cells)
        requests = []
        for row, columnIndex, rowIndex in rows:
            body = {
                'updateCells': {
                    'rows': [row],
                    'fields': '*',
                    'start': {
                        'sheetId': sheet_id,
                        'rowIndex': rowIndex,
                        'columnIndex': columnIndex,
                    },
                }
            }
            cells = cells[len(row):]
            requests.append(body)
        response = self.sheets_v4.spreadsheets().batchUpdate(
            spreadsheetId=self.spreadsheetId,
            body={'requests': requests},
        ).execute()
        # responses.append(response)
        return response

    def copy_to_spreadsheet(self, another_spreadsheet_id: str, sheet_name: Optional[str] = None,
                            sheet_id: int = int()) -> list[dict]:

        if sheet_name:
            sheet_id = find_sheet(self.sheets, title=sheet_name)

        body = {
            'destinationSpreadsheetId': another_spreadsheet_id
        }

        response = self.sheets_v4.spreadsheets().sheets().copyTo(
            spreadsheetId=self.spreadsheetId,
            sheetId=sheet_id,
            body=body,
        ).execute()
        return response

    def copy_all_to_spreadsheet(self, another_spreadsheet_id: str) -> list[dict]:

        body = {
            'destinationSpreadsheetId': another_spreadsheet_id
        }
        responses = []
        for sheet_id in self.sheets.values():
            response = self.sheets_v4.spreadsheets().sheets().copyTo(
                spreadsheetId=self.spreadsheetId,
                sheetId=sheet_id,
                body=body,
            ).execute()
            responses.append(response)
        return responses

    def get_all_sheets(self):
        response = self.sheets_v4.spreadsheets().get(
            spreadsheetId=self.spreadsheetId
        ).execute()
        return parse_sheets(response)

# TODO https://developers.google.com/drive/api/v2/reference/permissions#resource
# TODO https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/request#updatecellsrequest
