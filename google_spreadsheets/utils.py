from .Dataclasses import Cell, Borders, LeftBorder, RightBorder, TopBorder, BottomBorder, Sheet
from typing import Iterable, Union, Tuple, List, Sequence, Any, Iterator


def from_cells_to_google_format(cells: Iterable[Cell]) -> list[dict]:
    values = []
    for cell in cells:
        obj = {'userEnteredValue': {},
               'userEnteredFormat': {
                   'textFormat': {},
                   'backgroundColor': {},
               }}
        if cell.value is not None:
            try:
                if cell.value.startswith('='):
                    obj['userEnteredValue']['formulaValue'] = cell.value
                else:
                    obj['userEnteredValue']['stringValue'] = cell.value
            except AttributeError:
                obj['userEnteredValue']['numberValue'] = cell.value
        else:
            obj.pop('userEnteredValue')

        if (note := cell.note) is not None:
            obj['note'] = note

        if borders := borders_to_google_format(cell.borders):
            borders = {key: value for key, value in borders.items() if value}
            if borders:
                obj['userEnteredFormat']['borders'] = borders
        obj['userEnteredFormat']['textFormat']['bold'] = cell.bold
        obj['userEnteredFormat']['textFormat']['italic'] = cell.italic
        obj['userEnteredFormat']['textFormat']['strikethrough'] = cell.strikethrough
        obj['userEnteredFormat']['textFormat']['underline'] = cell.underline
        obj['userEnteredFormat']['textFormat']['fontSize'] = cell.font_size
        obj['userEnteredFormat']['textFormat']['fontFamily'] = cell.font_family
        obj['userEnteredFormat']['textFormat']['foregroundColor'] = cell.fr_color
        obj['userEnteredFormat']['backgroundColor'] = cell.bg_color

        values.append(obj)
    return values


def borders_to_google_format(borders: Borders) -> dict:
    result = {}
    for side in ['top', 'bottom', 'left', 'right']:
        result[side] = {}
        for attr in ['style', 'color', 'width']:
            if value := eval(f'borders.{side}.{attr}'):
                result[side] |= {attr: value}
    return result


def borders_from_google_format(dictionary: dict) -> Borders:
    borders = dictionary
    try:
        top = TopBorder(
            borders['top']['style'],
            borders['top']['width'],
            borders['top']['color'],
        )
    except (KeyError, TypeError):
        top = None
    try:
        bottom = BottomBorder(
            borders['bottom']['style'],
            borders['bottom']['width'],
            borders['bottom']['color'],
        )
    except (KeyError, TypeError):
        bottom = None

    try:
        left = LeftBorder(
            borders['left']['style'],
            borders['left']['width'],
            borders['left']['color'],
        )
    except (KeyError, TypeError):
        left = None

    try:
        right = RightBorder(
            borders['right']['style'],
            borders['right']['width'],
            borders['right']['color'],
        )
    except (KeyError, TypeError):
        right = None
    return Borders(top=top, bottom=bottom, left=left, right=right)


# def calc_ords(chars: str):
#     result = reduce(lambda a, b: ord(a) + ord(b), chars)
#     return result if isinstance(result, int) else ord(result)


def from_google_format_to_cell(response: dict, from_: str) -> Iterator[Cell]:
    """Inspect given data and return list of Cells
    TODO Not the whole data yet"""
    col, row = Cell.find_indexes(from_)

    data = inspector(response, ['data'])
    for idx in range(len(data['data'])):
        i = 0
        try:
            rowData = data['data'][idx]['rowData']
        except KeyError:
            i += 1
            continue
        for rowData in rowData:
            j = 0
            try:
                values = rowData['values']
            except KeyError:
                i += 1
                continue

            for val in values:
                name = Cell.from_indexes_to_name(col + j, row + i)

                try:
                    value = get_value(inspector(val, ['userEnteredValue'])['userEnteredValue'])
                except KeyError:
                    value = str()

                keys = [
                    'note', 'textFormat', 'formattedValue', 'backgroundColor', 'foregroundColor',
                    'fontFamily', 'bold', 'italic', 'underline', 'strikethrough', 'fontSize', 'borders'
                ]
                val_data = inspector(val, keys.copy())

                borders = Borders()
                if all_borders := key_error_handle(val_data, 'borders'):
                    borders = borders_from_google_format(all_borders)

                value = value if value else None
                note = key_error_handle(val_data, 'note') if key_error_handle(val_data, 'note') else None

                cell = Cell(
                    name, value, note,
                    bg_color=key_error_handle(val_data, 'backgroundColor'),
                    fr_color=key_error_handle(val_data, 'foregroundColor'),
                    font_family=key_error_handle(val_data, 'fontFamily') if key_error_handle(val_data,
                                                                                             'fontFamily') else 'Arial',
                    font_size=key_error_handle(val_data, 'fontSize') if key_error_handle(val_data, 'fontSize') else 10,
                    bold=key_error_handle(val_data, 'bold') if key_error_handle(val_data, 'bold') else False,
                    italic=key_error_handle(val_data, 'italic') if key_error_handle(val_data, 'italic') else False,
                    strikethrough=key_error_handle(val_data, 'strikethrough') if key_error_handle(val_data,
                                                                                                  'strikethrough') else False,
                    underline=key_error_handle(val_data, 'underline') if key_error_handle(val_data,
                                                                                          'underline') else False,
                    borders=borders if borders else None,
                    formatted_value=key_error_handle(val_data, 'formattedValue') if key_error_handle(val_data,
                                                                                                     'formattedValue') else None
                )
                yield cell
                j += 1
            i += 1


def key_error_handle(dictionary, key):
    try:
        return dictionary[key]
    except KeyError:
        return None


def get_value(dictionary: dict) -> Any:
    return list(dictionary.values())[0]


def find_range(range_: str):
    sing = range_.index('!') + 1
    range_ = range_[sing:].split(':')
    return range_


def sort_cells(cells: Iterable[Cell], by: str = 'both'):
    sorted_cells = []
    if by == 'both':
        sorted_cells.extend(sorted(cells, key=lambda x: sum(Cell.find_indexes(x.name))))
    elif by == 'col':
        sorted_cells.extend(sorted(cells, key=lambda x: Cell.find_indexes(x.name)[0]))
    elif by == 'row':
        sorted_cells.extend(sorted(cells, key=lambda x: Cell.find_indexes(x.name)[1]))
    else:
        raise ValueError('by can be only "both", "row" or "col"')
    return sorted_cells


def inspector(some_dict, keys: Union[List[str], Tuple[str]]) -> dict:
    '''
    "ba" - only for current block code
    another one - simple searching
    '''

    if isinstance(keys, tuple):
        keys = list(keys)

    res = {}
    while keys:
        if isinstance(some_dict, dict) and any((key := i) in some_dict for i in keys):
            res[key] = some_dict[key]
            keys.remove(key)
        else:
            try:
                items = some_dict.values()
            except AttributeError:
                items = some_dict
            try:
                for item in items:
                    if isinstance(item, dict):
                        res |= inspector(item, keys)
                    elif isinstance(item, Sequence) and not isinstance(item, str):
                        for i in item:
                            res |= inspector(i, keys)
                    else:
                        continue
                else:
                    break
            except:
                break
    return res


def additional_sort(cells: list[Cell]):
    result = []
    row = []
    flag = cells[0].row_idx
    for i in cells:
        if i.row_idx == flag:
            row.append(i)
        else:
            flag = i.row_idx
            result.extend(
                sort_cells(row, 'col')
            )
            row = [i]
    else:
        result.extend(
            sort_cells(row, 'col')
        )

    return result


def to_rows_format(cells: list[Cell]) -> \
        list[Union[tuple[dict[str, list[dict[str]]], Any, Any], tuple[dict[str, list[dict[str]]], Any, Any]]]:
    cells = sort_cells(cells, 'row')
    cells = additional_sort(cells)

    col_idx, row_idx = cells[0].col_idx, cells[0].row_idx
    result = []
    row = []
    row_ind = cells[0].row_idx
    col_ind = cells[0].col_idx
    for i in cells:
        if i.row_idx == row_ind and abs(col_ind - i.col_idx) < 2:
            row.append(i)
        else:
            row_ind = i.row_idx
            col_ind = i.col_idx
            values = {'values': from_cells_to_google_format(row)}
            result.append(
                (values, col_idx, row_idx)
            )
            col_idx, row_idx = i.col_idx, i.row_idx
            row = [i]
    else:
        values = {'values': from_cells_to_google_format(row)}
        result.append(
            (values, col_idx, row_idx)
        )

    return result


# TODO добавить dataclass Sheet
def parse_sheets(response: dict) -> list[Sheet]:
    sheets = []
    for sheet in response['sheets']:
        title = sheet['properties']['title']
        sheet_id = sheet['properties']['sheetId']
        sheets.append(Sheet(sheet_id, title))
    return sheets


def find_sheet(sheets: list[Sheet], id: int = None, title: str = None) -> Sheet:
    if id is None and title is None:
        raise ValueError('You must give although one argument')

    for sheet in sheets:
        if id == sheet.id or title == sheet.title:
            return sheet
    else:
        raise Exception('Sheet not found')
