from .other_utils import to_rgb
from typing import Optional, Union, NamedTuple, Literal

C = dict[str, float] | list[int, int, int] | tuple[int, int, int]


class BorderMixin:
    """
    style: can be SOLID, DASHED, DOTTED, DOUBLE
    width: can be 1, 2, 3
    color: background color can be hex color '#ffffff' or [255, 255, 255] or {red: 1, green: 1, blue: 1}
    """

    def __init__(
            self,
            style: Literal['SOLID', 'DASHED', 'DOTTED', 'DOUBLE'] = str(),
            width: Literal[1, 2, 3] = int(),
            color: C = None
        ): # noqa
        self.style = style
        self.width = width
        self.color = to_rgb(color) if color else {}


class LeftBorder(BorderMixin):
    pass


class RightBorder(BorderMixin):
    pass


class TopBorder(BorderMixin):
    pass


class BottomBorder(BorderMixin):
    pass


class Borders(BorderMixin):
    """
    If you want to set one or more specific borders
    you should create instance like Borders(params, left=LeftBorder(params)]
    """

    def __init__(self, style: str = str(), width: int = int(), color=None, *,
                 left: Optional[LeftBorder] = None, right: Optional[RightBorder] = None,
                 top: Optional[TopBorder] = None, bottom: Optional[BottomBorder] = None):
        super().__init__(style=style, width=width, color=color)

        self.top = TopBorder(style, width, color) if not top else top
        self.bottom = BottomBorder(style, width, color) if not bottom else bottom
        self.left = LeftBorder(style, width, color) if not left else left
        self.right = RightBorder(style, width, color) if not right else right


class Cell:
    """
    name: must have view 'A1'
    value: can be a string, num or formula
    note: note for the cell
    bg_color: background color can be hex color '#ffffff' or [255, 255, 255] or {red: 1, green: 1, blue: 1}
    fr_color: foreground color can be hex color '#ffffff' or [255, 255, 255] or {red: 1, green: 1, blue: 1}
    font_family: make a font
    font_size: make a size
    """
    SUB = 65

    def __init__(
            self,
            name: Optional[str] = None,
            value: Optional[Union[str, int, float]] = None,
            note: Optional[str] = None,
            *,
            bg_color: C = None,
            fr_color: C = None,
            font_family: str = 'Arial',
            font_size: int = 10,
            bold: bool = False,
            italic: bool = False,
            strikethrough: bool = False,
            underline: bool = False,
            borders: Borders | None = None,
            formatted_value: Union[int, float, str] = None,  # readonly
            col_idx: int | None = None,
            row_idx: int | None = None,
    ):
        self.name = name.upper() if name else self.from_indexes_to_name(col_idx, row_idx) \
            if col_idx is not None and row_idx is not None else ''
        self.value = value
        self.note = note
        self.bg_color = to_rgb(bg_color) if bg_color else {'red': 1.0, 'green': 1.0, 'blue': 1.0}
        self.fr_color = to_rgb(fr_color) if fr_color else {'red': 0, 'green': 0, 'blue': 0}
        self.font_family = font_family
        self.font_size = font_size
        self.bold = bold
        self.italic = italic
        self.strikethrough = strikethrough
        self.underline = underline
        self.borders = borders if borders is not None else Borders()
        self.col_idx, self.row_idx = self.find_indexes(self.name) if (
                col_idx is None or row_idx is None) else (col_idx, row_idx)
        self.formatted_value = formatted_value

    def __repr__(self):
        data = f'name={repr(self.name)}, value={repr(self.value)}, note={repr(self.note)}'
        return 'Cell(' + data + ')'

    def to_json(self):
        new = {}
        for k, v in self.__dict__.items():
            print(k, v)
            if not (k.endswith('__') and k.startswith('__')) and not callable(v) and k != 'borders':
                new[k] = v
        return new

    @staticmethod
    def sep_name(name: str) -> tuple[str, int | None]:
        """
        Separates 'A1' to 'A' and 1
        """
        col = ''
        while not name.isdigit() and name:
            col += name[0]
            name = name[1:]
        col = col if col else None
        row = int(name) if name.isdigit() else None
        return col, row

    @staticmethod
    def find_indexes(name: str) -> tuple[int, int]:
        """
        Translates A1-notasion to column and row indexes
        """
        col, row = Cell.sep_name(name)
        if col:
            if len(col) == 1:
                col = ord(col) - 65
            elif len(col) == 2:
                temp = ord(col[0]) - 65 + 1
                col = 26 * temp + ord(col[1]) - 65
            else:
                temp1 = ord(col[0]) - 65 + 1
                temp2 = ord(col[1]) - 65 + 1
                col = 26 ** 2 * temp1 + 26 * temp2 + ord(col[2]) - 65
        return col, (row - 1 if row else row)

    @staticmethod
    def from_indexes_to_name(col: int, row: int) -> str:
        """
        Translate column and row indexes to A1-notasion
        """
        name = ''

        if col / 26 >= 53:
            temp = int((col ** 1 / 26) // 27)
            name += chr(64 + temp) if temp <= 25 else chr(65 + 25)
            col -= (temp) * 26 ** 2 if temp <= 25 else temp * 26 ** 2

        if col / 26 >= 27:
            name += 'A'
            col -= 26 ** 2

        if col // 26 > 0:
            temp = col // 26
            name += chr(64 + temp)
            col -= temp * 26
        name += chr(64 + col + 1) + str(row + 1)
        return name


class Sheet(NamedTuple):
    id: int
    title: str
