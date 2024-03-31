from pprint import pprint

import time
import json
import datetime
from typing import TypeVar, Literal, Any
from dataclasses import dataclass
from google_spreadsheets.api import GoogleSheets, Cell
from google_spreadsheets.Dataclasses import Borders, RightBorder


ColIdx = TypeVar('ColIdx', bound=int)
RowIdx = TypeVar('RowIdx', bound=int)

SEASONS = (
    'Январь',
    'Февраль',
    'Март',
    'Апрель',
    'Май',
    'Июнь',
    'Июль',
    'Август',
    'Сентябрь',
    'Октябрь',
    'Ноябрь',
    'Декабрь',
)


@dataclass
class SheetProduct:
    plu: int
    name: str
    price: int
    row_idx: RowIdx


@dataclass
class UnderDateColumn:
    col_idx: ColIdx
    row_idx: RowIdx
    name: str


class SheetBase:
    def add_col_idx(self, num: int):
        self.col_idx += num
        if self.cols is not None:
            for under_date_col in self.cols:
                under_date_col.col_idx += num

    def find_col_idx(self, name: str) -> ColIdx | None:
        if self.cols is None:
            raise TypeError('cols attr is None')
        for col in self.cols:
            if col.name == name:
                return col.col_idx
        return None

    def to_cell(self, value: Any = None) -> Cell:
        return Cell(value=value, col_idx=self.col_idx, row_idx=self.row_idx)


@dataclass
class SheetDate(SheetBase):
    """Date"""
    date: datetime.date
    col_idx: RowIdx
    row_idx: ColIdx
    wide: int = 1
    cols: list[UnderDateColumn] | None = None


@dataclass
class SheetMonth(SheetBase):
    """Month"""
    name: str
    col_idx: ColIdx
    row_idx: RowIdx
    month: int = -1
    wide: int = 1
    cols: list[UnderDateColumn] | None = None

    def __post_init__(self):
        self.month = SEASONS.index(self.name) + 1


@dataclass
class Shipment:
    plu: int
    weight: float | int


@dataclass
class Income:
    plu: int
    weight: float | int


@dataclass
class Sale:
    plu: int
    weight: float | int
    customer: str


@dataclass
class Inventory:
    plu: int
    weight: float | int


class RateLimitWrapper(GoogleSheets):
    def _dec(self, method):
        def inner(*args, **kwargs):
            while True:
                try:
                    return method(*args, **kwargs)
                except Exception as e:
                    if 'RATE_LIMIT_EXCEEDED' in str(e):
                        time.sleep(5)
                        continue
                    else:
                        raise e
        return inner

    def __getattribute__(self, __name: str):
        var = super().__getattribute__(__name)
        if callable(var) and __name != '_dec':
            var = self._dec(var)
        return var


class AccountingSpreadsheet:
    """
    Class implements methods to manipulate accounting google spreadsheet
    """
    def __init__(self, spreadsheet_id: str, gid: int, creds_path: str, summarize_forward: bool = True) -> None:
        with open(creds_path, encoding='utf8') as file:
            credentials_str = file.read()
        credentials = json.loads(credentials_str)
        self._google = RateLimitWrapper(credentials, spreadsheet_id)
        self.gid = gid
        self._dates: list[SheetDate] | None = None
        self._products: list[SheetProduct] | None = None
        self._months: list[SheetMonth] | None = None
        self._summarize_forward = summarize_forward

        # call post init method
        self._post_init()

    def _post_init(self):
        if self._summarize_forward is True:
            self.summarize_month('last_date', update_exist=False)

    def _binary_dates_srch(self, target: datetime.date) -> int | None:
        """
        self._dates binary search
        """
        if target is None:
            return None
        # Check base case
        for i, sd in enumerate(self.dates):
            if sd.date == target:
                return i
        return None

    def _insert_date(self, target: datetime.date) -> int | None:
        """
        Inserts date if self.dates does not contain it
        """
        for i, sd in enumerate(self.dates):
            if target == sd.date:
                return None
            if target < sd.date:
                index = i
                break
        else:
            index = i + 1

        high = index
        prev_idx = index - 1
        prev_sheet_date = self.dates[prev_idx]
        month_idx = [m.month for m in self.months].index(prev_sheet_date.date.month)
        month = self.months[month_idx]
        sheet_month: SheetMonth | None = None
        add = 0 if prev_sheet_date.date.month == target.month else month.wide
        row_count, _ = self._google.get_size_of_sheet(self.gid)
        range_cells = (  # from to shift
            Cell(col_idx=prev_sheet_date.col_idx + prev_sheet_date.wide + add, row_idx=prev_sheet_date.row_idx), # noqa
            Cell(col_idx=prev_sheet_date.col_idx + prev_sheet_date.wide + add + 8, row_idx=prev_sheet_date.row_idx + row_count), # noqa
        )
        insert_range = lambda: self._google.insert_range( # noqa
                from_cell=range_cells[0],
                to_cell=range_cells[1],
                shift_dimension='COLUMNS',
                sheet_id=self.gid
            )
        self._google.append_dimension('COLUMNS', self.gid, 9)
        insert_range()
        from_cell, to_cell = (
            Cell(col_idx=prev_sheet_date.col_idx + prev_sheet_date.wide + add, row_idx=prev_sheet_date.row_idx), # noqa
            Cell(col_idx=prev_sheet_date.col_idx + prev_sheet_date.wide + 8 + add, row_idx=prev_sheet_date.row_idx), # noqa
        )
        date_cell = Cell(
            value=target.strftime('%d.%m.%Y'),
            col_idx=from_cell.col_idx,
            row_idx=from_cell.row_idx,
            bold=True
        )
        self._google.merge_cells(from_cell, to_cell, sheet_id=self.gid)
        nes_columns = (
            date_cell,
            Cell(value='Отгрузка', col_idx=from_cell.col_idx, row_idx=from_cell.row_idx + 1),
            Cell(value='Приход', col_idx=from_cell.col_idx, row_idx=from_cell.row_idx + 1),
            Cell(value='Реализация', col_idx=from_cell.col_idx, row_idx=from_cell.row_idx + 1),
            Cell(value='Реализация сумма', col_idx=from_cell.col_idx, row_idx=from_cell.row_idx + 1),
            Cell(value='Гл. Дом', col_idx=from_cell.col_idx, row_idx=from_cell.row_idx + 1),
            Cell(value='Кинологи', col_idx=from_cell.col_idx, row_idx=from_cell.row_idx + 1),
            Cell(value='Благотворительность', col_idx=from_cell.col_idx, row_idx=from_cell.row_idx + 1),
            Cell(value='Утилизация', col_idx=from_cell.col_idx, row_idx=from_cell.row_idx + 1),
            Cell(value='Остаток', col_idx=from_cell.col_idx, row_idx=from_cell.row_idx + 1),
        )
        self._google.update_cells(nes_columns, sheet_id=self.gid)
        sheet_date = SheetDate(
            date=target,
            col_idx=from_cell.col_idx,
            row_idx=0,
            wide=9,
            cols=[
                UnderDateColumn(name='Отгрузка', col_idx=from_cell.col_idx, row_idx=from_cell.row_idx + 1),
                UnderDateColumn(name='Приход', col_idx=from_cell.col_idx + 1, row_idx=from_cell.row_idx + 1),
                UnderDateColumn(name='Реализация', col_idx=from_cell.col_idx + 2, row_idx=from_cell.row_idx + 1),
                UnderDateColumn(name='Реализация сумма', col_idx=from_cell.col_idx + 3, row_idx=from_cell.row_idx + 1),
                UnderDateColumn(name='Гл. Дом', col_idx=from_cell.col_idx + 4, row_idx=from_cell.row_idx + 1),
                UnderDateColumn(name='Кинологи', col_idx=from_cell.col_idx + 5, row_idx=from_cell.row_idx + 1),
                UnderDateColumn(name='Благотворительность', col_idx=from_cell.col_idx + 6, row_idx=from_cell.row_idx + 1),
                UnderDateColumn(name='Утилизация', col_idx=from_cell.col_idx + 7, row_idx=from_cell.row_idx + 1),
                UnderDateColumn(name='Остаток', col_idx=from_cell.col_idx + 8, row_idx=from_cell.row_idx + 1),
            ]
        )
        self.dates.insert(high, sheet_date)
        for i in range(high + 1, len(self.dates)):
            self.dates[i].add_col_idx(9)
        for sm in self.months:
            if target.month <= sm.month:
                sm.add_col_idx(9)
        return high

    def _find_cols_indexes(self, *col_names: str, target: datetime.date | SheetDate) -> tuple[ColIdx | None, ...]:
        if isinstance(target, datetime.date):
            index = self._binary_dates_srch(target)
            if index is None:
                raise ValueError('No such date')
            target = self.dates[index]

        if target.cols is None:
            from_cell = Cell(col_idx=target.col_idx, row_idx=target.row_idx + 1)
            to_cell = Cell(col_idx=target.col_idx + target.wide - 1, row_idx=target.row_idx + 1)
            cells_gen = self._google.get_values(sheet_id=self.gid, from_=from_cell, to=to_cell)
            cols = {c.formatted_value or c.value: c.col_idx for c in cells_gen}
            cols_under_date: list[UnderDateColumn] = []
            for col_name, col_idx in cols.items():
                under_date_col = UnderDateColumn(col_idx, 1, col_name)
                cols_under_date.append(under_date_col)
            target.cols = cols_under_date

        col_indexes = list(target.find_col_idx(col_name) for col_name in col_names)
        return tuple(col_indexes)

    def append_col(self, target: datetime.date, col_name: str) -> ColIdx:
        index = self._binary_dates_srch(target)
        if index is None:
            raise ValueError('No such date in spreadsheet')
        sheet_date = self.dates[index]
        col_idx = self._find_cols_indexes(col_name, target=sheet_date)[0]
        if col_idx is not None:
            return col_idx
        _, row_count = self._google.get_size_of_sheet(self.gid)
        self._google.append_dimension('COLUMNS', self.gid, 1)
        range_cells = (
            Cell(col_idx=sheet_date.col_idx + sheet_date.wide, row_idx=0),  # from cell
            Cell(col_idx=sheet_date.col_idx + sheet_date.wide, row_idx=row_count),  # to cell
        )
        self._google.insert_range(range_cells[0], range_cells[1], 'COLUMNS', self.gid)
        sheet_date.wide += 1
        new_cells = [
            Cell(value=col_name, col_idx=sheet_date.col_idx + sheet_date.wide - 1, row_idx=1)
        ]
        self._google.update_cells(new_cells, self.gid)
        for i in range(index + 1, len(self.dates)):
            self.dates[i].add_col_idx(1)

        for sm in self.months:
            if sm.month == sheet_date.date.month:
                sm.add_col_idx(1)
        sheet_date.cols.append(
            UnderDateColumn(name=col_name, col_idx=sheet_date.cols[-1].col_idx + 1, row_idx=1)
        )
        return sheet_date.col_idx + sheet_date.wide - 1

    def update_date(self, target: datetime.date | SheetDate) -> None:
        if isinstance(target, SheetDate):
            target = target.date

        index = self._binary_dates_srch(target)
        if index is None:
            raise ValueError(f'No such date in spreadsheet {target}')
        (
            income_col_idx,
            sale_col_idx,
            main_dom_idx,
            kino_idx,
            blago_idx,
            util_idx,
            rem_col_idx
         ) = self._find_cols_indexes( # noqa
            'Приход',
            'Реализация',
            'Гл. Дом',
            'Кинологи',
            'Благотворительность',
            'Утилизация',
            'Остаток',
            target=target
        )
        (
            prev_rem_col_idx,
            prev_invent_col_idx
        ) = self._find_cols_indexes( # noqa
            'Остаток',
            'Инвентаризация',
            target=self.dates[index-1]
        ) # noqa

        new_cells: list[Cell] = []
        minus_cols = (sale_col_idx, main_dom_idx, kino_idx, blago_idx, util_idx)
        for p in self.products:
            val = '='
            if income_col_idx is not None:
                c = Cell(col_idx=income_col_idx, row_idx=p.row_idx)
                val += f'+{c.name}'

            for minus_col_idx in minus_cols:
                c = Cell(col_idx=minus_col_idx, row_idx=p.row_idx)
                val += f'-{c.name}'

            if prev_rem_col_idx is not None or prev_invent_col_idx is not None:
                if prev_invent_col_idx is not None and prev_rem_col_idx is not None:
                    invent_cell = Cell(col_idx=prev_invent_col_idx, row_idx=p.row_idx)
                    rem_cell = Cell(col_idx=prev_rem_col_idx, row_idx=p.row_idx)
                    val += f'+ЕСЛИ(ЕПУСТО({invent_cell.name}); {rem_cell.name}; {invent_cell.name})'
                else:
                    cell = Cell(col_idx=prev_invent_col_idx, row_idx=p.row_idx) if prev_rem_col_idx is None else Cell(col_idx=prev_rem_col_idx, row_idx=p.row_idx) # noqa
                    val += f'+{cell.name}'
            c = Cell(value=val, col_idx=rem_col_idx, row_idx=p.row_idx)
            new_cells.append(c)
        self._google.update_cells(new_cells, self.gid)

    @property
    def products(self) -> list[SheetProduct]:
        """
        Gets list of SheetProduct
        """
        if self._products is None:
            _, row_count = self._google.get_size_of_sheet(self.gid)
            from_cell = Cell(col_idx=0, row_idx=2)
            to_cell = Cell(col_idx=2, row_idx=row_count)

            products_cells_gen = self._google.get_values(
                sheet_id=self.gid,
                from_=from_cell,
                to=to_cell
            )
            data = {}
            row_idx = 2
            products: list[SheetProduct] = []
            for c in products_cells_gen:
                if c.row_idx != row_idx:
                    products.append(SheetProduct(**data, row_idx=row_idx))
                    row_idx = c.row_idx
                match c.col_idx:
                    case 0:  # plu column
                        plu = c.formatted_value or c.value
                        data['plu'] = int(plu)
                    case 1:  # name column
                        data['name'] = c.formatted_value or c.value
                    case 2:  # price column
                        price = c.formatted_value or c.value
                        data['price'] = int(price) if price is not None else 0
            else:
                products.append(SheetProduct(**data, row_idx=row_idx))

            self._products = products
        return self._products

    def _find_dates_with_months(self) -> tuple[list[SheetDate], list[SheetMonth]]:
        """
        Gets lists of SheetDate's and SheetMonth's
        """
        dates_cells_gen = self._google.get_values(sheet_id=self.gid, from_='E1', to='ZZZ1')
        dates: list[SheetDate] = []
        months: list[SheetMonth] = []
        wide = 1
        flag = False
        last_encountered_type: Literal['date', 'month'] = 'date'
        for c in list(dates_cells_gen):
            if c.value is None and c.formatted_value is None:
                if flag:
                    wide += 1
                continue
            cell_date = c.formatted_value or c.value
            try:
                date = datetime.datetime.strptime(cell_date, '%d.%m.%Y').date()
                sheet_date = SheetDate(date, c.col_idx, c.row_idx)
                if last_encountered_type == 'date' and len(dates) > 0:  # set previous date wide
                    dates[-1].wide = wide
                elif last_encountered_type == 'month' and len(dates) > 0:  # set last month wide
                    months[-1].wide = wide
                dates.append(sheet_date)
                flag = True
                wide = 1
                last_encountered_type = 'date'
            except ValueError:
                if cell_date in SEASONS:
                    sheet_month = SheetMonth(cell_date, c.col_idx, c.row_idx)
                    if last_encountered_type == 'date' and len(dates) > 0:  # set previous date wide
                        dates[-1].wide = wide
                    elif last_encountered_type == 'month' and len(dates) > 0:  # set last month wide
                        months[-1].wide = wide
                    months.append(sheet_month)
                    flag = True
                    wide = 1
                    last_encountered_type = 'month'
                else:
                    flag = False  # switch flag to false to forbid increasing wide

        # calculating last elem wide
        collection = dates if last_encountered_type == 'date' else months
        from_cell = Cell(col_idx=collection[-1].col_idx, row_idx=1)
        to_cell = Cell(col_idx=collection[-1].col_idx + wide, row_idx=1)
        cols_gen = self._google.get_values(sheet_id=self.gid, from_=from_cell, to=to_cell)
        wide = 1
        for c in cols_gen:
            if c.col_idx == collection[-1].col_idx:
                continue
            val = c.formatted_value or c.value
            if val is None:
                break
            wide += 1
        collection[-1].wide = wide
        return dates, months

    @property
    def months(self) -> list[SheetMonth]:
        """
        Gets list of SheetMonth's
        """
        if self._months is None:
            dates, months = self._find_dates_with_months()
            self._dates = dates
            self._months = months
        return self._months

    @property
    def dates(self) -> list[SheetDate]:
        """
        Gets list of SheetDate's
        """
        if self._dates is None:
            dates, months = self._find_dates_with_months()
            self._dates = dates
            self._months = months
        return self._dates

    def create_date(self, date: datetime.date | None = None) -> SheetDate:
        """
        Creates new date with needed columns such as income, sale and so on
        """
        if date is None:
            date = datetime.datetime.now().date()
        index = self._binary_dates_srch(date)
        if index is None:  # check there is no already created date
            index = self._insert_date(date)
            self.update_date(date)
            try:
                self.update_month(date.month)
            except ValueError:
                self.summarize_month(date.month)
            try:
                self.update_date(self.dates[index+1].date)
            except IndexError:
                pass
        return self.dates[index]

    def create_shipment(self, shipments: list[Shipment], date: datetime.date | None = None):
        """
        Cerate new shipments and update google spreadsheet
        """
        sheet_date = self.create_date(date)
        shipment_col_idx = self._find_cols_indexes('Отгрузка', target=sheet_date)[0]
        plus = {s.plu: {'shipment': s, 'row_idx': -1} for s in shipments}
        for p in self.products:
            if p.plu in plus:
                plus[p.plu]['row_idx'] = p.row_idx

        from_cell = Cell(col_idx=shipment_col_idx, row_idx=min(v['row_idx'] for v in plus.values() if v['row_idx'] != -1))
        to_cell = Cell(col_idx=shipment_col_idx, row_idx=max(v['row_idx'] for v in plus.values() if v['row_idx'] != -1))

        old_shipments_gen = self._google.get_values(sheet_id=self.gid, from_=from_cell, to=to_cell)
        old_shipments = {c.row_idx: c for c in old_shipments_gen}

        new_shipments: list[Cell] = []
        for data in plus.values():
            shipment: Shipment = data['shipment']
            row_idx: RowIdx = data['row_idx']
            try:
                old_shipment: Cell = old_shipments[row_idx]
                old_value = old_shipment.formatted_value or old_shipment.value
            except KeyError:
                old_value = 0

            if old_value is None:
                old_value = 0
            if isinstance(old_value, str):
                if old_value.isdigit():
                    old_value = int(old_value)
                else:
                    old_value = float(old_value)
            new_value = shipment.weight + old_value
            if isinstance(new_value, float):
                new_value = round(new_value, 3)
            new_cell = Cell(value=new_value, col_idx=shipment_col_idx, row_idx=row_idx)
            new_shipments.append(new_cell)
        self._google.update_cells(new_shipments, self.gid)

    def create_income(self, incomes: list[Income], date: datetime.date | None = None):
        """
        Create new income and update google spreadsheet
        """
        sheet_date = self.create_date(date)
        income_col_idx = self._find_cols_indexes('Приход', target=sheet_date)[0]
        plus = {i.plu: {'income': i, 'row_idx': -1} for i in incomes}
        for p in self.products:
            if p.plu in plus:
                plus[p.plu]['row_idx'] = p.row_idx

        from_cell = Cell(col_idx=income_col_idx, row_idx=min(v['row_idx'] for v in plus.values() if v['row_idx'] != -1))
        to_cell = Cell(col_idx=income_col_idx, row_idx=max(v['row_idx'] for v in plus.values() if v['row_idx'] != -1))

        old_incomes_gen = self._google.get_values(sheet_id=self.gid, from_=from_cell, to=to_cell)
        old_incomes = {c.row_idx: c for c in old_incomes_gen}

        new_incomes: list[Cell] = []
        for data in plus.values():
            income: Income = data['income']
            row_idx: RowIdx = data['row_idx']
            try:
                old_income: Cell = old_incomes[row_idx]
                old_value = old_income.formatted_value or old_income.value
            except KeyError:
                old_value = 0

            if old_value is None:
                old_value = 0
            if isinstance(old_value, str):
                if old_value.isdigit():
                    old_value = int(old_value)
                else:
                    if ',' in old_value:
                        old_value = old_value.replace(',', '.')
                    old_value = float(old_value)
            new_value = income.weight + old_value
            if isinstance(new_value, float):
                new_value = round(new_value, 3)
            new_cell = Cell(value=new_value, col_idx=income_col_idx, row_idx=row_idx)
            new_incomes.append(new_cell)
        self._google.update_cells(new_incomes, self.gid)


    def create_sale(self, sales: list[Sale], date: datetime.date | None = None):
        """
        Create new sale and update google spreadsheet
        """

        sheet_date = self.create_date(date)
        (
            sale_col_idx,
            sale_sum_col_idx,
            main_dom_col_idx,
            kino_col_idx,
            blago_col_idx,
            util_col_idx,
        ) = self._find_cols_indexes(
            'Реализация',
            'Реализация сумма',
            'Гл. Дом',
            'Кинологи',
            'Благотворительность',
            'Утилизация',
            target=sheet_date
        )
        plus = {s.plu: {'sale': s, 'row_idx': -1} for s in sales}
        for p in self.products:
            if p.plu in plus:
                plus[p.plu]['row_idx'] = p.row_idx

        cols = [sale_col_idx, sale_sum_col_idx, main_dom_col_idx, kino_col_idx, blago_col_idx, util_col_idx]
        from_cell = Cell(
            col_idx=min(col_idx for col_idx in cols if col_idx is not None),
            row_idx=min(v['row_idx'] for v in plus.values() if v['row_idx'] != -1)
        )
        to_cell = Cell(
            col_idx=max(col_idx for col_idx in cols if col_idx is not None),
            row_idx=max(v['row_idx'] for v in plus.values() if v['row_idx'] != -1)
        )

        old_cells_gen = self._google.get_values(sheet_id=self.gid, from_=from_cell, to=to_cell)
        old_sales: dict[RowIdx, Cell] = {}
        other_cells: dict[RowIdx, list[Cell]] = {}
        for c in old_cells_gen:
            if c.col_idx == sale_col_idx:
                old_sales[c.row_idx] = c
                continue
            if c.row_idx not in other_cells:
                other_cells[c.row_idx] = []
            other_cells[c.row_idx].append(c)

        new_sales: list[Cell] = []
        for data in plus.values():
            sale: Sale = data['sale']
            row_idx: RowIdx = data['row_idx']
            customers: dict[str, int | float] = {}
            if sale.customer in ('Гл. Дом', 'Кинологи', 'Благотворительность', 'Утилизация'):
                target_idx: int = -1
                match sale.customer:
                    case 'Гл. Дом':
                        target_idx = main_dom_col_idx
                    case 'Кинологи':
                        target_idx = kino_col_idx
                    case 'Благотворительность':
                        target_idx = blago_col_idx
                    case 'Утилизация':
                        target_idx = util_col_idx
                if target_idx is None:
                    raise ValueError(f'No such column "{sale.customer}"')
                cols = other_cells[row_idx]
                old_value = 0
                for c in cols:
                    if c.col_idx == target_idx:
                        value = c.formatted_value or c.value
                        if value is None:
                            old_value = 0
                        elif isinstance(value, str) and value.isdigit():
                            old_value = int(value)
                        elif isinstance(value, str):
                            value = value.replace(',', '.') if ',' in value else value
                            old_value = float(value)
                        else:
                            old_value = value
                        break
                new_value = old_value + sale.weight
                if isinstance(new_value, float):
                    new_value = round(new_value, 3)
                new_cell = Cell(value=new_value, row_idx=row_idx, col_idx=target_idx)
                new_sales.append(new_cell)
                continue
            try:
                old_sale: Cell = old_sales[row_idx]
                note = old_sale.note
                if note is not None:
                    for row in note.split('\n'):
                        customer, weight = row.split(' - ')
                        weight = int(weight) if weight.isdigit() else float(weight)
                        customers[customer] = weight

                old_value = old_sale.formatted_value or old_sale.value
            except KeyError:
                old_value = 0

            if old_value is None:
                old_value = 0
            if isinstance(old_value, str):
                if old_value.isdigit():
                    old_value = int(old_value)
                else:
                    if ',' in old_value:
                        old_value = old_value.replace(',', '.')
                    old_value = float(old_value)

            if sale.customer in customers:
                customers[sale.customer] += sale.weight
            else:
                customers[sale.customer] = sale.weight
            if isinstance(customers[sale.customer], float):
                customers[sale.customer] = round(customers[sale.customer], 3)

            new_value = sale.weight + old_value
            if isinstance(new_value, float):
                new_value = round(new_value, 3)
            new_note = '\n'.join(f'{customer} - {weight}' for customer, weight in customers.items())
            new_cell = Cell(value=new_value, note=new_note, col_idx=sale_col_idx, row_idx=row_idx)
            price_cell = Cell(col_idx=2, row_idx=row_idx)
            sum_formula = f'={new_cell.name} * {price_cell.name}'
            new_cell_sum = Cell(value=sum_formula, col_idx=sale_sum_col_idx, row_idx=row_idx)

            new_sales.append(new_cell)
            new_sales.append(new_cell_sum)
        self._google.update_cells(new_sales, self.gid)

    def update_month(self, month: int | Literal['last_date']):
        if month == 'last_date':
            month = self.dates[-1].date.month
        int_months = [m.month for m in self.months]
        index = int_months.index(month) # noqa
        sheet_month = self.months[index]

        needed_dates = [sd for sd in self.dates if sd.date.month == month]
        (
            shipment_cols,
            income_cols,
            sale_cols,
            sale_sum_cols,
            main_dom_cols,
            kino_cols,
            blago_cols,
            util_cols
         ) = [], [], [], [], [], [], [], []
        for sd in needed_dates:
            (
                shipment_col_idx,
                income_col_idx,
                sale_col_idx,
                sale_sum_col_idx,
                main_dom_col_idx,
                kino_col_idx,
                blago_col_idx,
                util_col_idx,
            ) = self._find_cols_indexes(  # noqa
                'Отгрузка',
                'Приход',
                'Реализация',
                'Реализация сумма',
                'Гл. Дом',
                'Кинологи',
                'Благотворительность',
                'Утилизация',
                target=sd
            )
            shipment_cols.append(shipment_col_idx)
            income_cols.append(income_col_idx)
            sale_cols.append(sale_col_idx)
            sale_sum_cols.append(sale_sum_col_idx)
            main_dom_cols.append(main_dom_col_idx)
            kino_cols.append(kino_col_idx)
            blago_cols.append(blago_col_idx)
            util_cols.append(util_col_idx)

        new_cells = []
        for p in self.products:
            shipment_formula = '=' + ' + '.join(Cell(col_idx=col_idx, row_idx=p.row_idx).name for col_idx in shipment_cols if col_idx is not None)
            income_formula = '=' + ' + '.join(Cell(col_idx=col_idx, row_idx=p.row_idx).name for col_idx in income_cols if col_idx is not None)
            sale_formula = '=' + ' + '.join(Cell(col_idx=col_idx, row_idx=p.row_idx).name for col_idx in sale_cols if col_idx is not None)
            sale_sum_formula = '=' + ' + '.join(Cell(col_idx=col_idx, row_idx=p.row_idx).name for col_idx in sale_sum_cols if col_idx is not None)
            main_dom_formula = '=' + ' + '.join(Cell(col_idx=col_idx, row_idx=p.row_idx).name for col_idx in main_dom_cols if col_idx is not None)
            kino_formula = '=' + ' + '.join(Cell(col_idx=col_idx, row_idx=p.row_idx).name for col_idx in kino_cols if col_idx is not None)
            blago_formula = '=' + ' + '.join(Cell(col_idx=col_idx, row_idx=p.row_idx).name for col_idx in blago_cols if col_idx is not None)
            util_formula = '=' + ' + '.join(Cell(col_idx=col_idx, row_idx=p.row_idx).name for col_idx in util_cols if col_idx is not None)

            shipment_formula = None if shipment_formula == '=' else shipment_formula
            income_formula = None if income_formula == '=' else income_formula
            sale_formula = None if sale_formula == '=' else sale_formula
            sale_sum_formula = None if sale_sum_formula == '=' else sale_sum_formula
            main_dom_formula = None if main_dom_formula == '=' else main_dom_formula
            kino_formula = None if kino_formula == '=' else kino_formula
            blago_formula = None if blago_formula == '=' else blago_formula
            util_formula = None if util_formula == '=' else util_formula

            shipment_cell = Cell(value=shipment_formula, col_idx=sheet_month.col_idx, row_idx=p.row_idx)
            income_cell = Cell(value=income_formula, col_idx=sheet_month.col_idx + 1, row_idx=p.row_idx)
            sale_cell = Cell(value=sale_formula, col_idx=sheet_month.col_idx + 2, row_idx=p.row_idx)
            sale_sum_cell = Cell(value=sale_sum_formula, col_idx=sheet_month.col_idx + 3, row_idx=p.row_idx)
            main_dom_cell = Cell(value=main_dom_formula, col_idx=sheet_month.col_idx + 4, row_idx=p.row_idx)
            kino_cell = Cell(value=kino_formula, col_idx=sheet_month.col_idx + 5, row_idx=p.row_idx)
            blago_cell = Cell(value=blago_formula, col_idx=sheet_month.col_idx + 6, row_idx=p.row_idx)
            util_cell = Cell(value=util_formula, col_idx=sheet_month.col_idx + 7, row_idx=p.row_idx)

            new_cells.extend(
                [shipment_cell, income_cell, sale_cell, sale_sum_cell, main_dom_cell, kino_cell, blago_cell, util_cell]
            )
        self._google.update_cells(new_cells, self.gid)

    def summarize_month(self, month: int | Literal['last_date'], update_exist: bool = False):
        if month == 'last_date':
            month = self.dates[-1].date.month

        int_months = [m.month for m in self.months]
        if month in int_months:
            if update_exist is True:
                self.update_month(month)
            return

        # calculating last needed date
        last_sheet_date = None
        for sd in self.dates:
            if month < sd.date.month:
                break
            if sd.date.month == month:
                last_sheet_date = sd

        _, row_count = self._google.get_size_of_sheet(self.gid)
        insert_range = lambda: self._google.insert_range( # noqa
            from_cell=Cell(col_idx=last_sheet_date.col_idx + last_sheet_date.wide, row_idx=0),
            to_cell=Cell(col_idx=last_sheet_date.col_idx + last_sheet_date.wide + 3, row_idx=row_count),
            shift_dimension='COLUMNS',
            sheet_id=self.gid
        )
        self._google.append_dimension('COLUMNS', self.gid, 8)
        insert_range()
        index = self.dates.index(last_sheet_date)
        for i in range(index + 1, len(self.dates)):
            self.dates[i].col_idx += 4
        from_cell = Cell(
            value=SEASONS[month-1],
            bold=True,
            col_idx=last_sheet_date.col_idx + last_sheet_date.wide,
            row_idx=0,
        )
        new_cells = [
            from_cell,
            Cell(value='Отгрузка', col_idx=from_cell.col_idx, row_idx=1), # noqa
            Cell(value='Приход', col_idx=from_cell.col_idx, row_idx=1), # noqa
            Cell(value='Реализация', col_idx=from_cell.col_idx, row_idx=1), # noqa
            Cell(value='Реализация сумма', col_idx=from_cell.col_idx, row_idx=1), # noqa
            Cell(value='Гл. Дом', col_idx=from_cell.col_idx, row_idx=1), # noqa
            Cell(value='Кинологи', col_idx=from_cell.col_idx, row_idx=1), # noqa
            Cell(value='Благотворительность', col_idx=from_cell.col_idx, row_idx=1), # noqa
            Cell(value='Утилизация', col_idx=from_cell.col_idx, row_idx=1), # noqa
        ]
        self._google.update_cells(new_cells, self.gid)
        sheet_month = SheetMonth(
            name=SEASONS[month-1],
            col_idx=last_sheet_date.col_idx + last_sheet_date.wide,
            row_idx=0,
            month=month, # noqa
            wide=8,
            cols=[
                UnderDateColumn(name='Отгрузка', col_idx=from_cell.col_idx, row_idx=1),
                UnderDateColumn(name='Приход', col_idx=from_cell.col_idx + 1, row_idx=1),
                UnderDateColumn(name='Реализация', col_idx=from_cell.col_idx + 2, row_idx=1),
                UnderDateColumn(name='Реализация сумма', col_idx=from_cell.col_idx + 3, row_idx=1),
                UnderDateColumn(name='Гл. Дом', col_idx=from_cell.col_idx + 4, row_idx=1),
                UnderDateColumn(name='Кинологи', col_idx=from_cell.col_idx + 5, row_idx=1),
                UnderDateColumn(name='Благотворительность', col_idx=from_cell.col_idx + 6, row_idx=1),
                UnderDateColumn(name='Утилизация', col_idx=from_cell.col_idx + 7, row_idx=1),
            ]
        )
        self.months.insert(month-1, sheet_month)
        for sd in self.dates:
            if month < sd.date.month:
                sd.add_col_idx(8)
        for sm in self.months:
            if month < sm.month:
                sm.add_col_idx(8)
        self.update_month(month)

    def do_inventory(self, inventories: list[Inventory], date: datetime.date | None = None):
        """
        Do inverntory
        """
        sheet_date = self.create_date(date)
        inventory_col_idx = self._find_cols_indexes('Инвентаризация', target=sheet_date)[0]
        if inventory_col_idx is None:
            inventory_col_idx = self.append_col(date, 'Инвентаризация')
            index = self.dates.index(sheet_date)
            try:
                next_sheet_date = self.dates[index+1]
                self.update_date(next_sheet_date)
            except IndexError:
                pass
        plus = {i.plu: {'inventory': i, 'row_idx': -1} for i in inventories}
        for p in self.products:
            if p.plu in plus:
                plus[p.plu]['row_idx'] = p.row_idx

        from_cell = Cell(col_idx=inventory_col_idx, row_idx=min(v['row_idx'] for v in plus.values() if v['row_idx'] != -1))
        to_cell = Cell(col_idx=inventory_col_idx, row_idx=max(v['row_idx'] for v in plus.values() if v['row_idx'] != -1))

        old_inventories_gen = self._google.get_values(sheet_id=self.gid, from_=from_cell, to=to_cell)
        old_inventories = {c.row_idx: c for c in old_inventories_gen}

        new_inventories: list[Cell] = []
        for data in plus.values():
            income: Inventory = data['inventory']
            row_idx: RowIdx = data['row_idx']
            try:
                old_inventory: Cell = old_inventories[row_idx]
                old_value = old_inventory.formatted_value or old_inventory.value
            except KeyError:
                old_value = 0

            if old_value is None:
                old_value = 0
            if isinstance(old_value, str):
                if old_value.isdigit():
                    old_value = int(old_value)
                else:
                    old_value = old_value.replace(',', '.') if ',' in old_value else old_value
                    old_value = float(old_value)
            new_value = income.weight + old_value
            if isinstance(new_value, float):
                new_value = round(new_value, 3)
            new_cell = Cell(value=new_value, col_idx=inventory_col_idx, row_idx=row_idx)
            new_inventories.append(new_cell)
        self._google.update_cells(new_inventories, self.gid)
