from pprint import pprint

import time
import json
import datetime
from typing import TypeVar
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
    """
    Product
    """
    plu: int
    name: str
    price: int
    row_idx: RowIdx


@dataclass
class SheetDate:
    """
    Date
    """
    date: datetime.date
    col_idx: RowIdx
    row_idx: ColIdx
    wide: int = 1


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
    def __init__(self, spreadsheet_id: str, gid: int, creds_path: str) -> None:
        with open(creds_path, encoding='utf8') as file:
            credentials_str = file.read()
        credentials = json.loads(credentials_str)
        self._google = RateLimitWrapper(credentials, spreadsheet_id)
        self.gid = gid
        self._dates: list[SheetDate] | None = None
        self._products: list[SheetProduct] | None = None

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

    def _insert_date(self, target: datetime.date) -> int:
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
        row_count, _ = self._google.get_size_of_sheet(self.gid)
        range_cells = (  # from to shift
            Cell(col_idx=self.dates[prev_idx].col_idx + self.dates[prev_idx].wide, row_idx=self.dates[prev_idx].row_idx), # noqa
            Cell(col_idx=self.dates[prev_idx].col_idx + self.dates[prev_idx].wide + 4, row_idx=self.dates[prev_idx].row_idx + row_count), # noqa
        )
        try:
            self._google.insert_range(
                from_cell=range_cells[0],
                to_cell=range_cells[1],
                shift_dimension='COLUMNS',
                sheet_id=self.gid
            )
        except Exception:
            self._google.append_dimension('COLUMNS', self.gid, 5)
            self._google.insert_range(
                from_cell=range_cells[0],
                to_cell=range_cells[1],
                shift_dimension='COLUMNS',
                sheet_id=self.gid
            )
        from_cell, to_cell = (
            Cell(col_idx=self.dates[prev_idx].col_idx + self.dates[prev_idx].wide, row_idx=self.dates[prev_idx].row_idx), # noqa
            Cell(col_idx=self.dates[prev_idx].col_idx + self.dates[prev_idx].wide + 4, row_idx=self.dates[prev_idx].row_idx), # noqa
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
            Cell(value='Реализация сумма', col_idx=from_cell.col_idx, row_idx=from_cell.row_idx + 1), # noqa
            Cell(value='Остаток', col_idx=from_cell.col_idx, row_idx=from_cell.row_idx + 1),
        )
        self._google.update_cells(nes_columns, sheet_id=self.gid)
        self.dates.insert(high, SheetDate(date=target, col_idx=from_cell.col_idx, row_idx=0, wide=5)) # noqa
        for i in range(high + 1, len(self.dates)):
            self.dates[i].col_idx += 5
        return high

    def _find_cols_indexes(self, *col_names: str, target: datetime.date | SheetDate) -> tuple[ColIdx]:
        if isinstance(target, datetime.date):
            index = self._binary_dates_srch(target)
            if index is None:
                raise ValueError('No such date')
            target = self.dates[index]

        from_cell = Cell(col_idx=target.col_idx, row_idx=target.row_idx + 1)
        to_cell = Cell(col_idx=target.col_idx + target.wide - 1, row_idx=target.row_idx + 1)
        cells_gen = self._google.get_values(sheet_id=self.gid, from_=from_cell, to=to_cell)
        cols = {c.formatted_value or c.value: c.col_idx for c in cells_gen}
        col_indexes = list(None for _ in col_names)
        for i, col_name in enumerate(col_names):
            col_idx = cols.get(col_name)
            if col_idx is None:
                continue
            col_indexes[i] = col_idx
        return tuple(col_indexes)

    def append_col(self, target: datetime.date, col_name: str) -> ColIdx:
        index = self._binary_dates_srch(target)
        if index is None:
            raise ValueError('No such date in spreadsheet')
        sheet_date = self.dates[index]
        col_idx = self._find_cols_indexes(col_name, target=sheet_date)[0]
        if col_idx is not None:
            return
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
            self.dates[i].col_idx += 1
        return sheet_date.col_idx + sheet_date.wide - 1

    def update_date(self, target: datetime.date | SheetDate) -> None:
        if isinstance(target, SheetDate):
            target = target.date

        index = self._binary_dates_srch(target)
        if index is None:
            raise ValueError(f'No such date in spreadsheet {target}')
        income_col_idx, sale_col_idx, rem_col_idx = self._find_cols_indexes('Приход', 'Реализация', 'Остаток', target=target) # noqa
        prev_rem_col_idx, prev_invent_col_idx = self._find_cols_indexes('Остаток', 'Инвентаризация', target=self.dates[index-1]) # noqa

        new_cells: list[Cell] = []
        for p in self.products:
            val = '='
            if income_col_idx is not None:
                c = Cell(col_idx=income_col_idx, row_idx=p.row_idx)
                val += f'+{c.name}'

            if sale_col_idx is not None:
                c = Cell(col_idx=sale_col_idx, row_idx=p.row_idx)
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
                        data['price'] = int(price)
            else:
                products.append(SheetProduct(**data, row_idx=row_idx))

            self._products = products
        return self._products

    @property
    def dates(self) -> list[SheetDate]:
        """
        Gets list of SheetDate's
        """
        if self._dates is None:
            dates_cells_gen = self._google.get_values(sheet_id=self.gid, from_='E1', to='ZZZ1')
            dates: list[SheetDate] = []
            wide = 1
            flag = False
            for c in list(dates_cells_gen):
                if c.value is None and c.formatted_value is None:
                    if flag:
                        wide += 1
                    continue
                cell_date = c.formatted_value or c.value
                try:
                    date = datetime.datetime.strptime(cell_date, '%d.%m.%Y').date()
                    sheet_date = SheetDate(date, c.col_idx, c.row_idx)
                    if len(dates) > 0:  # set previous date wide
                        dates[-1].wide = wide
                    dates.append(sheet_date)
                    flag = True
                    wide = 1
                except ValueError:
                    flag = False  # switch flag to false to forbid increasing wide
            # calculating last date wide
            from_cell = Cell(col_idx=dates[-1].col_idx, row_idx=1)
            to_cell = Cell(col_idx=dates[-1].col_idx + wide, row_idx=1)
            cols_gen = self._google.get_values(sheet_id=self.gid, from_=from_cell, to=to_cell)
            wide = 1
            for c in cols_gen:
                if c.col_idx == dates[-1].col_idx:
                    continue
                val = c.formatted_value or c.value
                if val is None:
                    break
                wide += 1
            dates[-1].wide = wide
            self._dates = dates
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
            shipment: Income = data['shipment']
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
            new_cell = Cell(value=new_value, col_idx=shipment_col_idx, row_idx=row_idx)
            new_shipments.append(new_cell)
        self._google.update_cells(new_shipments, self.gid)

    def create_income(self, incomes: list[Income], date: datetime.date | None = None):
        """
        Create new income and update google spreadsheet
        """
        is_need_summarize = False
        if self.dates[-1].date.month != date.month:
            is_need_summarize = True

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
            new_cell = Cell(value=new_value, col_idx=income_col_idx, row_idx=row_idx)
            new_incomes.append(new_cell)
        self._google.update_cells(new_incomes, self.gid)

        # summarize prev month
        if is_need_summarize:
            self.summarize_month(self.dates[-2].date.month)

    def create_sale(self, sales: list[Sale], date: datetime.date | None = None):
        """
        Create new sale and update google spreadsheet
        """
        is_need_summarize = False
        if self.dates[-1].date.month != date.month:
            is_need_summarize = True

        sheet_date = self.create_date(date)
        sale_col_idx, sale_sum_col_idx = self._find_cols_indexes('Реализация', 'Реализация сумма', target=sheet_date)
        plus = {s.plu: {'sale': s, 'row_idx': -1} for s in sales}
        for p in self.products:
            if p.plu in plus:
                plus[p.plu]['row_idx'] = p.row_idx

        from_cell = Cell(col_idx=sale_col_idx, row_idx=min(v['row_idx'] for v in plus.values() if v['row_idx'] != -1))
        to_cell = Cell(col_idx=sale_col_idx, row_idx=max(v['row_idx'] for v in plus.values() if v['row_idx'] != -1))

        old_sales_gen = self._google.get_values(sheet_id=self.gid, from_=from_cell, to=to_cell)
        old_sales = {c.row_idx: c for c in old_sales_gen}

        new_sales: list[Cell] = []
        for data in plus.values():
            sale: Sale = data['sale']
            row_idx: RowIdx = data['row_idx']
            customers: dict[str, int | float] = {}
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
            new_value = sale.weight + old_value
            new_note = '\n'.join(f'{customer} - {weight}' for customer, weight in customers.items())
            new_cell = Cell(value=new_value, note=new_note, col_idx=sale_col_idx, row_idx=row_idx)
            price_cell = Cell(col_idx=2, row_idx=row_idx)
            sum_formula = f'={new_cell.name} * {price_cell.name}'
            new_cell_sum = Cell(value=sum_formula, col_idx=sale_sum_col_idx, row_idx=row_idx)

            new_sales.append(new_cell)
            new_sales.append(new_cell_sum)
        self._google.update_cells(new_sales, self.gid)

        # summarize prev month
        if is_need_summarize:
            self.summarize_month(self.dates[-2].date.month)

    def summarize_month(self, month: int):
        needed_dates = [sd for sd in self.dates if sd.date.month == month]
        shipment_cols, income_cols, sale_cols, sale_sum_cols = [], [], [], []
        for sd in needed_dates:
            shipment_col_idx, income_col_idx, sale_col_idx, sale_sum_col_idx = self._find_cols_indexes(
                'Отгрузка', 'Приход', 'Реализация', 'Реализация сумма', target=sd
            )
            shipment_cols.append(shipment_col_idx)
            income_cols.append(income_col_idx)
            sale_cols.append(sale_col_idx)
            sale_sum_cols.append(sale_sum_col_idx)

        _, row_count = self._google.get_size_of_sheet(self.gid)
        last_sheet_date = needed_dates[-1]
        insert_range = lambda: self._google.insert_range( # noqa
            from_cell=Cell(col_idx=last_sheet_date.col_idx + last_sheet_date.wide, row_idx=0),
            to_cell=Cell(col_idx=last_sheet_date.col_idx + last_sheet_date.wide + 3, row_idx=row_count),
            shift_dimension='COLUMNS',
            sheet_id=self.gid
        )
        try:
            insert_range()
        except Exception:
            self._google.append_dimension('COLUMNS', self.gid, 4)
            insert_range()
        index = self.dates.index(needed_dates[-1])
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
            Cell(value='Приход', col_idx=from_cell.col_idx + 1, row_idx=1), # noqa
            Cell(value='Реализация', col_idx=from_cell.col_idx + 2, row_idx=1), # noqa
            Cell(value='Реализация сумма', col_idx=from_cell.col_idx + 3, row_idx=1,
                 borders=Borders(right=RightBorder('SOLID', 1))), # noqa
        ]

        for p in self.products:
            shipment_formula = '=' + ' + '.join(Cell(col_idx=col_idx, row_idx=p.row_idx).name for col_idx in shipment_cols if col_idx is not None) # noqa
            income_formula = '=' + ' + '.join(Cell(col_idx=col_idx, row_idx=p.row_idx).name for col_idx in income_cols if col_idx is not None) # noqa
            sale_formula = '=' + ' + '.join(Cell(col_idx=col_idx, row_idx=p.row_idx).name for col_idx in sale_cols if col_idx is not None) # noqa
            sale_sum_formula = '=' + ' + '.join(Cell(col_idx=col_idx, row_idx=p.row_idx).name for col_idx in sale_cols if col_idx is not None) # noqa

            shipment_cell = Cell(value=shipment_formula, col_idx=from_cell.col_idx, row_idx=p.row_idx) # noqa
            income_cell = Cell(value=income_formula, col_idx=from_cell.col_idx + 1, row_idx=p.row_idx)
            sale_cell = Cell(value=sale_formula, col_idx=from_cell.col_idx + 2, row_idx=p.row_idx)
            sale_sum_cell = Cell(value=sale_sum_formula, col_idx=from_cell.col_idx + 3, row_idx=p.row_idx,
                                 borders=Borders(right=RightBorder('SOLID', 1)))

            new_cells.append(shipment_cell)
            new_cells.append(income_cell)
            new_cells.append(sale_cell)
            new_cells.append(sale_sum_cell)

        self._google.update_cells(new_cells, self.gid)


    def do_inventory(self, inventories: list[Inventory], date: datetime.date | None = None):
        """
        Do inverntory
        """
        is_need_summarize = False
        if self.dates[-1].date.month != date.month:
            is_need_summarize = True

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
            income: Income = data['inventory']
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
                    old_value = float(old_value)
            new_value = income.weight + old_value
            new_cell = Cell(value=new_value, col_idx=inventory_col_idx, row_idx=row_idx)
            new_inventories.append(new_cell)
        self._google.update_cells(new_inventories, self.gid)

        # summarize prev month
        if is_need_summarize:
            self.summarize_month(self.dates[-2].date.month)
