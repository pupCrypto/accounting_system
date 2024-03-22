import os
import csv
from typing import TypeVar
from abc import ABC, abstractmethod
from spreadsheet import (
    Income as _Income,
    Sale as _Sale,
    Inventory as _Inventory,
    Shipment as _Shipment,
)


ProductCode = TypeVar('ProductCode', bound=str)
DestinationCode = TypeVar('DestinationCode', bound=str)
CustomerCode = TypeVar('CustomerCode', bound=str)
CustomerName = TypeVar('CustomerName', bound=str)
Weight = TypeVar('Weight', bound=float)
Amount = TypeVar('Amount', bound=int)
PLU = TypeVar('PLU', bound=int)
Cell = None


class Builder(ABC):
    """Base action builder"""
    def __init__(self, codes: list[str]) -> None:
        ItemData = TypeVar('ItemData', bound=dict[str, str])

        CUSTOMER_PATH = os.environ['CUSTOMER_PATH']
        ITEMS_PATH = os.environ['ITEMS_PATH']

        self._codes = codes
        self.items: dict[PLU, ItemData] = {}
        self.customers: dict[CustomerCode, CustomerName] = {}
        with open(CUSTOMER_PATH, 'r', encoding='utf8') as csv_file:
            spamreader = csv.reader(csv_file)
            for row in spamreader:
                customer_code, customer_name = row
                self.customers[customer_code] = customer_name

        with open(ITEMS_PATH, 'r', encoding='utf8') as csv_file:
            spamreader = csv.reader(csv_file)
            keys: list[str]  # names of columns
            for i, row in enumerate(spamreader):
                if i == 0:
                    keys = row
                    continue
                item_data = {keys[ii]: row[ii] for ii in range(len(row))}
                item_data['row_index'] = i
                plu = int(item_data['number'])
                self.items[plu] = item_data

    @abstractmethod
    def build(self):
        """
        Create Action instance with codes init param
        """
        raise NotImplementedError()


class ActionBuilder:
    @classmethod
    def get_builder(cls, codes: list[str]) -> Builder:
        """
        Traverse Builder subclasses and returns subclass
        if codes param contains __code__ subclass attr
        """
        for builder_cls in Builder.__subclasses__():
            if builder_cls.__code__ in codes:
                return builder_cls(codes)
        raise ValueError('In codes param has no special code')


class IncomeBuilder(Builder):
    """Income action builder"""
    __code__ = '0000002000001'

    def build(self):
        products: dict[PLU, Weight | Amount] = {}
        for code in self._codes:
            if code.startswith('21'):  # check if code is product code
                _, plu, weight, _ = code[:2], int(code[2:7]), code[7:12], code[12:]
                if plu not in products:
                    products[plu] = 0

                if self.items[plu]['shablon_osnovnoi_etiki'] == '1':
                    products[plu] += 1
                else:
                    products[plu] += float(weight) / 1000
                continue
        incomes: list[_Income] = [
            _Income(plu, weight) for plu, weight in products.items()
        ]
        return 'income', incomes


class ShipmentBuilder(Builder):
    """Shipment action builder"""
    __code__ = '0000000000019'

    def build(self):
        products: dict[PLU, Weight | Amount] = {}
        for code in self._codes:
            if code.startswith('21'):  # check if code is product code
                _, plu, weight, _ = code[:2], int(code[2:7]), code[7:12], code[12:]

                if plu not in products:
                    products[plu] = 0
                if self.items[plu]['shablon_osnovnoi_etiki'] == '1':
                    products[plu] += 1
                else:
                    products[plu] += float(weight) / 1000
                continue
        _products: list[_Sale] = [
            _Shipment(plu, weight) for plu, weight in products.items()
        ]
        return 'shipment', _products


class InventoryBuilder(Builder):
    """Inventory action builder"""
    __code__ = '0000004000001'

    def build(self):
        products: dict[PLU, Weight | Amount] = {}
        for code in self._codes:
            if code.startswith('21'):  # check if code is product code
                _, plu, weight, _ = code[:2], int(code[2:7]), code[7:12], code[12:]

                if plu not in products:
                    products[plu] = 0
                if self.items[plu]['shablon_osnovnoi_etiki'] == '1':
                    products[plu] += 1
                else:
                    products[plu] += float(weight) / 1000
                continue
        _products: list[_Sale] = [
            _Inventory(plu, weight) for plu, weight in products.items()
        ]
        return 'inventory', _products


class SaleBuilder(Builder):
    """Sale action builder"""
    __code__ = '0000003000001'

    def build(self):
        products: dict[PLU, Weight | Amount] = {}
        destination: DestinationCode | None = None
        customer: CustomerCode | None = None
        for code in self._codes:
            if code.startswith('21'):  # check if code is product code
                _, plu, weight, _ = code[:2], int(code[2:7]), code[7:12], code[12:]

                if plu not in products:
                    products[plu] = 0
                if self.items[plu]['shablon_osnovnoi_etiki'] == '1':
                    products[plu] += 1
                else:
                    products[plu] += float(weight) / 1000
                continue
            if code.startswith('123456789'):  # check if code is customer code
                if customer is not None:
                    raise ValueError('Customer code is already set')
                customer = code
                continue
            if code.startswith('987654321'):  # check if code is destination code
                if destination is not None:
                    raise ValueError('Destination code is already set')
                destination = code
                continue
        _products: list[_Sale] = [
            _Sale(plu, weight, self.customers[customer]) for plu, weight in products.items()
        ]
        return 'sale', _products
