from functools import wraps
from common.utilities import dataclass_to_dict
from strategy_center.center.base import OptionGroup


class DualDragonCombinations(OptionGroup):
    def __init__(self, strategy, is_dairy_task=True):
        super().__init__(strategy)
        self.is_dairy_task = is_dairy_task

    def on_bars(self, bars):
        print("DualDragonCombinations on_bars")

    def long_open(self, symbol, amount, price):        
        return super().long_open(symbol, amount, price, 
                                  {'is_dairy_task': 1 if self.is_dairy_task else 0})


    def short_open(self, symbol, amount, price):
        return super().short_open(symbol, amount, price, 
                                  {'is_dairy_task': 1 if self.is_dairy_task else 0})

