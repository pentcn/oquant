from functools import wraps
from common.utilities import dataclass_to_dict
from strategy_center.center.base import OptionGroup

def process_req(method):
    @wraps(method)
    def wrapper(self, *args, **kwargs):
        req_dict = method(self, *args, **kwargs)
        if self.is_dairy_task:
            req_dict['is_dairy_task'] = 1
            self.strategy.svars[req_dict['id']] = req_dict
        return req_dict
    return wrapper


class DualDragonCombinations(OptionGroup):
    def __init__(self, strategy, is_dairy_task=True):
        super().__init__(strategy)
        self.is_dairy_task = is_dairy_task

    def on_bars(self, bars):
        print("DualDragonCombinations on_bars")

    @process_req
    def long_open(self, *args, **kwargs):
        return super().long_open(*args, **kwargs)

    @process_req
    def long_close(self, *args, **kwargs):
        return super().long_close(*args, **kwargs)

    @process_req
    def short_open(self, *args, **kwargs):
        return super().short_open(*args, **kwargs)

    @process_req
    def short_close(self, *args, **kwargs):
        return super().short_close(*args, **kwargs)

    @process_req
    def combinate(self, *args, **kwargs):
        return super().combinate(*args, **kwargs)

    @process_req
    def release(self, *args, **kwargs):
        return super().release(*args, **kwargs)
