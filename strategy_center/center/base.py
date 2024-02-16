import pandas as pd
from functools import wraps
from abc import ABC, abstractmethod

from common.utilities import dataclass_to_dict
from strategy_center.center.store import StrategyVars

class BaseEngine(ABC):
    
    def __init__(self):
        self.data_feed = None
        self.strategy_list = []
    
    def add_data_feed(self, data_feed):
        self.data_feed = data_feed
        self.data_feed.set_engine(self)
        
        
    def add_strategy(self, strategy):
        if self.data_feed is None:
            raise Exception("请先添加数据源")
        
        strategy.engine = self   
        self.strategy_list.append(strategy)     
        # count = len(self.strategy_list) + 1
        # self.strategy_list[count] = strategy        
        self.data_feed.add_symbol(strategy.underlying_symbol)
        # return count

    def remove_strategy(self, strategy_id):
        del self.strategy_list[strategy_id]
        
    @abstractmethod
    def start(self):
        ...
        
    @abstractmethod
    def end(self):
        ...
        
    @abstractmethod
    def dispatch_bars(self, bar):
        ...
    
    @abstractmethod
    def reset_strategies(self):
        ...
   
    
class BaseDataFeed(ABC):
    def __init__(self):
        self.symbols = []
        self.engine = None
    
    def set_engine(self, engine):
        self.engine = engine
        
    def add_symbol(self, symbol):
        if symbol not in self.symbols:
            self.symbols.append(symbol)
    
    def remove_symbol(self, symbol):
        if symbol in self.symbols:
            self.symbols.remove(symbol)
    
    @abstractmethod
    def run(self):
        ...
       
        
class BaseStrategy(ABC):
    
    def __init__(self, id, account_id, name, underlying_symbol, trader, store_host, **kwargs):
        self.id = id
        self.account_id = account_id
        self.name = name
        self.underlying_symbol = underlying_symbol
        self.params = kwargs
        self.state = None
        self.holdings = None
        self.minutes_bars = None
        self.engine = None
        self.trader = trader
        
        self.svars = StrategyVars(host=store_host)

    def set_contracts(self, contracts):
        self.contracts = contracts
        
    def __getattr__(self, item):
        try:
            return self.params[item]
        except KeyError:
            raise AttributeError(f"'OptionStrategy' 策略{self.id}没有参数： '{item}'")
    
    @property
    def data_feed(self):
        return self.engine.data_feed if self.engine else None
    
    def on_bars(self, bars):
        if self.minutes_bars is None:
            self.minutes_bars = pd.DataFrame([bars[self.underlying_symbol]]) 
        else:
            self.minutes_bars.loc[len(self.minutes_bars)] = bars[self.underlying_symbol]
        
    @abstractmethod
    def run(self):
        ...
        
    @abstractmethod
    def stop(self):
        ...
    

class BaseTrader(ABC):
    
    def __init__(self):
        ...
    
    @abstractmethod
    def long_open(self, symbol, amount, price):
        ...
        
    @abstractmethod
    def long_close(self, symbol, amount, price):
        ...
        
    @abstractmethod
    def short_open(self, symbol, amount, price):
        ...
        
    @abstractmethod
    def short_close(self, symbol, amount, price):
        ...
        
        
# class OptionTrader(BaseTrader):
    
#     def __init__(self):
#         super().__init__()
        
#     @abstractmethod
#     def combinate(self, symbol_1, amount_1, symbol_2, amount_2):
#         ...
        
#     @abstractmethod
#     def release(self, combination_id):
#         ...
        
        
def trader_action(method):
    @wraps(method)
    def wrapper(self, *args, **kwargs):
        if self.trader is not None:
            req = method(self, *args, **kwargs)
            req_dict = dataclass_to_dict(req)
            req_dict['strategy_id'] = self.strategy.id
            self.strategy.svars[req.id] = dataclass_to_dict(req_dict)
            return req_dict
    return wrapper

class OptionGroup(ABC):
    def __init__(self, strategy):
        self.options = []
        self.strategy = strategy
        self.create_time = None
        self.destroy_time = None
        self.trader = strategy.trader
        
    def add_options(self, option):
        self.options.append(option)
    
    @trader_action
    def long_open(self, *args, **kwargs):
        return self.trader.long_open(*args, **kwargs)
        
    @trader_action
    def long_close(self, *args, **kwargs):
        return self.trader.long_close(*args, **kwargs)
        
    @trader_action
    def short_open(self, *args, **kwargs):
        return self.trader.short_open(*args, **kwargs)
        
    @trader_action
    def short_close(self, *args, **kwargs):
        return self.trader.short_close(*args, **kwargs)
            
    @trader_action
    def combinate(self, *args, **kwargs):
        return self.trader.combinate(*args, **kwargs)

    @trader_action
    def release(self, *args, **kwargs):
        return self.trader.release(*args, **kwargs)
    
    @abstractmethod
    def on_bars(self, bars):
        pass
        
class Trader(ABC):
    
    def __init__(self):
        ...
    
    @abstractmethod
    def long_open(self, symbol, amount, price):
        ...
        
    @abstractmethod
    def long_close(self, symbol, amount, price):
        ...
        
    @abstractmethod
    def short_open(self, symbol, amount, price):
        ...
        
    @abstractmethod
    def short_close(self, symbol, amount, price):
        ...
        
        
