import pandas as pd
from abc import ABC, abstractmethod
from common.constant import RunMode

from strategy_center.center.store import (
    StrategyVars,
    StrategyTrades, 
    StrategyHoldings
)
class BaseEngine(ABC):
    
    def __init__(self, run_mode=RunMode.BACKTEST):
        self.run_mode = run_mode
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
        self.data_feed.add_symbol(strategy.underlying_symbol)
        
        if self.run_mode == RunMode.BACKTEST:
            strategy.svars.clear(strategy.id)
            strategy.trades.clear(strategy.id)
            strategy.holdings.clear(strategy.id)

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
        self.trades = StrategyTrades(account_id, host=store_host)
        self.holdings = StrategyHoldings(account_id, host=store_host)

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
    def on_trade_response(self, body):
        ...
    
    @abstractmethod
    def run(self):
        ...
        
    @abstractmethod
    def stop(self):
        ...
    

class BaseTrader(ABC):
    
    def __init__(self, store_host):
        self.svars = StrategyVars(host=store_host)
    
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


class OptionGroup(ABC):
    def __init__(self, strategy):
        self.options = []
        self.strategy = strategy
        self.create_time = None
        self.destroy_time = None
        self.trader = strategy.trader
        
    def add_options(self, option):
        self.options.append(option)
    
    def long_open(self, symbol, amount, price, extra_info=None):
        return self.trader.long_open(self.strategy, symbol, amount, price, extra_info)
        
    def long_close(self, symbol, amount, price, extra_info=None):
        return self.trader.long_close(self.strategy, symbol, amount, price, extra_info)
        
    def short_open(self, symbol, amount, price, extra_info=None):
        return self.trader.short_open(self.strategy, symbol, amount, price, extra_info)
        
    def short_close(self, symbol, amount, price, extra_info=None):
        return self.trader.short_close(self.strategy, symbol, amount, price, extra_info)
            
    def combinate(self, *args, **kwargs):
        return self.trader.combinate(self.strategy, *args, **kwargs)

    def release(self, *args, **kwargs):
        return self.trader.release(self.strategy, *args, **kwargs)
    
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
        
        
