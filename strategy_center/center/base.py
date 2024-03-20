import pandas as pd
from datetime import datetime, time
from abc import ABC, abstractmethod
from uuid import uuid4

from common.calendar import ChinaMarketCalendar
from common.constant import RunMode
from strategy_center.center.store import (
    StrategyVars,
    StrategyTrades,
    StrategyHoldings,
    StrategyGroups,
    GroupPrice
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
            strategy.svars_store.clear(strategy.id)
            strategy.trades_store.clear(strategy.id)
            strategy.holdings_store.clear(strategy.id)
            strategy.groups_store.clear(strategy.id)
            strategy.groups_prices_store.clear(strategy.id)

    def remove_strategy(self, strategy_id):
        del self.strategy_list[strategy_id]
    
    def reset_strategies(self):
        for strategy in self.strategy_list:
            strategy.reset()

        
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
    calendar = None
    
    def __init__(self, id, account_id, name, underlying_symbol, trader, store_host, **kwargs):
        self.id = id
        self.account_id = account_id
        self.name = name
        self.underlying_symbol = underlying_symbol
        self.params = kwargs
        self.state = None
        self.minutes_bars = None
        self.engine = None
        self.groups = []
        self.trader = trader
        
        self.svars_store = StrategyVars(host=store_host)
        self.trades_store = StrategyTrades(account_id, host=store_host)
        self.holdings_store = StrategyHoldings(account_id, host=store_host)
        self.groups_store = StrategyGroups(account_id, host=store_host)
        self.groups_prices_store = GroupPrice(account_id, host=store_host)
        self.last_trade_date = None
        
        if BaseStrategy.calendar is None:
            BaseStrategy.calendar = ChinaMarketCalendar()

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
        
        if len(bars) > 0:
            self.last_trade_date = BaseStrategy.calendar.get_last_traded_date(bars[self.underlying_symbol]['datetime'])
    
    @abstractmethod
    def on_trade_response(self, body):
        ...
    
    @abstractmethod
    def run(self):
        ...
        
    @abstractmethod
    def stop(self):
        ...
    
    @abstractmethod
    def reset(self):
        ...

class BaseTrader(ABC):
    
    def __init__(self, store_host):
        self.svars_store = StrategyVars(host=store_host)
    
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


class BaseGroup(ABC):
    def __init__(self, strategy):
        self.id = None
        self.options = []
        self.history_bars = {}
        self.strategy = strategy
        self.create_time = None
        self.destroy_time = None
        self.last_bars = None
        self.trader = strategy.trader
        
    def create_id(self):
        self.id = str(uuid4())
    
    def set_id(self, group_id):
        self.id = group_id
        
    def add_options(self, option):
        self.options.append(option)
        
    @property
    def run_mode(self):
        return self.strategy.engine.run_mode
    
    def on_bars(self, bars):
        self.last_bars = bars
        dt = list(bars.values())[0]['datetime']
        if len(bars) > 1:
            data = {k:v['close'] for k, v in bars.items()}
            self.history_bars.update({dt: data})
            
        if self.run_mode == RunMode.BACKTEST:
            if datetime.strptime(dt, '%Y-%m-%d %H:%M:%S').time() == time(15):
                self.save_prices(self.history_bars)
        else:
            print('Todo: 在实盘模式下，保存交易单元的K线')
    
    def save_prices(self, prices):
        self.strategy.groups_prices_store.save(self.strategy.id, self.id, prices)
    
    def save_trade_to_history(self, symbol, price):
        undl_symbol = self.strategy.underlying_symbol
        dt = self.last_bars[undl_symbol]['datetime']
        data = {undl_symbol: self.last_bars[undl_symbol]['close'], symbol: price}
        if dt in self.history_bars:
            self.history_bars[dt].update(data)
        else:
            self.history_bars.update({dt: data})
    
    def long_open(self, symbol, amount, price, extra_info=None):
        if extra_info is None:
            extra_info = {
                'group_id': self.id,
            }
        else:
            extra_info['group_id'] = self.id
        self.options.append(symbol)
        self.save_trade_to_history(symbol, price)
        return self.trader.long_open(self.strategy, symbol, amount, price, extra_info)
        
    def long_close(self, symbol, amount, price, extra_info=None):
        if extra_info is None:
            extra_info = {
                'group_id': self.id,
            }
        else:
            extra_info['group_id'] = self.id
        self.options = [opt for opt in self.options if opt != symbol]
        return self.trader.long_close(self.strategy, symbol, amount, price, extra_info)
        
    def short_open(self, symbol, amount, price, extra_info=None):
        if extra_info is None:
            extra_info = {
                'group_id': self.id,
            }
        else:
            extra_info['group_id'] = self.id
        self.options.append(symbol)
        self.save_trade_to_history(symbol, price)
        return self.trader.short_open(self.strategy, symbol, amount, price, extra_info)
        
    def short_close(self, symbol, amount, price, extra_info=None):
        if extra_info is None:
            extra_info = {
                'group_id': self.id,
            }
        else:
            extra_info['group_id'] = self.id
        self.options = [opt for opt in self.options if opt != symbol]
        return self.trader.short_close(self.strategy, symbol, amount, price, extra_info)
            
    def combinate(self, symbol_1, amount_1, symbol_2, amount_2, extra_info=None):
        if extra_info is None:
            extra_info = {
                'group_id': self.id,
            }
        else:
            extra_info['group_id'] = self.id
        return self.trader.combinate(self.strategy, symbol_1, amount_1, symbol_2, amount_2, extra_info)

    def release(self, combination_id, extra_info=None):
        if extra_info is None:
            extra_info = {
                'group_id': self.id,
            }
        else:
            extra_info['group_id'] = self.id
        return self.trader.release(self.strategy, combination_id, extra_info)
    
        
        
