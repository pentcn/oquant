from abc import ABC, abstractmethod

class BaseEngine(ABC):
    
    def __init__(self):
        self.data_feed = None
        self.strategy_list = {}
    
    def add_data_feed(self, data_feed):
        self.data_feed = data_feed
        self.data_feed.set_engine(self)
        
        
    def add_strategy(self, strategy):
        strategy.engine = self
        
        count = len(self.strategy_list) + 1
        self.strategy_list[count] = strategy        
        return count

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
        self.symbols.append(symbol)
    
    def remove_symbol(self, symbol):
        self.symbols.remove(symbol)
    
    @abstractmethod
    def run(self):
        ...
        
class BaseStrategy(ABC):
    
    def __init__(self, id, account_id, name, underlying_symbol, **kwargs):
        self.id = id
        self.account_id = account_id
        self.name = name
        self.underlying_symbol = underlying_symbol
        self.params = kwargs
        self.state = None
        self.holdings = None
        self.minutes_bars = None
        self.engine = None

    def set_contracts(self, contracts):
        self.contracts = contracts
        
    def __getattr__(self, item):
        try:
            return self.params[item]
        except KeyError:
            raise AttributeError(f"'OptionStrategy' 策略{self.id}没有参数： '{item}'")
    
    @abstractmethod
    def run(self):
        ...
        
    @abstractmethod
    def stop(self):
        ...
        
    @abstractmethod
    def on_bars(self, bars):
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
        
class OptionTrader(BaseTrader):
    
    def __init__(self):
        super().__init__()
        
    @abstractmethod
    def combinate(self, symbol_1, amount_1, symbol_2, amount_2):
        ...
        
    @abstractmethod
    def release(self, combination_id):
        ...
        
class OptionGroup(ABC):
    
    def __init__(self):
        self.options = []
        self.strategy = None
        self.create_time = None
        self.destroy_time = None
        self.trader = None
    
    @abstractmethod
    def on_bars(self, bars):
        ...