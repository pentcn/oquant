from datetime import datetime, time, timedelta
from strategy_center.center.strategy import OptionStrategy
from strategy_center.center.option_group import DualDragonCombinations
from common.utilities import get_fourth_wednesday

class DualDragon(OptionStrategy):
    def __init__(self, id, account_id, name, underlying_symbol, trader, store_host, exit_days=25):
        super().__init__(id, account_id, name, underlying_symbol, trader, store_host, exit_days=exit_days)        
        self.day_group = DualDragonCombinations(self)
     
    def load(self):
        ...
    
    def on_bars(self, bars):
        super().on_bars(bars)
        
        undl_bar = bars[self.underlying_symbol]
        dt = datetime.strptime(undl_bar['datetime'], "%Y-%m-%d %H:%M:%S")
        if dt.time() ==  time(9, 30):
            expired_date = get_fourth_wednesday(dt.year, dt.month)
            month_type = 1 if 0 <=(expired_date - dt.date()).days <= self.exit_days else 0
            symbol = self.data_feed.get_option_symbol(self.underlying_symbol, undl_bar['close'], month_type, '购', 3)
            bar = self.data_feed.get_option_bar(self.underlying_symbol, symbol, undl_bar['datetime'])

            self.day_group.short_open(symbol, 1, bar['close'])

        
    
    def run(self):
        ...
    
    def stop(self):
        ...