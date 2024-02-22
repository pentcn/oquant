from datetime import datetime, time, timedelta
from strategy_center.center.strategy import OptionStrategy
from strategy_center.center.option_group import DualDragonCombinations
from common.utilities import get_fourth_wednesday

class DualDragon(OptionStrategy):
    def __init__(self, id, account_id, name, underlying_symbol, trader, store_host, exit_days=25):
        super().__init__(id, account_id, name, underlying_symbol, trader, store_host, exit_days=exit_days)        
        self.day_group = DualDragonCombinations(self)
        self.day_group.create_id()
     
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

            self.day_group.long_open(symbol, 5, bar['close'])
            self.day_group.short_open(symbol, 5, bar['close'])
            self.day_group.short_open(symbol, 3, bar['close'] * 0.8)
            self.day_group.long_close(symbol, 7, bar['close'] * 0.5)
            self.day_group.long_open(symbol, 3, bar['close'] * 0.8)
            self.day_group.short_close(symbol, 7, bar['close'] * 0.5)
            
            self.day_group.combinate(symbol, 1, symbol, -1)
            self.day_group.release('test')
            ...

        
    def on_trade_response(self, body):
        super().on_trade_response(body)
        ...
         
    def run(self):
        ...
        
    def reset(self):
        super().reset()
        
        # 建立双龙入海策略的当天交易组
        self.day_group = DualDragonCombinations(self)
        self.day_group.create_id()
        
        # 重置分组信息
        groups = self.groups_store.get_all(self.id)
        for group in groups:
            combination = DualDragonCombinations(self)
            combination.set_id(group['group_id'])
            
            if group['combinations'] != []:
                threshold_prices = self.get_threshold_prices(group['combinations'][0], group['positions'])
            
            
            
            
            self.groups.append(combination)
    
    def get_threshold_prices(self, comb_info, positions):
        
        ...    
