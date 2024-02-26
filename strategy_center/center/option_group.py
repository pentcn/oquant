import re
from datetime import datetime, time
from common.utilities import get_fourth_wednesday
from strategy_center.center.base import OptionGroup
from datetime import timedelta


class DualDragonCombinations(OptionGroup):
    def __init__(self, strategy, amount=1, move_ratio=0.5, is_dairy_task=True):
        super().__init__(strategy)
        self.is_dairy_task = is_dairy_task
        
        self.exit_days = 25
        self.step = 3
        self.move_ratio = move_ratio
        self.amount = amount
        
        self.info = {}

    def on_bars(self, bars):
        undl_bar = bars[self.strategy.underlying_symbol]
        dt = datetime.strptime(undl_bar['datetime'], '%Y-%m-%d %H:%M:%S')
        
        # 每天9:30卖出一组期权
        if self.is_dairy_task and self.info == {} and dt.time() == time(9, 30):
            self._sell_options(undl_bar, dt)

    def _sell_options(self, undl_bar, dt):
        data_feed = self.strategy.data_feed
        underlying_symbol = self.strategy.underlying_symbol
        expired_date = get_fourth_wednesday(dt.year, dt.month)
        month_type = 1 if 0 <=(expired_date - dt.date()).days <= self.exit_days else 0
            
        call_symbol = data_feed.get_option_symbol(underlying_symbol, 
                                                                undl_bar['close'], 
                                                                month_type, 
                                                                '购', 
                                                                self.step)
        call_bar = data_feed.get_option_bar(underlying_symbol, call_symbol, undl_bar['datetime'])
        self.short_open(call_symbol, self.amount, call_bar['close'], {'group': {
            'call': {
                'symbol': call_symbol,
                'price': call_bar['close'],
                'amount': self.amount,
                'exit_date': self.get_exit_date(dt, month_type,  expired_date),
                'move_price': self.get_move_price(undl_bar['close'], call_symbol)}

        }})

        put_symbol = data_feed.get_option_symbol(underlying_symbol, 
                                                 undl_bar['close'], 
                                                 month_type, 
                                                 '沽', 
                                                 -self.step)
        put_bar = data_feed.get_option_bar(underlying_symbol, put_symbol, undl_bar['datetime'])
        self.short_open(put_symbol, self.amount, put_bar['close'], {'group': {
            'put': {
                'symbol': put_symbol,
                'price': put_bar['close'],
                'amount': self.amount,
                'exit_date': self.get_exit_date(dt, month_type,  expired_date),
                'move_price': self.get_move_price(undl_bar['close'], put_symbol)}

        }})
        
        self.combinate(call_symbol, -self.amount, put_symbol, -self.amount)
        
        
        
        
    def get_exit_date(self, dt, month_type,  expired_date):
        (year, month) = (dt.year, dt.month + 1) if dt.month < 12 else (dt.year+1, 1)
        next_expired_date = get_fourth_wednesday(year, month)
        if (month_type == 1 and dt.date() >= expired_date 
            or month_type == 0 and dt.date() < expired_date):            
            exit_date = next_expired_date
        else:
            diff_days = (expired_date - dt.date()).days
            exit_date = next_expired_date - timedelta(days=diff_days)
            
        return exit_date.strftime('%Y-%m-%d')
    
    def get_move_price(self, undl_price, symbol):
        name = self.strategy.data_feed.get_option_name(symbol)
        match = re.search(r'(\d+)([A-Za-z]?)$', name)
        if not match:
            return
        
        contract_price = int(match[0]) / 1000
        if contract_price > undl_price:
            return contract_price - (contract_price - undl_price) * self.move_ratio
        else:
            return contract_price + (undl_price - contract_price) * self.move_ratio
            
