from datetime import datetime
from center.base import BaseStrategy


class OptionStrategy(BaseStrategy):
    
    def __init__(self, id, account_id, name, underlying_symbol, trader, store_host, **kwargs):
        super().__init__(id, account_id, name, underlying_symbol, trader, store_host, **kwargs)
    
    def start(self):
        print("OptionEngine start")
    
    def stop(self):
        print("OptionEngine stop")
    
    def on_bars(self, bars):
        super().on_bars(bars)
        
    def get_holdings_symbols(self):
        symbols = []
        print("OptionEngine get_option_symbol")
        return symbols
    
    def on_trade_response(self, body):
        if self.minutes_bars is not None:
            active_time = self.minutes_bars.iloc[-1]['datetime']
            dt = datetime.strptime(active_time, '%Y-%m-%d %H:%M:%S')
            body['date'] = dt.strftime('%Y-%m-%d')
            body['time'] = dt.strftime('%H:%M:%S')
        self.trades.save(body)
        ...