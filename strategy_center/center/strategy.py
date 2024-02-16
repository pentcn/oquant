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