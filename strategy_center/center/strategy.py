from center.base import BaseStrategy

class OptionStrategy(BaseStrategy):
    
    def __init__(self, id, account_id, name, underlying_symbol, **kwargs):
        super().__init__(id, account_id, name, underlying_symbol, **kwargs)
    
    def start(self):
        print("OptionEngine start")
    
    def stop(self):
        print("OptionEngine stop")
    
    def on_bars(self, bars):
        print("OptionEngine on_bars")