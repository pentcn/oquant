from strategy_center.center.strategy import OptionStrategy

class DualDragon(OptionStrategy):
    def __init__(self, id, account_id, name, underlying_symbol, **kwargs):
        super().__init__(id, account_id, name, underlying_symbol, **kwargs)
        self.day_group = None
        
    def load(self):
        ...
    
    def on_bars(self, bars):
        print("DualDragon on_bars")
        pass
    
    def run(self):
        ...
    
    def stop(self):
        ...