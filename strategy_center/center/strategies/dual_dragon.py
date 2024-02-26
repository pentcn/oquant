from strategy_center.center.strategy import OptionStrategy
from strategy_center.center.option_group import DualDragonCombinations


class DualDragon(OptionStrategy):
    def __init__(self, id, account_id, name, underlying_symbol, trader, store_host, amount=1):
        super().__init__(id, account_id, name, underlying_symbol, trader, store_host, amount=amount)        
        self.day_group = None
             
    def load(self):
        print('Todo: load day_group')
    
    def on_bars(self, bars):     
        super().on_bars(bars)
        
        if self.groups == []:
            self.day_group = DualDragonCombinations(self, amount=1, is_dairy_task=True)
            self.day_group.create_id()
            self.groups.append(self.day_group)
            
        [group.on_bars(bars) for group in self.groups]        
        
    def on_trade_response(self, body):
        super().on_trade_response(body)
        ...
         
    def run(self):
        ...
        
    def reset(self):
        super().reset()
        
        self.day_group = None
        self.groups = []
    
       
