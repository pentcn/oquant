from strategy_center.center.strategy import OptionStrategy
from strategy_center.center.option_group import DualDragonCombinations


class DualDragon(OptionStrategy):
    def __init__(self, id, account_id, name, underlying_symbol, trader, store_host, amount=1, move_ratio=0.5):
        super().__init__(id, account_id, name, underlying_symbol, trader, store_host, amount=amount, move_ratio=0.5)       
        

             
    def before_market(self):
        has_dairy_task = False
        groups = self.groups_store.get_all(self.id)
        for group_info in groups:
            amount = group_info['positions'][0]['amount']
            move_ratio = group_info['positions'][0]['move_ratio']
            if group_info['combinations'] == []:
                group = DualDragonCombinations(self, amount, move_ratio, is_dairy_task=False)
                group.set_id(group_info['group_id'])
                has_dairy_task = True
            else:
                group = DualDragonCombinations(self, amount, move_ratio, is_dairy_task=False)
                group.set_id(group_info['group_id'])
                group.combination_info = group_info['combinations']
                if 'positions' in group_info and group_info['positions'] != []:
                    group.options = [pos['symbol'] for pos in group_info['positions']]
            for pos in group_info['positions']:
                if pos['type'] == 'call':
                    group.call_info = pos
                elif pos['type'] == 'put':
                    group.put_info = pos
                else:
                    raise Exception('type error')
            self.groups.append(group)
        
        if not has_dairy_task:
            group = DualDragonCombinations(self, self.amount, self.move_ratio, is_dairy_task=True)
            group.create_id()
            self.groups.append(group)
            
        self.subscribe(self.underlying_symbol)
    
    def on_bars(self, bars):     
        super().on_bars(bars)
        
        for i, group in enumerate(self.groups):
            group_bars = {self.underlying_symbol: bars[self.underlying_symbol]}
            group_bars.update({k: v for k, v in bars.items() if k in group.options})
            group.on_bars(group_bars)
        
    def on_trade_response(self, body):
        super().on_trade_response(body)
        ...
         
    def run(self):
        ...
        
    def reset(self):
        super().reset()
        
        self.day_group = None
        self.groups = []
    
       
