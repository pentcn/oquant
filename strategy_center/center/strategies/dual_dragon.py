from strategy_center.center.strategy import OptionStrategy
from strategy_center.center.option_group import DualDragonCombinations


class DualDragon(OptionStrategy):
    def __init__(self, id, account_id, name, underlying_symbol, trader, store_host, amount=1, move_ratio=0.5):
        super().__init__(id, account_id, name, underlying_symbol, trader, store_host, amount=amount, move_ratio=0.5)       
        

             
    def load(self):
        has_dairy_task = False
        groups = self.groups_store.get_all(self.id)
        for group_info in groups:
            amount = group_info['positions'][0]['amount']
            move_ratio = group_info['positions'][0]['move_ratio']
            if group_info['combinations'] == []:
                group = DualDragonCombinations(self, amount, move_ratio, is_dairy_task=True)
                group.set_id(group_info['group_id'])
                has_dairy_task = True
            else:
                group = DualDragonCombinations(self, amount, move_ratio)
                group.set_id(group_info['group_id'])
                group.combination_info = group_info['combinations']
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
    
    def on_bars(self, bars):     
        super().on_bars(bars)
        
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
    
       
