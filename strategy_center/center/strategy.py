import traceback
import threading
from datetime import datetime, date
from center.base import BaseStrategy
from common.constant import Offset, Direction

db_lock = threading.Lock()
class OptionStrategy(BaseStrategy):
    
    def __init__(self, id, account_id, name, underlying_symbol, trader, store_host, **kwargs):
        super().__init__(id, account_id, name, underlying_symbol, trader, store_host, **kwargs)
        self.day_contracts = []
        self.groups = []
    
    def start(self):
        print("OptionEngine start")
    
    def stop(self):
        self.trader.stop()
    
    def on_bars(self, bars):
        super().on_bars(bars)
        if self.groups == []:
            self.load()
        
    def load(self):
        print('请在继承类中实现load方法')
    
    def on_trade_response(self, body):
        try:
            with db_lock:
                if self.minutes_bars is not None:
                    active_time = self.minutes_bars.iloc[-1]['datetime']
                    dt = datetime.strptime(active_time, '%Y-%m-%d %H:%M:%S')
                    body['date'] = dt.strftime('%Y-%m-%d')
                    body['time'] = dt.strftime('%H:%M:%S')        
                self.trades_store.save(body)
                
                last_date = self.calendar.get_last_traded_date(body['date'])
                self.holdings_store.update(self.id, body.copy(), last_date)
                
                self.update_groups(body.copy())
        except Exception as e:
            print(e)
            traceback.print_exc()

    
    def dispatch_to_group(self, body):
        if body is None:
            return
        
        for group in self.groups:
            if group.id == body['group_id']:
                group.on_updated(body)
                break
    
    def update_groups(self, trade_info):
        if trade_info['direction'] in [Direction.LONG.value, Direction.SHORT.value]:
            trade_info['amount'] = trade_info['amount'] * (1 if trade_info['direction'] == Direction.LONG.value else -1) 
        info  = {                 
                    'symbol': f'{trade_info["code"]}.{trade_info["exchange"]}',
                    'name': trade_info['name'],
                    'amount': trade_info['amount'] ,
                    'price': trade_info['price']
                }
        if 'group' in trade_info:
            if 'call' in trade_info['group']:
                info['type'] = 'call'
                info['move_ratio'] = trade_info['group']['call']['move_ratio']
            elif 'put' in trade_info['group']:
                info['type'] = 'put'
                info['move_ratio'] = trade_info['group']['put']['move_ratio']
            if 'type' in info:
                for k, v in trade_info['group'][info['type']].items():
                    if k not in ['symbol', 'price', 'amount']:
                        info[k] = v
        
        group = None
        old_group = self.groups_store[trade_info['group_id']]
        if old_group is None:
            if trade_info['offset'] == Offset.OPEN.value:
                group = {
                    'group_id': trade_info['group_id'],
                    'strategy_id': trade_info['strategy_id'],
                    'create_date': trade_info['date'],
                    'create_time': trade_info['time'],
                    'profit': 0,
                    'positions':[info],
                    'combinations': []
                }
                self.groups_store.add(group)
                self.dispatch_to_group(group)
        else:
            group = old_group
            positions = group['positions']
            if trade_info['offset'] == Offset.OPEN.value: 
                index = [idx for idx, p in enumerate(positions) if 
                         (p['symbol'] == info['symbol']
                          and p['amount']/abs(p['amount']) == info['amount']/abs(info['amount']) )]
                if index == []:           
                    positions.append(info)
                else:
                    pos = positions[index[0]]
                    pos['price'] = (pos['price']*pos['amount'] + info['price']*info['amount'])/(pos['amount'] + info['amount'])
                    pos['amount'] = pos['amount'] + info['amount']
            elif trade_info['offset'] == Offset.CLOSE.value:                  
                indexes = [idx for idx,p in enumerate(positions) if
                           (p['symbol'] == info['symbol'] 
                            and p['amount']/abs(p['amount']) != info['amount']/abs(info['amount']))]
                if indexes == []:
                    return  # 按道理说，不应该出现这种情况，平了不存在的仓位，但50ETF20201214出现了此情况
                index = indexes[0]
                pos = positions[index]
                profit = (pos['price'] - info['price']) * info['amount']
                group['profit'] += profit
                pos['amount'] = pos['amount'] + info['amount']                  
 
                group['profit'] += profit
            elif trade_info['offset'] == Offset.COMBINATE.value:
                combinations = group['combinations']
                if trade_info['code'] not in combinations:
                    combinations.append(
                        {
                            'comb_id': trade_info['remark'],
                            'exchange': trade_info['exchange'],
                            'code': trade_info['code'],
                            'amount': f'{trade_info["amount"]}/{trade_info["amount2"]}'
                         })
            elif trade_info['offset'] == Offset.RELEASE.value:
                combinations = group['combinations']
                index = [idx for idx, comb in enumerate(combinations) if comb['comb_id'] == trade_info['code']]
                if index != []:
                    del combinations[index[0]]
            
            # 删除空仓
            empty_position_index = [idx for idx, p in enumerate(group['positions']) if p['amount'] == 0]
            if empty_position_index != []:
                for i in range(len(empty_position_index)):
                    del group['positions'][empty_position_index[i]]
           
            if group['positions'] == []: # 删除空分组
                self.groups_store.delete(group['group_id'])
                self.dispatch_to_group(None)
            else:    
                self.groups_store.update(group)
                self.dispatch_to_group(group)
    
    
    def reset(self):
        self.day_contracts = []    
           
