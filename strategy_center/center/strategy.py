from datetime import datetime
from center.base import BaseStrategy
from common.constant import Offset, Direction


class OptionStrategy(BaseStrategy):
    
    def __init__(self, id, account_id, name, underlying_symbol, trader, store_host, **kwargs):
        super().__init__(id, account_id, name, underlying_symbol, trader, store_host, **kwargs)
        self.day_contracts = []
    
    def start(self):
        print("OptionEngine start")
    
    def stop(self):
        if self.trader.response_mq is not None:
            self.trader.response_mq.stop()
    
    def on_bars(self, bars):
        super().on_bars(bars)
    
    def on_trade_response(self, body):
        if self.minutes_bars is not None:
            active_time = self.minutes_bars.iloc[-1]['datetime']
            dt = datetime.strptime(active_time, '%Y-%m-%d %H:%M:%S')
            body['date'] = dt.strftime('%Y-%m-%d')
            body['time'] = dt.strftime('%H:%M:%S')
        self.trades.save(body)
        self.holdings.update(self.id, body.copy())
        self.update_groups(body.copy())
    
    def update_groups(self, trade_info):
        trade_info['amount'] = trade_info['amount'] * (1 if trade_info['direction'] == Direction.LONG.value else -1) 
        info  = {                 
                    'symbol': f'{trade_info["code"]}.{trade_info["exchange"]}',
                    'amount': trade_info['amount'] ,
                    'price': trade_info['price']
                }
        group = None
        old_group = self.groups[trade_info['group_id']]
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
                self.groups.add(group)
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
                index = [idx for idx,p in enumerate(positions) if
                           (p['symbol'] == info['symbol'] 
                            and p['amount']/abs(p['amount']) != info['amount']/abs(info['amount']))][0]
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
                
            self.groups.update(group)
        
    def reset(self):
        self.day_contracts = []    
    
           
