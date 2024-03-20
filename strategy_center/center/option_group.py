import re
from datetime import datetime, time, date
from common.utilities import get_fourth_wednesday
from strategy_center.center.base import BaseGroup
from datetime import timedelta
from icecream import ic


class OptionGroup(BaseGroup):
    
    def __init__(self, strategy):
        super().__init__(strategy)

    def get_pallel_date(self, dt, month_type,  expired_date):
        """
        根据离最近的期权行权日的相差日期计算离目标月份行权日对应的日期

        参数：
            dt：日期输入
            month_type：表示月份类型的整数，0 表示当月，1 表示下一月
            expired_date：dt所在月份的期权到期日期

        返回：
            一个格式化的字符串，表示以 'YYYY-MM-DD' 格式计算出的退出日期。
        """
        (year, month) = (dt.year, dt.month + 1) if dt.month < 12 else (dt.year+1, 1)
        next_expired_date = get_fourth_wednesday(year, month)
        
        if month_type == 1 and dt.date() >= expired_date:            
            exit_date = next_expired_date
        elif month_type == 0 and dt.date() < expired_date:
            exit_date = expired_date
        else:
            diff_days = (expired_date - dt.date()).days
            exit_date = next_expired_date - timedelta(days=diff_days)
            
        return exit_date.strftime('%Y-%m-%d')

    def find_contract_symbol(self, base_price, month_type, option_type, step):
        if type(step) is float:
            symbol_info = self.strategy.data_feed.get_option_symbol_by_percent(self.strategy.underlying_symbol, 
                                                            base_price, 
                                                            month_type, 
                                                            option_type, 
                                                            step if option_type=='购' else -step)
        elif type(step) is int:
            symbol_info = self.strategy.data_feed.get_option_symbol_by_rank(self.strategy.underlying_symbol, 
                                                            base_price, 
                                                            month_type, 
                                                            option_type, 
                                                            step if option_type=='购' else -step)
        if symbol_info is not None:
            idx = 0
            if len(symbol_info) > 1:
                idx = 1 if option_type == '购' else 0
            symbol = symbol_info[idx]['code']
            price = int(symbol_info[idx]['strike_price']) / 1000
            
            if (option_type == '购' and price > base_price) or (option_type == '沽' and price < base_price):
                return symbol

    def get_symbol_and_price(self, base_price, month_type, option_type, datetime, step):
        symbol = self.find_contract_symbol(base_price, month_type, option_type, step)
        if symbol is not None:
            bar = self.strategy.data_feed.get_option_bar(self.strategy.underlying_symbol, symbol, datetime)
            if bar['amount'] > 0:
                return symbol, bar['close']
            else: 
                step = step * 0.95
                return self.get_symbol_and_price(base_price, month_type, option_type, datetime, step)
        return None, None

    def find_target(self, base_price, option_type, sdatetime, exit_days, step, move_ratio, extra_info={}):
        def get_target_info(base_price, option_type, move_ratio, extra_info, dt, expired_date, month_type, symbol, price):
            exit_date = self.get_pallel_date(dt, month_type, expired_date)
            move_price = self.get_move_price(base_price, symbol, move_ratio)
            extra_key = 'call' if option_type == '购' else 'put'
            extra_value = {
                        'symbol': symbol,
                        'price': price,
                        'exit_date': exit_date,
                        'move_price': move_price,
                        }
            extra_value.update(extra_info)
            extra_info = {'group': {extra_key: extra_value}}
            return symbol, price, extra_info    
        
        dt = datetime.strptime(sdatetime, '%Y-%m-%d %H:%M:%S')
        expired_date = get_fourth_wednesday(dt.year, dt.month)
        start_month_type = 1 if (expired_date - dt.date()).days <= exit_days else 0
        for month_type in range(start_month_type, 4):
            symbol, price = self.get_symbol_and_price(base_price, month_type, option_type, sdatetime, step)
            if price is not None:
                return get_target_info(base_price, option_type, move_ratio, extra_info, dt, expired_date, month_type, symbol, price)
        
        return None, None, None
        # 如果没有找到合约，找反方向的实值合约，做同方向操作
        # op_type = '沽' if option_type == '购' else '购'
        # symbol, price = self.get_symbol_and_price(base_price, month_type, op_type, sdatetime, step)
        # if price is not None:
        #     return get_target_info(base_price, option_type, move_ratio, extra_info, dt, expired_date, month_type, symbol, price)

        # raise Exception('无法找到交易标的')

    def get_move_price(self, undl_price, symbol, move_ratio, is_out_money=True):
        name = self.strategy.data_feed.get_option_name(symbol)
        match = re.search(r'(\d+)([A-Za-z]?)$', name)
        if not match:
            return
        
        contract_price = int(match[0]) / 1000
        if is_out_money:
            if contract_price > undl_price:
                return contract_price - (contract_price - undl_price) * move_ratio
            else:
                return contract_price + (undl_price - contract_price) * move_ratio
        else:
            if contract_price > undl_price:
                return contract_price + (undl_price - contract_price) * move_ratio
            else:
                return contract_price - (contract_price - undl_price) * move_ratio
                
                
class StraddleGroup(OptionGroup):
    def __init__(self, strategy, amount=1, move_ratio=0.3, exit_days=25, step=0.06, both_moving=True):
        super().__init__(strategy)
        
        self.exit_days = exit_days
        self.step = step
        self.both_moving = both_moving
        self.move_ratio = move_ratio
        self.amount = amount
                
        self.call_info = None
        self.put_info = None
        self.combination_info = None

    def _move_group(self, undl_time, undl_price, both=True):
        data_feed = self.strategy.data_feed
        underlying_symbol = self.strategy.underlying_symbol
        if self.combination_info is not None and self.combination_info != []:
            if undl_price > self.call_info['move_price']:
                base_info = self.call_info
                other_info = self.put_info
            if undl_price < self.put_info['move_price']:    
                base_info = self.put_info
                other_info = self.call_info
            if undl_price > self.call_info['move_price'] or undl_price < self.put_info['move_price']:    
                # 解除组合
                self.release(self.combination_info[0]['comb_id'])
                
                # 计算新合约信息
                op_type = '购' if base_info == self.call_info else '沽'
                op_type2 = 'call' if op_type=='购' else 'put'
                base_symbol, base_price, extra_info = self.find_target(undl_price, 
                                                                       op_type, 
                                                                       undl_time, 
                                                                       self.exit_days, 
                                                                       self.step,
                                                                       self.move_ratio,
                                                                        {                        
                                                                        'amount': self.amount,
                                                                        'move_ratio': self.move_ratio
                                                                        })
                if base_symbol is not None:
                    extra_info['group'][op_type2]['exit_date'] = base_info['exit_date']
                
                # 平仓超价的合约
                bar = data_feed.get_option_bar(underlying_symbol, base_info['symbol'], undl_time)
                self.long_close(base_info['symbol'], abs(base_info['amount']), bar['close'])
                
                # 开仓新合约
                if base_symbol is not None:
                    self.short_open(base_symbol, abs(base_info['amount']), base_price, extra_info)
                
                # 作另一端合约的处理
                if both:
                    # 计算新合约信息
                    op_type = '购' if other_info == self.call_info else '沽'
                    op_type2 = 'call' if op_type=='购' else 'put'
                    other_symbol, other_price, extra_info = self.find_target(undl_price, 
                                                                             op_type, 
                                                                             undl_time, 
                                                                             self.exit_days, 
                                                                             self.step,
                                                                             self.move_ratio,
                                                                            {                        
                                                                            'amount': self.amount,
                                                                            'move_ratio': self.move_ratio
                                                                            }
                                                                             )
                    
                    # todo: 如果other_symbol同原合约代码相同，则不新开仓
                    extra_info['group'][op_type2]['exit_date'] = base_info['exit_date']
                    
                    # 平仓以前组合的合约
                    bar = data_feed.get_option_bar(underlying_symbol, other_info['symbol'], undl_time)
                    self.long_close(other_info['symbol'], abs(other_info['amount']), bar['close'])
                
                    # 开仓新合约
                    if base_symbol is not None:
                        self.short_open(other_symbol, abs(other_info['amount']), other_price, extra_info)
                elif base_symbol is None:
                    # 平仓以前组合的合约
                    bar = data_feed.get_option_bar(underlying_symbol, other_info['symbol'], undl_time)
                    self.long_close(other_info['symbol'], abs(other_info['amount']), bar['close'])
                
                
                if base_symbol is not None:
                    if both:
                        self.combinate(base_symbol, base_info['amount'], other_symbol, other_info['amount'])
                    else:
                        self.combinate(base_symbol, base_info['amount'], other_info['symbol'], other_info['amount'])
                    
                if hasattr(self, 'on_moved'):
                    self.on_moved(undl_time, undl_price)
   
    def _sell_options(self, undl_bar):
        self._sell_call(undl_bar)
        self._sell_put(undl_bar)      
        
    def _sell_call(self, undl_bar):
        call_symbol, call_price, extra_info = self.find_target(undl_bar['close'], 
                                                               '购', 
                                                               undl_bar['datetime'], 
                                                               self.exit_days, 
                                                               self.step,
                                                               self.move_ratio,
                                                               {                        
                                                                'amount': self.amount,
                                                                'move_ratio': self.move_ratio
                                                            }
                                                               )
        if call_symbol is not None:
            self.short_open(call_symbol, self.amount, call_price, extra_info)
            
            if self.put_info is not None:
                put_symbol = self.put_info['symbol']
                self.combinate(call_symbol, -self.amount, put_symbol, -self.amount)
        elif self.put_info is not None:
            # 如何当天的合约范围操过指定范围，则平仓本group
            bar = self.strategy.data_feed.get_option_bar(self.strategy.underlying_symbol, self.put_info['symbol'], undl_bar['datetime'])
            self.long_close(self.put_info['symbol'], abs(self.put_info['amount']), bar['close'])
        
    def _sell_put(self, undl_bar):
        put_symbol, put_price, extra_info = self.find_target(undl_bar['close'], 
                                                             '沽', 
                                                             undl_bar['datetime'], 
                                                             self.exit_days, 
                                                             self.step,                                                             
                                                             self.move_ratio,
                                                             {                        
                                                              'amount': self.amount,
                                                              'move_ratio': self.move_ratio
                                                              }
                                                             )
        if put_symbol is not None:
            self.short_open(put_symbol, self.amount, put_price, extra_info)
            
            if self.call_info is not None:
                call_symbol = self.call_info['symbol']        
                self.combinate(call_symbol, -self.amount, put_symbol, -self.amount)
        elif self.call_info is not None:
            # 如何当天的合约范围操过指定范围，则平仓本group
            bar = self.strategy.data_feed.get_option_bar(self.strategy.underlying_symbol, self.call_info['symbol'], undl_bar['datetime'])
            self.long_close(self.call_info['symbol'], abs(self.call_info['amount']), bar['close'])
    
    def _is_none_group(self):
        return self.call_info is None and self.put_info is None
    
    def _check_and_close_group(self, dt):
        data_feed = self.strategy.data_feed
        underlying_symbol = self.strategy.underlying_symbol
        if self.combination_info is not None and self.combination_info != []:
            if self.call_info['exit_date'] <= dt.strftime('%Y-%m-%d'):
                self.release(self.combination_info[0]['comb_id'])
                call_bar = data_feed.get_option_bar(underlying_symbol, self.call_info['symbol'], dt.strftime('%Y-%m-%d %H:%M:%S'))
                self.long_close(self.call_info['symbol'], abs(self.call_info['amount']), call_bar['close'])
                put_bar = data_feed.get_option_bar(underlying_symbol, self.put_info['symbol'], dt.strftime('%Y-%m-%d %H:%M:%S'))
                self.long_close(self.put_info['symbol'], abs(self.put_info['amount']), put_bar['close'])
        elif self.call_info is not None and self.call_info['exit_date'] <= dt.strftime('%Y-%m-%d'):
                call_bar = data_feed.get_option_bar(underlying_symbol, self.call_info['symbol'], dt.strftime('%Y-%m-%d %H:%M:%S'))
                self.long_close(self.call_info['symbol'], abs(self.call_info['amount']), call_bar['close'])
        elif self.put_info is not None and self.put_info['exit_date'] <= dt.strftime('%Y-%m-%d'):
                put_bar = data_feed.get_option_bar(underlying_symbol, self.put_info['symbol'], dt.strftime('%Y-%m-%d %H:%M:%S'))
                self.long_close(self.put_info['symbol'], abs(self.put_info['amount']), put_bar['close'])


class DualDragonCombinations(StraddleGroup):
    def __init__(self, strategy, amount=1, move_ratio=0.5, exit_days=25, step=3, both_moving=False, is_dairy_task=True):
        super().__init__(strategy, amount, move_ratio, exit_days, step, both_moving)
        
        self.is_dairy_task = is_dairy_task
        
        self.long_call = None
        self.long_put = None
        

    def on_bars(self, bars):  
        super().on_bars(bars)    
        
        undl_bar = bars[self.strategy.underlying_symbol]
        dt = datetime.strptime(undl_bar['datetime'], '%Y-%m-%d %H:%M:%S')
        
        if self.is_dairy_task:
            if dt.time() == time(9, 35):
                self._sell_call(undl_bar)
            elif dt.time() == time(9, 41):
                self._sell_put(undl_bar)
        
        # 每天收盘前完成符合条件的平仓任务
        if dt.time() == time(14, 55) :
            self._check_and_close_group(dt)
            
        # 移仓任务
        if dt.time() in [time(9, 30), time(10, 30), time(11, 29), time(14), time(14, 50)] :
            self._move_group(undl_bar['datetime'], undl_bar['close'], both=self.both_moving)
            

    def on_updated(self, group_info):
        if group_info is None:
            self.call_info = None
            self.put_info = None
            self.combination_info = None
        else:
            self.combination_info = group_info['combinations']
            for pos in group_info['positions']:
                if pos['type'] == 'call':
                    self.call_info = pos
                elif pos['type'] == 'put':
                    self.put_info = pos
                    
    def on_moved(self, undl_time, undl_price):
        long_step = 1
        test = self.find_target(undl_price, 
                         '沽', 
                         undl_time, 
                         self.exit_days, 
                         long_step, 
                         self.move_ratio,
                         {                        
                            'amount': self.amount,
                            'move_ratio': self.move_ratio
                            })
        
        test_1 = self.find_target(undl_price, 
                         '购', 
                         undl_time, 
                         self.exit_days, 
                         long_step, 
                         self.move_ratio,
                         {                        
                            'amount': self.amount,
                            'move_ratio': self.move_ratio
                            })
        # ic(undl_time, undl_price, test, test_1)


    

            
