import re
from datetime import datetime, time, date
from common.utilities import get_fourth_wednesday
from strategy_center.center.base import OptionGroup
from datetime import timedelta
from icecream import ic


class DualDragonCombinations(OptionGroup):
    def __init__(self, strategy, amount=1, move_ratio=0.5, is_dairy_task=True):
        super().__init__(strategy)
        self.is_dairy_task = is_dairy_task
        
        self.exit_days = 25
        self.step = 0.06
        self.both_moving = True
        self.move_ratio = move_ratio
        self.amount = amount

        
        self.call_info = None
        self.put_info = None
        self.combination_info = None

    def on_bars(self, bars):  
        super().on_bars(bars)    
        
        undl_bar = bars[self.strategy.underlying_symbol]
        dt = datetime.strptime(undl_bar['datetime'], '%Y-%m-%d %H:%M:%S')
        
        # 每天9:30卖出一组期权
        # if self.is_dairy_task and self._is_none_group() and dt.time() == time(9, 30):
        #     if dt.date() == date(2020, 6, 17):
        #         self._sell_options(undl_bar)
        
        if self.is_dairy_task:
            if dt.date() == date(2020, 6, 17):
                if dt.time() == time(9, 35):
                    self._sell_call(undl_bar)
                elif dt.time() == time(9, 41):
                    self._sell_put(undl_bar)
        
        
        
        # 每天9:30完成符合条件的平仓任务
        if dt.time() == time(14, 55) :
            self._check_and_close_group(dt)
            
        # 移仓任务
        if dt.time() == time(14, 50):
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
                base_symbol, base_price, extra_info = self.find_target(undl_price, op_type, undl_time)
                extra_info['group'][op_type2]['exit_date'] = base_info['exit_date']
                
                # 平仓超价的合约
                bar = data_feed.get_option_bar(underlying_symbol, base_info['symbol'], undl_time)
                self.long_close(base_info['symbol'], abs(base_info['amount']), bar['close'])
                
                # 开仓新合约
                self.short_open(base_symbol, abs(base_info['amount']), base_price, extra_info)
                
                # 作另一端合约的处理
                if both:
                    # 计算新合约信息
                    op_type = '购' if other_info == self.call_info else '沽'
                    op_type2 = 'call' if op_type=='购' else 'put'
                    other_symbol, other_price, extra_info = self.find_target(undl_price, op_type, undl_time)
                    
                    # todo: 如果other_symbol同原合约代码相同，则不新开仓
                    extra_info['group'][op_type2]['exit_date'] = base_info['exit_date']
                    
                    # 平仓以前组合的合约
                    bar = data_feed.get_option_bar(underlying_symbol, other_info['symbol'], undl_time)
                    self.long_close(other_info['symbol'], abs(other_info['amount']), bar['close'])
                
                    # 开仓新合约
                    self.short_open(other_symbol, abs(other_info['amount']), other_price, extra_info)
                
                if both:
                    self.combinate(base_symbol, base_info['amount'], other_symbol, other_info['amount'])
                else:
                    self.combinate(base_symbol, base_info['amount'], other_info['symbol'], other_info['amount'])
                ...
   
   
    def _sell_options(self, undl_bar):
        self._sell_call(undl_bar)
        self._sell_put(undl_bar)      

        
        
        
    def _sell_call(self, undl_bar):
        call_symbol, call_price, extra_info = self.find_target(undl_bar['close'], '购', undl_bar['datetime'])
        self.short_open(call_symbol, self.amount, call_price, extra_info)
        
        if self.put_info is not None:
            put_symbol = self.put_info['symbol']
            self.combinate(call_symbol, -self.amount, put_symbol, -self.amount)
        
    def _sell_put(self, undl_bar):
        put_symbol, put_price, extra_info = self.find_target(undl_bar['close'], '沽', undl_bar['datetime'])
        self.short_open(put_symbol, self.amount, put_price, extra_info)
        
        if self.call_info is not None:
            call_symbol = self.call_info['symbol']        
            self.combinate(call_symbol, -self.amount, put_symbol, -self.amount)
            
    
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
                
    def get_exit_date(self, dt, month_type,  expired_date):
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
    
    def get_move_price(self, undl_price, symbol):
        name = self.strategy.data_feed.get_option_name(symbol)
        match = re.search(r'(\d+)([A-Za-z]?)$', name)
        if not match:
            return
        
        contract_price = int(match[0]) / 1000
        if contract_price > undl_price:
            return contract_price - (contract_price - undl_price) * self.move_ratio
        else:
            return contract_price + (undl_price - contract_price) * self.move_ratio
    
    def find_target(self, base_price, option_type, sdatetime):
        dt = datetime.strptime(sdatetime, '%Y-%m-%d %H:%M:%S')
        expired_date = get_fourth_wednesday(dt.year, dt.month)
        start_month_type = 1 if (expired_date - dt.date()).days <= self.exit_days else 0
        for month_type in range(start_month_type, 4):
            symbol, price = self.get_symbol_and_price( base_price, month_type, option_type, sdatetime, self.step)
            if price is not None:
                exit_date = self.get_exit_date(dt, month_type, expired_date)
                move_price = self.get_move_price(base_price, symbol)
                extra_key = 'call' if option_type == '购' else 'put'
                extra_value = {
                        'symbol': symbol,
                        'price': price,
                        'amount': self.amount,
                        'exit_date': exit_date,
                        'move_price': move_price,
                        'move_ratio': self.move_ratio}
                extra_info = {'group': {extra_key: extra_value}}
                return symbol, price, extra_info
            
        raise Exception('无法找到交易标的')
    
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
        
    def find_contract_symbol(self, base_price, month_type, option_type, step):
        symbol_info =self.strategy.data_feed.get_option_symbol_by_percent(self.strategy.underlying_symbol, 
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
            
            if (option_type == '购' and price > base_price * 1.01) or (option_type == '沽' and price < base_price * 0.99):
                return symbol