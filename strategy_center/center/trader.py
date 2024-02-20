import json
import random
from functools import wraps
from strategy_center.center.base import BaseTrader
from strategy_center.center.mq import MessageQueueSender, MessageQueueReceiver
from common.utilities import dataclass_to_dict
from common.constant import (
    Request,
    Direction,
    Offset
)

def mq_trade(method):
    @wraps(method)
    def wrapper(self, strategy, *args, **kwargs):
        self.check_mq()
        strategy, extra_info, req = method(self, strategy, *args, **kwargs)
        #保存请求数据
        req_dict = dataclass_to_dict(req)
        req_dict['strategy_id'] = strategy.id
        if extra_info and len(extra_info) > 0:
            req_dict.update(extra_info)
        strategy.svars_store[req.id] = req_dict 
        self.mq.send(req)
        return req_dict
    return wrapper

class OptionTrader(BaseTrader):
    def __init__(self, store_host, data_feed, mq_params):
        super().__init__(store_host)
        self.data_feed = data_feed

        self.host = mq_params['host']
        self.account_id = mq_params['account_id']
        self.user_name = mq_params['user_name'] if 'user_name' in mq_params else ''
        self.password = mq_params['password'] if 'password' in mq_params else ''
        self.vhost = mq_params['vhost'] if 'vhost' in mq_params else ''

    def check_mq(self):
        if not hasattr(self, 'mq') or self.mq.connection.is_closed:
            self.mq = MessageQueueSender(self.user_name, 
                                         self.password, 
                                         self.host, 
                                         self.vhost, 
                                         self.account_id)
        
        if not hasattr(self, 'response_mq') or self.response_mq.connection.is_closed:
            self.response_mq = MessageQueueReceiver(self.user_name, 
                                                    self.password, 
                                                    self.host, 
                                                    self.vhost,
                                                    f'res{self.account_id}', 
                                                    self.on_response)
            self.response_mq.start()
            
    @mq_trade
    def long_open(self, strategy, symbol, amount, price, extra_info):
        strategy.day_contracts.append(symbol)
        strategy.day_contracts = list(set(strategy.day_contracts))
        
        parts = symbol.split('.')
        code, exchange = parts[0], parts[1]        
        return strategy, extra_info, Request(direction=Direction.LONG, 
                     offset=Offset.OPEN, 
                     exchange=exchange, 
                     code=code, 
                     amount=amount,
                     price=price)
    @mq_trade
    def long_close(self, strategy, symbol, amount, price, extra_info):
        parts = symbol.split('.')
        code, exchange = parts[0], parts[1]
        return strategy, extra_info, Request(direction=Direction.LONG, 
                     offset=Offset.CLOSE, 
                     exchange=exchange, 
                     code=code, 
                     amount=amount,
                     price=price)

    @mq_trade  
    def short_open(self, strategy, symbol, amount, price, extra_info):
        strategy.day_contracts.append(symbol)
        strategy.day_contracts = list(set(strategy.day_contracts))
        
        parts = symbol.split('.')
        code, exchange = parts[0], parts[1]
        return strategy, extra_info, Request(direction=Direction.SHORT, 
                     offset=Offset.OPEN, 
                     exchange=exchange, 
                     code=code, 
                     amount=amount,
                     price=price)

    @mq_trade
    def short_close(self, strategy, symbol, amount, price, extra_info):
        parts = symbol.split('.')
        code, exchange = parts[0], parts[1]
        return strategy, extra_info, Request(direction=Direction.SHORT, 
                     offset=Offset.CLOSE, 
                     exchange=exchange, 
                     code=code, 
                     amount=amount,
                     price=price)
    
    @mq_trade
    def combinate(self, strategy, symbol_1, amount_1, symbol_2, amount_2, extra_info):
        parts = symbol_1.split('.')
        exchange_1, code_1 = parts[1], parts[0]
        parts = symbol_2.split('.')
        _, code_2 = parts[1], parts[0]
        return strategy, extra_info, Request(direction=Direction.NET, 
                     offset=Offset.COMBINATE, 
                     exchange=exchange_1, 
                     code=f'{code_1}/{code_2}', 
                     amount=amount_1,
                     amount2=amount_2)
    
    @mq_trade  
    def release(self, strategy, combination_id, extra_info):
        return strategy, extra_info, Request(direction=Direction.NET, offset=Offset.RELEASE, exchange='', 
                             code=combination_id)
        
    def on_response(self, message):
        obj = json.loads(message)
        if 'req_id' in obj:
            body = self.svars_store[obj['req_id']]['body']
            body = json.loads(body)
            body['remark'] = obj['remark']
            if self.data_feed is not None and self.data_feed.engine is not None:
                self.data_feed.engine.on_trade_response(body)
            # print(body)
        ...
    
class BacktestOptionTrader(OptionTrader):
    def __init__(self, store_host, data_feed, mq_params):
        super().__init__(store_host, data_feed, mq_params)
        

    def long_open(self, strategy, symbol, amount, price, extra_info):
        req_dict = super().long_open(strategy, symbol, amount, price, extra_info)
        res_message = json.dumps({'req_id': req_dict['id'], 'remark': ''})
        self.mq.response(res_message)
        return req_dict
        
    
    def long_close(self, strategy, symbol, amount, price, extra_info):
        req_dict = super().long_close(strategy, symbol, amount, price, extra_info)
        res_message = json.dumps({'req_id': req_dict['id'], 'remark': ''})
        self.mq.response(res_message)
        return req_dict
    
    def short_open(self, strategy, symbol, amount, price, extra_info):
        req_dict = super().short_open(strategy, symbol, amount, price, extra_info)
        res_message = json.dumps({'req_id': req_dict['id'], 'remark': ''})
        self.mq.response(res_message)
        return req_dict
    
    def short_close(self, strategy, symbol, amount, price, extra_info):
        req_dict = super().short_close(strategy, symbol, amount, price, extra_info)
        res_message = json.dumps({'req_id': req_dict['id'], 'remark': ''})
        self.mq.response(res_message)
        return req_dict
    
    def combinate(self, strategy, symbol_1, amount_1, symbol_2, amount_2, extra_info):
        req_dict = super().combinate(strategy, symbol_1, amount_1, symbol_2, amount_2, extra_info)
        res_message = json.dumps({'req_id': req_dict['id'], 'remark': f'{random.randint(100000, 999999)}'})
        self.mq.response(res_message)
        return req_dict

    def release(self, strategy, combination_id, extra_info):
        req_dict = super().release(strategy, combination_id, extra_info)
        res_message = json.dumps({'req_id': req_dict['id'], 'remark': ''})
        self.mq.response(res_message)
        return req_dict
    
    