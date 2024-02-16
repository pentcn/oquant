import json
import random
from functools import wraps
from strategy_center.center.base import BaseTrader
from strategy_center.center.mq import MessageQueueSender, MessageQueueReceiver
from common.constant import (
    Request,
    Direction,
    Offset
)

def mq_trade(method):
    @wraps(method)
    def wrapper(self, *args, **kwargs):
        self.check_mq()
        req = method(self, *args, **kwargs)
        self.mq.send(req)
        return req
    return wrapper

class OptionTrader(BaseTrader):
    def __init__(self, data_feed, mq_params):
        super().__init__()
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
    def long_open(self, symbol, amount, price):
        parts = symbol.split('.')
        exchange, code = parts[0], parts[1]
        return Request(direction=Direction.LONG, 
                     offset=Offset.OPEN, 
                     exchange=exchange, 
                     code=code, 
                     amount=amount,
                     price=price)
    @mq_trade
    def long_close(self, symbol, amount, price):
        parts = symbol.split('.')
        exchange, code = parts[0], parts[1]
        return Request(direction=Direction.LONG, 
                     offset=Offset.CLOSE, 
                     exchange=exchange, 
                     code=code, 
                     amount=amount,
                     price=price)

    @mq_trade  
    def short_open(self, symbol, amount, price):
        parts = symbol.split('.')
        exchange, code = parts[0], parts[1]
        return Request(direction=Direction.SHORT, 
                     offset=Offset.OPEN, 
                     exchange=exchange, 
                     code=code, 
                     amount=amount,
                     price=price)

    @mq_trade
    def short_close(self, symbol, amount, price):
        parts = symbol.split('.')
        exchange, code = parts[0], parts[1]
        return Request(direction=Direction.SHORT, 
                     offset=Offset.CLOSE, 
                     exchange=exchange, 
                     code=code, 
                     amount=amount,
                     price=price)
    
    @mq_trade
    def combinate(self, symbol_1, amount_1, symbol_2, amount_2):
        parts = symbol_1.split('.')
        exchange_1, code_1 = parts[0], parts[1]
        parts = symbol_2.split('.')
        _, code_2 = parts[0], parts[1]
        return Request(direction=Direction.NET, 
                     offset=Offset.COMBINATE, 
                     exchange=exchange_1, 
                     code=f'{code_1}/{code_2}', 
                     amount=amount_1,
                     amount2=amount_2)
    
    @mq_trade  
    def release(self, combination_id, exchange_id):
        return Request(direction=Direction.NET, offset=Offset.RELEASE, exchange=exchange_id, 
                             code=combination_id)
        
    def on_response(self, message):
        print(message)
    
class BacktestOptionTrader(OptionTrader):
    def __init__(self, data_feed, mq_params):
        super().__init__(data_feed, mq_params)
        

    def long_open(self, symbol, amount, price):
        req = super().long_open(symbol, amount, price)
        res_message = json.dumps({'req_id': req.id, 'remark': ''})
        self.mq.response(res_message)
        return req
        
    
    def long_close(self, symbol, amount, price):
        req = super().long_close(symbol, amount, price)
        res_message = json.dumps({'req_id': req.id, 'remark': ''})
        self.mq.response(res_message)
        return req
    
    def short_open(self, symbol, amount, price):
        req = super().short_open(symbol, amount, price)
        res_message = json.dumps({'req_id': req.id, 'remark': ''})
        self.mq.response(res_message)
        return req
    
    def short_close(self, symbol, amount, price):
        req = super().short_close(symbol, amount, price)
        res_message = json.dumps({'req_id': req.id, 'remark': ''})
        self.mq.response(res_message)
        return req
    
    def combinate(self, symbol_1, amount_1, symbol_2, amount_2):
        req = super().combinate(symbol_1, amount_1, symbol_2, amount_2)
        res_message = json.dumps({'req_id': req.id, 'remark': ''})
        self.mq.response(res_message)
        return req

    def release(self, combination_id):
        req = super().release(combination_id)
        res_message = json.dumps({'req_id': req.id, 'remark': f'{random.randint(100000, 999999)}'})
        self.mq.response(res_message)
        return req
    
    