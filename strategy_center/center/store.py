import json
from time import sleep
from datetime import datetime
from pymongo import MongoClient, errors

from common.constant import Direction, Offset

class MongoDBManager:

    def __init__(self, db_name, host='localhost', port=27017, retry_delay=1, max_retries=3):
        """实际的初始化方法"""
        self.host = host
        self.port = port
        self.db_name = db_name
        self.retry_delay = retry_delay
        self.max_retries = max_retries
        self.client = None
        self.db = None
        self._connect()

    def _connect(self):
        """建立 MongoDB 连接"""
        try:
            self.client = MongoClient(self.host, self.port, serverSelectionTimeoutMS=5000)
            self.db = self.client[self.db_name]
            self.client.admin.command('ismaster')
        except errors.ServerSelectionTimeoutError:
            print("Failed to connect to server.")

    def _reconnect(self):
        """尝试重新连接"""
        for attempt in range(self.max_retries):
            sleep(self.retry_delay)
            try:
                self._connect()
                print("Reconnected to MongoDB server.")
                return
            except errors.ServerSelectionTimeoutError:
                print(f"Reconnect attempt {attempt + 1} failed.")
        raise Exception("Could not reconnect to MongoDB server.")

    def _auto_reconnect(func):
        """装饰器: 自动处理重连逻辑"""
        def wrapper(self, collection_name, *args, **kwargs):
            try:
                return func(self, collection_name, *args, **kwargs)
            except (errors.AutoReconnect, errors.ServerSelectionTimeoutError):
                print("Lost connection to MongoDB server. Attempting to reconnect...")
                self._reconnect()
                return func(self, collection_name, *args, **kwargs)
        return wrapper

    @_auto_reconnect
    def insert_data(self, collection_name, data):
        """插入数据到指定的集合"""
        collection = self.db[collection_name]
        return collection.insert_one(data).inserted_id if isinstance(data, dict) else collection.insert_many(data).inserted_ids

    @_auto_reconnect
    def find_data(self, collection_name, query={}):
        """从指定的集合查询数据"""
        collection = self.db[collection_name]
        return list(collection.find(query))

    @_auto_reconnect
    def find_one(self, collection_name, query={}):
        collection = self.db[collection_name]
        return collection.find_one(query)

    @_auto_reconnect
    def update_data(self, collection_name, query, update_values):
        """更新指定集合中符合条件的数据"""
        collection = self.db[collection_name]
        doc = self.find_one(collection_name, query)
        if doc is None:
            return collection.insert_one(update_values)
        else:
            return collection.update_one(query, {'$set': update_values})
        # return collection.update_many(query, {'$set': update_values})

    @_auto_reconnect
    def delete_data(self, collection_name, query):
        """从指定集合删除符合条件的数据"""
        collection = self.db[collection_name]
        return collection.delete_many(query)
    
    @_auto_reconnect
    def has_document(self, collection_name, query):        
        collection = self.db[collection_name]
        doc = collection.find_one(query)
        return True if doc else False
    

    def close_connection(self):
        """关闭 MongoDB 连接"""
        if self.client:
            self.client.close()


class StrategyVars(MongoDBManager):
    
    def __init__(self, db_name='oquant_runtime', host='127.0.0.1'):
        super().__init__(db_name, host)
        self.collection_name = 'vars'
    
    def __setitem__(self, key, json_object):
        doc = {
            'uuid': key,
            'date': datetime.now().strftime('%Y-%m-%d'),
            'time': datetime.now().strftime('%H:%M:%S'),
            'body': json.dumps(json_object)       
        }
        
        
        if 'id' in json_object :
            cond = {'uuid':json_object['id']}
            if self.has_document(self.collection_name, cond):
                self.update_data(self.collection_name, cond, doc)
                return
        
        self.insert_data(self.collection_name, doc)
        
    def __getitem__(self, key):
        cond = {'uuid': key}
        return self.find_one(self.collection_name, cond)
    
    def clear(self, strategy_id):
        cond = {
            "body": {
                "$regex": f'.*"strategy_id": "{strategy_id}".*'
            }
        }
        self.delete_data(self.collection_name, cond)


class StrategyTrades(MongoDBManager):
    
    def __init__(self,account_id,  db_name='oquant_runtime', host='127.0.0.1'):
        super().__init__(db_name, host)
        self.collection_name = f'{account_id}:trades'
    
    def save(self, trade_info):
        self.insert_data(self.collection_name, trade_info)
        
    def clear(self, strategy_id):
        cond = {'strategy_id': strategy_id}
        self.delete_data(self.collection_name, cond)


class StrategyGroups(MongoDBManager):
    
    def __init__(self,account_id,  db_name='oquant_runtime', host='127.0.0.1'):
        super().__init__(db_name, host)
        self.collection_name = f'{account_id}:groups'
    
    def __getitem__(self, group_id):
        query = {'group_id': group_id}
        return self.find_one(self.collection_name, query)
    
    def add(self, groups_info):
        self.insert_data(self.collection_name, groups_info)
        
    def update(self, groups_info):
        self.update_data(self.collection_name, {'group_id': groups_info['group_id']}, groups_info)
        
    def clear(self, strategy_id):
        cond = {'strategy_id': strategy_id}
        self.delete_data(self.collection_name, cond)


        
class StrategyHoldings(MongoDBManager):
    
    def __init__(self,account_id,  db_name='oquant_runtime', host='127.0.0.1'):
        super().__init__(db_name, host)
        self.collection_name = f'{account_id}:holdings'
    
    def save(self, trade_info):
        self.insert_data(self.collection_name, trade_info)
        
    def clear(self, strategy_id):
        cond = {'strategy_id': strategy_id}
        self.delete_data(self.collection_name, cond)        
        
    def update(self, strategy_id, trade_info):
        cond = {'date': trade_info['date'], 'strategy_id': strategy_id}
        old_holdings = self.find_one(self.collection_name, cond)
        if old_holdings is None:
            old_holdings = self.get_last_holdings(strategy_id)
            if old_holdings is None:
                old_holdings = {
                    'strategy_id': strategy_id,
                    'date': trade_info['date'],
                    'positions': [],
                    'combinations': []
                }
        
        holdings = old_holdings      
        if trade_info['direction'] == Direction.NET.value: 
            holdings['combinations'] = self.update_combinations(holdings['combinations'], trade_info)
        else:
            holdings['positions'] = self.update_positions(holdings['positions'], trade_info)       
        
        self.update_data(self.collection_name, cond, holdings)
    
    def update_combinations(self, combinations, new_trade_info):
        if new_trade_info['offset'] == Offset.RELEASE.value:
            combinations = [comb for comb in combinations if comb['id'] != new_trade_info['code']]
        else:
            combinations.append({
                'date': new_trade_info['date'],
                'time': new_trade_info['time'],
                'id': new_trade_info['remark'],
                'exchange': new_trade_info['exchange'],
                'code': new_trade_info['code'],
                'amount': f'{new_trade_info["amount"]}/{new_trade_info["amount2"]}',
            })
        return combinations
    
    def update_positions(self, positions, new_trade_info):
        is_buy = 1 if new_trade_info['direction'] == Direction.LONG.value else -1
        symbol = f'{new_trade_info["code"]}.{new_trade_info["exchange"]}'
        if new_trade_info['offset'] == Offset.OPEN.value:
            pos_index = [idx for idx, position in enumerate(positions) 
                        if (position['symbol'] == symbol 
                            and position['direction'] == new_trade_info['direction'])]
        else:
            pos_index = [idx for idx, position in enumerate(positions) 
                        if (position['symbol'] == symbol 
                            and position['direction'] != new_trade_info['direction'])]
        if pos_index == []:
            positions.append(
                {
                    'direction': new_trade_info['direction'],
                    'offset': new_trade_info['offset'],
                    'symbol': symbol,
                    'amounts': [new_trade_info['amount'] * is_buy],
                    'prices': [new_trade_info['price']],
                    'profit': 0
                }
            )
        else:
            index = pos_index[0]
            last_profit = positions[index]['profit']
            if new_trade_info['offset'] == Offset.CLOSE.value:
                profit = 0
                sub_indexes = [idx for idx, v in enumerate(positions[index]['amounts']) if v/abs(v) != is_buy]
                if sub_indexes == []:
                    print('没有需要平仓的持仓')
                else:
                    zero_indexes = []
                    for i in sub_indexes:
                        amt = positions[index]['amounts'][i]
                        if abs(amt) >= abs(new_trade_info['amount']):
                            positions[index]['amounts'][i] += new_trade_info['amount'] * is_buy
                            _p = new_trade_info['amount'] * (positions[index]['prices'][i] - new_trade_info['price']) 
                            profit += _p * is_buy
                            new_trade_info['amount'] = 0
                            if positions[index]['amounts'][i] == 0:
                                zero_indexes.append(i)                      
                            break
                        else:
                            new_trade_info['amount'] += positions[index]['amounts'][i] * is_buy
                            _p = positions[index]['amounts'][i] * (positions[index]['prices'][i] - new_trade_info['price']) 
                            profit += -_p 
                            positions[index]['amounts'][i] = 0
                            zero_indexes.append(i)
                    if new_trade_info['amount'] > 0:
                        print('持仓数量不足，无法正确平仓')
                    positions[index]['amounts'] = [v for i, v in enumerate(positions[index]['amounts']) if i not in zero_indexes]
                    positions[index]['prices'] = [v for i, v in enumerate(positions[index]['prices']) if i not in zero_indexes]
                    positions[index]['profit'] = last_profit + profit
            else:                
                positions[index]['amounts'].append(new_trade_info['amount'] * is_buy)
                positions[index]['prices'].append(new_trade_info['price'])
            
        return positions
                         
    def get_last_holdings(self, strategy_id):
        print('todo: get_last_holdings')
        return