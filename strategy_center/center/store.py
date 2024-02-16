import json
from time import sleep
from datetime import datetime
from pymongo import MongoClient, errors

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
        return collection.update_many(query, {'$set': update_values})

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
