from datetime import date
from center.engine import OptionEngine
from center.data_feed import WindETFOptionFileData
from center.strategies.dual_dragon import DualDragon
from center.trader import BacktestOptionTrader

base_dir = r'D:\My Workspaces\2023\ETFOptions\data\options\wind'
start_date = date(2020, 6, 17)
end_date = date(2020, 7, 6)
store_host = '127.0.0.1'
mq_params = {
    'user_name': 'test',
    'password': 'test',
    'host': 'localhost',
    'vhost': 'test',
    'account_id': '1234'
}

def clear_queue(mq_params):
    import pika
    credentials = pika.PlainCredentials(mq_params['user_name'], mq_params['password'])
    parameters = pika.ConnectionParameters(mq_params['host'], 5672, mq_params['vhost'], credentials, heartbeat=60)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    channel.queue_purge(f'res{mq_params["account_id"]}')
    channel.queue_purge(f'req{mq_params["account_id"]}')
    connection.close()
    print('queue cleared')
    return True



if __name__ == '__main__':
    clear_queue(mq_params)
    
    
    data_feed = WindETFOptionFileData(base_dir, start_date, end_date)
    trader = BacktestOptionTrader(store_host, data_feed, mq_params)
    strategy_50etf_option = DualDragon('3', mq_params['account_id'], 'str_50etf_option', '510050.SH', trader, store_host, amount=1) 
    # strategy_300etf_option = DualDragon('2', mq_params['account_id'], 'str_300etf_option', '510300.SH', trader, store_host, amount=1)
    
    engine = OptionEngine()
    engine.add_data_feed(data_feed)
    engine.add_strategy(strategy_50etf_option)
    # engine.add_strategy(strategy_300etf_option)
    
    # data_feed.add_symbol('10002000.SH')
    engine.start()