from datetime import date
from center.engine import OptionEngine
from center.data_feed import WindETFOptionFileData
from center.strategies.dual_dragon import DualDragon
from center.trader import BacktestOptionTrader

base_dir = r'D:\My Workspaces\2023\ETFOptions\data\options\wind'
start_date = date(2020, 6, 17)
store_host = '127.0.0.1'
mq_params = {
    'user_name': 'test',
    'password': 'test',
    'host': 'localhost',
    'vhost': 'test',
    'account_id': '1234'
}

if __name__ == '__main__':
    data_feed = WindETFOptionFileData(base_dir, start_date)
    trader = BacktestOptionTrader(data_feed, mq_params)
    strategy_50etf_option = DualDragon('1', mq_params['account_id'], 'str_50etf_option', '510050.SH', trader, store_host) 
    strategy_300etf_option = DualDragon('2', mq_params['account_id'], 'str_300etf_option', '510300.SH', trader, store_host)
    
    engine = OptionEngine()
    engine.add_data_feed(data_feed)
    engine.add_strategy(strategy_50etf_option)
    engine.add_strategy(strategy_300etf_option)
    
    # data_feed.add_symbol('10002000.SH')
    engine.start()