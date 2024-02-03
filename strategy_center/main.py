from datetime import date
from center.engine import OptionEngine
from center.data_feed import WindETFOptionFileData
from center.strategies.dual_dragon import DualDragon

base_dir = r'D:\My Workspaces\2023\ETFOptions\data\options\wind\50ETF'
start_date = date(2020, 6, 17)

if __name__ == '__main__':
    data_feed = WindETFOptionFileData(base_dir, start_date)
    str_50etf_option = DualDragon('1', '1234', 'str_50etf_option', '510050.SH')
    str_300etf_option = DualDragon('2', '5678', 'str_300etf_option', '510300.SH')
    
    engine = OptionEngine()
    engine.add_data_feed(data_feed)
    engine.add_strategy(str_50etf_option)
    engine.add_strategy(str_300etf_option)
    engine.start()