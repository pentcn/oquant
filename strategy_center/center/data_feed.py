import pandas as pd
import re
from datetime import datetime
from pathlib import Path
from center.base import BaseDataFeed


class OptionsDataFeed(BaseDataFeed):
    def __init__(self):
        super().__init__()
        self.contracts = {}
        self.contracts_data = {}
    
    def add_contract_data(self, symbol):
        if symbol not in self.contracts:
            op_file_path = self.contracts[symbol]['remark']
            self.options_data[symbol] = pd.read_csv(op_file_path) 
        
class WindETFOptionFileData(OptionsDataFeed):
    def __init__(self, base_path, start_date, end_date=datetime.now().date()):
        super().__init__()
        self.base_path = base_path
        self.etf_path = Path(self.base_path, 'underlying', '1min')
        self.options_root_path = Path(self.base_path, 'contracts', '1min')        
        self.start_date = start_date
        self.end_date = end_date
        
    def run(self):
        active_date = self.start_date
        while active_date <= self.end_date:
            data_path = self.etf_path / f'{active_date.strftime("%Y%m%d")}.csv'
            if data_path.exists():
                data_df = pd.read_csv(data_path)
                contracts_path = self.options_root_path / f'{active_date.strftime("%Y%m%d")}'
                if contracts_path.exists():
                    self._add_contract(contracts_path)
                for symbol in self.symbols:
                    op_file_path = self.contracts[symbol]['remark']
                    self.contracts_data[symbol] = pd.read_csv(op_file_path)               
                
                self._send_bars(data_df)
                self.engine.reset_strategies()
            active_date += datetime.timedelta(days=1)

    def _add_contract(self, contracts_path):
        for filename in contracts_path.iterdir():
            if filename.suffix == '.csv':
                pattern = r'(\d{8}\.\w{2})-(\w+)([沽购])(\d{4})?年?(\d{1,2})月(\d+)([A-Z]?)'
                match = re.match(pattern, filename.stem)
                if match:
                    code, _, option_type, _, month, strike_price, appendix = match.groups()        
                    self.contracts[code] = {
                                        'code': code,
                                        'option_type': option_type,
                                        'month': month,
                                        'strike_price': strike_price,
                                        'appendix': appendix,
                                        'remark': str(filename)
                                    }
            
    def _send_bars(self, data_df):
        # 这个函数按行读取data_df的数据，并根据列datatime的值读取self.contracts_data中对应的数据，再发送到engine
        