import pandas as pd
import re
from datetime import datetime, timedelta
from pathlib import Path
from abc import abstractmethod
from center.base import BaseDataFeed


class OptionsDataFeed(BaseDataFeed):
    def __init__(self):
        super().__init__()
        self.contracts = {}
        self.contracts_data = {}
    
    @abstractmethod
    def get_option_symbol(self, month_type, op_type, rank):
        ...
        
class WindETFOptionFileData(OptionsDataFeed):
    def __init__(self, base_path, start_date, end_date=datetime.now().date()):
        super().__init__()
        self.base_path = Path(base_path)
        self.start_date = start_date
        self.end_date = end_date
        self.etf_symbols = []
        self.contract_symbols = []
        
    def add_symbol(self, symbol):
        super().add_symbol(symbol)
        if len(symbol) <= 9 and symbol not in self.etf_symbols:# 510050.SH
            self.etf_symbols.append(symbol)
        elif len(symbol) > 9 and symbol not in self.contract_symbols:
            self.contract_symbols.append(symbol)
    
    def remove_symbol(self, symbol):
        super().remove_symbol(symbol)
        if symbol in self.etf_symbols:
            self.etf_symbols.remove(symbol)
        elif symbol in self.etf_symbols:
            self.contract_symbols.remove(symbol)
            
    def run(self):
        active_date = self.start_date
        while active_date <= self.end_date:
            all_df = self._read_daily_data(active_date)
            self._send_all_bar(all_df)
            
            self.engine.reset_strategies()
            active_date += timedelta(days=1)   
                 
        
    def get_option_symbol(self, underly_symbol, base_price, month_type, op_type, rank, has_appendix=False):
        df = self.contracts[underly_symbol]
        month = sorted(list(set(list(df['month'].astype(int).values))))[month_type]
        df = df.loc[df['month']==str(month)]
        df = df.loc[df['option_type']==op_type]
        df = df.loc[df['appendix']!=''] if has_appendix else df.loc[df['appendix']=='']
        df = df.loc[df['option_type']==op_type]
        
        df = df.sort_values('strike_price')
        df.reset_index(drop=True, inplace=True)
        
        strike_price = df['strike_price'].astype(int).values / 1000
        low_indexes = [i for i, v in enumerate(strike_price) if v <= base_price]
        low_price = strike_price[low_indexes[-1]] if len(low_indexes) > 0 else None
        high_indexes = [i for i, v in enumerate(strike_price) if v >= base_price]
        high_price = strike_price[high_indexes[0]] if len(high_indexes) > 0 else None        
        low_price = low_price if low_price is not None else high_price
        high_price = high_price if high_price is not None else low_price
        diff = 2 * base_price - low_price - high_price
        if abs(diff) >= (high_price - low_price)/2:
            atm_price = low_price if diff > 0 else high_price
        else:
            atm_price = low_price if op_type=='购' else high_price
        index = list(strike_price).index(atm_price) + rank
        df = df.loc[index]
        
        return df['code'] if not df.empty else None
    
    def get_option_bar(self, underly_symbol, option_symbol, datetime):
        if option_symbol not in self.contracts_data:
            self.add_symbol(option_symbol)
            df = self.contracts[underly_symbol]
            data_path = df.loc[df['code']==option_symbol]['remark'].values[0]
            data = pd.read_csv(data_path)
            self.contracts_data[option_symbol] = data
        
        df = self.contracts_data[option_symbol]
        return df.loc[df['datetime']==datetime].to_dict(orient='records')[0]
        ...
    
    def _read_daily_data(self, active_date):
        all_df = {}
        contract_root_path = []
        for symbol in self.etf_symbols:
            name = self._convert_symbol_to_name(symbol)
            contract_root_path.append(self.base_path / name / 'contracts'/ '1min'/ f'{active_date.strftime("%Y%m%d")}')
            data_path = self.base_path / name / 'underlying' / '1min'/ f'{active_date.strftime("%Y%m%d")}.csv'
            if data_path.exists():
                data_df = pd.read_csv(data_path)
                all_df[symbol] = data_df
        
        for symbol in self.contract_symbols:
            data_path = self._get_contract_path(symbol, contract_root_path)
            if data_path is None:
                continue
            data_df = pd.read_csv(data_path)
            all_df[symbol] = data_df
            
        for i, contract_path in enumerate(contract_root_path):
            if not contract_path.exists():
                continue
            data_df = self._add_contract(contract_path)
            if data_df is not None:
                self.contracts[self.etf_symbols[i]] = data_df
        
        return all_df
              
    def _get_contract_path(self, symbol, contract_root_path):
        for daily_path in contract_root_path:
            if daily_path.exists():
                for filename in daily_path.iterdir():
                    if filename.suffix == '.csv' and filename.stem.startswith(symbol):
                        return str(filename)
            else:
                print(f'{daily_path}文件不存在')
                return None
        return None
        # raise ValueError(f'Unsupported symbol: {symbol}')
    
            
    def _convert_symbol_to_name(self, symbol):
        symbols = {'510050.SH': '50ETF', '510300.SH': '沪深300'} 
        if symbol in symbols:
            return symbols[symbol]
        else:
            raise ValueError(f'Unsupported symbol: {symbol}') 

    def _add_contract(self, contracts_path):
        if  not contracts_path.exists():
            return
        
        contracts = []
        for filename in contracts_path.iterdir():
            if filename.suffix == '.csv':
                pattern = r'(\d{8}\.\w{2})-(\w+)([沽购])(\d{4})?年?(\d{1,2})月(\d+)([A-Z]?)'
                match = re.match(pattern, filename.stem)
                if match:
                    code, _, option_type, _, month, strike_price, appendix = match.groups()        
                    contracts.append({
                                        'code': code,
                                        'option_type': option_type,
                                        'month': month,
                                        'strike_price': strike_price,
                                        'appendix': appendix,
                                        'remark': str(filename)
                                    })
        return pd.DataFrame(contracts) if len(contracts) > 0 else None
            
    def _send_all_bar(self, data_df):   
        if len(data_df) == 0:
            return
        
        base_df = {k: v for i, (k, v) in enumerate(data_df.items()) if i < 1}
        other_df = {k: v for i, (k, v) in enumerate(data_df.items()) if i >= 1}
        
        all_bar = {}
        base_name = list(base_df.keys())[0]
        for idx, row in base_df[base_name].iterrows():
            all_bar[base_name] = self._convert_to_dict(row)
            for name, df in other_df.items():
                bar_data = df.loc[df['datetime']==row['datetime']]
                if not bar_data.empty:
                    all_bar[name] = self._convert_to_dict(bar_data.iloc[0])
            if len(all_bar) > 0:
                self.engine.dispatch_bars(all_bar)
    
    def _convert_to_dict(self, row):
        bar = {
            'datetime': row['datetime'],
            'open': row['open'],
            'high': row['high'],
            'low': row['low'],
            'close': row['close'],
            'amount': row['amount'],
            'volume': row['volume']
        }
        
        if 'hv_hv' in row.keys():
            bar.update({
                'position': row['position'],
                'hv_hv': row['hv_hv'],
            })
        
        return bar
        