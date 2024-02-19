import pandas as pd

from datetime import datetime
from pathlib import Path

class ChinaMarketCalendar:
    
    def __init__(self):
        self.data_file = Path(__file__).parent / 'calendar.csv'
        if self.data_file.exists():
           self.data = pd.read_csv(self.data_file)
           self.data['trade_date'] = pd.to_datetime(self.data['trade_date']).dt.date
        else:
             self.update()
        
        if self.get_last_traded_date() is None:
            self.update()
            if self.get_last_traded_date() is None:
                print('交易日历的日期范围不足支撑当前交易')
                
    def update(self):
        import akshare as ak
        data = ak.tool_trade_date_hist_sina()
        data.to_csv(self.data_file, index=False)
        data['trade_date'] = pd.to_datetime(data['trade_date']).dt.date
        self.data = data

    def get_last_traded_date(self, date=datetime.now().date()):
        if type(date) is str:
            date = date.split(' ')[0]
            if '-' in date:                
                date = datetime.strptime(date, '%Y-%m-%d').date()
            else:
                date = datetime.strptime(date, '%Y%m%d').date()
            
        dt = self.data[self.data['trade_date']<date]
        if dt.empty:
            return None
        return dt.iloc[-1]['trade_date']
    
if __name__ == '__main__':
    calendar = ChinaMarketCalendar()
    print(calendar.get_last_traded_date())
    print(calendar.get_last_traded_date('20220801'))
    print( calendar.get_last_traded_date('2025-08-01'))
    ...