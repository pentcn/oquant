from datetime import datetime
from pathlib import Path
from center.base import BaseDataFeed


class WindETFOptionFileData(BaseDataFeed):
    def __init__(self, base_path, start_date, end_date=datetime.now().date):
        super().__init__()
        self.base_path = base_path
        self.etf_path = Path(self.base_path, 'underlying', '1min')
        self.options_root_path = Path(self.base_path, 'contracts', '1min')
        self.start_date = start_date
        self.end_date = end_date
        
    def run(self):
        ...