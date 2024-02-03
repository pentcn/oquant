from strategy_center.center.base import BaseEngine


class OptionEngine(BaseEngine):
    
    def __init__(self):
        super().__init__()
        
    def dispatch_bars(self, bar):
        ...
        
    def start(self):
        self.data_feed.run()
        
    def end(self):
        ...
    
    