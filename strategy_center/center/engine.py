from strategy_center.center.base import BaseEngine


class OptionEngine(BaseEngine):
    
    def __init__(self):
        super().__init__()
        
    def dispatch_bars(self, bar):
        print('todo')
        
    def start(self):
        self.data_feed.run()
        
    def end(self):
        print('todo')
    
    def reset_strategies(self):
        print('todo')
    
    