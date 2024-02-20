from strategy_center.center.base import BaseEngine


class OptionEngine(BaseEngine):
    
    def __init__(self):
        super().__init__()
        
    def dispatch_bars(self, all_bars):
        bar_symbols = all_bars.keys()
        for strategy in self.strategy_list:
            symbols = [strategy.underlying_symbol] + strategy.day_contracts
            sub_bars = {symbol: all_bars[symbol] for symbol in symbols if symbol in bar_symbols}
            if len(sub_bars) > 0:
                # try:
                    strategy.on_bars(sub_bars)
                    ...
                # except Exception as e:
                #     print(f'策略运行错误：{e}', e)
        
        # bar_symbols = all_bars.keys()
        # def process_strategy(strategy):
        #     symbols = [strategy.underlying_symbol] + strategy.get_option_symbols()
        #     sub_bars = {symbol: all_bars[symbol] for symbol in symbols if symbol in bar_symbols}
        #     strategy.on_bars(sub_bars)
        
        # with ThreadPoolExecutor(max_workers=len(self.strategy_list)) as executor:
        #     executor.map(process_strategy, self.strategy_list)
    
    def on_trade_response(self, body):
        for strategy in self.strategy_list:
            if strategy.id == body['strategy_id']:
                strategy.on_trade_response(body)
                break
            
    def start(self):
        self.data_feed.run()
        
    def end(self):
        for strategy in self.strategy_list:
            strategy.stop()
    
