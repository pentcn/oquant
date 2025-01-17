from dataclasses import dataclass
from uuid import uuid4
from enum import Enum

class RunMode(Enum):
    BACKTEST = "backtest"
    REALTIME = "realtime"
    
    
class Direction(Enum):
    LONG = "买"
    SHORT = "卖"
    NET = "净"

class Offset(Enum):
    OPEN = "开"
    CLOSE = "平"
    COMBINATE = "构建联合保证金"
    RELEASE = "解除联合保证金"
    
    
@dataclass
class Request:
    direction: Direction
    offset: Offset
    exchange: str
    code: str
    name: str = ''
    amount: int = 0
    amount2: int = 0
    price: float = 0.0
    price2: float = 0.0
    id: str = None
    
    def __post_init__(self):
        if self.id is None:
            self.id = str(uuid4())
        if self.amount2 == 0:
            self.amount2 = self.amount
    