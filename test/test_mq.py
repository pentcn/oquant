import pika
import json
from datetime import datetime

from common.constant import (
    Request,
    Direction,
    Offset
)
from common.utilities import dataclass_to_dict

class MessageQueueSender:
    
    def __init__(self, host, account_id):
        self.host = host
        self.account_id = account_id
        
    def send(self, request):       
        connection = pika.BlockingConnection(pika.ConnectionParameters(self.host))
        channel = connection.channel()
        queue_name = f'req{self.account_id}'
        channel.queue_declare(queue=queue_name, durable=True)
        message = json.dumps(dataclass_to_dict(request)).encode('utf-8')

        channel.basic_publish(exchange='',
                            routing_key=queue_name,
                            body=message)
        print(datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'))
        print(" [x] Sent '{}'".format(message))
        connection.close()
        
if __name__ == '__main__':    
    # 开仓    
    task_open = []
    task_open.append(Request(direction=Direction.SHORT, offset=Offset.OPEN, exchange='SHO', code='10006511', amount=30))
    task_open.append(Request(direction=Direction.SHORT, offset=Offset.OPEN, exchange='SHO', code='10006508', amount=30))
    task_open.append(Request(direction=Direction.NET, offset=Offset.COMBINATE, exchange='SHO', code='10006508/10006511', amount=30, amount2=30))
    
    # 平仓
    task_close = []    
    task_close.append(Request(direction=Direction.NET, offset=Offset.RELEASE, exchange='SHO', 
                             code='2024020200000258'))
    task_close.append(Request(direction=Direction.LONG, offset=Offset.CLOSE, exchange='SHO', 
                             code='10006508', amount=30))
    task_close.append(Request(direction=Direction.LONG, offset=Offset.CLOSE, exchange='SHO', 
                             code='10006511', amount=30))
    # 移仓
    task_move = []
    task_move.append(Request(direction=Direction.NET, offset=Offset.RELEASE, exchange='SHO', 
                             code='2024020200000258'))
    task_move.append(Request(direction=Direction.LONG, offset=Offset.CLOSE, exchange='SHO', 
                             code='10006508', amount=30))
    task_move.append(Request(direction=Direction.LONG, offset=Offset.CLOSE, exchange='SHO', 
                             code='10006511', amount=30))
    task_move.append(Request(direction=Direction.SHORT, offset=Offset.OPEN, exchange='SHO', 
                             code='10006510', amount=30))
    task_move.append(Request(direction=Direction.SHORT, offset=Offset.OPEN, exchange='SHO', 
                             code='10006507', amount=30))
    task_move.append(Request(direction=Direction.NET, offset=Offset.COMBINATE, exchange='SHO', code='10006507/10006510', amount=30, amount2=30))
    
    
    sender = MessageQueueSender('localhost', '840092285')
    requests = task_move
    # requests = task_open
    # requests = task_close
    for request in requests:
        sender.send(request)
        ...
        # 中断便于下一条消息获取