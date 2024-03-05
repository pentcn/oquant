import pika
import asyncio
import json
import threading
import time
import traceback
from pika.adapters.asyncio_connection import AsyncioConnection
from datetime import datetime
from common.utilities import dataclass_to_dict
from common.constant import Request

class Sender:
    
    def __init__(self, user_name, password, host, vhost, queue_name):
        self.user_name = user_name
        self.password = password
        self.host = host
        self.vhost = vhost
        self.queue_name = queue_name
        self.connection = None
        self.channel = None
    
    def _create_connection(self):
        """创建到 RabbitMQ 的连接。"""
        credentials = pika.PlainCredentials(self.user_name, self.password)
        parameters = pika.ConnectionParameters(self.host, 5672, self.vhost, credentials, heartbeat=2)
        self.connection = pika.BlockingConnection(parameters)

    
    def _ensure_connection(self):
        """确保 RabbitMQ 连接是开启的，如果没有则创建它。"""
        if self.connection is None or self.connection.is_closed:
            self._create_connection()
        if self.channel is None or self.channel.is_closed:
            self.channel = self.connection.channel()
            self.channel.queue_declare(queue=self.queue_name, durable=True)

    def send(self, request):
        """发送消息到 'res' 队列，如果连接关闭则尝试重新连接。"""
        try:
            self._ensure_connection()
            if type(request) == Request:
                message = json.dumps(dataclass_to_dict(request)).encode('utf-8')
            else:
                message = request.encode('utf-8')
            self.channel.basic_publish(
                exchange='',
                routing_key=self.queue_name,
                body=message)
        except pika.exceptions.ConnectionClosedByBroker:
            print("Connection closed by broker, trying to reconnect...")
            traceback.print_exc()
            self.connection = None  # 重置连接
            self.send(request)  # 递归调用自身以重试
        except pika.exceptions.AMQPChannelError as err:
            print(f"Channel error: {err}, stopping...")
            traceback.print_exc()
            self.connection = None
            self.send(request)
        except pika.exceptions.AMQPConnectionError:
            print("Connection was closed, trying to reopen...")
            traceback.print_exc()
            self.connection = None
            self.send(request)

    def stop(self):
        if self.channel and self.channel.is_open:
            self.channel.stop_consuming()
        if self.connection and self.connection.is_open:
            self.connection.close()
        self.channel = None
        self.connection = None
            
            
class Receiver:    
    
    def __init__(self, user_name, password, host, vhost, queue_name, on_message_callback):
        self.user_name = user_name
        self.password = password
        self.host = host
        self.vhost = vhost
        self.queue_name = queue_name
        self.connection = None
        self.channel = None
        self.consuming = False
        self.on_message_callback = on_message_callback

    def _create_connection(self):
        """创建到 RabbitMQ 的连接。"""
        credentials = pika.PlainCredentials(self.user_name, self.password)
        parameters = pika.ConnectionParameters(self.host, 5672, self.vhost, credentials, heartbeat=2)
        self.connection = pika.BlockingConnection(parameters)

    
    def _ensure_connection(self):
        """确保 RabbitMQ 连接是开启的，如果没有则创建它。"""
        if self.connection is None or self.connection.is_closed:
            self._create_connection()
        if self.channel is None or self.channel.is_closed:
            self.channel = self.connection.channel()
            self.channel.queue_declare(queue=self.queue_name, durable=True)
    
    def start(self):
        def consume():
            # print("Starting consuming thread...")
            while self.consuming:
                try:
                    # with self.lock:
                    self._ensure_connection()
                    # print("Consuming messages...")
                    self.channel.basic_consume(
                        queue=self.queue_name,
                        on_message_callback=self.on_message,
                        auto_ack=False)
                    self.channel.start_consuming()
                except (pika.exceptions.AMQPConnectionError, pika.exceptions.ConnectionClosedByBroker) as e:
                    print(f"Connection error, attempting to reconnect: {e}")
                    self.channel = None  # 重置 channel
                    self.connection = None  # 重置 connection
                    traceback.print_exc()
                    time.sleep(5)  # 等待一段时间再次尝试重连
                except Exception as ex:
                    print(f"An error occurred in consuming thread: {ex}")
                    traceback.print_exc()
                finally:
                    if self.channel and self.channel.is_open:
                        # print("Closing channel...")
                        self.channel.close()
            # print("Stop consuming thread...")

        if self.on_message_callback:
            self.consuming = True
            self.consume_thread = threading.Thread(target=consume)
            # print("Waiting for consuming thread to finish...")
            self.consume_thread.start()
            
    def on_message(self, channel, method, properties, body):
        # print("Received message:", body.decode('utf-8'))
        self.on_message_callback(body)
        self.channel.basic_ack(delivery_tag=method.delivery_tag)
        
    def stop(self):
        self.consuming = False  # 停止消费循环
        if self.consume_thread:
            if self.channel and self.channel.is_open:
                self.channel.stop_consuming()
            self.consume_thread.join()  # 等待消费线程结束
        if self.connection and self.connection.is_open:
            self.connection.close()
        self.channel = None
        self.connection = None


class MessageQueue:
    
        def __init__(self, user_name, password, host, vhost, queue_name, on_message_callback, request_side=True):
            send_queue_name = f'res{queue_name}' if request_side else f'req{queue_name}'    
            receive_queue_name = f'req{queue_name}' if request_side else f'res{queue_name}'
            
            self.sender = Sender(user_name, password, host, vhost, send_queue_name)
            self.receiver = Receiver(user_name, password, host, vhost, receive_queue_name, on_message_callback)
            
        def start(self):
            self.receiver.start()
        
        def stop(self):
            self.receiver.stop()    
        
        def send(self, request):
            self.sender.send(request)
        
        

def on_message_arrived(message):
    print(f"Received message: {message.decode('utf-8')}")

if __name__ == '__main__':
    # sender = Sender('test', 'test', 'localhost', 'test', 'res1234')
    # sender.send('Hello, world!')
    # sender.stop()
    # 示例使用
        
    request_side = MessageQueue('test', 'test', 'localhost', 'test', '1234', on_message_arrived)
    request_side.start()
    response_side = MessageQueue('test', 'test', 'localhost', 'test', 'res1234', on_message_arrived, request_side=False)
    response_side.start()
    request_side.send('Hello, world!')
    
    input()
    request_side.stop()
    response_side.stop()
    