import pika
import json
import threading
import time
from datetime import datetime

from common.utilities import dataclass_to_dict

class MessageQueueSender:

    def __init__(self, user_name, password, host, vhost, account_id):
        self.user_name = user_name
        self.password = password
        self.host = host
        self.vhost = vhost
        self.account_id = account_id
        self.connection = None
        self.channel = None
        self.ensure_connection()

    def ensure_connection(self):
        """确保 RabbitMQ 连接是开启的，如果没有则创建它。"""
        if not self.connection or self.connection.is_closed:
            self.create_connection()
        if not self.channel or self.channel.is_closed:
            self.channel = self.connection.channel()
            queue_name = f'req{self.account_id}'
            self.channel.queue_declare(queue=queue_name, durable=True)

    def create_connection(self):
        """创建到 RabbitMQ 的连接。"""
        credentials = pika.PlainCredentials(self.user_name, self.password)
        parameters = pika.ConnectionParameters(self.host, 5672, self.vhost, credentials)
        self.connection = pika.BlockingConnection(parameters)

    def send(self, request):
        """发送消息，如果连接关闭则尝试重新连接。"""
        try:
            self.ensure_connection()
            queue_name = f'req{self.account_id}'
            message = json.dumps(dataclass_to_dict(request)).encode('utf-8')

            self.channel.basic_publish(exchange='',
                                       routing_key=queue_name,
                                       body=message)
            # print(datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'))
            # print(f" [x] Sent '{message}'")
        except pika.exceptions.ConnectionClosedByBroker:
            print("Connection closed by broker, trying to reconnect...")
            self.connection = None  # 重置连接
            self.send(request)  # 递归调用自身以重试
        except pika.exceptions.AMQPChannelError as err:
            print(f"Channel error: {err}, stopping...")
        except pika.exceptions.AMQPConnectionError:
            print("Connection was closed, trying to reopen...")
            self.create_connection()
            self.send(request)
    
    def response(self, json_str):
        try:
            self.ensure_connection()
            queue_name = f'res{self.account_id}'
            self.channel.basic_publish(exchange='',
                                       routing_key=queue_name,
                                       body=json_str)
            # print(datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'))
            # print(f" [x] Response '{json_str}'")
        except pika.exceptions.ConnectionClosedByBroker:
            print("Connection closed by broker, trying to reconnect...")
            self.connection = None  # 重置连接
            self.response(json_str)  # 递归调用自身以重试
        except pika.exceptions.AMQPChannelError as err:
            print(f"Channel error: {err}, stopping...")
        except pika.exceptions.AMQPConnectionError:
            print("Connection was closed, trying to reopen...")
            self.create_connection()
            self.response(json_str)

    def close(self):
        """关闭 RabbitMQ 的连接。"""
        if self.connection and self.connection.is_open:
            self.connection.close()
            
            
class MessageQueueReceiver:
    def __init__(self, user_name, password, host, vhost, queue_name, on_message_callback):
        self.user_name = user_name
        self.password = password
        self.host = host
        self.vhost = vhost
        self.queue_name = queue_name
        self.on_message_callback = on_message_callback
        self.connection = None
        self.channel = None
        self.should_reconnect = False
        self.consumer_thread = None

    def _connect(self):
        credentials = pika.PlainCredentials(self.user_name, self.password)
        parameters = pika.ConnectionParameters(self.host, 5672, self.vhost, credentials)
        self.connection = pika.BlockingConnection(parameters)
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.queue_name, durable=True)

    def _on_message(self, ch, method, properties, body):
        self.on_message_callback(body)

    def _start_consuming(self):
        self.channel.basic_consume(queue=self.queue_name,
                                   on_message_callback=self._on_message,
                                   auto_ack=True)
        try:
            self.channel.start_consuming()
        except pika.exceptions.ConnectionClosedByBroker:
            self.should_reconnect = True
        except Exception as e:
            print(f"Error while consuming: {e}")
            self.should_reconnect = True

    def start(self):
        self.should_reconnect = False
        self._connect()
        self.consumer_thread = threading.Thread(target=self._start_consuming)
        self.consumer_thread.start()

    def _reconnect(self):
        while self.should_reconnect:
            try:
                print("Attempting to reconnect to the broker...")
                self._connect()
                self.should_reconnect = False
                print("Reconnected to the broker. Resuming consumption.")
                self.start()
            except pika.exceptions.AMQPConnectionError:
                print("Failed to reconnect to the broker. Trying again in 5 seconds...")
                time.sleep(5)

    def stop(self):
        self.should_reconnect = False
        if self.connection and self.connection.is_open:
            try:
                self.channel.stop_consuming()
                self.connection.close()
                time.sleep(1)  # 等待消费线程退出
            except pika.exceptions.ConnectionClosed:
                pass

        if self.consumer_thread:
            self.consumer_thread.join()

