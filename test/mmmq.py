import pika
import json
import threading
import time
from datetime import datetime
from common.utilities import dataclass_to_dict

class MessageQueue:
    def __init__(self, user_name, password, host, vhost, queue_name, on_message_callback=None):
        self.user_name = user_name
        self.password = password
        self.host = host
        self.vhost = vhost
        self.receive_queue_name = f'req{queue_name}'
        self.send_queue_name = f'res{queue_name}'
        self.on_message_callback = on_message_callback
        self.connection = None
        self.channel = None
        self.lock = threading.Lock()
        self.consuming = False  # 控制消费线程的运行
        self.consume_thread = None  # 用于存储消费线程

        # print("Starting consuming thread...")
        self.start_consuming()
        # print("Waiting for consuming thread to finish...")

    def _ensure_connection(self):
        """确保 RabbitMQ 连接是开启的，如果没有则创建它。"""
        # print("Ensuring connection...")
        if self.connection is None or self.connection.is_closed:
            # print("Creating connection...")
            self._create_connection()
        if self.channel is None or self.channel.is_closed:
            self.channel = self.connection.channel()
            self.channel.queue_declare(queue=self.receive_queue_name, durable=True)
            self.channel.queue_declare(queue=self.send_queue_name, durable=True)

    def _create_connection(self):
        """创建到 RabbitMQ 的连接。"""
        credentials = pika.PlainCredentials(self.user_name, self.password)
        parameters = pika.ConnectionParameters(self.host, 5672, self.vhost, credentials, heartbeat=0)
        self.connection = pika.BlockingConnection(parameters)

    def send(self, request):
        """发送消息到 'res' 队列，如果连接关闭则尝试重新连接。"""
        try:
            with self.lock:
                self._ensure_connection()
                message = json.dumps(dataclass_to_dict(request)).encode('utf-8')
                self.channel.basic_publish(
                    exchange='',
                    routing_key=self.send_queue_name,
                    body=message)
        except pika.exceptions.ConnectionClosedByBroker:
            print("Connection closed by broker, trying to reconnect...")
            self.connection = None  # 重置连接
            self.send(request)  # 递归调用自身以重试
        except pika.exceptions.AMQPChannelError as err:
            print(f"Channel error: {err}, stopping...")
        except pika.exceptions.AMQPConnectionError:
            print("Connection was closed, trying to reopen...")
            self._create_connection()
            self.send(request)

    def start_consuming(self):
        """启动一个线程来接收 'req' 队列的消息。"""
        def consume():
            # print("Starting consuming thread...")
            while self.consuming:
                try:
                    with self.lock:
                        self._ensure_connection()
                        # print("Consuming messages...")
                        self.channel.basic_consume(
                            queue=self.receive_queue_name,
                            on_message_callback=self.on_message_callback,
                            auto_ack=True)
                        self.channel.start_consuming()
                except (pika.exceptions.AMQPConnectionError, pika.exceptions.ConnectionClosedByBroker) as e:
                    print(f"Connection error, attempting to reconnect: {e}")
                    self.channel = None  # 重置 channel
                    self.connection = None  # 重置 connection
                    time.sleep(5)  # 等待一段时间再次尝试重连
                except Exception as ex:
                    print(f"An error occurred in consuming thread: {ex}")
                finally:
                    if self.channel and self.channel.is_open:
                        print("Closing channel...")
                        self.channel.close()

        if self.on_message_callback:
            self.consuming = True
            thread = threading.Thread(target=consume)
            # print("Waiting for consuming thread to finish...")
            thread.start()

    def stop(self):
        self.consuming = False  # 停止消费循环
        if self.consume_thread:
            # print("Waiting for consuming thread to finish...")
            self.consume_thread.join()  # 等待消费线程结束
        if self.channel and self.channel.is_open:
            self.channel.stop_consuming()
        if self.connection and self.connection.is_open:
            self.connection.close()
        self.channel = None
        self.connection = None


# 示例使用
def on_message_callback(body):
    print("Received:", body)

mq = MessageQueue('test', 'test', 'localhost', 'test', '1234', on_message_callback)
mq.send('Hello, RabbitMQ!')
# input()

mq.stop()
