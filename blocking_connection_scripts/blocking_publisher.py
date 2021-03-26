import pika
import time


class PublishEngine:
    def __init__(self):
        self._messages = 1000
        self._message_interval = 0
        self._queue_name = "blocking_con_queue"
        self._connection = None
        self._channel = None
        self._host = "saitest"

    def make_connection(self):
        credentials = pika.PlainCredentials("guest", "guest")
        parameters = pika.ConnectionParameters(self._host, 5672, "/", credentials, socket_timeout=300)
        self._connection = pika.BlockingConnection(parameters)
        print("---- CONNECTION SUCCESSFUL ----\n")
        return self._connection

    def channel(self):
        self._channel = self._connection.channel()
        print("---- CHANNEL OPENED ----\n")

    def declare_queue(self):
        self._channel.queue_declare(queue=self._queue_name, durable=True)
        print("---- QUEUE DECLARED ----\n")

    def publish_message(self):
        message_count = 0
        while message_count < self._messages:
            message_count += 1
            message_body = f" Message Published #{message_count}"
            self._channel.basic_publish(exchange="", routing_key=self._queue_name,
                                        body=message_body, properties=pika.BasicProperties(delivery_mode=2))
            print(f"Message Published #{message_count}")
            time.sleep(self._message_interval)

    def close_connection(self):
        self._connection.close()
        print("---- CONNECTION CLOSED ----\n")

    def run(self):
        self.make_connection()
        self.channel()
        self.declare_queue()
        self.publish_message()
        self.close_connection()


if __name__ == "__main__":
    engine = PublishEngine()
    engine.run()
