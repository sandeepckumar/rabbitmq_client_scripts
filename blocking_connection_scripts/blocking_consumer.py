import pika
import time


class ConsumeEngine:
    def __init__(self):
        self._message_interval = 1
        self._queue_name = "blocking_con_queue"
        self._connection = None
        self._channel = None
        self._host = "vishnutest"

    def connection(self):
        credentials = pika.PlainCredentials("guest", "guest")
        parameters = pika.ConnectionParameters(self._host, 5672, "/", credentials, socket_timeout=300)
        self._connection = pika.BlockingConnection(parameters)
        print("--- CONNECTION SUCCESSFUL -- ")

    def channel(self):
        self._channel = self._connection.channel()
        print("--- CHANNEL OPENED ---")

    def declare_queue(self):
        self._channel.queue_declare(queue=self._queue_name, durable=True)
        print("--- QUEUE DECLARED ---")
        print(" [*] WAITING FOR MESSAGES. TO EXIT PRESS CTRL+C ")

    def on_message(self, channel, method, properties, body):
        print(f" [x] READING {body}")
        time.sleep(self._message_interval)
        print(" [x] DONE")
        self._channel.basic_ack(delivery_tag=method.delivery_tag)

    def consume_message(self):
        self._channel.basic_qos(prefetch_count=1)
        self._channel.basic_consume(self._queue_name, self.on_message)
        self._channel.start_consuming()

    def run(self):
        self.connection()
        self.channel()
        self.declare_queue()
        self.consume_message()


if __name__ == "__main__":
    engine = ConsumeEngine()
    engine.run()
