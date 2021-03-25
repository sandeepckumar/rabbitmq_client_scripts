import pika
import time

class ConsumerEngine:
    def __init__(self):
        self._message_interval = 3
        self._queue_name= None
        self._connection = None
        self._channel = None
        self._exchange = "covid_stats.feed.exchange"
        self._host = "vinodtest"

    def connection(self):
        credentials = pika.PlainCredentials("guest","guest")
        parameters = pika.ConnectionParameters(self._host, 5672, "/", credentials, socket_timeout=300)
        self._connection = pika.BlockingConnection(parameters)
        print("--- CONNECTION ESTABLISHED ---")

    def channel(self):
        self._channel = self._connection.channel()
        print("--- CHANNEL OPENED ---")

    def declare_exchange(self):
        self._channel.exchange_declare(exchange=self._exchange, exchange_type="direct")
        print("--- EXCHANGE DECLARED ---")

    def declare_queue(self):
        queue = self._channel.queue_declare(queue="", exclusive=True)
        self._queue_name = queue.method.queue
        print("--- QUEUE DECLARED ---")
        print(" [x] WAITING FOR THE MESSAGE. PRESS CTRL+C TO EXIT")

    def make_binding(self):
        self._channel.queue_bind(exchange=self._exchange,
                                 routing_key="covid_blr", queue=self._queue_name)
        print(f"--- EXCHANGE [{self._exchange}] & QUEUE [{self._queue_name}] ARE BOUND --- ")

    def on_message(self, channel, method, properties, body):
        print(f" [X] FEED RECEIVED --- {body}")
        time.sleep(self._message_interval)

    def consume_message(self):
        self._channel.basic_consume(self._queue_name, self.on_message, auto_ack=True)
        self._channel.start_consuming()

    def run(self):
        self.connection()
        self.channel()
        self.declare_exchange()
        self.declare_queue()
        self.make_binding()
        self.consume_message()

if __name__ == "__main__":
    engine = ConsumerEngine()
    engine.run()