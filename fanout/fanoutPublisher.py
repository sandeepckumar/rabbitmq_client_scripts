import pika
import time
from random import randint

class PublishEngine:
    def __init__(self):
        self._messages = 100
        self._message_interval = 1
        self._connection = None
        self._channel = None
        self._exchange = "covid.feed.exchange"
        self._host = "saitest"

    def make_connection(self):
        credentials = pika.PlainCredentials("guest", "guest")
        parameters = pika.ConnectionParameters(self._host, 5672, "/", credentials, socket_timeout=300)
        self._connection = pika.BlockingConnection(parameters)
        print("--- CONNECTION SUCCESSFUL ---")

    def channel(self):
        self._channel = self._connection.channel()
        print("--- CHANNEL OPENED ---")

    def declare_exhange(self):
        self._channel.exchange_declare(exchange=self._exchange, exchange_type="fanout")
        print("--- EXCHANGE DECLARED ---")

    def publish_message(self):
        message_count = 0
        infected = 0
        cured = 0
        while message_count < self._messages:
            message_count += 1
            infected += randint(0,9)
            cured = infected - cured
            message_body = f"Hyderabad | Covid Stats | Infected: {infected} | Cured: {cured}"
            self._channel.basic_publish(exchange=self._exchange, routing_key="", body=message_body,
                                        properties=pika.BasicProperties(delivery_mode=2))
            print(f"--- Message Published {message_count} ---")
            time.sleep(self._message_interval)

    def close_connection(self):
        self._connection.close()
        print("--- CONNECTION CLOSED ---")

    def run(self):
        self.make_connection()
        self.channel()
        self.declare_exhange()
        self.publish_message()
        self.close_connection()

if __name__ == "__main__":
    engine = PublishEngine()
    engine.run()

