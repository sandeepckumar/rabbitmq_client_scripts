import pika
import logging
from pika.frame import *

LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')
LOGGER = logging.getLogger(__name__)


class ConsumeEngine:
    def __init__(self):
        self._consumer_tag = None
        self._channel = None
        self._connection = None
        self._queue = "async_q"
        self._exchange = ""
        self._host = "vishnutest"

    def on_open(self, connection):
        print("--- CONNECTION OPENED ---")
        self._channel = self._connection.channel(on_open_callback=self.on_channel_open)

    def on_declare(self, channel):
        print("--- QUEUE DECLARED ---")
        self._channel.add_on_cancel_callback(callback=self.on_consume_cancelled)
        self._consumer_tag = self._channel.basic_consume(on_message_callback=self.on_message, queue=self._queue)

    def on_consume_cancelled(self, method_frame):
        print(method_frame)
        if self._channel:
            self._channel.close()

    def on_message(self, channel, basic_deliver, properties, body):
        self._channel.basic_ack(basic_deliver.delivery_tag)
        # print(basic_deliver)
        # print(f"DELIVERY TAG: {basic_deliver.delivery_tag}")
        # print(properties)
        print(f"RECEIVED MESSAGE: {body}")

    def on_channel_open(self, channel):
        print("--- CHANNEL OPENED ----")
        argument_list = {'x-queue-master-locator': 'random'}
        self._channel.queue_declare(queue=self._queue, callback=self.on_declare, durable=True, arguments=argument_list)

    def on_close(self, connection, reply_code, reply_message):
        print(reply_code)
        print(reply_message)
        print("--- CONNECTION CLOSED ---")

    def stop_consuming(self):
        print("=== KEYBOARD INTERRUPT RECEIVED ===")
        if self._channel:
            self._channel.basic_cancel(self.on_cancelok, self._consumer_tag)

    def on_cancelok(self, unused_frame):
        self._channel.close()
        self.close_connection()

    def close_connection(self):
        self._connection.close()

    def run(self):
        logging.basicConfig(level=logging.ERROR, format=LOG_FORMAT)
        credentials = pika.PlainCredentials("guest", "guest")
        parameters = pika.ConnectionParameters(self._host, 5672, "/", credentials, socket_timeout=300)
        self._connection = pika.SelectConnection(parameters, on_open_callback=self.on_open)
        self._connection.add_on_close_callback(self.on_close)
        print("--- STARTING EVENT IO LOOP ---")

        try:
            self._connection.ioloop.start()
        except KeyboardInterrupt:
            self.stop_consuming()


if __name__ == '__main__':
    engine = ConsumeEngine()
    engine.run()