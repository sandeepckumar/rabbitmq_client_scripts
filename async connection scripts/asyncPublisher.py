import pika
import logging

log_format = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')
logger = logging.getLogger(__name__)

class publish_engine:
    def __init__(self):
        self._number_of_messages = 1000
        self._channel = None
        self._connection = None
        self._url ="saitest"
        self._port = 5672
        self._queue = "async_q"
        self._routing_key = "async_q"
        self._exchange=""

    def on_open(self, connection):
        # Invoked when the connection is open
        print("--- CONNECTION OPENED ---")
        self._channel = self._connection.channel(on_open_callback=self.on_channel_open)

    def on_declare(self, channel):
        print("--- QUEUE DECLARED ---")
        while self._number_of_messages > 0:
            print(self._number_of_messages)

            self._channel.basic_publish(exchange=self._exchange,
                                        routing_key=self._routing_key,
                                        body="MESSAGE PUBLISHED #" + str(self._number_of_messages),
                                        properties=pika.BasicProperties(content_type='text/plain',
                                                                        delivery_mode=2))
            self._number_of_messages -= 1
        self._connection.close()

    def on_channel_open(self, channel):
        print("--- CHANNEL OPENED ---")
        argument_list = {"x-queue-master-locator": "random"}
        self._channel.queue_declare(queue=self._queue, callback=self.on_declare, durable=True, arguments=argument_list)

    def on_close(self, connection, reply_code, ):
        print(reply_code)
        # print(reply_message)
        print("--- CONNECTION CLOSED ---\n")

    def run(self):
        logging.basicConfig(level=logging.ERROR, format=log_format)
        creds = pika.PlainCredentials("guest", "guest")
        params = pika.ConnectionParameters(self._url, self._port, "/", creds, socket_timeout=300)
        self._connection = pika.SelectConnection(params, on_open_callback=self.on_open)
        self._connection.add_on_close_callback(self.on_close)

        try:
            self._connection.ioloop.start()
        except KeyboardInterrupt:
            self._connection.close()


if __name__ == "__main__":
    engine = publish_engine()
    engine.run()
