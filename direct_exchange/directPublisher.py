import pika, time
from random import randint


class PublishEngine:
    def __init__(self):
        self._messages = 100
        self._message_interval = 1
        self._connection = None
        self._channel = None
        self._exchange = "covid_stats.feed.exchange"
        self._host = "vinodtest"

    def make_connection(self):
        credentials = pika.PlainCredentials("guest", "guest")
        parameters = pika.ConnectionParameters(self._host, 5672, "/", credentials, socket_timeout=300)
        self._connection = pika.BlockingConnection(parameters)
        print("--- CONNECTION ESTABLISHED ---")

    def channel(self):
        self._channel = self._connection.channel()
        print("--- CHANNEL OPENED ---")

    def declare_exchange(self):
        self._channel.exchange_declare(exchange=self._exchange, exchange_type="direct")
        print("--- EXCHANGE DECLARED ---")

    def publish_message(self):
        message_count = 0
        hyd_infected = 0
        hyd_cured = 0
        blr_infected = 0
        blr_cured = 0
        chennai_infected = 0
        chennai_cured = 0
        while message_count < self._messages:
            message_count += 1
            hyd_infected += randint(0, 100)
            hyd_cured = hyd_infected - hyd_cured
            blr_infected += randint(0, 100)
            blr_cured = blr_infected - blr_cured
            chennai_infected += randint(0, 100)
            chennai_cured = chennai_infected - chennai_cured

            msg_body = f"COVID UPDATE| HYDERABAD | INFECTED: {hyd_infected}| CURED: {hyd_cured}"
            self._channel.basic_publish(exchange=self._exchange, routing_key="covid_hyd", body=msg_body
                                        , properties=pika.BasicProperties(delivery_mode=2))

            msg_body = f"COVID UPDATE| BANGALORE | INFECTED: {blr_infected}| CURED: {blr_cured}"
            self._channel.basic_publish(exchange=self._exchange, routing_key="covid_blr", body=msg_body
                                        , properties=pika.BasicProperties(delivery_mode=2))

            msg_body = f"COVID UPDATE| CHENNAI | INFECTED: {chennai_infected}| CURED: {chennai_cured}"
            self._channel.basic_publish(exchange=self._exchange, routing_key="covid_chennai", body=msg_body
                                        , properties=pika.BasicProperties(delivery_mode=2))

            print(f" --- PUBLISHED COVID STATS FOR HYDERABAD, BANGALORE & HYDERABAD - MSG# {message_count} ---")
            time.sleep(self._message_interval)

    def close_connection(self):
        self._connection.close()
        print("--- CONNECTION CLOSED ---")

    def run(self):
        self.make_connection()
        self.channel()
        self.declare_exchange()
        self.publish_message()
        self.close_connection()


if __name__ == "__main__":
    engine = PublishEngine()
    engine.run()
