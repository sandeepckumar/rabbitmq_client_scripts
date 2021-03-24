import pika

connection = pika.BlockingConnection(pika.ConnectionParameters("saitest"))
channel = connection.channel()
channel.queue_declare(queue="hello")
channel.basic_publish(exchange='', routing_key="hello", body="hello world!")
print(" [x] Sent 'hello world!'")
connection.close()