import pika
import os
from producer_interface import mqProducerInterface

class mqProducer(mqProducerInterface):
    # Constructor 
    def __init__(self, routing_key: str, exchange_name: str):
        # Saving the parameters needed to instantiate the class
        self.routing_key = routing_key
        self.exchange_name = exchange_name
        # Calling function to set up connection to RabbitMQ
        self.setupRMQConnection()
        pass

    # setupRMQConnection Function: Establish connection to the RabbitMQ service.
    # publishOrder: Publish a simple UTF-8 string message from the parameter.
    # publishOrder: Close Channel and Connection.
    def setupRMQConnection(self):
        # Setting up RabbitMQ
        self.con_params = pika.URLParameters(os.environ["AMQP_URL"])
        self.connection = pika.BlockingConnection(parameters=self.con_params)

        # Establishing a channel to RabbitMQ
        self.channel = self.connection.channel()

        # Creating exchange
        self.exchange = self.channel.exchange_declare(self.exchange_name)
        pass

    def publishOrder(self, message: str):
        # Basic Publish to Exchange
        self.channel.basic_publish(
            exchange=self.exchange_name,
            routing_key=self.routing_key,
            body=message,
        )

        # Closing channed & connection
        self.channel.close()
        self.connection.close()
