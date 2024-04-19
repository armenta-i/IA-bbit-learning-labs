# Copyright 2024 Bloomberg Finance L.P.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Copyright 2024 Bloomberg Finance L.P.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


class mqConsumerInterface:
    def __init__(
        self, binding_key: str, exchange_name: str, queue_name: str
    ) -> None:
        # Save parameters to class variables

        # Call setupRMQConnection
        pass

    def setupRMQConnection(self) -> None:
        # Set-up Connection to RabbitMQ service

        # Establish Channel

        # Create Queue if not already present

        # Create the exchange if not already present

        # Bind Binding Key to Queue on the exchange

        # Set-up Callback function for receiving messages
        pass

    def on_message_callback(
        self, channel, method_frame, header_frame, body
    ) -> None:
        # Acknowledge message

        #Print message (The message is contained in the body parameter variable)

        pass

    def startConsuming(self) -> None:
        # Print " [*] Waiting for messages. To exit press CTRL+C"

        # Start consuming messages
        pass
    
    def __del__(self) -> None:
        # Print "Closing RMQ connection on destruction"
        
        # Close Channel

        # Close Connection
        
        pass


import pika
import os
# from consumer_interface import mqConsumerInterface
class mqConsumer(mqConsumerInterface):
    def __init__(
        self, binding_key: str, exchange_name: str, queue_name: str
    ) -> None:
        # Save parameters to class variables
        self.binding_key = binding_key
        self.exchange_n = exchange_name
        self.queue_name = queue_name

        # Call setupRMQConnection
        self.setupRMQConnection()
        pass

    def setupRMQConnection(self) -> None:
        # Set-up Connection to RabbitMQ service
        self.con_params = pika.URLParameters(os.environ["AMQP_URL"])
        self.connection = pika.BlockingConnection(parameters=self.con_params)
        # Establish Channel
        self.channel = self.onnection.channel()

        # Create Queue if not already present
        self.channel.queue_declare(queue="Queue Name")
        # Create the exchange if not already present
        self.exchange = self.channel.exchange_declare(exchange="Exchange Name")
        # Bind Binding Key to Queue on the exchange
        self.channel.queue_bind(
            queue= "Queue Name",
            routing_key= "Routing Key",
            exchange="Exchange Name",
        )
        # Set-up Callback function for receiving messages
        self.channel.basic_consume(
            "Queue Name", self.on_message_callback, auto_ack=False
        )
        pass

    def on_message_callback(
        self, channel, method_frame, header_frame, body
    ) -> None:
        # Acknowledge message
        self.channel.basic_ack(method_frame.delivery_tag, False)
        #Print message (The message is contained in the body parameter variable)
        print(f"The message is contained in the body parameter variable {body}")
        pass

    def startConsuming(self) -> None:
        # Print " [*] Waiting for messages. To exit press CTRL+C"
        print("[*] Waiting for messages. To exit press CTRL+C")
        # Start consuming messages
        self.channel.start_consuming()
        pass
    
    def __del__(self) -> None:
        # Print "Closing RMQ connection on destruction"
        print("Closisng RMQ connection on destruction")
        # Close Channel
        self.channel.close()
        # Close Connection
        self.connection.close()
        pass





