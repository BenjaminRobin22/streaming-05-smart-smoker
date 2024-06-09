"""
This script is designed to consume the data from three queues one that contains time stamps and the temps for the smoker and two that contain time stamps and temps for each meat 
We are looking to give an alert is the The smoker temperature decreases by more than 15 degrees F in 2.5 minutes or any food temperature changes less than 1 degree F in 10 minutes. This first meat product in my smoker is Pork Butt and the second is Brisket 
Ben Robin 6/9/2024 completed started on 05/26/2024
"""

import pika
import sys
import webbrowser
from datetime import datetime
from collections import deque

SMOKER_QUEUE = '01-smoker'
FOOD_A_QUEUE = '02-food-A'
FOOD_B_QUEUE = '02-food-B'
SMOKER_MAX_LENGTH = 5
FOOD_MAX_LENGTH = 20
SMOKER_TEMP_DROP_THRESHOLD = 15.0
FOOD_TEMP_STALL_THRESHOLD = 1.0

smoker_deque = deque(maxlen=SMOKER_MAX_LENGTH)
foodA_deque = deque(maxlen=FOOD_MAX_LENGTH)
foodB_deque = deque(maxlen=FOOD_MAX_LENGTH)


def check_smoker_alert():
    if len(smoker_deque) == SMOKER_MAX_LENGTH and (smoker_deque[0][1] - smoker_deque[-1][1]) >= SMOKER_TEMP_DROP_THRESHOLD:
        timestamp = datetime.strftime(smoker_deque[-1][0], "%m/%d/%y %H:%M:%S")
        print(f"{timestamp}, {smoker_deque[-1][1]}, SMOKER ALERT - Temp dropped by 15 degrees or more in the last 2.5 minutes.")

def check_food_stall(deque, food_name):
    if len(deque) == FOOD_MAX_LENGTH and abs(deque[0][1] - deque[-1][1]) <= FOOD_TEMP_STALL_THRESHOLD:
        timestamp = datetime.strftime(deque[-1][0], "%m/%d/%y %H:%M:%S")
        print(f"{timestamp}, {deque[-1][1]}, {food_name} alert {food_name} Stalled - temp changed by 1 degree or less in the last 10 min.")

def listen_for_tasks():
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
        channel = connection.channel()

        for queue_name in [SMOKER_QUEUE, FOOD_A_QUEUE, FOOD_B_QUEUE]:
            channel.queue_declare(queue=queue_name, durable=True)

        def callback(ch, method, properties, body):
            message = eval(body.decode())
            timestamp, temp = message
            timestamp = datetime.strptime(timestamp, '%m/%d/%y %H:%M:%S')

            if method.routing_key == SMOKER_QUEUE:
                smoker_deque.append((timestamp, temp))
                check_smoker_alert()
            elif method.routing_key == FOOD_A_QUEUE:
                foodA_deque.append((timestamp, temp))
                check_food_stall(foodA_deque, "Pork Butt")
            elif method.routing_key == FOOD_B_QUEUE:
                foodB_deque.append((timestamp, temp))
                check_food_stall(foodB_deque, "Brisket")

            ch.basic_ack(delivery_tag=method.delivery_tag)

        channel.basic_qos(prefetch_count=1)
        for queue_name in [SMOKER_QUEUE, FOOD_A_QUEUE, FOOD_B_QUEUE]:
            channel.basic_consume(queue=queue_name, on_message_callback=callback)

        print(" [*] Ready for work. To exit press CTRL+C")
        channel.start_consuming()
    
    except KeyboardInterrupt:
        print("Interrupted")
        sys.exit(0)

if __name__ == "__main__":
    offer_rabbitmq_admin_site()
    listen_for_tasks()
