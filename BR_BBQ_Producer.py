"""
This script is designed to parse a CSV file into three queues that track the smoker temps and two different food temps. It reads these temps every 30 seconds though it misses some readings.

Ben Robin 
"""

import csv
import pika
import sys
import webbrowser
import time
from datetime import datetime
from util_logger import setup_logger

# Call setup_logger to initialize logging
logger, log_file_name = setup_logger(__file__)

def offer_rabbitmq_admin_site():
    """Offer to open the RabbitMQ Admin website"""
    ans = input("Would you like to monitor RabbitMQ queues? y or n ")
    print()
    if ans.lower() == "y":
        webbrowser.open_new("http://localhost:15672/#/queues")
        print()

def connect_rabbitmq():
    """Connect to RabbitMQ server and return the connection and channel"""
    try:
        # Create a blocking connection to the RabbitMQ server
        conn = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
        # Use the connection to create a communication channel
        ch = conn.channel()

        # Delete existing queues and declare them anew
        queues = ["01-smoker", "02-food-A", "02-food-B"]
        for queue_name in queues:
            ch.queue_delete(queue=queue_name)
            ch.queue_declare(queue=queue_name, durable=True)

        return conn, ch
    except pika.exceptions.AMQPConnectionError as e:
        logger.error(f"Error: Connection to RabbitMQ server failed: {e}")
        sys.exit(1)

def send_message(channel, queue_name, message):
    """Send a message to the specified RabbitMQ queue"""
    try:
        channel.basic_publish(exchange='', routing_key=queue_name, body=str(message))
        logger.info(f" [x] Sent {message} to {queue_name}")
    except Exception as e:
        logger.error(f"Error: Could not send message to {queue_name}: {e}")

def process_csv():
    """Process the CSV file and send messages to RabbitMQ queues"""
    try:
        # Full path to the CSV file
        csv_file_path = "C:/Users/benja/OneDrive/Documents/Git/streaming-05-smart-smoker/smoker-temps.csv"
        with open(csv_file_path, newline='', encoding='utf-8-sig') as csvfile:
            reader = csv.DictReader(csvfile)
            conn, ch = connect_rabbitmq()
            for data_row in reader:
                timestamp_str = data_row['Time (UTC)']
                smoker_temp_str = data_row['Channel1']
                food_A_temp_str = data_row['Channel2']
                food_B_temp_str = data_row['Channel3']

                # Check if the temperature strings are empty and convert to float
                if smoker_temp_str:
                    smoker_temp = float(smoker_temp_str)
                    send_message(ch, "01-smoker", (timestamp_str, smoker_temp))
                if food_A_temp_str:
                    food_A_temp = float(food_A_temp_str)
                    send_message(ch, "02-food-A", (timestamp_str, food_A_temp))
                if food_B_temp_str:
                    food_B_temp = float(food_B_temp_str)
                    send_message(ch, "02-food-B", (timestamp_str, food_B_temp))

            
            # Close the connection to the server
            conn.close()

    except FileNotFoundError as e:
        logger.error(f"Error: File not found: {e}")
    except Exception as e:
        logger.error(f"Error: An error occurred while processing the CSV file: {e}")

if __name__ == "__main__":
    offer_rabbitmq_admin_site()
    process_csv()
