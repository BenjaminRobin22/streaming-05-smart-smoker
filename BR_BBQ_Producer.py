import csv
import pika
import sys
import webbrowser
import pathlib
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

def process_csv():
    """Process the CSV file and send messages to RabbitMQ queues"""
    try:
        # Full path to the CSV file
        csv_file_path = "C:/Users/benja/OneDrive/Documents/Git/streaming-05-smart-smoker/smoker-temps.csv"
        with open(csv_file_path, newline='', encoding='utf-8-sig') as csvfile:
            reader = csv.DictReader(csvfile)
            for data_row in reader:
                timestamp = data_row['Time (UTC)']
                smoker_temp_str = data_row['Channel1']
                food_A_temp_str = data_row['Channel2']
                food_B_temp_str = data_row['Channel3']

                # Check if the temperature strings are empty
                if smoker_temp_str:
                    smoker_temp = float(smoker_temp_str)
                    send_message("01-smoker", (timestamp, smoker_temp))
                if food_A_temp_str:
                    food_A_temp = float(food_A_temp_str)
                    send_message("02-food-A", (timestamp, food_A_temp))
                if food_B_temp_str:
                    food_B_temp = float(food_B_temp_str)
                    send_message("02-food-B", (timestamp, food_B_temp))

    except FileNotFoundError:
        logger.error("CSV file not found.")
        sys.exit(1)
    except ValueError as e:
        logger.error(f"Error processing CSV: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        sys.exit(1)

def send_message(queue_name: str, message: tuple):
    """
    Publish a message to the specified queue.

    Parameters:
        queue_name (str): The name of the queue
        message (tuple): The message to be sent to the queue
    """
    try:
        conn, ch = connect_rabbitmq()
        ch.basic_publish(exchange="", routing_key=queue_name, body=str(message))
        logger.info(f"Sent message to {queue_name}: {message}")
    except Exception as e:
        logger.error(f"Error sending message to {queue_name}: {e}")
    finally:
        # Close the connection to the server
        conn.close()

if __name__ == "__main__":
    offer_rabbitmq_admin_site()
    process_csv()
