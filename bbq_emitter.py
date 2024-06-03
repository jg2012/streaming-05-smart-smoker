import pika
import pandas as pd
import time
import threading
import webbrowser

def offer_rabbitmq_admin_site():
    """Offer to open the RabbitMQ Admin website"""
    ans = input("Would you like to monitor RabbitMQ queues? y or n ")
    print()
    if ans.lower() == "y":
        webbrowser.open_new("http://localhost:15672/#/queues")
        print()

def send_message(host: str, queue_name: str, message: str):
    """
    Creates and sends a message to the queue each execution.
    This process runs and finishes.

    Parameters:
        host (str): the host name or IP address of the RabbitMQ server
        queue_name (str): the name of the queue
        message (str): the message to be sent to the queue
    """

    try:
        conn = pika.BlockingConnection(pika.ConnectionParameters(host))
        ch = conn.channel()
        ch.queue_declare(queue=queue_name, durable=True)
        ch.basic_publish(exchange="", routing_key=queue_name, body=message)
        print(f" [x] Sent {message}")
    except pika.exceptions.AMQPConnectionError as e:
        print(f"Error: Connection to RabbitMQ server failed: {e}")
        sys.exit(1)
    finally:
        conn.close()

def read_temps_from_csv(file_path: str):
    """
    Generator to read temperature data from a CSV file.

    Parameters:
        file_path (str): the path to the CSV file

    Yields:
        tuple: a tuple containing time, smoker_temp, food_a_temp, and food_b_temp
    """
    df = pd.read_csv(file_path)
    for _, row in df.iterrows():
        yield row['Time (UTC)'], row['Channel1'], row['Channel2'], row['Channel3']

def simulate_temperature_readings_from_csv(file_path: str):
    """
    Simulates temperature readings for the smoker and two foods.
    Sends the readings to RabbitMQ queues.
    """
    host = "localhost"
    queues = ["smoker_queue", "food_a_queue", "food_b_queue"]

    for time_utc, smoker_temp, food_a_temp, food_b_temp in read_temps_from_csv(file_path):
        send_message(host, queues[0], f"{time_utc},Smoker,{smoker_temp}")
        send_message(host, queues[1], f"{time_utc},Food A,{food_a_temp}")
        send_message(host, queues[2], f"{time_utc},Food B,{food_b_temp}")
        
        time.sleep(30)  # Wait for 30 seconds between readings

def start_simulation(file_path: str):
    threading.Thread(target=simulate_temperature_readings_from_csv, args=(file_path,)).start()

if __name__ == "__main__":
    offer_rabbitmq_admin_site()
    start_simulation("smoker-temps.csv")
