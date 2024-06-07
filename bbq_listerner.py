import pika
import sys
import os
import time
from collections import deque
import threading

# Define the threshold values for significant events
SMOKER_TEMP_DROP_THRESHOLD = 15.0
FOOD_TEMP_STALL_THRESHOLD = 1.0

# Define the monitoring periods in seconds
SMOKER_MONITOR_PERIOD = 2.5 * 60
FOOD_MONITOR_PERIOD = 10 * 60

# Deques to store temperature readings for monitoring
smoker_temps = deque(maxlen=int(SMOKER_MONITOR_PERIOD / 30))
food_a_temps = deque(maxlen=int(FOOD_MONITOR_PERIOD / 30))
food_b_temps = deque(maxlen=int(FOOD_MONITOR_PERIOD / 30))

def process_smoker_temp(ch, method, properties, body):
    message = body.decode().split(',')
    time_utc, sensor, temp = message
    temp = float(temp)
    smoker_temps.append((time_utc, temp))
    print(f" [x] Smoker Queue Received: {time_utc}, {temp}")
    if len(smoker_temps) == smoker_temps.maxlen:
        if smoker_temps[0][1] - temp > SMOKER_TEMP_DROP_THRESHOLD:
            print(f"{time_utc} - Smoker alert! Temperature dropped by more than 15 degrees F in 2.5 minutes.")
    ch.basic_ack(delivery_tag=method.delivery_tag)

def process_food_temp(ch, method, properties, body, food_temps, food_name):
    message = body.decode().split(',')
    time_utc, sensor, temp = message
    temp = float(temp)
    food_temps.append((time_utc, temp))
    print(f" [x] {food_name} Queue Received: {time_utc}, {temp}")
    
    if len(food_temps) == food_temps.maxlen:
        # Calculate the temperature change between the latest reading and the reading from 10 minutes ago
        temp_change = temp - food_temps[0][1]
        if abs(temp_change) <= FOOD_TEMP_STALL_THRESHOLD:
            print(f"{time_utc} - {food_name} stall! Temperature changed less than 1 degree F in 10 minutes.")
            # Here you can add code to send an alert message to a designated queue or perform any other action you desire
    ch.basic_ack(delivery_tag=method.delivery_tag)



def start_consumer(queue_name, callback, *args):
    host = "localhost"
    conn = pika.BlockingConnection(pika.ConnectionParameters(host))
    ch = conn.channel()
    ch.queue_declare(queue=queue_name, durable=True)
    ch.basic_qos(prefetch_count=1)
    ch.basic_consume(queue=queue_name, on_message_callback=lambda ch, method, properties, body: callback(ch, method, properties, body, *args))
    ch.start_consuming()

if __name__ == "__main__":
    threading.Thread(target=start_consumer, args=("smoker_queue", process_smoker_temp)).start()
    threading.Thread(target=start_consumer, args=("food_a_queue", process_food_temp, food_a_temps, "Food A")).start()
    threading.Thread(target=start_consumer, args=("food_b_queue", process_food_temp, food_b_temps, "Food B")).start()
