import csv
import random
import time
import logging

logging.basicConfig(filename="out9.txt", level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

INPUT_FILE = "people.csv"

def stream_data(input_file):
    with open(input_file, "r") as file:
        reader = csv.reader(file)
        next(reader)  # Skip header

        for row in reader:
            name, age, city = row  # Unpack the row tuple
            message = f"[{name}, {age}, {city}]"
            logging.info(message)
            time.sleep(random.randint(1, 3))  # Generate one record every 1-3 seconds

if __name__ == "__main__":
    try:
        logging.info("Starting streaming process.")
        stream_data(INPUT_FILE)
        logging.info("Streaming complete!")
    except Exception as e:
        logging.error(f"An error occurred: {e}")