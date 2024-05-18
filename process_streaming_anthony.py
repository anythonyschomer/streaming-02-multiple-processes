import csv
import random
import time

INPUT_FILE = "people.csv"  # Replace with your CSV file name

def stream_data(input_file):
    with open(input_file, "r") as file:
        reader = csv.reader(file)
        next(reader)  # Skip header

        for row in reader:
            # Unpack the row tuple into named values
            # Example: name, age, city = row
            # Replace with your own column names
            name, age, city = row

            # Print the record to the terminal
            record = f"[{name}, {age}, {city}]"
            print(record)

            time.sleep(random.randint(1, 3))  # Generate one record every 1-3 seconds

if __name__ == "__main__":
    try:
        print("Starting streaming process...")
        stream_data(INPUT_FILE)
    except Exception as e:
        print(f"An error occurred: {e}")