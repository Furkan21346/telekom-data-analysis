import json #this module is used to convert Python objects into JSON format and vice versa.
import time #this module is used to introduce delays in the execution of the program.
import random #this module is used to generate random numbers and make random selections from lists.
from datetime import datetime #this module is used to work with date and time, allowing us to get the current timestamp.

BASE_STATIONS = ["Kizilay_Center", "Tunali_Hilmi", "Cayyolu_Gordion", "Eryaman_Optimum", "Batikent_Square"]
USAGE_TYPES = ["Data_Download", "Voice_Call", "SMS", "Video_Stream"]
# Since in real life most of the connections are successful, we will have higher probability for successful connection
STATUSES = ["Success", "Success", "Success", "Success", "Error", "Connection_Dropped"] 

def generate_telecom_data():
    data = {
        "user_id": random.randint(10000, 99999), # Generate a random user ID
        "base_station": random.choice(BASE_STATIONS),
        "usage_type": random.choice(USAGE_TYPES),
        "status": random.choice(STATUSES),
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    return data

if __name__ == "__main__":
    print("Data generation started. Press CTRL+C in the terminal to stop.\n")
    try:
        while True:
            new_record = generate_telecom_data()
            
            # we are converting the dictionary to JSON format.
            json_data = json.dumps(new_record)
            
            print(json_data)
            
            # we are introducing a delay of 1 second between each data record.
            time.sleep(1)
            
    except KeyboardInterrupt:
        print("\nData generation stopped by the user.")