import time
import json
import re
from main import on_message

class SimlatedMessage:
	def __init__(self, payload):
		self.payload = json.dumps(payload)
		

#function to simulate MQTT messages from log file
def simulate_mqtt_from_log(log_file_path):
	with open(log_file_path, "r") as log_file:
		for line in log_file:
			#use regular expression to extract timestamp and message
			match = re.match(r"(\d{4}-d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}) - Received message: (.*)", line)
			#check if the line contains teh expected substring
			if match:
				timestamp, message_part = match.groups()
			
			#extract the message part
			#timestamp, _, mesasge_part = line.partition(" - Received message: ")
				#remove leading and trailing whitespaces
				message_part = message_part.strip()
			
				try:
					#parse JSON message
					message_data = json.loads(message_part)
					#simulate message processing
					on_message(None, None, SimulatedMessage(message_data))
					#if needed adjust the delay bewtween simulated messages
					#time.sleep(1)
				except json.JSONDecodeError as e:
					print(f"Error decoding JSON on line {line}: {e}")
			

if __name__ == "__main__":
	log_file_path = "mqtt_activity.log"
	simulate_mqtt_from_log(log_file_path)
