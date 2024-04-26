import time
import json
import re
import paho.mqtt.client as mqtt
import time
from datetime import datetime

LOG_FILE_PATH = "mqtt_activity.log"


# function to simulate MQTT messages from log file
def get_mqtt_messages_from_log(log_file_path):
	times = []
	topics = []
	messages = []
	with open(log_file_path, "r") as log_file:
		for line in log_file:
			# Define regex patterns
			date_pattern = r'\d{4}-\d{2}-\d{2}'
			time_pattern = r'\d{2}:\d{2}:\d{2},\d{3}'
			message_dictionary_pattern = r'{.*?}'
			message_string_pattern = r'"([^"]*)"'
			topic_string_pattern = r'(?<=from topic )\b\w+\b'

			# Extract components using regex
			date = re.search(date_pattern, line).group()
			message_time = re.search(time_pattern, line).group()
			message_match = re.search(message_dictionary_pattern, line)
			if message_match is not None:  # Message may not be a dictionary
				message_item = message_match.group()
			else:
				message_item = re.search(message_string_pattern, line).group()
			topic_string = re.search(topic_string_pattern, line).group()

			times.append(time_string_to_datetime(date, message_time))
			topics.append(topic_string)
			messages.append(message_item)

	return times, topics, messages


def time_string_to_datetime(date_string, time_string):
	# Parse the time strings into datetime objects
	datetime_time = datetime.strptime(date_string + " " + time_string, "%Y-%m-%d %H:%M:%S,%f")
	return datetime_time


def convert_times_to_seconds(times):
	times_in_seconds = [0]
	for i in range(len(times) - 1):
		delta_time = times[i + 1] - times[i]
		times_in_seconds.append(delta_time.total_seconds())
	return times_in_seconds


def send_data(topic_name, message_data):
	payload = json.dumps(message_data)
	client.publish(topic_name, payload)


if __name__ == "__main__":
	# MQTT broker settings
	broker_address = "localhost"
	broker_port = 1883
	client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
	client.connect(broker_address, broker_port, 60)

	log_file_path = "mqtt_activity.log"
	times, topics, messages = get_mqtt_messages_from_log(LOG_FILE_PATH)
	times = convert_times_to_seconds(times)

	for i in range(len(times)):
		print("Waiting %f seconds" % times[i])
		time.sleep(times[i])
		print("Sending topic: %s with message: %s" % (topics[i], messages[i]))
		send_data(topics[i], messages[i])

	print("Log Complete")
