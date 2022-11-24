import random
from paho.mqtt import client as mqtt_client
import mqtt_pb2
import portnums_pb2
import mesh_pb2
import telemetry_pb2
import re
from datetime import datetime
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

# Node class for device location
class node:
    def __init__(self, Node, DeviceID, Latitude, Longitude):
        self.Node = Node
        self.DeviceID = DeviceID
        self.Latitude = Latitude
        self.Longitude = Longitude

# Creating a node list to relate node numbers to friendly names and geolocations
nodes = []

# Appending the list to
nodes.append(node("FRIENDLY NAME", NODENUMBER, LATITUDE, LONGITUDE))

# MQTT broker variables
broker = '192.168.1.1'
port = 1883
topic = "msh/2/c/LongFast/#"
# generate client ID with pub prefix randomly
client_id = f'python-mqtt-{random.randint(0, 1000)}'

# InfluxDB variables
token = "TOKEN"
org = "my-org"
bucket = "my-bucket"

IDBClient = InfluxDBClient(url="http://localhost:8086", token=token)

def connect_mqtt() -> mqtt_client:
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)

    client = mqtt_client.Client(client_id)
    client.on_connect = on_connect
    client.connect(broker, port)
    return client


def subscribe(client: mqtt_client):
    def on_message(client, userdata, msg):
        if msg.payload != b'online' and msg.payload != b'offline':
            # Decode the protobuf with the mqtt pb2
            NodeInfo = mqtt_pb2.ServiceEnvelope()
            NodeInfo.ParseFromString(msg.payload)

            # TESTING: print mqtt output
            #print(NodeInfo)

            # If the mqtt output is a Telemetry topic, run this block
            if NodeInfo.packet.decoded.portnum == portnums_pb2.PortNum.TELEMETRY_APP:

                # Save the DeviceID as a variable for later
                DeviceID = re.search("from: ([0-9]+)", str(NodeInfo))

                # Decode the protobuf with the telemetry pb2
                meshData = telemetry_pb2.Telemetry()
                meshData.ParseFromString(NodeInfo.packet.decoded.payload)

                # Save the temperature, relative humidity and barometric pressure as a variable for later
                temperature = re.search("temperature: (-?\d{1,2}\.\d{2})", str(meshData))
                relative_humidity = re.search("relative_humidity: (\d{2}\.\d+)", str(meshData))
                barometric_pressure = re.search("barometric_pressure: (\d{3,4}\.\d+)", str(meshData))

                # TESTING: Print mesh output
                #print(str(meshData))

                # If the Telemetry packet contains a temperature reading, run this block
                if temperature:
                    # Clear variables
                    latitude = ""
                    longitude = ""
                    Node = ""

                    # TESTING: Print device ID
                    #print(str(DeviceID.group(1)))

                    # Search for the node friendly name and geolocation based on the node number
                    for obj in nodes:
                        if str(obj.DeviceID) == str(DeviceID.group(1)):
                            node = obj.Node
                            latitude = obj.Latitude
                            longitude = obj.Longitude

                    # Print the results to the terminal
                    print("Node: " + node)
                    print("DeviceID: " + DeviceID.group(1))
                    print("Temperature: " + temperature.group(1))
                    print("Relative Humidity: " + relative_humidity.group(1))
                    print("Barometric Pressure: " + barometric_pressure.group(1))
                    print("Latitude: " + str(latitude))
                    print("Longitude: " + str(longitude))

                    # Create a point containing the data that will be sent to the InfuxDB database
                    write_api = IDBClient.write_api(write_options=SYNCHRONOUS)
                    point = Point("mem")\
                            .tag("Node", node) \
                            .field("Node", node) \
                            .field("DeviceID", str(DeviceID.group(1))) \
                            .field("Temperature", float(temperature.group(1))) \
                            .field("Relative Humidity", float(relative_humidity.group(1))) \
                            .field("Barometric Pressure", float(barometric_pressure.group(1))) \
                            .field("Latitude", float("{:.6f}".format(latitude))) \
                            .field("Longitude", float("{:.6f}".format(longitude))) \
                            .time(datetime.utcnow(), WritePrecision.NS)

                    # Send point to the InfluxDB database
                    write_api.write(bucket, org, point)

    client.subscribe(topic)
    client.on_message = on_message

def run():
    client = connect_mqtt()
    subscribe(client)
    client.loop_forever()

if __name__ == '__main__':
    run()
