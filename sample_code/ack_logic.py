#All three read included except for write_to_plc nodes and send_to_azure_iot_hub nodes
#ack > 0 condition added from read_acknowledgement_signal function along with all the advanced changes
import pyads
from azure.iot.device import IoTHubDeviceClient, Message
import json
from datetime import datetime
import time
import threading
import signal
from threading import Lock
import queue 
import logging

#Error connecting to OPC UA servers or reading nodes: timed out

custom_c = None
stop_thread = threading.Event()
last_processed_message_ids = set()
MAX_MESSAGE_ID_HISTORY = 1000
# Global queue to hold incoming requests
request_queue = queue.Queue()
client_lock1 = Lock()
client_lock2 = Lock()



def get_current_date_time():
    now = datetime.now()
    current_date = now.strftime("%Y-%m-%d")
    current_time = now.strftime("%H:%M:%S")
    return current_date, current_time

# TwinCAT ADS connection parameters
AMS_NET_ID = '172.16.51.163.1.1'  # Your AMS Net ID
ADS_PORT = 851  # Standard ADS port
ACK_NODES_FILE = "ack_nodes.txt"  # File containing acknowledgment-related nodes

# Function to load acknowledgment node names from file
def load_ack_nodes():
    try:
        with open(ACK_NODES_FILE, "r") as file:
            return [line.strip() for line in file.readlines()]
    except Exception as e:
        print(f"❌ Error loading ack_nodes.txt: {e}")
        return []

#CONNECTION_STRING= "HostName=WP-STAGING-2.azure-devices.net;DeviceId=WP-STAGING-Device03;SharedAccessKey=OqvWyxqL8QYKYVHgf39Jnto7rxWTZyLgki/+OJFkA2E="  
CONNECTION_STRING = "HostName=CarParking-T1.azure-devices.net;DeviceId=samsanplc;SharedAccessKey=t9JHmAprVj7KVDUuyukEIMoSb+QaurHqiNe/pEoFKlc="
#Nodes to read from the PLC
try:
    with open('nodes.txt', 'r') as file:
        PYADS_VARIABLES = json.load(file)
except Exception as e:
    print(" Eror loading pyads nodes: {e}")
    PYADS_VARIABLES = []

#Nodes to write to the PLC
try:
    with open('write_nodes.txt', 'r') as file:
        WRITE_NODES = json.load(file)
except Exception as e:
    print(f"❌ Error loading write_nodes.txt: {e}")
    WRITE_NODES = []


# Function to read non-zero values from specified nodes
def get_non_zero_values(client, node_ids):
    non_zero_values = []
    for key, node_id in node_ids.items():
        try:
            data_value = client.get_node(node_id).get_data_value()
            actual_value = data_value.Value
            
            # Check if the StatusCode is good
            if data_value.StatusCode.is_good():
                # Handle Variant values correctly
                if isinstance(actual_value, ua.Variant):
                    int_value = actual_value.Value
                    # Check for Int16 range
                    if isinstance(int_value, int) and -32768 <= int_value <= 32767:
                        if int_value > 0:
                            non_zero_values.append(int_value)
                else:
                    print(f"Unexpected type for {key}: {type(actual_value)}")
            else:
                print(f"Bad status for {key}: {data_value.StatusCode}")
        except Exception as e:
            print(f"Error reading {key}: {e}")
    return non_zero_values


def connect_to_plc():
    try:
        plc = pyads.Connection(AMS_NET_ID, ADS_PORT)
        plc.open()
        print(f"Connected to PLC: {AMS_NET_ID}")
        return plc
    except Exception as e:
        print(f"Error connecting to PLC: {e}")
        return None


# Read data from PLC using pyads
def read_plc_nodes(plc, plc_name):
    """
    Reads PLC data using pyads and organizes data into:
    1. PARKING_SITE_CONFIG
    2. QUEUE_STATUS_UPDATES
    3. PARKING_MAP
    """
    current_date, current_time = datetime.now().strftime("%Y-%m-%d"), datetime.now().strftime("%H:%M:%S")

    # Define message structures
    formatted_dict1 = {
        "Message_Id": "PARKING_SITE_CONFIG",
        "System_Date": current_date,
        "System_Time": current_time,
        "System_Code_No": plc_name,
    }

    formatted_dict2 = {
        "Message_Id": "QUEUE_STATUS_UPDATES",
        "System_Date": current_date,
        "System_Time": current_time,
        "System_Code_No": plc_name,
        "System_Type": "0",
        "System_No": "0",
        "Queue_Data": {}
    }

    parking_map = {
        "Message_Id": "PARKING_MAP",
        "System_Date": current_date,
        "System_Time": current_time,
        "Is_PLC_Connected": "1",
        "System_Code_No": plc_name,
        "System_Type": "0",
        "System_No": "0",
        "Is_PLC_Connected": "1",
        "value": {}
    }

    # Elevator data storage
    elevator_data = {f"Elevator[{i}]": [] for i in range(1, 5)}

    # Type mapping for pyads
    type_mapping = {
        "PLCTYPE_BOOL": pyads.PLCTYPE_BOOL,
        "PLCTYPE_INT": pyads.PLCTYPE_INT,
        "PLCTYPE_BYTE": pyads.PLCTYPE_BYTE,
        "PLCTYPE_UINT": pyads.PLCTYPE_UINT,
        "PLCTYPE_WORD": pyads.PLCTYPE_WORD
    }

    print("Reading values from PLC...")

    try:
        for var in PYADS_VARIABLES:
            var_name = var['name']
            var_type = type_mapping.get(var['type'], None)

            try:
                # Read the value from PLC
                value = plc.read_by_name(var_name, var_type) if var_type else None
                
                # Default fallback values for safety
                if value is None:
                    value = False if "BOOL" in var['type'] else 0  

                # Debugging output
                print(f"Read {var_name}: {value}")

                # Assign data to the correct message
                if "Global_Variables.Elevator" in var_name:
                    for i in range(1, 5):
                        if f"Global_Variables.Elevator[{i}]." in var_name:
                            elevator_data[f"Elevator[{i}]"].append(value)
                            break
                elif "Global_Variables" in var_name:
                    formatted_dict1[var_name] = value  # Add to PARKING_SITE_CONFIG
                    if var_name == "Global_Variables.CurrentFloorNo":
                        break  # Stop at this variable
                else:
                    parking_map["value"][var_name] = value  # Add to PARKING_MAP

            except Exception as read_error:
                print(f"Error reading {var_name}: {read_error}")

        print("Elevator Data Collected:", json.dumps(elevator_data, indent=2))

        # Attach elevator data to QUEUE_STATUS_UPDATES
        formatted_dict2["Queue_Data"] = elevator_data

        return formatted_dict1, formatted_dict2, parking_map

    except Exception as e:
        print(f"Error reading from pyads: {e}")
        return None, None, None

# Write data to PLC using pyads
def write_to_plc(plc, data, type_mapping):
    """
    Writes received IoT message values to PLC nodes, node by node.
    """

    try:

        for node in WRITE_NODES:
            var_name = node['name']
           #var_type = TYPE_MAPPING.get(node['type'], None)
            var_type = type_mapping.get(node['type'], None)

            # Ensure variable exists in incoming data
            if var_name in data and var_type:
                try:
                    # Write value to PLC
                    plc.write_by_name(var_name, data[var_name], var_type)
                    print(f"✅ Written {data[var_name]} to {var_name}")

                except Exception as e:
                    print(f"❌ Error writing {var_name}: {e}")

            else:
                print(f"⚠️ Skipped {var_name} (Not in incoming data)")

    except Exception as e:
        print(f"❌ General write error: {e}")

# Send data to Azure IoT Hub
def send_to_azure_iot_hub(json_output, client):
    try:
        if custom_c is None:
            return

        json_str = json.dumps(json_output)
        message = Message(json_str)
        custom_c.send_message(message)
        print(f"Message sent to Azure IoT Hub: {json_output}")

    except Exception as e:
        print(f"Error sending message to Azure IoT Hub: {e}")

# Process request queue for writing to PLC
def process_queue(plc):
    while True:
        data = request_queue.get()
        if data is None:
            break  # Exit signal
        write_to_plc(plc, data)
        request_queue.task_done()

# Continuously read from PLC and send data to Azure
def send_data_continuously(interval, plc):
    while not stop_thread.is_set():
        plc_name = "PRT79"
        data1, data2, data3 = read_plc_nodes(plc, plc_name)

        if data1:
            send_to_azure_iot_hub(data1, custom_c)
        if data2:
            send_to_azure_iot_hub(data2, custom_c)
        if data3:
            send_to_azure_iot_hub(data3, custom_c)

        time.sleep(interval)


def on_message_received(message):
    try:
        raw_data = message.data.decode('utf-8').strip()
        print(f"📩 Received message from Azure IoT Hub: {raw_data}")

        data = json.loads(raw_data)  # Convert message to Python dict

        # Define type mapping
        type_mapping = {
            "PLCTYPE_BOOL": pyads.PLCTYPE_BOOL,
            "PLCTYPE_INT": pyads.PLCTYPE_INT,
            "PLCTYPE_BYTE": pyads.PLCTYPE_BYTE,
            "PLCTYPE_UINT": pyads.PLCTYPE_UINT,
            "PLCTYPE_WORD": pyads.PLCTYPE_WORD
        }

        # Check if message contains variables to write to PLC
        valid_write_nodes = {node["name"] for node in WRITE_NODES}
        plc_write_data = {key: data[key] for key in data if key in valid_write_nodes}

        # Load acknowledgment node names
        ack_nodes = load_ack_nodes()

        # Connect to PLC
        plc = pyads.Connection(AMS_NET_ID, ADS_PORT)
        plc.open()

        # 📝 **Write to PLC if data exists**
        if plc_write_data:
            print(f"📝 Writing to PLC: {plc_write_data}")
            write_to_plc(plc, plc_write_data, type_mapping)

        # 🔄 **Read acknowledgment-related values dynamically**
        ack_values = {}
        for node in ack_nodes:
            try:
                ack_values[node] = plc.read_by_name(node, pyads.PLCTYPE_BYTE)
            except Exception as e:
                print(f"❌ Error reading {node}: {e}")

        # Extract acknowledgment signal
        ack_signal = ack_values.get("Global_Variables.Acknowledgement_signal", 0)

        # 🚀 **Only send acknowledgment if ack_signal > 0**
        if ack_signal > 0:
            current_date, current_time = get_current_date_time()

            # 📩 **Create acknowledgment message**
            ack_message = {
                "Message_Id": "REQUEST_ACKNOWLEDGEMENT",
                "System_Date": current_date,
                "System_Time": current_time,
                "System_Code_No": "PRT79",  # Replace with dynamic plc_name if needed
                "System_Type": "0",
                "System_No": "0",
                "Token_No": ack_values.get("Global_Variables.Token_No", "NA"),
                "Request_Type_Value": ack_values.get("Global_Variables.Request_Type", "NA"),
                "Ack_Status": ack_signal,  # Send ack_signal as-is
            }

            # Send acknowledgment message to Azure IoT Hub
            send_to_azure_iot_hub(ack_message, custom_c)
            print(f"✅ Sent acknowledgment message: {ack_message}")

        else:
            print("⚠️ Request_Ack not ready")

        # Close PLC connection
        plc.close()

    except Exception as e:
        print(f"❌ Error processing Azure message: {e}")

# Main execution
if __name__ == "__main__":
    c = IoTHubDeviceClient.create_from_connection_string(CONNECTION_STRING)
    custom_c = c

    signal.signal(signal.SIGINT, lambda sig, frame: stop_thread.set())
    signal.signal(signal.SIGTERM, lambda sig, frame: stop_thread.set())

    # Connect to PLC
    plc = connect_to_plc()
    if not plc:
        exit()

    # Start worker thread for processing PLC requests
    worker_thread = threading.Thread(target=process_queue, args=(plc,))
    worker_thread.start()

    # Start the thread for sending data continuously
    send_data_thread = threading.Thread(target=send_data_continuously, args=(5, plc))
    send_data_thread.start()

    # Start listening to Azure IoT Hub
    c.on_message_received = on_message_received
    c.connect()
    
    send_data_thread.join()
    request_queue.put(None)  # Signal queue to exit
    worker_thread.join()

    print("Program exited cleanly.")