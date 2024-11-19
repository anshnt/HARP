import pymongo
import json

# MongoDB connection details
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["CI_Configurations"]  # Connect to the CI_Configurations database

# Function to check if a collection exists
def collection_exists(name):
    return name in db.list_collection_names()

# Function to insert or update data into the node_config collection
def insert_or_update_node_config(data):
    collection = db["node_config"]
    for item in data["node_info"]:
        node_config = {
            "system_name": item.get("system_name", None),
            "cluster_name": item.get("cluster_name", None),
            "node_type": item.get("node_type", None),
            "total_compute_nodes": item.get("total_compute_nodes", None),
            "processor_type": item.get("processor_type", None),
            "processor_cores": item.get("processor_cores", None),
            "processor_clock_speed": item.get("processor_clock_speed", None),
            "memory_size_GB": item.get("memory_size_GB", None),
            "processor_model": item.get("processor_model", None),
            "processor_architecture": item.get("processor_architecture", None),
            "sockets_count": item.get("sockets_count", None),
            "gpu_type": item.get("gpu_type", None),
            "gpu_model": item.get("gpu_model", None),
            "gpu_peak_performance": item.get("gpu_peak_performance", None),
            "gpu_memory": item.get("gpu_memory", None),
            "disk_space": item.get("disk_space", None)
        }
        collection.update_one({"system_name": item["system_name"], "cluster_name": item["cluster_name"]},
                              {"$set": node_config}, upsert=True)
    print("Data inserted/updated in the node_config collection.")

# Function to insert or update data into the queue_config collection
def insert_or_update_queue_config(data):
    collection = db["queue_config"]
    for queue in data["batchLogicalQueues"]:
        queue_config = {
            "cluster_name": data["host"],
            "queue_name": queue["hpcQueueName"],
            "minNodeCount": queue.get("minNodeCount", None),
            "maxNodeCount": queue.get("maxNodeCount", None),
            "minCoresPerNode": queue.get("minCoresPerNode", None),
            "maxCoresPerNode": queue.get("maxCoresPerNode", None),
            "minMemoryMB": queue.get("minMemoryMB", None),
            "maxMemoryMB": queue.get("maxMemoryMB", None),
            "minMinutes": queue.get("minMinutes", None),
            "maxMinutes": queue.get("maxMinutes", None)
        }
        collection.update_one({"cluster_name": data["host"], "queue_name": queue["hpcQueueName"]},
                              {"$set": queue_config}, upsert=True)
    print("Data inserted/updated in the queue_config collection.")

# Function to insert or update data into the cluster_to_queue collection
def insert_or_update_cluster_to_queue(data):
    collection = db["cluster_to_queue"]
    for queue in data["batchLogicalQueues"]:
        cluster_to_queue = {
            "cluster_name": data["host"],
            "queue_name": queue["hpcQueueName"]
        }
        collection.update_one({"cluster_name": data["host"], "queue_name": queue["hpcQueueName"]},
                              {"$set": cluster_to_queue}, upsert=True)
    print("Data inserted/updated in the cluster_to_queue collection.")

# Function to insert or update data into the sc_clusters collection
def insert_or_update_sc_clusters(user_data, sam_data):
    collection = db["sc_clusters"]
    for item in user_data["node_info"]:
        sc_cluster = {
            "sc_system": item.get("system_name", None),
            "cluster_name": sam_data.get("host", None)
        }
        collection.update_one({"sc_system": item["system_name"], "cluster_name": sam_data["host"]},
                              {"$set": sc_cluster}, upsert=True)
    print("Data inserted/updated in the sc_clusters collection.")

# Function to insert or update data into the queue_service_info collection
def insert_or_update_queue_service_info(data):
    collection = db["queue_service_info"]
    for queue in data["batchLogicalQueues"]:
        gpu_flag = 1 if "gpu" in queue["name"].lower() else 0
        service_type = "gpu" if gpu_flag == 1 else "standard"
        node_type = "GPU" if gpu_flag == 1 else "CPU"

        queue_service_info = {
            "cluster_name": data["host"],
            "queue_name": queue["hpcQueueName"],
            "gpu_flag": gpu_flag,
            "service_type": service_type,
            "node_type": node_type
        }
        collection.update_one({"cluster_name": data["host"], "queue_name": queue["hpcQueueName"]},
                              {"$set": queue_service_info}, upsert=True)
    print("Data inserted/updated in the queue_service_info collection.")

# Function to insert or update data into the cost collection
def insert_or_update_cost(cost_data):
    collection = db["cost"]
    for item in cost_data:
        sc_system = item.get("sc_system", None)
        service_type = item.get("service_type", None)  # Get service_type with a default value of None
        item["service_type"] = service_type  # Update the dictionary with the modified value
        collection.update_one({"sc_system": sc_system, "service_type": service_type},
                              {"$set": item}, upsert=True)
    print("Data inserted/updated in the cost collection.")

# Read user input JSON data and process
def process_user_input_json_file(file_path):
    with open(file_path, "r") as f:
        user_data = json.load(f)
    insert_or_update_node_config(user_data)
    return user_data

# Read Sam's code JSON data and process
def process_sams_json_file(file_path):
    with open(file_path, "r") as f:
        sam_data = json.load(f)
    insert_or_update_queue_config(sam_data)
    insert_or_update_cluster_to_queue(sam_data)
    insert_or_update_queue_service_info(sam_data)
    return sam_data

# Read cost JSON data and process
def process_cost_json_file(file_path):
    with open(file_path, "r") as f:
        cost_data = json.load(f)
    insert_or_update_cost(cost_data)

# Process user input JSON file
user_data = process_user_input_json_file("/Users/akankshajain/Desktop/final pipeline/user_input.json")

# Process Sam's code JSON file
sam_data = process_sams_json_file("/Users/akankshajain/Desktop/final pipeline/system_def.json")

# Process cost JSON file
process_cost_json_file("/Users/akankshajain/Desktop/final pipeline/cost_file.json")

# Process sc_clusters data
insert_or_update_sc_clusters(user_data, sam_data)

# Close the MongoDB connection
client.close()
