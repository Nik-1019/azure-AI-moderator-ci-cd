import azure.functions as func
import json
import os
import uuid
from datetime import datetime
from azure.cosmos import CosmosClient

app = func.FunctionApp()

# Initialize Cosmos DB client
conn_str = os.getenv("CosmosDBConnection")
cosmos_client = CosmosClient.from_connection_string(conn_str) if conn_str else None
database = cosmos_client.get_database_client("StreamData") if cosmos_client else None
analytics_container = database.get_container_client("analytics") if database else None
stats_container = database.get_container_client("stats") if database else None

# HTTP endpoint for testing
@app.route(route="event", methods=["POST"], auth_level=func.AuthLevel.ANONYMOUS)
def create_event(req: func.HttpRequest) -> func.HttpResponse:
    try:
        event_data = req.get_json()
        if not event_data:
            return func.HttpResponse(json.dumps({"error": "No data provided"}), status_code=400, mimetype="application/json")
        
        # Process the event data
        update_stats_sync(event_data)
        send_alert_sync(event_data)
        log_analytics_sync(event_data)
        
        return func.HttpResponse(json.dumps({"status": "processed"}), mimetype="application/json")
    except Exception as e:
        return func.HttpResponse(json.dumps({"error": str(e)}), status_code=500, mimetype="application/json")

# GET endpoint for stream statistics
@app.route(route="stats/{stream_id}", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def get_stream_stats(req: func.HttpRequest) -> func.HttpResponse:
    try:
        stream_id = req.route_params.get("stream_id")
        if not stream_id:
            return func.HttpResponse(json.dumps({"error": "Missing stream_id"}), status_code=400, mimetype="application/json")
        
        if stats_container:
            try:
                stats_doc = stats_container.read_item(item=stream_id, partition_key=stream_id)
                response_data = {
                    "stream_id": stats_doc["stream_id"],
                    "follower_count": stats_doc["follower_count"],
                    "sub_count": stats_doc["sub_count"],
                    "total_donations": stats_doc["total_donations"]
                }
            except:
                response_data = {
                    "stream_id": stream_id,
                    "follower_count": 0,
                    "sub_count": 0,
                    "total_donations": 0
                }
        else:
            response_data = {
                "stream_id": stream_id,
                "follower_count": 0,
                "sub_count": 0,
                "total_donations": 0
            }
        
        return func.HttpResponse(json.dumps(response_data), mimetype="application/json")
    except Exception as e:
        return func.HttpResponse(json.dumps({"error": str(e)}), status_code=500, mimetype="application/json")

# Health check endpoint
@app.route(route="health", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def health_check(req: func.HttpRequest) -> func.HttpResponse:
    return func.HttpResponse(json.dumps({"status": "healthy"}), mimetype="application/json")

# Queue trigger for processing events
@app.queue_trigger(arg_name="msg", queue_name="alerts", connection="AzureWebJobsStorage")
def process_alert(msg: func.QueueMessage):
    event_data = json.loads(msg.get_body().decode('utf-8'))
    
    # Process the event data
    update_stats_sync(event_data)
    send_alert_sync(event_data)
    log_analytics_sync(event_data)
    
    return "Event processed"

# Helper functions
def update_stats_sync(event_data: dict):
    if not stats_container:
        return "updated"
    
    stream_id = event_data.get("stream_id", "default")
    event_type = event_data.get("type", "")
    
    try:
        stats_doc = stats_container.read_item(item=stream_id, partition_key=stream_id)
    except:
        stats_doc = {
            "id": stream_id,
            "stream_id": stream_id,
            "follower_count": 0,
            "sub_count": 0,
            "total_donations": 0
        }
    
    if event_type == "follower":
        stats_doc["follower_count"] += 1
    elif event_type == "subscription":
        stats_doc["sub_count"] += 1
    elif event_type == "donation":
        stats_doc["total_donations"] += event_data.get("amount", 0)
    
    stats_container.upsert_item(stats_doc)
    return "updated"

def send_alert_sync(event_data: dict):
    event_type = event_data.get("type", "event")
    username = event_data.get("username", "unknown")
    alert_message = f"New {event_type} from {username}!"
    return "sent"

def log_analytics_sync(event_data: dict):
    if not analytics_container:
        return "logged"
    
    document_id = str(uuid.uuid4())
    analytics_doc = {
        "id": document_id,
        "stream_id": event_data.get("stream_id"),
        "type": event_data.get("type"),
        "username": event_data.get("username"),
        "amount": event_data.get("amount"),
        "timestamp": datetime.utcnow().isoformat()
    }
    
    analytics_container.create_item(analytics_doc)
    return {"status": "logged", "document_id": document_id}