import asyncio
from azure.eventhub.aio import EventHubConsumerClient
from azure.eventhub import TransportType
from flask import Flask, jsonify, send_from_directory
from flask_cors import CORS
import threading, queue, json, re

CONNECTION_STR = (
    "Endpoint=sb://iothub-ns-proyectoso-57474124-7fe5641915.servicebus.windows.net/;"
    "SharedAccessKeyName=iothubowner;"
    "SharedAccessKey=ThuPaV/lEn9pb4DeTDiHlbZ4LZyjWTDqFAIoTLDjAmE=;"
    "EntityPath=proyectosocial2025"
)
EVENTHUB_NAME = "proyectosocial2025"
event_queue = queue.Queue()

app = Flask(__name__, static_folder="web")
CORS(app)

@app.route("/")
def index():
    return send_from_directory("web", "index.html")

@app.route("/data")
def get_data():
    data_list = []
    while not event_queue.empty():
        data_list.append(event_queue.get())
    return jsonify(data_list)

async def receive_events():
    client = EventHubConsumerClient.from_connection_string(
        conn_str=CONNECTION_STR,
        consumer_group="$Default",
        eventhub_name=EVENTHUB_NAME,
        transport_type=TransportType.AmqpOverWebsocket  # usa 443
    )

    async def on_event(partition_context, event):
        try:
            message = event.body_as_str()
            print(f"📥 Recibido: {message}")
            event_queue.put(json.loads(message))
        except Exception as e:
            print("⚠️ Error al procesar:", e)

    async with client:
        print("✅ Conectado al Event Hub vía HTTPS (443). Escuchando...")
        await client.receive(on_event=on_event, starting_position="-1")

def start_eventhub_listener():
    asyncio.run(receive_events())

def start_flask_server():
    print("🌐 Servidor en http://localhost:5000")
    app.run(host="0.0.0.0", port=5000, debug=False)

if __name__ == "__main__":
    threading.Thread(target=start_eventhub_listener, daemon=True).start()
    start_flask_server()
