import json
import pickle
import paho.mqtt.client as mqtt
from supabase import create_client, Client

MODEL_PATH = "co_model_b1.pkl"

SUPABASE_URL = ""
SUPABASE_SERVICE_ROLE_KEY = ""

supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

LIVE_ROW_ID = 1   # single row that will always be updated

# Load trained model
with open(MODEL_PATH, "rb") as f:
    model = pickle.load(f)

labels = model["labels"]
states = model["states"]
tolerance = model["tolerance"]

print("SMART-WATT NILM Inference Started")


def predict(power):
    detected = []

    for label, appliance_states, tol in zip(labels, states, tolerance):
        if not appliance_states:
            continue

        closest = min(appliance_states, key=lambda s: abs(s - power))
        diff = abs(power - closest)

        if diff <= tol and closest != 0:
            detected.append({
                "label": label,
                "matched_state": float(closest),
                "diff": float(diff)
            })

    return detected


def update_reading(voltage, current, power, appliances):
    response = (
        supabase
        .table("energy_readings")
        .upsert({
            "id": LIVE_ROW_ID,
            "voltage": float(voltage),
            "current": float(current),
            "power": float(power),
            "detected_appliances": appliances
        })
        .execute()
    )

    return response


# MQTT callback when ESP32 sends power reading
def on_message(client, userdata, msg):
    try:
        payload_str = msg.payload.decode("utf-8")
        data = json.loads(payload_str)

        voltage = float(data.get("voltage", 0))
        current = float(data.get("current", 0))
        power = float(data.get("power", 0))

        appliances = predict(power)

        print(f"Voltage: {voltage:.1f}V | Current: {current:.2f}A | Power: {power:.1f}W")

        if appliances:
            print("Detected appliances:")
            for app in appliances:
                print(
                    f"  - {app['label']} "
                    f"(matched {app['matched_state']:.1f}W, diff {app['diff']:.1f}W)"
                )
        else:
            print("Detected appliances: []")

        update_reading(voltage, current, power, appliances)

        print("Updated row in energy_readings")

    except json.JSONDecodeError:
        print("Error: received invalid JSON payload")
    except Exception as e:
        print("Error:", e)


client = mqtt.Client()
client.on_message = on_message

client.connect("localhost", 1883)
client.subscribe("smartwatt/readings")

client.loop_forever()
