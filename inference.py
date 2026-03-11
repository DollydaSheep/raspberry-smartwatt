import json
import pickle
import time
from collections import deque

import paho.mqtt.client as mqtt
from supabase import create_client, Client

MODEL_PATH = "co_model_b1.pkl"

SUPABASE_URL = "https://yuvamvpfxhzuvvnvtqhj.supabase.co"
SUPABASE_SERVICE_ROLE_KEY = "REPLACE_THIS_WITH_A_NEW_ROTATED_KEY"

supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

LIVE_ROW_ID = 1  # single row that will always be updated

# -----------------------------
# TUNABLE PARAMETERS
# -----------------------------
SMOOTHING_WINDOW = 3          # moving average window for power
EVENT_THRESHOLD_W = 25.0      # ignore tiny changes
EVENT_DEBOUNCE_SEC = 2.0      # minimum gap between accepted events
GLOBAL_TOLERANCE_FLOOR = 20.0 # minimum tolerance if model tolerance is too small

# Per-appliance temporal rules.
# Use your real appliance names here if needed.
DEFAULT_RULES = {
    "min_on_sec": 5.0,
    "cooldown_sec": 3.0,
}

APPLIANCE_RULES = {
    "Fridge freezer": {"min_on_sec": 60.0, "cooldown_sec": 10.0},
    "Washer dryer": {"min_on_sec": 30.0, "cooldown_sec": 10.0},
    "Light": {"min_on_sec": 2.0, "cooldown_sec": 1.0},
    "HTPC": {"min_on_sec": 10.0, "cooldown_sec": 3.0},
}

# -----------------------------
# LOAD MODEL
# -----------------------------
with open(MODEL_PATH, "rb") as f:
    model = pickle.load(f)

labels = model["labels"]
states = model["states"]
tolerance = model.get("tolerance", [50.0] * len(labels))

print("SMART-WATT Event-Based NILM Started")

# -----------------------------
# RUNTIME STATE
# -----------------------------
power_buffer = deque(maxlen=SMOOTHING_WINDOW)
last_smoothed_power = None
last_event_time = 0.0

# Track current state of each appliance instance
# We keep separate entries even if labels repeat, e.g. two "Light" entries.
appliance_runtime = []
for idx, (label, appliance_states, tol) in enumerate(zip(labels, states, tolerance)):
    numeric_states = sorted(
        {float(s) for s in appliance_states if isinstance(s, (int, float)) or str(s).replace(".", "", 1).isdigit()}
    )
    nonzero_states = [s for s in numeric_states if s > 0]

    appliance_runtime.append({
        "id": idx,
        "label": label,
        "states": nonzero_states,              # only ON states
        "tolerance": max(float(tol), GLOBAL_TOLERANCE_FLOOR),
        "is_on": False,
        "matched_state": 0.0,
        "last_on_time": 0.0,
        "last_off_time": 0.0,
        "last_change_time": 0.0,
    })


def get_rules(label: str) -> dict:
    return APPLIANCE_RULES.get(label, DEFAULT_RULES)


def smooth_power(power: float) -> float:
    power_buffer.append(power)
    return sum(power_buffer) / len(power_buffer)


def current_detected_appliances():
    detected = []
    for app in appliance_runtime:
        if app["is_on"]:
            detected.append({
                "id": app["id"],
                "label": app["label"],
                "matched_state": float(app["matched_state"]),
            })
    return detected


def find_best_on_candidate(delta_p: float, now_ts: float):
    """
    For a positive edge, look among appliances currently OFF.
    Match deltaP to one of their nonzero states.
    """
    best = None

    for app in appliance_runtime:
        if app["is_on"]:
            continue

        rules = get_rules(app["label"])
        if now_ts - app["last_change_time"] < rules["cooldown_sec"]:
            continue

        for state in app["states"]:
            diff = abs(delta_p - state)
            if diff <= app["tolerance"]:
                candidate = {
                    "app": app,
                    "target_state": state,
                    "diff": diff,
                }
                if best is None or candidate["diff"] < best["diff"]:
                    best = candidate

    return best


def find_best_off_candidate(delta_p: float, now_ts: float):
    """
    For a negative edge, look among appliances currently ON.
    Match abs(deltaP) to the currently active matched_state.
    """
    best = None
    abs_delta = abs(delta_p)

    for app in appliance_runtime:
        if not app["is_on"]:
            continue

        rules = get_rules(app["label"])
        if now_ts - app["last_change_time"] < rules["cooldown_sec"]:
            continue

        if now_ts - app["last_on_time"] < rules["min_on_sec"]:
            continue

        expected_drop = app["matched_state"]
        diff = abs(abs_delta - expected_drop)

        if diff <= app["tolerance"]:
            candidate = {
                "app": app,
                "target_state": 0.0,
                "diff": diff,
            }
            if best is None or candidate["diff"] < best["diff"]:
                best = candidate

    return best


def apply_event(candidate: dict, delta_p: float, now_ts: float):
    app = candidate["app"]

    if delta_p > 0:
        app["is_on"] = True
        app["matched_state"] = float(candidate["target_state"])
        app["last_on_time"] = now_ts
        app["last_change_time"] = now_ts
        event = {
            "event": "ON",
            "id": app["id"],
            "label": app["label"],
            "matched_state": float(app["matched_state"]),
            "delta": float(delta_p),
            "diff": float(candidate["diff"]),
            "timestamp": now_ts,
        }
    else:
        previous_state = app["matched_state"]
        app["is_on"] = False
        app["matched_state"] = 0.0
        app["last_off_time"] = now_ts
        app["last_change_time"] = now_ts
        event = {
            "event": "OFF",
            "id": app["id"],
            "label": app["label"],
            "matched_state": float(previous_state),
            "delta": float(delta_p),
            "diff": float(candidate["diff"]),
            "timestamp": now_ts,
        }

    return event


def process_event(delta_p: float, now_ts: float):
    global last_event_time

    if abs(delta_p) < EVENT_THRESHOLD_W:
        return None

    if now_ts - last_event_time < EVENT_DEBOUNCE_SEC:
        return None

    if delta_p > 0:
        candidate = find_best_on_candidate(delta_p, now_ts)
    else:
        candidate = find_best_off_candidate(delta_p, now_ts)

    if candidate is None:
        return None

    last_event_time = now_ts
    return apply_event(candidate, delta_p, now_ts)


def update_reading(voltage, current, power, smoothed_power, delta_p, appliances, last_event):
    payload = {
        "id": LIVE_ROW_ID,
        "voltage": float(voltage),
        "current": float(current),
        "power": float(power),
        "smoothed_power": float(smoothed_power),
        "delta_power": float(delta_p),
        "detected_appliances": appliances,
        "last_event": last_event,
        "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }

    response = (
        supabase
        .table("energy_readings")
        .upsert(payload)
        .execute()
    )
    return response


def on_message(client, userdata, msg):
    global last_smoothed_power

    try:
        payload_str = msg.payload.decode("utf-8")
        data = json.loads(payload_str)

        voltage = float(data.get("voltage", 0))
        current = float(data.get("current", 0))
        power = float(data.get("power", 0))

        now_ts = time.time()
        smoothed = smooth_power(power)

        if last_smoothed_power is None:
            last_smoothed_power = smoothed
            delta_p = 0.0
            detected = current_detected_appliances()
            update_reading(voltage, current, power, smoothed, delta_p, detected, None)
            print(f"Init | V={voltage:.1f}V I={current:.2f}A P={power:.1f}W Smoothed={smoothed:.1f}W")
            return

        delta_p = smoothed - last_smoothed_power
        event = process_event(delta_p, now_ts)
        detected = current_detected_appliances()

        print(
            f"V={voltage:.1f}V | I={current:.2f}A | "
            f"P={power:.1f}W | Smooth={smoothed:.1f}W | ΔP={delta_p:+.1f}W"
        )

        if event:
            print(
                f"EVENT: {event['event']} -> {event['label']} "
                f"(state {event['matched_state']:.1f}W, diff {event['diff']:.1f}W)"
            )
        else:
            print("EVENT: none")

        if detected:
            print("Detected appliances:")
            for app in detected:
                print(f"  - {app['label']} (state {app['matched_state']:.1f}W)")
        else:
            print("Detected appliances: []")

        update_reading(voltage, current, power, smoothed, delta_p, detected, event)

        last_smoothed_power = smoothed

    except json.JSONDecodeError:
        print("Error: received invalid JSON payload")
    except Exception as e:
        print("Error:", e)


client = mqtt.Client()
client.on_message = on_message

client.connect("localhost", 1883)
client.subscribe("smartwatt/readings")

client.loop_forever()