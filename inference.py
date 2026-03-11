import json
import pickle
import time
from collections import deque
from datetime import datetime, timezone

import paho.mqtt.client as mqtt
from supabase import create_client, Client

MODEL_PATH = "co_model_b1.pkl"

SUPABASE_URL = "https://yuvamvpfxhzuvvnvtqhj.supabase.co"
SUPABASE_SERVICE_ROLE_KEY = "YOUR_NEW_ROTATED_SERVICE_ROLE_KEY"

supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

LIVE_ROW_ID = 1  # single row that will always be updated

# -----------------------------
# TUNABLE PARAMETERS
# -----------------------------
SMOOTHING_WINDOW = 3
EVENT_THRESHOLD_W = 25.0
EVENT_DEBOUNCE_SEC = 2.0
GLOBAL_TOLERANCE_FLOOR = 20.0

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
# NILM RUNTIME STATE
# -----------------------------
power_buffer = deque(maxlen=SMOOTHING_WINDOW)
last_smoothed_power = None
last_event_time = 0.0

appliance_runtime = []
for idx, (label, appliance_states, tol) in enumerate(zip(labels, states, tolerance)):
    numeric_states = sorted(
        {
            float(s)
            for s in appliance_states
            if isinstance(s, (int, float)) or str(s).replace(".", "", 1).isdigit()
        }
    )
    nonzero_states = [s for s in numeric_states if s > 0]

    appliance_runtime.append({
        "id": idx,
        "label": label,
        "states": nonzero_states,
        "tolerance": max(float(tol), GLOBAL_TOLERANCE_FLOOR),
        "is_on": False,
        "matched_state": 0.0,
        "last_on_time": 0.0,
        "last_off_time": 0.0,
        "last_change_time": 0.0,
    })

# -----------------------------
# ENERGY + AGGREGATION STATE
# -----------------------------
last_raw_ts = None
total_energy_kwh = 0.0

# bucket for one second worth of incoming raw samples
current_second_epoch = None
current_second_bucket = {
    "sum_voltage": 0.0,
    "sum_current": 0.0,
    "sum_power": 0.0,
    "count": 0,
}

# completed 1-second averaged logs for current minute
minute_second_logs = []
current_minute_key = None


def reset_second_bucket():
    return {
        "sum_voltage": 0.0,
        "sum_current": 0.0,
        "sum_power": 0.0,
        "count": 0,
    }


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


def update_live_reading(voltage, current, power, smoothed_power, delta_p, appliances, last_event, total_kwh):
    payload = {
        "id": LIVE_ROW_ID,
        "voltage": float(voltage),
        "current": float(current),
        "power": float(power),
        "detected_appliances": appliances,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }

    return supabase.table("energy_readings").upsert(payload).execute()


def flush_minute_aggregate(minute_key: str):
    """
    Insert one row into energy_readings_aggregate using all finalized 1-second logs
    collected for that minute.
    """
    global minute_second_logs

    if not minute_second_logs:
        return

    n = len(minute_second_logs)

    avg_voltage = sum(x["voltage"] for x in minute_second_logs) / n
    avg_current = sum(x["current"] for x in minute_second_logs) / n
    avg_power = sum(x["power_w"] for x in minute_second_logs) / n

    # sum energy from each 1-second average
    minute_energy_kwh = sum(x["energy_kwh"] for x in minute_second_logs)

    payload = {
        "power_w": float(avg_power),
        "energy_kwh": float(minute_energy_kwh),
        "voltage": float(avg_voltage),
        "current": float(avg_current),
        "recorded_at": f"{minute_key}:00+00:00",
    }

    try:
        supabase.table("energy_readings_aggregate").insert(payload).execute()
        print(
            f"[AGG INSERT] {payload['recorded_at']} | "
            f"Pavg={avg_power:.2f}W | Vavg={avg_voltage:.2f}V | "
            f"Iavg={avg_current:.3f}A | E={minute_energy_kwh:.6f} kWh"
        )
    except Exception as e:
        print("Error inserting minute aggregate:", e)

    minute_second_logs = []


def finalize_second_log(second_epoch: int):
    """
    Finalize the current 1-second bucket into one averaged second log,
    then append it to the current minute collection.
    """
    global current_second_bucket, minute_second_logs, current_minute_key

    if current_second_bucket["count"] == 0:
        return

    avg_voltage = current_second_bucket["sum_voltage"] / current_second_bucket["count"]
    avg_current = current_second_bucket["sum_current"] / current_second_bucket["count"]
    avg_power = current_second_bucket["sum_power"] / current_second_bucket["count"]

    # 1-second energy from average power
    second_energy_kwh = avg_power / 3600000.0

    second_dt = datetime.fromtimestamp(second_epoch, tz=timezone.utc)
    minute_key = second_dt.strftime("%Y-%m-%dT%H:%M")

    # if minute changed, flush the previous minute first
    if current_minute_key is None:
        current_minute_key = minute_key
    elif minute_key != current_minute_key:
        flush_minute_aggregate(current_minute_key)
        current_minute_key = minute_key

    minute_second_logs.append({
        "voltage": float(avg_voltage),
        "current": float(avg_current),
        "power_w": float(avg_power),
        "energy_kwh": float(second_energy_kwh),
        "recorded_at": second_dt.isoformat(),
    })

    current_second_bucket = reset_second_bucket()


def add_raw_sample_to_second_bucket(ts_epoch: float, voltage: float, current: float, power: float):
    """
    Group all raw incoming readings by second, average them later.
    """
    global current_second_epoch, current_second_bucket

    sec = int(ts_epoch)

    if current_second_epoch is None:
        current_second_epoch = sec

    if sec != current_second_epoch:
        finalize_second_log(current_second_epoch)
        current_second_epoch = sec

    current_second_bucket["sum_voltage"] += voltage
    current_second_bucket["sum_current"] += current
    current_second_bucket["sum_power"] += power
    current_second_bucket["count"] += 1


def update_total_energy(power_w: float, now_ts: float):
    """
    Integrate energy continuously using raw message timestamps.
    """
    global last_raw_ts, total_energy_kwh

    if last_raw_ts is None:
        last_raw_ts = now_ts
        return total_energy_kwh

    dt_sec = now_ts - last_raw_ts
    if dt_sec < 0:
        dt_sec = 0

    total_energy_kwh += (power_w * dt_sec) / 3600000.0
    last_raw_ts = now_ts
    return total_energy_kwh


def on_message(client, userdata, msg):
    global last_smoothed_power

    try:
        payload_str = msg.payload.decode("utf-8")
        data = json.loads(payload_str)

        voltage = float(data.get("voltage", 0))
        current = float(data.get("current", 0))
        power = float(data.get("power", 0))

        now_ts = time.time()

        # update cumulative energy
        total_kwh = update_total_energy(power, now_ts)

        # build per-second buckets for later 1-minute aggregate
        add_raw_sample_to_second_bucket(now_ts, voltage, current, power)

        # NILM smoothing/event logic
        smoothed = smooth_power(power)

        if last_smoothed_power is None:
            last_smoothed_power = smoothed
            delta_p = 0.0
            detected = current_detected_appliances()
            update_live_reading(voltage, current, power, smoothed, delta_p, detected, None, total_kwh)

            print(
                f"Init | V={voltage:.1f}V I={current:.2f}A "
                f"P={power:.1f}W Smooth={smoothed:.1f}W Energy={total_kwh:.6f} kWh"
            )
            return

        delta_p = smoothed - last_smoothed_power
        event = process_event(delta_p, now_ts)
        detected = current_detected_appliances()

        print(
            f"V={voltage:.1f}V | I={current:.2f}A | "
            f"P={power:.1f}W | Smooth={smoothed:.1f}W | "
            f"dP={delta_p:+.1f}W | Energy={total_kwh:.6f} kWh"
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

        update_live_reading(voltage, current, power, smoothed, delta_p, detected, event, total_kwh)

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