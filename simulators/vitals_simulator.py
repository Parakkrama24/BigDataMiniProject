import random
import uuid
from datetime import datetime,timedelta
from common.sim_clock import now as sim_now
import time
from common.config import load_settings

def generate_baseline(patient_id: str) -> dict:
    heart_rate = random.gauss(75, 8)
    spo2 = random.gauss(97, 1)
    systolic_bp = random.gauss(118, 10)
    diastolic_bp = random.gauss(76, 8)
    temperature = random.gauss(36.8, 0.3)

    return {
        "heart_rate": heart_rate,
        "spo2": spo2,
        "systolic_bp": systolic_bp,
        "diastolic_bp": diastolic_bp,
        "temperature": temperature,
    }


def generate_reading(patient_id: str, baseline: dict) -> dict:
    heart_rate = baseline["heart_rate"] + random.gauss(0, 2)
    spo2 = baseline["spo2"] + random.gauss(0, 0.3)
    systolic_bp = baseline["systolic_bp"] + random.gauss(0, 2)
    diastolic_bp = baseline["diastolic_bp"] + random.gauss(0, 2)
    temperature = baseline["temperature"] + random.gauss(0, 0.1)

    heart_rate = max(30, min(220, heart_rate))
    spo2 = max(50, min(100, spo2))
    systolic_bp = max(60, min(250, systolic_bp))
    diastolic_bp = max(30, min(150, diastolic_bp))
    temperature = max(30.0, min(43.0, temperature))

    return {
        "patient_id": patient_id,
        "heart_rate": heart_rate,
        "spo2": spo2,
        "systolic_bp": systolic_bp,
        "diastolic_bp": diastolic_bp,
        "temperature": temperature,
        "event_id": str(uuid.uuid4()),
        "timestamp": sim_now().isoformat(),
    }


def apply_spike(reading: dict) -> dict:
    spike_type = random.choice(["tachycardia", "hypoxia", "fever", "hypotension"])

    if spike_type == "tachycardia":
        reading["heart_rate"] = random.uniform(125, 160)
    elif spike_type == "hypoxia":
        reading["spo2"] = random.uniform(80, 89)
    elif spike_type == "fever":
        reading["temperature"] = random.uniform(38.6, 40.5)
    elif spike_type == "hypotension":
        reading["systolic_bp"] = random.uniform(70, 89)

    return reading


def maybe_spike(reading: dict, spike_probability: float) -> dict:
    if random.random() < spike_probability:
        reading = apply_spike(reading)
    return reading


def apply_null_noise(reading: dict, null_probability: float) -> dict:
    vital_fields = ["heart_rate", "spo2", "systolic_bp", "diastolic_bp", "temperature"]
    for field in vital_fields:
        if random.random() < null_probability:
            reading[field] = None
    return reading

def apply_late_timestamp(reading: dict, late_probability: float) -> dict:
    if random.random() < late_probability:
        current = datetime.fromisoformat(reading["timestamp"])
        delay = timedelta(seconds=random.uniform(30, 300))
        reading["timestamp"] = (current - delay).isoformat()
    return reading

def stream_vitals():
    settings = load_settings()["simulator"]
    random.seed(settings["seed"])

    patient_ids = [f"P{i+1:03d}" for i in range(settings["num_patients"])]
    baselines = {pid: generate_baseline(pid) for pid in patient_ids}

    while True:
        for pid in patient_ids:
            reading = generate_reading(pid, baselines[pid])
            reading = maybe_spike(reading, settings["spike_probability"])
            reading = apply_null_noise(reading, settings["null_field_probability"])
            reading = apply_late_timestamp(reading, settings["late_event_probability"])

            yield reading

            if random.random() < settings["duplicate_probability"]:
                yield reading

        time.sleep(settings["emit_interval_seconds"])

if __name__ == "__main__":
    import json

    for reading in stream_vitals():
        print(json.dumps(reading))