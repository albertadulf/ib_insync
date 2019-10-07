import time
import uuid


def unique_id():
    return uuid.uuid4().hex


def tick_ms():
    return int(time.time() * 1000)
