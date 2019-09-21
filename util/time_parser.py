import time

from dateutil.parser import parse

def elastic_time_parse(str):
    """Parse str as time. Ignore nanosec order."""
    return parse(str)
