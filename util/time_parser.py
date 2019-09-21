import time

from dateutil.parser import parse

def elastic_time_parse(str, logger = None):
    """Parse str as time. Ignore nanosec order."""
    try:
        ret = parse(str)
    except ValueError as e:
        ret = str

    return ret
