import time

from dateutil.parser import parse
from dateutil.tz import tzutc



def elastic_time_parse(str, logger = None):
    """Parse str as time. Ignore nanosec order."""
    try:
        ret = parse(str)
        if ret.tzinfo == None:
            ret = ret.replace(tzinfo = tzutc())

    except ValueError as e:
        ret = str

    return ret
