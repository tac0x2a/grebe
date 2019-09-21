import time

from dateutil.parser import parse
from dateutil.tz import tzutc

def elastic_time_parse(src, logger = None):
    """Parse src string as datetime. Ignore nanosec order. If src is NOT valid format, return it self as string."""
    try:
        ret = parse(src)
        if ret.tzinfo == None:
            ret = ret.replace(tzinfo = tzutc())

    except ValueError as e:
        ret = src

    return ret
