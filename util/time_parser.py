import time

from dateutil.parser import parse
from dateutil.tz import tzutc

import re

NanosecPattern = re.compile(r".+\.(\d+).*")

def elastic_time_parse(src, logger = None):
    """Parse src string as datetime and nanosec part. Raise exception if src format is NOT valid. """
    nano = 0

    ret = parse(src)
    if ret.tzinfo == None:
        ret = ret.replace(tzinfo = tzutc())

    print(ret)

    m = NanosecPattern.match(src)
    if(m != None):
        nano = int(m.group(1)[0:9].ljust(9, '0'))

    return [ret, nano]
