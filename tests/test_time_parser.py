#!/usr/bin/env python

import pytest
from util import time_parser

def test_my_first_test():
    """MyFirstTest"""
    t = time_parser.parse("2019/09/20")
    assert t == None

if __name__ == '__main__':
    pytest.main(['-v', __file__])