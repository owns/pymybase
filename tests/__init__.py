"""
Elias Wood (owns13927@yahoo.com)
2015-04-09
test classes for pymybase
"""

import testbase
import test_mydb
import test_myloggingbase
from tests import test_myjsonflattener
import test_mythread

__all__ = ['test_myloggingbase','test_mydb','test_mythread',
           'test_myjsonflattener']

def run_test():
    import unittest
    unittest.main()

if __name__ == '__main__':
    run_test()