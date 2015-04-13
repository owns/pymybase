"""
Elias Wood (owns13927@yahoo.com)
2015-04-09
test classes for pymybase
"""

import testbase
import test_mydb
import test_myloggingbase
from tests import test_myjson2csv
import test_mythread

__all__ = ['test_myloggingbase','test_mydb','test_mythread',
           'test_myjson2csv']

def run_test():
    import unittest
    unittest.main()

if __name__ == '__main__':
    run_test()