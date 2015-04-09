"""
Elias Wood (owns13927@yahoo.com)
2015-04-07
base class for unit tests (init logging)
"""
import unittest
import logging

import sys
if '..' not in sys.path: sys.path.append('..')
from main import init_logging

from myloggingbase import MyLoggingBase

#===============================================================================
# Base Test Class
#===============================================================================
class TestBase(unittest.TestCase,MyLoggingBase):
    """
    base class for unit testing - initializes logging.
    """
    def __init__(self,*args,**keys):
        """
        base class for unit testing - initializes logging.
        """
        MyLoggingBase.__init__(self)
        unittest.TestCase.__init__(self, *args,**keys)
        
    def setUp(self):
        """init logging for testing - no file"""
        init_logging(file_log_lvl=None,console_log_lvl='DEBUG')

    def tearDown(self):
        """close logging & whatever else is needed..."""
        logging.shutdown() # clean up!
        
#===============================================================================
# Run Test
#===============================================================================
def run_test():
    unittest.main()

#===============================================================================
# Main
#===============================================================================
if __name__ == '__main__':
    run_test()