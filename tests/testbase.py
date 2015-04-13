"""
Elias Wood (owns13927@yahoo.com)
2015-04-07
base class for unit tests (init logging)
"""
import unittest
import logging

import sys
if '..' not in sys.path: sys.path.append('..')
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
        self._init_logging(file_log_lvl=None,console_log_lvl='DEBUG',
                           show_warning=False)

    def tearDown(self):
        """close logging & whatever else is needed..."""
        logging.shutdown() # clean up!
    
    #===========================================================================
    # Generate temporary new files
    #===========================================================================
    def get_new_file_name(self,f='aaa.txt'):
        """generates a temporary folder to hold the desired temp file."""
        # necessary imports
        import tempfile
        import os.path
        
        # generate new unique folder and filename
        d = tempfile.mkdtemp()
        fname = os.path.join(d,f)
        
        # add cleanup at end!
        self.addCleanup(TestBase.remove_fd_file,d,fname)
        
        # return the new filename
        return fname
    
    @staticmethod
    def remove_fd_file(fd,fname):
        import os
        if os.path.exists(fname): # del file
            try: os.remove(fname)
            except (WindowsError,IOError) as e: pass# print repr(e) @UnusedVariable
        if os.path.exists(fd): # del folder
            try: os.rmdir(fd)
            except (WindowsError,IOError) as e: pass#print repr(e) @UnusedVariable
        
#===============================================================================
# Run Test
#===============================================================================
def run_test():
    import os; os.chdir('..')
    suite = unittest.TestLoader().loadTestsFromTestCase(TestBase)
    unittest.TextTestRunner().run(suite)

#===============================================================================
# Main
#===============================================================================
if __name__ == '__main__':
    run_test()