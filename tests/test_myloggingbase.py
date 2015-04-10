"""
Elias Wood (owns13927@yahoo.com)
2015-04-06
unit test for myloggingbase
"""
import sys
if '..' not in sys.path: sys.path.append('..')
from myloggingbase import MyLoggingBase
from testbase import TestBase

#===============================================================================
# Test MyLoggingBase
#===============================================================================
class Test_MyLoggingBase(TestBase):
    def test_logger_name(self):
        """test default name"""
        self.assertEqual(MyLoggingBase().logger.name,'MyLoggingBase')
        # test setting own name
        self.assertEqual(MyLoggingBase('test name').logger.name,'test name')
    
    def test_get_current_timestamp(self):
        """for file working???"""
        a = MyLoggingBase()
        s = a.get_current_timestamp(for_file=True)
        self.assertTrue(':' not in s,msg='this method must make the str appropriate for a filename')

        # match standard?
        import re
        s = a.get_current_timestamp(for_file=False) # default
        re_s = '[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2},[0-9]{1,6}'
        # e.g. '2015-04-06T18:30:21,344000'
        self.assertNotEqual(re.match(re_s,s),None,msg='make sure the timestamp is in the correct format!')
        
    def test_can_log(self):
        """test logging works - can log things"""
        a = MyLoggingBase()

        a.logger.debug('hello world')
        a.logger.info('hello world')
        a.logger.warning('hello world')
        a.logger.error('hello world')
        a.logger.critical('hello world')
                
        
#===============================================================================
# Run Test
#===============================================================================
def run_test():
    import unittest
    unittest.main()

#===============================================================================
# Main
#===============================================================================
if __name__ == '__main__':
    run_test()