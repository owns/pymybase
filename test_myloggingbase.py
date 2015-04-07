"""
Elias Wood (owns13927@yahoo.com)
2015-04-06
unit test for myloggingbase
"""
import unittest

from myloggingbase import MyLoggingBase

class Test_MyLoggingBase(unittest.TestCase):
    def test_logger_name(self):
        # test default name
        self.assertEqual(MyLoggingBase().logger.name,'MyLoggingBase')
        # test setting own name
        self.assertEqual(MyLoggingBase('test name').logger.name,'test name')
    
    def test_get_current_timestamp(self):
        # for file working???
        s = MyLoggingBase().get_current_timestamp(for_file=True)
        self.assertTrue(':' not in s,msg='this method must make the str appropriate for a filename')

        # match standard?
        import re
        s = MyLoggingBase().get_current_timestamp(for_file=False)
        re_s = '[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2},[0-9]{1,6}'
        # e.g. '2015-04-06T18:30:21,344000'
        self.assertNotEqual(re.match(re_s,s),None,msg='make sure the timestamp is in the correct format!')
         

def run_test():
    unittest.main()

if __name__ == '__main__':
    #run_test()
    a = MyLoggingBase()

    print a.get_current_timestamp()