"""
Elias Wood (owns13927@yahoo.com)
2015-04-06
unit test for myjson2csv
"""
import sys
if '..' not in sys.path: sys.path.append('..')
from myjson2csv import MyJSON2CSV
from testbase import TestBase

#===============================================================================
# Test MyJSON2CSV
#===============================================================================
class Test_MyJSON2CSV(TestBase):
    def test_file_io(self):
        """test open,close and overwrite."""
        fname = self.get_new_file_name('aaa.csv')
        # test open/close
        a = MyJSON2CSV(fname)
        a.begin_writing() # only open when this method is called
        self.assertTrue(a.is_open(), 'failed to open...')
        a.close(); del a
        
        # test overwrite
        a = MyJSON2CSV(fname)
        a.begin_writing() # only open when this method is called
        self.assertNotEqual(fname, a.get_filename(),'file overwritten!')
        a.close(); del a
    
    def test_setting_headers(self):
        """test if setting the headers manually works correctly"""
        fname = self.get_new_file_name('aaa.csv')
        a = MyJSON2CSV(fname)
        a.set_headers('k','a','b','c','d.a','d.b','e')
        d = dict(a=1,b='2',c=u'3',d=dict(a=4,b=5),e=[1,2,3])
        for i in xrange(100):
            d['k'] = i
            a.write_json_object(d)
        self.assertEqual(len(a._missed_headers),0,'the wrong number of fields were determined to be missed {}'.format(a._missed_headers))
        a.close(); del a
    
    def test_setting_headers_missed(self):
        """test setting the headers manually and missing a key (top level)"""
        fname = self.get_new_file_name('aaa.csv')
        a = MyJSON2CSV(fname)
        a.set_headers('k','b','c','d.a','d.b','e')
        
        for i in xrange(100):
            d = dict(a=1,b='2',c=u'3',d=dict(a=4,b=5),e=[1,2,3],k=i)
            a.write_json_object(d)
        
        self.assertIn('a',a._missed_headers,'MyJSON2CSV failed to discover there was a key not being exported!')
        self.assertEqual(len(a._missed_headers),1,'the wrong number of fields were determined to be missed {}'.format(a._missed_headers))
        a.close(); del a
        
    def test_assuming_headers(self):
        """test the class determining what the heads are (with all the same keys)."""
        fname = self.get_new_file_name('aaa.csv')
        a = MyJSON2CSV(fname)
        d = dict(a=1,b='2',c=u'3',d=dict(a=4,b=5),e=[1,2,3])
        for i in xrange(100):
            d['k'] = i
            a.write_json_object(d)
        self.assertEqual(len(a._missed_headers),0,'no fields should be missed')
        a.close(); del a
    
    def test_assuming_headers_missed(self):
        """test the class determining what the heads are, but the first record
        doesn't have all the keys (top level)"""
        fname = self.get_new_file_name('aaa.csv')
        a = MyJSON2CSV(fname)
        d = dict(a=1,b='2',c=u'3',d=dict(a=4,b=5),e=[1,2,3])
        for i in xrange(100):
            a.write_json_object(d)
            d['k'] = i
        
        self.assertIn('k',a._missed_headers,'MyJSON2CSV failed to discover key k is missing from the csv')
        self.assertEqual(len(a._missed_headers),1,'only one field should be missed')
        a.close(); del a
    
    def test_auto_numbering(self):
        """test to make sure auto numbering works"""
        fname = self.get_new_file_name('aaa.csv')
        a = MyJSON2CSV(fname)
        a.add_row_number(self)
        d = dict(a=1,b='2',c=u'3',d=dict(a=4,b=5),e=[1,2,3])
        for i in xrange(100):  # @UnusedVariable
            d['k'] = i
            a.write_json_object(d)
            
        self.assertEqual(a._cur_row_num,100,'there should have only been 100 rows written...')
        a.close(); del a
    
    def test_bare_writing(self):
        """best the writerow directly feature"""
        fname = self.get_new_file_name('aaa.csv')
        a = MyJSON2CSV(fname)
        a.add_row_number(self)
        a.set_headers('a','b','c')
        for i in xrange(100):  # @UnusedVariable
            a.writerow((i,i+1,i+2))
        
        self.assertEqual(a._cur_row_num,100,'there should have only been 100 rows written...')
        self.assertIn(MyJSON2CSV._row_num_header_name,a._headers,'auto gen number failed to be added')
        a.close(); del a
        
#===============================================================================
# Run Test
#===============================================================================
def run_test():
    import os; os.chdir('..')
    import unittest
    suite = unittest.TestLoader().loadTestsFromTestCase(Test_MyJSON2CSV)
    unittest.TextTestRunner().run(suite)

#===============================================================================
# Main
#===============================================================================
if __name__ == '__main__':
    run_test()
    